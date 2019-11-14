/**
 * Copyright 2019 SAP SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/util/wait"
)

const podName = "mariadb"

var (
	binlogCancel context.CancelFunc
	backupCancel context.CancelFunc
	logUpdate    chan<- time.Time
	logger       *logrus.Entry
)

type (
	Health struct {
		sync.Mutex
		Ready bool
	}
	Manager struct {
		cfg             config.Config
		backup          *Backup
		maria           *k8s.Maria
		restore         *Restore
		Storage         storage.Storage
		updateSts       *updateStatus
		Health          *Health
		lastBackupTime  string
		backupCheckSums map[string]int64
		verifyTimer     *time.Timer
	}
)

func init() {
	logger = log.WithFields(logrus.Fields{"component": "manager"})
}

func NewManager(c config.Config) (m *Manager, err error) {
	s3, err := storage.NewS3(c.S3, c.ServiceName)
	b, err := NewBackup(c, s3)
	if err != nil {
		return
	}

	mr, err := k8s.NewMaria(c.Namespace)
	if err != nil {
		return
	}
	us := updateStatus{up: false}

	prometheus.MustRegister(NewMetricsCollector(c.MariaDB, &us))

	return &Manager{
		cfg:             c,
		backup:          b,
		maria:           mr,
		restore:         NewRestore(c),
		Storage:         s3,
		updateSts:       &us,
		Health:          &Health{Ready: true},
		backupCheckSums: make(map[string]int64),
	}, err
}

func (m *Manager) Start() (err error) {
	ctx := context.Background()
	ctx, backupCancel = context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		err := m.startBackup(ctx)
		done <- err
	}()
	err = <-done
	return
}

func (m *Manager) startBackup(ctx context.Context) (err error) {
	_, err = checkBackupDirExistsAndCreate(m.cfg.BackupDir)
	if err != nil {
		return
	}
	for c := time.Tick(time.Duration(m.cfg.FullBackupIntervalInHours) * time.Hour); ; {
		//verify last full backup cycle
		p, err := m.Storage.DownloadLatestBackup()
		if err != nil {
			m.onVerifyError(fmt.Errorf("error loading backup for verifying: %s", err.Error()))
			m.uploadVerfiyStatus(p)
		} else {
			go m.verifyBackup(m.lastBackupTime, p)
		}

		m.lastBackupTime = time.Now().Format(time.RFC3339)
		bpath := path.Join(m.cfg.BackupDir, m.lastBackupTime)
		ch := make(chan time.Time)
		mp, err := m.createMysqlDump(bpath)
		if err != nil {
			logger.Error(err)
			time.Sleep(time.Duration(1) * time.Minute)
			continue
		}
		ctxBin := context.Background()
		ctxBin, binlogCancel = context.WithCancel(ctxBin)
		go m.onBinlogRotation(ch)
		var eg errgroup.Group
		eg.Go(func() error {
			return m.backup.runBinlog(ctxBin, mp, m.lastBackupTime, ch)
		})
		go func() {
			if err = eg.Wait(); err != nil {
				logger.Error(fmt.Errorf("Error saving log files %s", err.Error()))
				return
			}
		}()

		m.updateSts.Lock()
		m.updateSts.fullBackup = time.Now()
		m.updateSts.Unlock()

		select {
		case <-c:
			continue
		case <-ctx.Done():
			logger.Info("stop backup")
			binlogCancel()
			if m.verifyTimer != nil {
				m.verifyTimer.Stop()
			}
			return nil
		}
	}
}

func (m *Manager) createMysqlDump(bpath string) (mp mysql.Position, err error) {
	logger.Debug("Starting full backup")
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := HealthCheck(m.cfg.MariaDB)
		if err != nil || !s.Ok {
			return false, nil
		}
		return true, nil
	})
	//Only do backups if db is healthy
	if err = wait.Poll(5*time.Second, 1*time.Minute, cf); err != nil {
		return mp, fmt.Errorf("Cannot do backup. Database not getting healthy: %s", err.Error())
	}
	// Stop binlog
	if binlogCancel != nil {
		binlogCancel()
	}

	err = m.backup.createMysqlDump(bpath)
	if err != nil {
		return mp, fmt.Errorf("Error creating mysqlDump: %s", err.Error())
	}
	mp, err = readMetadata(bpath)
	if err != nil {
		return mp, fmt.Errorf("Error cannot read binlog metadata: %s", err.Error())
	}

	defer os.RemoveAll(bpath)
	logger.Debug("Finished full backup")
	return
}

func (m *Manager) verifyBackup(lastBackupTime, backupFolder string) {
	var err error
	logger.Info("Start verifying backup")
	defer func() {
		os.RemoveAll(backupFolder)
		m.verifyTimer = nil
		m.uploadVerfiyStatus(backupFolder)
	}()

	if lastBackupTime != "" && len(m.cfg.MariaDB.VerifyTables) > 0 {
		m.backupCheckSums, err = getCheckSumForTable(m.cfg.MariaDB)
		if err != nil {
			logger.Error("cannot load checksums")
		}
	}
	cfg := config.Config{
		MariaDB: config.MariaDB{
			Host:         fmt.Sprintf("%s-%s-verify", m.cfg.ServiceName, podName),
			Port:         3306,
			User:         m.cfg.MariaDB.User,
			Password:     m.cfg.MariaDB.Password,
			Version:      m.cfg.MariaDB.Version,
			VerifyTables: m.cfg.MariaDB.VerifyTables,
			Databases:    m.cfg.MariaDB.Databases,
		},
	}

	dp, err := m.maria.CreateMariaDeployment(cfg.MariaDB)
	if err != nil {
		m.onVerifyError(fmt.Errorf("error creating mariadb for verifying: %s", err.Error()))
		return
	}
	svc, err := m.maria.CreateMariaService(cfg.MariaDB)
	if err != nil {
		m.onVerifyError(fmt.Errorf("error creating mariadb for verifying: %s", err.Error()))
		return
	}

	defer func() {
		if err = m.maria.DeleteMariaResources(dp, svc); err != nil {
			logger.Error(fmt.Errorf("error deleting mariadb resources for verifying: %s", err.Error()))
		}
	}()

	r := NewRestore(cfg)
	if err = r.verifyRestore(backupFolder); err != nil {
		m.onVerifyError(fmt.Errorf("error restoring backup for verifying: %s", err.Error()))
		return
	}
	m.updateSts.Lock()
	m.updateSts.VerifyBackup = 1
	m.updateSts.Unlock()

	if len(m.backupCheckSums) > 0 && len(m.cfg.MariaDB.VerifyTables) > 0 {
		if err = m.verifyChecksums(cfg); err != nil {
			m.onVerifyError(fmt.Errorf("error doing table checksum: %s", err.Error()))
		} else {
			m.updateSts.Lock()
			m.updateSts.VerifyTables = 1
			m.updateSts.Unlock()
		}
	}
	logger.Info("Done verifying backup successful")
}

func (m *Manager) verifyChecksums(cfg config.Config) (err error) {
	rs, err := getCheckSumForTable(cfg.MariaDB)
	if err != nil {
		return fmt.Errorf("error verifying backup: %s", err.Error())
	}
	if err = compareChecksums(m.backupCheckSums, rs); err != nil {
		return fmt.Errorf("error verifying backup: %s", err.Error())
	}
	logger.Debug("Checksum successful", rs, m.backupCheckSums)
	return
}

func (m *Manager) onVerifyError(err error) {
	logger.Error(err.Error())
	m.updateSts.Lock()
	m.updateSts.VerifyBackup = 0
	m.updateSts.VerifyTables = 0
	m.updateSts.Unlock()
	return
}

func (m *Manager) uploadVerfiyStatus(backupFolder string) {
	m.updateSts.RLock()
	out, err := yaml.Marshal(m.updateSts)
	m.updateSts.RUnlock()
	if err == nil {
		u := strconv.FormatInt(time.Now().Unix(), 10)
		//remove restore and servicename dir from path
		vp := strings.Replace(backupFolder, filepath.Join(constants.RESTOREFOLDER, m.cfg.ServiceName), "", 1)
		m.Storage.WriteStream(vp+"/verify_"+u+".yaml", "", bytes.NewReader(out))
	} else {
		logger.Error(fmt.Errorf("cannot write verify status: %s", err.Error()))
	}
}

func (m *Manager) onBinlogRotation(c chan time.Time) {
	for {
		t, ok := <-c
		if !ok {
			break
		}

		m.updateSts.Lock()
		m.updateSts.incBackup = t
		m.updateSts.Unlock()

		if m.verifyTimer == nil {
			m.verifyTimer = time.AfterFunc(time.Duration(15)*time.Minute, func() {
				p, err := m.Storage.DownloadLatestBackup()
				if err != nil {
					m.onVerifyError(fmt.Errorf("error loading backup for verifying: %s", err.Error()))
					return
				}
				go m.verifyBackup(m.lastBackupTime, p)
			})
		}
	}
}

func (m *Manager) Stop() {
	backupCancel()
}

func (m *Manager) GetConfig() config.Config {
	return m.cfg
}

func (m *Manager) Restore(p string) (err error) {
	m.Health.Lock()
	m.Health.Ready = false
	m.Health.Unlock()
	defer func() {
		m.Health.Lock()
		m.Health.Ready = true
		m.Health.Unlock()
	}()
	err = m.maria.CheckPodNotReady()
	if err != nil {
		return fmt.Errorf("cannot set pod to status: NotReady. Reason: %s", err.Error())
	}
	if err = m.restore.restore(p); err != nil {
		return
	}
	//only remove backup files when restore was succesful, so manual restore is possible!
	os.RemoveAll(p)

	return
}
