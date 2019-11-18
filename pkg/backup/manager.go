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
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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
	us := updateStatus{up: 1}

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
		log.Debug("Start full backup cycle")
		//verify last full backup cycle
		if m.lastBackupTime == "" {
			m.verifyLatestBackup(false)
		}

		// Stop binlog
		if binlogCancel != nil {
			binlogCancel()
		}

		m.lastBackupTime = time.Now().Format(time.RFC3339)
		bpath := path.Join(m.cfg.BackupDir, m.lastBackupTime)
		ch := make(chan time.Time)
		mp, err := m.createMysqlDump(bpath)
		if err != nil {
			logger.Error(fmt.Sprintf("error creating mysqldump: %s", err.Error()))
			m.updateStatus(0, time.Time{})
			if err = m.initRestore(err); err != nil {
				time.Sleep(time.Duration(10) * time.Second)
				continue
			}
			time.Sleep(time.Duration(5) * time.Minute)
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
				m.updateStatus(0, time.Time{})
			}
		}()

		m.updateStatus(1, time.Now())

		select {
		case <-c:
			m.lastBackupTime = ""
			continue
		case <-ctx.Done():
			logger.Info("stop backup")
			// Stop binlog
			if binlogCancel != nil {
				binlogCancel()
			}
			if m.verifyTimer != nil {
				m.verifyTimer.Stop()
			}
			return nil
		}
	}
}

func (m *Manager) updateStatus(s int, t time.Time) {
	m.updateSts.Lock()
	if !t.IsZero() {
		m.updateSts.fullBackup = t
	}
	m.updateSts.up = s
	m.updateSts.Unlock()
}

func (m *Manager) createMysqlDump(bpath string) (mp mysql.Position, err error) {
	logger.Debug("Starting full backup")
	defer os.RemoveAll(bpath)
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := HealthCheck(m.cfg.MariaDB)
		if err != nil {
			_, ok := err.(*DatabaseMissingError)
			if ok {
				return false, err
			}
			return false, nil
		} else if !s.Ok {
			return false, fmt.Errorf("Tables corrupt: %s", s.Details)
		}
		return true, nil
	})
	//Only do backups if db is healthy
	if err = wait.Poll(10*time.Second, 1*time.Minute, cf); err != nil {
		return mp, fmt.Errorf("Cannot do backup: %w", err)
	}

	if err = m.backup.createMysqlDump(bpath); err != nil {
		return mp, fmt.Errorf("Error creating mysqlDump: %w", err)
	}

	mp, err = readMetadata(bpath)
	if err != nil {
		return mp, fmt.Errorf("Error cannot read binlog metadata: %s", err.Error())
	}

	logger.Debug("Finished full backup")
	return
}

func (m *Manager) initRestore(err error) error {
	var ed *DatabaseMissingError
	if errors.As(err, &ed) && m.cfg.EnableInitRestore {
		var eb *storage.NoBackupError
		bf, err := m.Storage.DownloadLatestBackup()
		if errors.As(err, &eb) {
			logger.Info("Cannot restore. No backup available")
			return nil
		}
		if err != nil {
			return err
		}
		if err != nil {
			logger.Error(err.Error())
		}
		log.Info("Starting init restore")
		return m.restore.restore(bf)
	}
	return nil
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
				m.verifyLatestBackup(true)
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
