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
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/maria"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/util/wait"
)

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
	ChecksumStatus struct {
		timestamp int64            `yaml:"timestamp"`
		Checksums map[string]int64 `yaml:"checksums"`
	}
	Manager struct {
		cfg             config.BackupService
		backup          *Backup
		maria           *k8s.Maria
		restore         *Restore
		Storage         *storage.Manager
		updateSts       *updateStatus
		Health          *Health
		lastBackupTime  string
		backupCheckSums map[string]int64
		errCh           chan error
	}
)

func init() {
	logger = log.WithFields(logrus.Fields{"component": "manager"})
}

func NewManager(c config.Config) (m *Manager, err error) {
	s, err := storage.NewManager(c.StorageService, c.ServiceName, c.BackupService.MariaDB.LogBin)
	if err != nil {
		return
	}
	us := updateStatus{
		fullBackup: make(map[string]int, 0),
		incBackup:  make(map[string]int, 0),
	}
	for _, v := range s.GetStorageServices() {
		us.incBackup[v] = 0
		us.fullBackup[v] = 0
	}
	b, err := NewBackup(c, s)
	if err != nil {
		return
	}

	mr, err := k8s.NewMaria(c.Namespace)
	if err != nil {
		return
	}

	prometheus.MustRegister(NewMetricsCollector(c.BackupService.MariaDB, &us))
	bs := c.BackupService
	bs.MariaDB.Host = "127.0.0.1"
	return &Manager{
		cfg:             c.BackupService,
		backup:          b,
		maria:           mr,
		restore:         NewRestore(bs),
		Storage:         s,
		updateSts:       &us,
		Health:          &Health{Ready: true},
		backupCheckSums: make(map[string]int64),
		errCh:           make(chan error, 1),
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
	return err
}

func (m *Manager) startBackup(ctx context.Context) (err error) {
	_, err = checkBackupDirExistsAndCreate(m.cfg.BackupDir)
	if err != nil {
		return
	}

	for c := time.Tick(time.Duration(m.cfg.FullBackupIntervalInHours) * time.Hour); ; {
		logger.Debug("starting backup cycle")
		// Stop binlog
		if binlogCancel != nil {
			binlogCancel()
			time.Sleep(time.Duration(100) * time.Millisecond)
		}

		m.lastBackupTime = time.Now().Format(time.RFC3339)
		bpath := path.Join(m.cfg.BackupDir, m.lastBackupTime)
		ch := make(chan error)
		mp, err := m.createMysqlDump(bpath)
		if err != nil {
			logger.Error(fmt.Sprintf("error creating mysqldump: %s", err.Error()))
			if err = m.handleMysqlDumpError(err); err != nil {
				time.Sleep(time.Duration(2) * time.Minute)
				continue
			}
			continue
		}
		m.setUpdateStatus(m.updateSts.fullBackup, m.Storage.GetStorageServices(), true)
		ctxBin := context.Background()
		ctxBin, binlogCancel = context.WithCancel(ctxBin)
		go m.onBinlogRotation(ch)
		var eg errgroup.Group
		eg.Go(func() error {
			return m.backup.runBinlog(ctxBin, mp, m.lastBackupTime, ch)
		})
		go func() {
			if err = eg.Wait(); err != nil {
				m.setUpdateStatus(m.updateSts.incBackup, m.Storage.GetStorageServices(), false)
				logger.Error(fmt.Errorf("error saving log files: %s", err.Error()))
				if ctx.Err() == nil {
					m.errCh <- err
				}
			}
		}()

		select {
		case <-c:
			m.createTableChecksum()
			m.lastBackupTime = ""
			continue
		case <-m.errCh:
			continue
		case <-ctx.Done():
			logger.Info("stop backup")
			// Stop binlog
			if binlogCancel != nil {
				binlogCancel()
			}
			return nil
		}
	}
}

func (m *Manager) createMysqlDump(bpath string) (mp mysql.Position, err error) {
	logger.Info("starting full backup")
	defer os.RemoveAll(bpath)
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := maria.HealthCheck(m.cfg.MariaDB)
		if err != nil {
			_, ok := err.(*maria.DatabaseMissingError)
			if ok {
				return false, err
			}
			return false, nil
		} else if !s.Ok {
			return false, fmt.Errorf("tables corrupt: %s", s.Details)
		}
		return true, nil
	})
	//Only do backups if db is healthy
	if err = wait.Poll(5*time.Second, 5*time.Minute, cf); err != nil {
		_, err := maria.HealthCheck(m.cfg.MariaDB)
		connErr, ok := err.(*maria.DatabaseConnectionError)
		if ok {
			return mp, fmt.Errorf("cannot start backup: %w", connErr)
		}
		return mp, fmt.Errorf("cannot start backup: %w", err)
	}

	if err = m.backup.createMysqlDump(bpath); err != nil {
		return mp, fmt.Errorf("error creating mysqlDump: %w", err)
	}

	mp, err = readMetadata(bpath)
	if err != nil {
		return mp, fmt.Errorf("error cannot read binlog metadata: %s", err.Error())
	}

	logger.Debug("finished full backup")
	return
}

func (m *Manager) handleMysqlDumpError(err error) error {
	var missingErr *maria.DatabaseMissingError
	var connErr *maria.DatabaseConnectionError
	stsError := make([]string, 0)
	svc := m.Storage.GetStorageServices()
	if errors.As(err, &missingErr) && m.cfg.EnableInitRestore || errors.As(err, &connErr) && m.cfg.EnableRestoreOnDBFailure {
		m.setUpdateStatus(m.updateSts.fullBackup, stsError, false)
		var eb *storage.NoBackupError
		bf, errb := m.Storage.DownloadLatestBackup("")
		if errors.As(errb, &eb) {
			logger.Info("cannot restore. no backup available")
			return nil
		}
		if errb != nil {
			return err
		}
		logger.Infof("starting restore due to %s ", err.Error())
		return m.restore.restore(bf)
	}

	merr, ok := err.(*multierror.Error)
	if !ok {
		return fmt.Errorf("unknown error: %s", err.Error())
	}

	if len(svc) == len(merr.Errors) {
		m.setUpdateStatus(m.updateSts.fullBackup, stsError, false)
		return fmt.Errorf("cannot write to any storage: %s", err.Error())
	}
	for _, e := range merr.Errors {
		err, ok := e.(*storage.StorageError)
		if !ok {
			return fmt.Errorf("unknown error: %s", err.Error())
		}
		stsError = append(stsError, err.Storage)
	}
	m.setUpdateStatus(m.updateSts.fullBackup, stsError, false)

	return nil
}

func (m *Manager) setUpdateStatus(field map[string]int, storages []string, up bool) {
	find := func(i string) bool {
		for _, v := range storages {
			if v == i {
				return true
			}
		}
		return false
	}
	m.updateSts.Lock()
	defer m.updateSts.Unlock()

	upFnc := func(b bool) int {
		if b {
			return 1
		}
		return 0
	}
	for _, s := range m.Storage.GetStorageServices() {
		if find(s) {
			field[s] = upFnc(up)
		} else {
			field[s] = upFnc(!up)
		}
	}
}

func (m *Manager) onBinlogRotation(c chan error) {
	for {
		err, ok := <-c
		if !ok {
			logger.Debug("binlog rotation channel closed")
			break
		}
		if err == nil {
			m.setUpdateStatus(m.updateSts.incBackup, m.Storage.GetStorageServices(), true)
			continue
		}
		stsError := make([]string, 0)
		merr, ok := err.(*multierror.Error)
		if merr != nil {
			if !ok {
				m.setUpdateStatus(m.updateSts.incBackup, m.Storage.GetStorageServices(), false)
				logger.Errorf("unknown error: %s", merr.Error())
			}
			if len(merr.Errors) > 0 {
				for i := 0; i < len(merr.Errors); i++ {
					switch e := merr.Errors[i].(type) {
					case *storage.StorageError:
						stsError = append(stsError, e.Storage)
						logger.Errorf("error writing log to storage %s. error: ", e.Storage, merr.Error())
					default:
						log.Error(merr.Errors[i])
					}
					// means all storage services returned an error.
					if i == len(m.Storage.GetStorageServices())-1 {
						m.errCh <- err
					}
				}
			}
			m.setUpdateStatus(m.updateSts.incBackup, stsError, false)
		}
	}
}

func (m *Manager) Stop() {
	backupCancel()
}

func (m *Manager) GetConfig() config.BackupService {
	return m.cfg
}

func (m *Manager) createTableChecksum() (err error) {
	cs, err := maria.GetCheckSumForTable(m.cfg.MariaDB, m.cfg.MariaDB.VerifyTables)
	if err != nil {
		return
	}
	out, err := yaml.Marshal(cs)
	if err != nil {
		return
	}
	err = m.Storage.WriteStreamAll(m.lastBackupTime+"/tablesChecksum.yaml", "", bytes.NewReader(out))
	if err != nil {
		logger.Error(fmt.Errorf("cannot upload verify status: %s", err.Error()))
		return
	}
	return
}

func (m *Manager) Restore(p string) (err error) {
	logger.Info("STARTING RESTORE")
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
