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
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/database"
	dberror "github.com/sapcc/maria-back-me-up/pkg/error"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/util/wait"
)

var (
	binlogCancel  context.CancelFunc
	backupCancel  context.CancelFunc
	logUpdate     chan<- time.Time
	logger        *logrus.Entry
	scheduleTimer *time.Timer
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
		cfg             config.Config
		Db              database.Database
		k8sDB           *k8s.Database
		Storage         *storage.Manager
		updateSts       *updateStatus
		Health          *Health
		cronSch         cron.Schedule
		lastBackupTime  string
		backupCheckSums map[string]int64
		errCh           chan error
	}
)

func init() {
	logger = log.WithFields(logrus.Fields{"component": "manager"})
}

func NewManager(c config.Config) (m *Manager, err error) {
	s, err := storage.NewManager(c.Storages, c.ServiceName, c.Database.LogNameFormat)
	if err != nil {
		return
	}

	db, err := database.NewDatabase(c, s)
	if err != nil {
		return
	}
	us := updateStatus{
		fullBackup: make(map[string]int, 0),
		incBackup:  make(map[string]int, 0),
	}
	for _, v := range s.GetStorageServicesKeys() {
		us.incBackup[v] = 0
		us.fullBackup[v] = 0
	}

	mr, err := k8s.New(c.Namespace)
	if err != nil {
		return
	}

	p := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	cronSch, err := p.Parse(c.Backup.FullBackupCronSchedule)
	if err != nil {
		return
	}

	prometheus.MustRegister(NewMetricsCollector(&us))
	return &Manager{
		Db:              db,
		cfg:             c,
		k8sDB:           mr,
		Storage:         s,
		updateSts:       &us,
		Health:          &Health{Ready: true},
		backupCheckSums: make(map[string]int64),
		errCh:           make(chan error, 1),
		cronSch:         cronSch,
	}, err
}

func (m *Manager) Start() (err error) {
	go m.readErrorChannel()
	ctx := context.Background()
	ctx, backupCancel = context.WithCancel(ctx)
	return m.startBackup(ctx)
}

func (m *Manager) Stop() {
	backupCancel()
	if binlogCancel != nil {
		binlogCancel()
	}
}

func (m *Manager) startBackup(ctx context.Context) (err error) {
	_, err = checkBackupDirExistsAndCreate(m.cfg.Backup.BackupDir)
	if err != nil {
		return
	}
	go m.scheduleBackup()
	cronBackup := cron.New()
	cronBackup.AddFunc(m.cfg.Backup.FullBackupCronSchedule, m.scheduleBackup)
	cronBackup.Start()
	select {
	case <-ctx.Done():
		logger.Info("stopping backup cron")
		cronBackup.Stop()
		return
	}
}

func (m *Manager) scheduleBackup() {
	logger.Debug("starting full backup cycle")
	if err := m.createTableChecksum(); err != nil {
		logger.Error("cannot create checksum", err)
	}
	m.lastBackupTime = ""
	// Stop binlog
	if binlogCancel != nil {
		binlogCancel()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}

	m.lastBackupTime = time.Now().Format(time.RFC3339)
	bpath := path.Join(m.cfg.Backup.BackupDir, m.lastBackupTime)
	ch := make(chan error)
	mp, err := m.createFullBackup(bpath)
	if err != nil {
		logger.Error(fmt.Sprintf("error creating full backup: %s", err.Error()))
		if err = m.handleBackupError(err, m.updateSts.fullBackup); err != nil {
			m.Stop()
			logger.Error(fmt.Sprintf("cannot handle full backup error. Retrying in 2min: %s", err.Error()))
			m.setUpdateStatus(m.updateSts.fullBackup, m.Storage.GetStorageServicesKeys(), false)
			time.Sleep(time.Duration(2) * time.Minute)
			m.Start()
		}
		return
	}
	m.setUpdateStatus(m.updateSts.fullBackup, m.Storage.GetStorageServicesKeys(), true)
	ctxBin := context.Background()
	ctxBin, binlogCancel = context.WithCancel(ctxBin)
	go m.onBinlogRotation(ch)
	var eg errgroup.Group
	eg.Go(func() error {
		return m.Db.StartIncBackup(ctxBin, mp, m.lastBackupTime, ch)
	})
	go func() {
		if err = eg.Wait(); err != nil {
			m.handleBackupError(err, m.updateSts.incBackup)
			m.setUpdateStatus(m.updateSts.incBackup, m.Storage.GetStorageServicesKeys(), false)
			m.errCh <- err
		}
	}()
}

func (m *Manager) createFullBackup(bpath string) (bp database.LogPosition, err error) {
	logger.Info("creating full backup dump")
	defer os.RemoveAll(bpath)
	cf := wait.ConditionFunc(func() (bool, error) {
		s, err := m.Db.HealthCheck()
		if err != nil {
			_, ok := err.(*dberror.DatabaseMissingError)
			if ok {
				return false, err
			}
			_, ok = err.(*dberror.DatabaseNoTablesError)
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
		_, err := m.Db.HealthCheck()
		connErr, ok := err.(*dberror.DatabaseConnectionError)
		if ok {
			return bp, fmt.Errorf("cannot start backup: %w", connErr)
		}
		return bp, fmt.Errorf("cannot start backup: %w", err)
	}
	bp, err = m.Db.CreateFullBackup(bpath)

	if err != nil {
		return bp, err
	}

	logger.Debug("finished full backup")
	return
}

func (m *Manager) handleBackupError(err error, backup map[string]int) error {
	var missingErr *dberror.DatabaseMissingError
	var connErr *dberror.DatabaseConnectionError
	var noTablesErr *dberror.DatabaseNoTablesError
	stsError := make([]string, 0)
	svc := m.Storage.GetStorageServicesKeys()
	if ((errors.As(err, &missingErr) && m.cfg.Backup.EnableInitRestore) || (errors.As(err, &noTablesErr) && m.cfg.Backup.EnableInitRestore)) || (errors.As(err, &connErr) && m.cfg.Backup.EnableRestoreOnDBFailure) {
		var eb *storage.NoBackupError
		var errb error
		var bf string
		//try to find a latest/successful backup in any of the available storages
		for _, k := range svc {
			bf, errb = m.Storage.DownloadLatestBackup(k)
			if errb == nil {
				break
			}
		}
		if errors.As(errb, &eb) {
			return fmt.Errorf("cannot restore. no backup available")
		}
		if errb != nil {
			return fmt.Errorf("cannot do init restore. err: %s", errb.Error())
		}
		logger.Infof("starting restore due to %s, using backup %s", err.Error(), bf)
		return m.Db.Restore(bf)
	}

	merr, ok := err.(*multierror.Error)
	if !ok {
		return fmt.Errorf("unknown error: %s", err.Error())
	}

	//backups cant be written to any storage
	if len(svc) == len(merr.Errors) {
		return fmt.Errorf("cannot write to any storage: %s", err.Error())
	}

	//backups cant be written to all storages
	m.setUpdateStatus(backup, svc, true)
	for _, e := range merr.Errors {
		err, ok := e.(*storage.StorageError)
		if !ok {
			return fmt.Errorf("unknown error: %s", err.Error())
		}
		stsError = append(stsError, err.Storage)
	}
	m.setUpdateStatus(backup, stsError, false)

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
	for _, s := range m.Storage.GetStorageServicesKeys() {
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
			m.setUpdateStatus(m.updateSts.incBackup, m.Storage.GetStorageServicesKeys(), true)
			continue
		}
		stsError := make([]string, 0)
		merr, ok := err.(*multierror.Error)
		if merr != nil {
			if !ok {
				m.setUpdateStatus(m.updateSts.incBackup, m.Storage.GetStorageServicesKeys(), false)
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
					if i == len(m.Storage.GetStorageServicesKeys())-1 {
						m.errCh <- err
					}
				}
			}
			m.setUpdateStatus(m.updateSts.incBackup, stsError, false)
		}
	}
}

func (m *Manager) GetConfig() config.Config {
	return m.cfg
}

func (m *Manager) createTableChecksum() (err error) {
	cs, err := m.Db.GetCheckSumForTable(m.cfg.Database.VerifyTables, false)
	if err != nil {
		return
	}
	out, err := yaml.Marshal(cs)
	if err != nil {
		return
	}
	err = m.Storage.WriteStreamAll(m.lastBackupTime+"/tablesChecksum.yaml", "", bytes.NewReader(out))
	if err != nil {
		logger.Error(fmt.Errorf("cannot upload table checksums: %s", err.Error()))
		return
	}
	return
}

func (m *Manager) Restore(p string) (err error) {
	logger.Info("starting restore")
	m.Health.Lock()
	m.Health.Ready = false
	m.Health.Unlock()
	logger.Debug("restore with sidecar: ", *m.cfg.SideCar)
	defer func() {
		if m.cfg.SideCar != nil && !*m.cfg.SideCar {
			ip, err := m.k8sDB.GetPodIP(fmt.Sprintf("app=%s-mariadb", m.cfg.ServiceName))
			if err != nil {
				logger.Error("Cannot set pod databse to ready")
			}
			if err = sendReadinessRequest([]byte(`{"ready":true}`), ip); err != nil {
				logger.Error("Cannot set pod databse to ready")
			}
		}
		m.Health.Lock()
		m.Health.Ready = true
		m.Health.Unlock()
	}()
	if m.cfg.SideCar != nil && !*m.cfg.SideCar {
		if err = sendReadinessRequest([]byte(`{"ready":false}`), m.cfg.Database.Host); err != nil {
			return
		}
	}

	if err = m.k8sDB.CheckPodNotReady(fmt.Sprintf("app=%s-mariadb", m.cfg.ServiceName)); err != nil {
		return fmt.Errorf("cannot set pod to status: NotReady. reason: %s", err.Error())
	}
	if err = m.Db.Restore(p); err != nil {
		return
	}
	logger.Info("restore successful")
	//only remove backup files when restore was succesful, so manual restore is possible!
	os.RemoveAll(p)

	return
}

func (m *Manager) readErrorChannel() {
	for {
		err, ok := <-m.errCh
		if !ok {
			logger.Debug("error channel closed")
			break
		}

		if err == nil {
			continue
		}
		if err != nil {
			logger.Error(fmt.Errorf("error reading log files: %s", err.Error()))
			m.Stop()
			time.Sleep(time.Duration(2) * time.Minute)
			m.Start()
		}
	}
}

func sendReadinessRequest(jsonRdy []byte, h string) (err error) {
	client := &http.Client{}
	u, err := url.Parse(fmt.Sprintf("http://%s:8080", h))
	if err != nil {
		return
	}
	u.Path = path.Join(u.Path, "/pod/readiness")
	logger.Debug(fmt.Sprintf("updating mariadb pod readiness %s", u.String()))

	req, err := http.NewRequest(http.MethodPatch, u.String(), bytes.NewBuffer(jsonRdy))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	_, err = client.Do(req)
	if err != nil {
		return
	}
	return
}

func checkBackupDirExistsAndCreate(d string) (p string, err error) {
	if _, err := os.Stat(d); os.IsNotExist(err) {
		err = os.MkdirAll(d, os.ModePerm)
		return d, err
	}
	return
}
