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
)

var (
	binlogCancel context.CancelFunc
	backupCancel context.CancelFunc
	binlogChan   chan error
	logger       *logrus.Entry
)

type (
	// Health backup health status
	Health struct {
		sync.Mutex
		Ready bool
	}
	// ChecksumStatus backup checksum struct
	ChecksumStatus struct {
		Checksums map[string]int64 `yaml:"checksums"`
	}
	// Manager that handles the backup cycle
	Manager struct {
		cfg             config.Config
		Db              database.Database
		k8sDB           *k8s.Database
		Storage         *storage.Manager
		updateSts       *UpdateStatus
		Health          *Health
		cronSch         cron.Schedule
		lastBackupTime  string
		backupCheckSums map[string]int64
		errCh           chan error
		cronBackup      *cron.Cron
	}
)

func init() {
	logger = log.WithFields(logrus.Fields{"component": "manager"})
}

// NewManager returns a manager instance
func NewManager(s *storage.Manager, db database.Database, k *k8s.Database, c config.Config) (m *Manager, err error) {

	us := NewUpdateStatus()
	for _, v := range s.GetStorageServicesKeys() {
		us.IncBackup[v] = 0
		us.FullBackup[v] = 0
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
		k8sDB:           k,
		Storage:         s,
		updateSts:       &us,
		Health:          &Health{Ready: true},
		backupCheckSums: make(map[string]int64),
		cronSch:         cronSch,
	}, err
}

// Start a backup cycle
func (m *Manager) Start() (err error) {
	if m.cronBackup != nil {
		return errors.New("backup already running")
	}

	ctx := context.Background()
	ctx, backupCancel = context.WithCancel(ctx)
	m.errCh = make(chan error, 1)
	go m.readErrorChannel()
	return m.startBackup(ctx)
}

// Stop the backup cycle
func (m *Manager) Stop() (ctx context.Context) {
	backupCancel()
	m.stopIncBackup()
	if m.cronBackup == nil {
		return context.TODO()
	}
	close(m.errCh)
	ctx = m.cronBackup.Stop()

	<-ctx.Done()
	m.cronBackup = nil
	return ctx
}

func (m *Manager) startBackup(ctx context.Context) (err error) {
	_, err = checkBackupDirExistsAndCreate(m.cfg.Backup.BackupDir)
	if err != nil {
		return
	}
	go m.scheduleBackup(ctx)
	m.cronBackup = cron.New()
	_, err = m.cronBackup.AddFunc(m.cfg.Backup.FullBackupCronSchedule, func() { m.scheduleBackup(ctx) })
	if err != nil {
		return
	}
	m.cronBackup.Start()
	return
}

func (m *Manager) scheduleBackup(ctx context.Context) {
	logger.Debug("starting full backup cycle")
	defer func() {
		m.lastBackupTime = ""
	}()

	if ctx.Err() != nil {
		log.Error("full backup already in process")
		return
	}
	// Stop binlog
	m.stopIncBackup()
	log.Debug("check if db is up and running")
	if err := m.Db.Up(2*time.Minute, false); err != nil {
		log.Error("cannot connect to database")
		m.Stop()
		err = m.Start()
		if err != nil {
			log.Error("cannot start backup cycle")
			return
		}
		return
	}
	m.lastBackupTime = time.Now().Format(time.RFC3339)
	if err := m.createTableChecksum(m.lastBackupTime); err != nil {
		logger.Error("cannot create checksum: ", err)
	}
	bpath := path.Join(m.cfg.Backup.BackupDir, m.lastBackupTime)
	if ctx.Err() != nil {
		return
	}

	// reset update status for all storages before next full backup cycle
	// this will show hanging backups in the metrics
	m.setUpdateStatus(m.updateSts.FullBackup, m.Storage.GetStorageServicesKeys(), false)
	m.setUpdateStatus(m.updateSts.IncBackup, m.Storage.GetStorageServicesKeys(), false)

	lp, err := m.createFullBackup(bpath)
	m.setUpdateStatus(m.updateSts.FullBackup, m.Storage.GetStorageServicesKeys(), true)
	if err != nil {
		logger.Error("error creating full backup: ", err.Error())
		if err = m.handleBackupError(err, m.updateSts.FullBackup); err != nil {
			m.setUpdateStatus(m.updateSts.FullBackup, m.Storage.GetStorageServicesKeys(), false)
			m.errCh <- err
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	m.createIncBackup(lp, m.lastBackupTime)
}

func (m *Manager) createFullBackup(bpath string) (bp database.LogPosition, err error) {
	logger.Info("creating full backup dump")
	defer os.RemoveAll(bpath)
	_, err = m.Db.HealthCheck()
	if err != nil {
		return bp, fmt.Errorf("cannot start backup: %w", err)
	}
	bp, err = m.Db.CreateFullBackup(bpath)

	if err != nil {
		return bp, err
	}

	logger.Debug("finished full backup")
	return
}

func (m *Manager) createIncBackup(lp database.LogPosition, backupTime string) {
	logger.Info("creating incremental backup")
	ctx := context.Background()
	ctx, binlogCancel = context.WithCancel(ctx)
	binlogChan = make(chan error)
	go m.onBinlogRotation(binlogChan)
	var eg errgroup.Group
	eg.Go(func() error {
		return m.Db.StartIncBackup(ctx, lp, backupTime, binlogChan)
	})
	go func() {
		if err := eg.Wait(); err != nil {
			hErr := m.handleBackupError(err, m.updateSts.IncBackup)
			if hErr != nil {
				logger.Errorf("error handling backup error: %s", hErr.Error())
			}
			m.setUpdateStatus(m.updateSts.IncBackup, m.Storage.GetStorageServicesKeys(), false)
			m.errCh <- err
		}
	}()
}

func (m *Manager) stopIncBackup() {
	if binlogCancel != nil {
		binlogCancel()
	}
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
			return errors.New("cannot restore. no backup available")
		}
		if errb != nil {
			return fmt.Errorf("cannot do init restore. err: %s", errb.Error())
		}
		logger.Infof("starting restore due to %s, using backup %s", err.Error(), bf)
		return m.Db.Restore(bf)
	}

	merr, ok := err.(*multierror.Error)
	if !ok {
		return err
	}

	//backups cant be written to any storage
	if len(svc) == len(merr.Errors) {
		return fmt.Errorf("cannot write to any storage: %s", err.Error())
	}

	//backups cant be written to all storages
	m.setUpdateStatus(backup, svc, true)
	for _, e := range merr.Errors {
		err, ok := e.(*storage.Error)
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
			m.setUpdateStatus(m.updateSts.IncBackup, m.Storage.GetStorageServicesKeys(), true)
			continue
		}
		stsError := make([]string, 0)
		merr, ok := err.(*multierror.Error)
		if merr != nil {
			if !ok {
				m.setUpdateStatus(m.updateSts.IncBackup, m.Storage.GetStorageServicesKeys(), false)
				logger.Errorf("unknown error: %s", merr.Error())
			}
			if len(merr.Errors) > 0 {
				for i := 0; i < len(merr.Errors); i++ {
					switch e := merr.Errors[i].(type) {
					case *storage.Error:
						stsError = append(stsError, e.Storage)
						logger.Errorf("error writing log to storage %s. error: %s", e.Storage, merr.Error())
					default:
						log.Error(merr.Errors[i])
					}
					// means all storage services returned an error.
					if i == len(m.Storage.GetStorageServicesKeys())-1 {
						m.errCh <- err
					}
				}
			}
			m.setUpdateStatus(m.updateSts.IncBackup, stsError, false)
		}
	}
}

func (m *Manager) createTableChecksum(backupTime string) (err error) {
	if len(m.cfg.Database.VerifyTables) == 0 {
		logger.Info("no verify tables supplied. skipping table checksums")
		return
	}
	cs, err := m.Db.GetCheckSumForTable(m.cfg.Database.VerifyTables, false)
	if err != nil {
		return
	}
	out, err := yaml.Marshal(cs)
	if err != nil {
		return
	}
	events := make(chan storage.StreamEvent, 1)
	errors := make(chan error, 1)
	defer close(errors)

	events <- &storage.ByteEvent{Value: out}

	go func() {
		errors <- m.Storage.WriteStreamAll(backupTime+"/tablesChecksum.yaml", "", events, false)
	}()
	close(events)
	err = <-errors
	if err != nil {
		logger.Error(fmt.Errorf("cannot upload table checksums: %s", err.Error()))
		return
	}
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
		m.updateSts.Restarts.Inc()
		logger.Error(fmt.Sprintf("cannot handle backup error: %s. -> Restarting in 2min", err.Error()))
		m.Stop()
		time.Sleep(time.Duration(2) * time.Minute)
		var eg errgroup.Group
		eg.Go(m.Start)
		if err = eg.Wait(); err != nil {
			logger.Error(fmt.Sprintf("cannot start backup cycle: %s.", err.Error()))
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
