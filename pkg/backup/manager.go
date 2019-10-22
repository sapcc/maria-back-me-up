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
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/siddontang/go-mysql/mysql"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/util/wait"
)

var (
	binlogCancel   context.CancelFunc
	backupCancel   context.CancelFunc
	lastBackupTime time.Time
)

type Manager struct {
	cfg      config.Config
	backup   *Backup
	verifier *Verifier
	updatec  chan<- Update
	restore  *Restore
	Storage  storage.Storage
}

func NewManager(c config.Config) (m *Manager, err error) {
	s3, err := storage.NewS3(c.S3, c.ServiceName)
	b, err := NewBackup(c, s3)
	if err != nil {
		return
	}

	v, err := NewVerifier("mariadbbackup")
	if err != nil {
		return
	}

	return &Manager{
		cfg:      c,
		backup:   b,
		verifier: v,
		restore:  NewRestore(c),
		Storage:  s3,
	}, err
}

func (m *Manager) StartBackup() (err error) {
	_, err = checkBackupDirExistsAndCreate(m.cfg.BackupDir)
	if err != nil {
		return
	}
	ctx := context.Background()
	ctx, backupCancel = context.WithCancel(ctx)
	for c := time.Tick(time.Duration(m.cfg.FullBackupIntervalInSeconds) * time.Second); ; {
		cf := wait.ConditionFunc(func() (bool, error) {
			s, err := HealthCheck(m.cfg.MariaDB)
			if err != nil || !s.Ok {
				return false, nil
			}
			return true, nil
		})
		//Only do backups if db is healthy
		if err = wait.Poll(5*time.Second, 1*time.Minute, cf); err != nil {
			return err
		}
		// Stop binlog
		if binlogCancel != nil {
			binlogCancel()
		}
		lastBackupTime = time.Now()
		bpath := path.Join(m.cfg.BackupDir, lastBackupTime.Format(time.RFC3339))
		err := m.backup.createMysqlDump(bpath)
		if err != nil {
			return fmt.Errorf("Error creating mysqlDump: %w", err)
		}
		ctxBin := context.Background()
		ctxBin, binlogCancel = context.WithCancel(ctxBin)
		mp, err := readMetadata(bpath)
		if err != nil {
			return fmt.Errorf("Error cannot read binlog metadata: %w", err)
		}
		var eg errgroup.Group
		eg.Go(func() error {
			return m.flushLogs(ctxBin)
		})
		eg.Go(func() error {
			return m.backup.runBinlog(ctxBin, mp, lastBackupTime.Format(time.RFC3339))
		})
		go func() {
			if err = eg.Wait(); err != nil {
				log.Fatal(err)
				return
			}
		}()

		select {
		case <-c:
			continue
		case <-ctx.Done():
			log.Info("stop backup")
			binlogCancel()
			return nil
		}
	}
}

func (m *Manager) StartVerifyBackup(ctx context.Context) error {
	cfg := config.Config{
		MariaDB: config.MariaDB{
			Host:     fmt.Sprintf("%s-mariadb-verify", m.cfg.ServiceName),
			Port:     3306,
			User:     m.cfg.MariaDB.User,
			Password: m.cfg.MariaDB.Password,
		},
	}
	for c := time.Tick(time.Duration(m.cfg.IncrementalBackupIntervalInSeconds) * time.Second); ; {

		dp, err := m.verifier.createMariaDBDeployment(cfg.MariaDB)
		svc, err := m.verifier.createMariaDBService(cfg.MariaDB)
		if err != nil {
			log.Fatal("error creating mariadb for verifying: ", err.Error())
			continue
		}
		p, err := m.Storage.GetLatestBackup()
		if err != nil {
			log.Error("error restoring backup for verifying: ", err.Error())
		}
		r := NewRestore(cfg)
		if err = r.Restore(p); err != nil {
			log.Error("error restoring backup for verifying: ", err.Error())
			continue
		}
		if err = m.verifier.verifyBackup(cfg.MariaDB); err != nil {
			log.Error("error verifying backup: ", err.Error())
		}
		if err = m.verifier.deleteMariaDB(dp, svc); err != nil {
			log.Error("error deleting mariadb resources for verifying: ", err.Error())
			continue
		}

		select {
		case <-c:
			continue
		case <-ctx.Done():
			log.Info("stop verifier")
			binlogCancel()
			return nil
		}
	}
}

func (m *Manager) StopBackup() {
	backupCancel()
}

func (m *Manager) GetConfig() config.Config {
	return m.cfg
}

func (m *Manager) RestoreBackup(p string) error {
	return m.restore.Restore(p)
}

func (m *Manager) HardRestoreBackup(p string) error {
	return m.restore.HardRestore(p)
}

func (b *Backup) sendUpdate(i int, err error) {
	b.updatec <- Update{
		Backup: map[int]string{
			i: time.Now().String(),
		},
		Err: err,
	}
}

func (m *Manager) flushLogs(ctx context.Context) (err error) {
	for c := time.Tick(time.Duration(m.cfg.VerifyBackupIntervalInSeconds) * time.Second); ; {
		if err = m.backup.flushLogs(ctx); err != nil {
			return err
		}
		select {
		case <-c:
			continue
		case <-ctx.Done():
			log.Info("stop flush")
			return
		}
	}
}

func checkBackupDirExistsAndCreate(d string) (p string, err error) {
	if _, err := os.Stat(d); os.IsNotExist(err) {
		err = os.MkdirAll(d, os.ModePerm)
		return d, err
	}
	return
}

func readMetadata(p string) (mp mysql.Position, err error) {
	meta := metadata{}
	yamlBytes, err := ioutil.ReadFile(path.Join(p, "/metadata"))
	if err != nil {
		return mp, fmt.Errorf("read config file: %s", err.Error())
	}
	//turn string to valid yaml
	yamlCorrect := strings.ReplaceAll(string(yamlBytes), "\t", "  ")
	r, _ := regexp.Compile("([a-zA-Z])[\\:]([^\\s])")
	err = yaml.Unmarshal([]byte(r.ReplaceAllString(yamlCorrect, `$1: $2`)), &meta)
	if err != nil {
		return mp, fmt.Errorf("parse config file: %s", err.Error())
	}
	mp.Name = meta.Status.Log
	mp.Pos = meta.Status.Pos

	return
}
