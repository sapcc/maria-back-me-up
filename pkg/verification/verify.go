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

package verification

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/maria"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const podName = "mariadb"

type Verification struct {
	storage            *storage.Manager
	maria              *k8s.Maria
	serviceName        string
	lastBackup         storage.Backup
	storageServiceName string
	cfg                config.VerificationService
	cfgMariaDB         config.MariaDB
	status             *Status
	logger             *logrus.Entry
}

func NewVerification(serviceName, storageServiceName string, cs config.StorageService, cv config.VerificationService, cm config.MariaDB, m *k8s.Maria) (*Verification, error) {
	s, err := storage.NewManager(cs, serviceName, cm.LogBin)
	return &Verification{
		serviceName:        serviceName,
		storage:            s,
		maria:              m,
		cfg:                cv,
		cfgMariaDB:         cm,
		storageServiceName: storageServiceName,
		status:             NewStatus(serviceName, storageServiceName),
		logger:             logger.WithField("service", serviceName),
	}, err
}

func (v *Verification) Start(ctx context.Context) (err error) {
	for c := time.Tick(time.Duration(v.cfg.IntervalInMinutes) * time.Minute); ; {
		if err := v.verifyLatestBackup(); err != nil {
			logger.Error(err)
		}
		select {
		case <-c:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func (v *Verification) verifyLatestBackup() (err error) {
	v.status.Reset()
	var restoreFolder string
	bs, err := v.storage.ListFullBackups(v.storageServiceName)
	if err != nil {

	}
	backups := sortBackupsByTime(bs)

	if v.lastBackup.Time.Unix() < 0 {
		logger.Debug("first run")
		//first run!
		restoreFolder, err = v.storage.DownloadBackup(v.storageServiceName, backups[len(backups)-1])
		if err != nil {
			var e *storage.NoBackupError
			if errors.As(err, &e) {
				v.logger.Info(e.Error())
				return
			}
			v.status.SetVerifyRestore(0, fmt.Errorf("error loading backup for verifying: %s", err.Error()))
			return
		}

		v.lastBackup = backups[len(backups)-1]

	} else if v.lastBackup.Time.Unix() < backups[len(backups)-1].Time.Unix() {
		logger.Debug("found new full backup")
		//Found new full backup
		restoreFolder, err = v.storage.DownloadBackup(v.storageServiceName, v.lastBackup)
		if err != nil {
			return
		}
	}

	v.verifyBackup(restoreFolder)
	v.lastBackup = backups[len(backups)-1]
	return
}

func (v *Verification) verifyBackup(restoreFolder string) {
	var err error
	v.logger.Infof("Start verifying backup for service %s", v.serviceName)

	defer func() {
		if err = os.RemoveAll(restoreFolder); err != nil {
			v.logger.Error(err)
		}

		v.status.UploadStatus(restoreFolder, v.storage)
	}()

	cfg := config.BackupService{
		MariaDB: config.MariaDB{
			Host:         fmt.Sprintf("%s-%s-%s-verify", v.storageServiceName, v.serviceName, podName),
			Port:         3306,
			User:         "root",
			Password:     "verify_passw0rd",
			Version:      v.cfgMariaDB.Version,
			LogBin:       v.cfgMariaDB.LogBin,
			Databases:    v.cfgMariaDB.Databases,
			VerifyTables: v.cfgMariaDB.VerifyTables,
		},
	}

	dp, err := v.maria.CreateMariaDeployment(cfg.MariaDB)
	svc, err := v.maria.CreateMariaService(cfg.MariaDB)
	defer func() {
		if err = v.maria.DeleteMariaResources(dp, svc); err != nil {
			v.logger.Error(fmt.Errorf("backup verify error: error deleting mariadb resources: %s", err.Error()))
		}
	}()
	if err != nil {
		v.logger.Error(fmt.Errorf("backup verify error: error creating mariadb resources: %s", err.Error()))
		v.status.SetVerifyRestore(0, fmt.Errorf("error creating mariadb: %s", err.Error()))
		return
	}

	r := backup.NewRestore(cfg)
	if err = r.VerifyRestore(restoreFolder); err != nil {
		v.status.SetVerifyRestore(0, fmt.Errorf("error restoring backup: %s", err.Error()))
		return
	}
	v.status.SetVerifyRestore(1, nil)
	if out, err := maria.RunMysqlDiff(v.cfgMariaDB, cfg.MariaDB); err != nil {
		//This is very bad. 1 or more tables are different or missing
		v.status.SetVerifyDiff(0, fmt.Errorf("error mysqldiff: %s", string(out)))
		return
	}
	v.status.SetVerifyDiff(1, nil)
	if len(v.cfgMariaDB.VerifyTables) > 0 {
		if err = v.verifyChecksums(cfg.MariaDB, restoreFolder); err != nil {
			v.status.SetVerifyChecksum(0, fmt.Errorf("error table checksum: %s", err.Error()))
		} else {
			v.status.SetVerifyChecksum(1, nil)
		}
	}
	v.logger.Info("successfully verified backup")
}

func (v *Verification) verifyChecksums(cfg config.MariaDB, restorePath string) (err error) {
	cs, err := v.loadChecksums(restorePath)
	if err != nil {
		return
	}
	if len(cs.TablesChecksum) == 0 {
		return fmt.Errorf("no checksums found")
	}

	rs, err := maria.GetCheckSumForTable(cfg, v.cfgMariaDB.VerifyTables)
	if err != nil {
		return fmt.Errorf("error verifying backup: %s", err.Error())
	}
	if err = compareChecksums(cs.TablesChecksum, rs.TablesChecksum); err != nil {
		return fmt.Errorf("error verifying backup: %s", err.Error())
	}
	v.logger.Infof("successfully verified checksum %s", cs)
	return
}

func (v *Verification) loadChecksums(restorePath string) (cs maria.Checksum, err error) {
	files, err := ioutil.ReadDir(restorePath)
	if err != nil {
		return
	}
	for _, f := range files {
		if strings.Contains(f.Name(), "tablesChecksum") {
			by, err := ioutil.ReadFile(path.Join(restorePath, f.Name()))
			if err != nil {
				return cs, err
			}
			if err = yaml.Unmarshal(by, &cs); err != nil {
				return cs, err
			}
		}
	}
	return
}

func sortBackupsByTime(backups []storage.Backup) (sorted []storage.Backup) {
	var modTime time.Time
	sorted = make([]storage.Backup, 0)
	for _, b := range backups {
		if !b.Time.Before(modTime) {
			if b.Time.After(modTime) {
				modTime = b.Time
			}
			sorted = append(sorted, b)
		}
	}
	return
}

func compareChecksums(cs map[string]int64, with map[string]int64) error {
	for k, v := range cs {
		if with[k] != v {
			return fmt.Errorf("Backup verify table checksum mismatch for table %s", k)
		}
	}
	return nil
}