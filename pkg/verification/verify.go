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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/database"
	"github.com/sapcc/maria-back-me-up/pkg/k8s"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const podName = "mariadb"

// Verification struct for the verification process
type Verification struct {
	storage            storage.Storage
	k8sDb              *k8s.Database
	db                 database.Database
	serviceName        string
	currentFullBackup  storage.Backup
	cfg                config.VerificationService
	status             *Status
	logger             *logrus.Entry
	totalVerifications int
}

// NewVerification creates a verification instance
func NewVerification(serviceName string, s storage.Storage, cv config.VerificationService, db database.Database, kd *k8s.Database) *Verification {
	if cv.RunAfterIncBackups == 0 {
		cv.RunAfterIncBackups = 5
	}
	return &Verification{
		serviceName:        serviceName,
		totalVerifications: 0,
		storage:            s,
		db:                 db,
		k8sDb:              kd,
		cfg:                cv,
		status:             NewStatus(serviceName, s.GetStorageServiceName()),
		logger:             logger.WithFields(logrus.Fields{"service": serviceName, "storage": s.GetStorageServiceName()}),
	}
}

// Start a verification process
func (v *Verification) Start(ctx context.Context) (err error) {
	d := time.NewTicker(time.Duration(10) * time.Minute)
	for {
		b, err := v.getLatestBackup()
		if err != nil {
			logger.Error(err)
			continue
		}
		totalInc, err := v.storage.GetTotalIncBackupsFromDump(b.Key)
		if err != nil {
			logger.Error(err)
			continue
		}

		if v.currentFullBackup.Time.Unix() < b.Time.Unix() {
			v.totalVerifications = 0
		}
		logger.Debugf("total verification count: %d. current incremental backup count: %d", v.totalVerifications, totalInc/v.cfg.RunAfterIncBackups)
		if totalInc/v.cfg.RunAfterIncBackups > v.totalVerifications {
			logger.Debug("verifying latest backup")
			if err := v.verifyLatestBackup(b); err != nil {
				logger.Error(err)
				continue
			}
			v.currentFullBackup = b
			v.totalVerifications = totalInc / v.cfg.RunAfterIncBackups
		}
		select {
		case <-d.C:
			continue
		case <-ctx.Done():
			return err
		}

	}
}

func (v *Verification) getLatestBackup() (s storage.Backup, err error) {
	bs, err := v.storage.GetFullBackups()
	if err != nil {
		return
	}
	if len(bs) == 0 {
		return s, errors.New("no backup found")
	}
	backups := sortBackupsByTime(bs)
	return backups[len(backups)-1], err
}

func (v *Verification) verifyLatestBackup(latestBackup storage.Backup) (err error) {
	v.status.Reset()
	var restoreFolder string
	if v.currentFullBackup.Time.Unix() > 0 && v.currentFullBackup.Time.Unix() < latestBackup.Time.Unix() {
		v.logger.Debug("found new full backup")
		restoreFolder, err = v.downloadBackup(v.currentFullBackup)
		if err != nil {
			return
		}
	} else {
		path := strings.Split(latestBackup.Key, "/")
		if len(path) < 1 {
			return errors.New("wrong backup path")
		}
		err = v.createTableChecksum(path[1])
		if err != nil {
			return
		}
		restoreFolder, err = v.downloadBackup(latestBackup)
		if err != nil {
			return
		}
	}

	if restoreFolder == "" {
		return
	}

	v.verifyBackup(restoreFolder)
	return
}

func (v *Verification) downloadBackup(b storage.Backup) (restoreFolder string, err error) {
	restoreFolder, err = v.storage.DownloadBackup(b)
	if err != nil {
		var e *storage.NoBackupError
		if errors.As(err, &e) {
			v.logger.Info(e.Error())
			return restoreFolder, nil
		}
		v.status.SetVerifyRestore(0, fmt.Errorf("error loading backup for verifying: %s", err.Error()))
		return
	}
	return
}

func (v *Verification) verifyBackup(restoreFolder string) {
	var err error
	v.logger.Infof("Start verifying backup for service %s", v.serviceName)
	dbCfg := v.db.GetConfig()
	defer func() {
		v.status.Upload(restoreFolder, dbCfg.LogNameFormat, v.serviceName, v.storage)
		if err = os.RemoveAll(restoreFolder); err != nil {
			v.logger.Error(err)
		}
	}()
	verifyDbcfg := config.DatabaseConfig{
		Host:          fmt.Sprintf("%s-%s-%s-verify", v.storage.GetStorageServiceName(), v.serviceName, podName),
		Type:          dbCfg.Type,
		Port:          3306,
		User:          "root",
		Password:      "verify_passw0rd",
		Version:       dbCfg.Version,
		LogNameFormat: dbCfg.LogNameFormat,
		Databases:     dbCfg.Databases,
		VerifyTables:  dbCfg.VerifyTables,
	}

	_, err = v.k8sDb.CreateDatabaseDeployment(verifyDbcfg.Host, verifyDbcfg)
	_, err = v.k8sDb.CreateDatabaseService(verifyDbcfg.Host, verifyDbcfg)
	defer func() {
		if err = v.k8sDb.ScaleDatabaseResources(verifyDbcfg.Host, 0); err != nil {
			v.logger.Error(fmt.Errorf("backup verify error: error deleting mariadb resources: %s", err.Error()))
		}
	}()
	if err != nil {
		v.logger.Error(fmt.Errorf("backup verify error: error creating mariadb resources: %s", err.Error()))
		v.status.SetVerifyRestore(0, fmt.Errorf("error creating mariadb: %s", err.Error()))
		return
	}

	db, err := database.NewDatabase(config.Config{Database: verifyDbcfg, SideCar: &[]bool{false}[0]}, nil, nil)
	if err != nil {
		v.status.SetVerifyRestore(0, fmt.Errorf("error restoring backup: %s", err.Error()))
		return
	}

	if err = db.VerifyRestore(restoreFolder); err != nil {
		v.status.SetVerifyRestore(0, fmt.Errorf("error restoring backup: %s", err.Error()))
		return
	}
	v.status.SetVerifyRestore(1, nil)
	if out, err := v.db.GetDatabaseDiff(dbCfg, verifyDbcfg); err != nil {
		//This is very bad. 1 or more tables are different or missing
		v.status.SetVerifyDiff(0, fmt.Errorf("error mysqldiff: %s", string(out)))
		return
	}
	v.status.SetVerifyDiff(1, nil)
	if len(dbCfg.VerifyTables) > 0 {
		if err = v.verifyChecksums(verifyDbcfg, restoreFolder); err != nil {
			v.status.SetVerifyChecksum(0, fmt.Errorf("error table checksum: %s", err.Error()))
		} else {
			v.status.SetVerifyChecksum(1, nil)
		}
	}
	v.logger.Info("successfully verified backup")
}

func (v *Verification) verifyChecksums(dbcfg config.DatabaseConfig, restorePath string) (err error) {
	cfg := config.Config{Database: dbcfg, SideCar: &[]bool{false}[0]}
	db, err := database.NewDatabase(cfg, nil, nil)
	if err != nil {
		return
	}

	csOrigin, err := v.loadChecksums(restorePath)
	if err != nil {
		return err
	}
	v.logger.Debugf("successfully loaded checksum %+v", csOrigin)

	if len(csOrigin.TablesChecksum) == 0 {
		return errors.New("no checksums found")
	}

	csBackup, err := db.GetCheckSumForTable(db.GetConfig().VerifyTables, false)
	if err != nil {
		return err
	}
	if err = compareChecksums(csOrigin.TablesChecksum, csBackup.TablesChecksum); err != nil {
		return err
	}
	v.logger.Infof("successfully verified checksum")
	return
}

func (v *Verification) createTableChecksum(backupTime string) (err error) {
	cs, err := v.db.GetCheckSumForTable(v.db.GetConfig().VerifyTables, false)
	if err != nil {
		return
	}
	out, err := yaml.Marshal(cs)
	if err != nil {
		return
	}

	if err = v.storage.WriteStream(backupTime+"/tablesChecksum.yaml", "", bytes.NewReader(out), nil, false); err != nil {
		logger.Error(fmt.Errorf("cannot upload table checksums: %s", err.Error()))
		return
	}
	return
}

func (v *Verification) loadChecksums(restorePath string) (cs database.Checksum, err error) {
	files, err := os.ReadDir(restorePath)
	if err != nil {
		return
	}
	for _, f := range files {
		if strings.Contains(f.Name(), "tablesChecksum") {
			by, err := os.ReadFile(path.Join(restorePath, f.Name()))
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
			return fmt.Errorf("backup verify table checksum mismatch for table %s", k)
		}
	}
	return nil
}
