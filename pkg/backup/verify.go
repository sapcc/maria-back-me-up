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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"gopkg.in/yaml.v2"
)

func (m *Manager) verifyBackup(lastBackupTime string) {
	var err error
	var backupFolder string
	logger.Info("Start verifying backup")

	defer func() {
		os.RemoveAll(backupFolder)
		m.verifyTimer = nil
		m.uploadVerfiyStatus(backupFolder)
	}()
	backupFolder, err = m.Storage.DownloadLatestBackup()
	if err != nil {
		var e *storage.NoBackupError
		if errors.As(err, &e) {
			logger.Info(e.Error())
			return
		}
		m.onVerifyError(fmt.Errorf("error loading backup for verifying: %s", err.Error()))
		return
	}

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
