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

func (m *Manager) verifyLatestBackup(withChecksum bool, resetTimer bool) {
	backupFolder, err := m.Storage.DownloadLatestBackup(0)
	if err != nil {
		var e *storage.NoBackupError
		if errors.As(err, &e) {
			logger.Info(e.Error())
			return
		}
		m.updateVerifyStatus(0, 0, fmt.Errorf("error loading backup for verifying: %s", err.Error()))
		return
	}
	go m.verifyBackup(withChecksum, backupFolder, resetTimer)
	return
}

func (m *Manager) verifyBackup(withChecksum bool, backupFolder string, resetTimer bool) {
	var err error
	logger.Info("Start verifying backup")

	defer func() {
		os.RemoveAll(backupFolder)
		if resetTimer {
			m.verifyTimer = nil
		}
		m.uploadVerfiyStatus(backupFolder)
	}()

	if withChecksum && len(m.cfg.MariaDB.VerifyTables) > 0 {
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
	svc, err := m.maria.CreateMariaService(cfg.MariaDB)
	defer func() {
		if err = m.maria.DeleteMariaResources(dp, svc); err != nil {
			logger.Error(fmt.Errorf("backup verify error: error deleting mariadb resources: %s", err.Error()))
		}
	}()
	if err != nil {
		m.updateVerifyStatus(0, 0, fmt.Errorf("error creating mariadb: %s", err.Error()))
		return
	}

	r := NewRestore(cfg)
	if err = r.verifyRestore(backupFolder); err != nil {
		m.updateVerifyStatus(0, 0, fmt.Errorf("error restoring backup: %s", err.Error()))
		return
	}
	if out, err := runMysqlDiff(m.cfg.MariaDB, cfg.MariaDB); err != nil {
		//This is very bad. 1 or more tables are different or missing
		m.updateVerifyStatus(0, 0, fmt.Errorf("error mysqldiff: %s", string(out)))
		return
	}
	m.updateVerifyStatus(1, 0, nil)
	if len(m.backupCheckSums) > 0 && len(m.cfg.MariaDB.VerifyTables) > 0 {
		if err = m.verifyChecksums(cfg); err != nil {
			m.updateVerifyStatus(1, 0, fmt.Errorf("error table checksum: %s", err.Error()))
		} else {
			m.updateVerifyStatus(1, 1, nil)
		}
	}
	logger.Info("successfully verified backup")
}

func (m *Manager) verifyChecksums(cfg config.Config) (err error) {
	rs, err := getCheckSumForTable(cfg.MariaDB)
	if err != nil {
		return fmt.Errorf("error verifying backup: %s", err.Error())
	}
	if err = compareChecksums(m.backupCheckSums, rs); err != nil {
		return fmt.Errorf("error verifying backup: %s", err.Error())
	}
	logger.Debug("Checksum successful ", rs, m.backupCheckSums)
	return
}

func (m *Manager) updateVerifyStatus(vb, vt int, err error) {
	m.updateSts.Lock()
	defer m.updateSts.Unlock()
	m.updateSts.VerifyTables = vt
	m.updateSts.VerifyBackup = vb
	if err != nil {
		m.updateSts.VerifyError = err.Error()
		logger.Error(fmt.Errorf("backup verify error: %s", err.Error()))
	} else {
		m.updateSts.VerifyError = ""
	}
}

func (m *Manager) uploadVerfiyStatus(backupFolder string) {
	m.updateSts.RLock()
	out, err := yaml.Marshal(m.updateSts)
	m.updateSts.RUnlock()
	if err != nil {
		logger.Error(fmt.Errorf("cannot marshal verify status: %s", err.Error()))
		return
	}
	u := strconv.FormatInt(time.Now().Unix(), 10)
	//remove restore and servicename dir from path
	vp := strings.Replace(backupFolder, filepath.Join(constants.RESTOREFOLDER, m.cfg.ServiceName), "", 1)
	logger.Debug("Uploading verify status to: ", vp+"/verify_"+u+".yaml")
	err = m.Storage.WriteStream(0, vp+"/verify_"+u+".yaml", "", bytes.NewReader(out))
	err = m.Storage.WriteStream(1, vp+"/verify_"+u+".yaml", "", bytes.NewReader(out))
	if err != nil {
		logger.Error(fmt.Errorf("cannot upload verify status: %s", err.Error()))
	}
}
