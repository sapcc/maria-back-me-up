/**
 * Copyright 2021 SAP SE
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

package database

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	dberror "github.com/sapcc/maria-back-me-up/pkg/error"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"k8s.io/apimachinery/pkg/util/wait"
)

type (
	MockDB struct {
		cfg         config.Config
		storage     *storage.Manager
		logPosition LogPosition
		flushTimer  *time.Timer
		healthError bool
		incError    bool
		dbError     bool
	}
)

func NewMockDB(c config.Config, sm *storage.Manager) (*MockDB, error) {
	return &MockDB{
		cfg:         c,
		storage:     sm,
		healthError: false,
		incError:    false,
		dbError:     false,
	}, nil
}

func (m *MockDB) CreateFullBackup(path string) (bp LogPosition, err error) {
	bp.Name = "test"
	bp.Pos = 3000
	if m.dbError {
		err = &dberror.DatabaseMissingError{}
		return bp, err
	}
	return bp, m.storage.WriteFolderAll(path)
}

func (m *MockDB) GetLogPosition() LogPosition {
	return m.logPosition
}

func (m *MockDB) GetConfig() config.DatabaseConfig {
	return m.cfg.Database
}

func (m *MockDB) Restore(path string) (err error) {
	return
}

func (m *MockDB) VerifyRestore(path string) (err error) {
	return
}

func (m *MockDB) GetCheckSumForTable(verifyTables []string, withIP bool) (cs Checksum, err error) {
	cs.TablesChecksum = make(map[string]int64)
	return
}

func (m *MockDB) HealthCheck() (status Status, err error) {
	if m.healthError {
		status.Ok = false
		return status, fmt.Errorf("db not ok")
	}
	status.Ok = true
	return
}

func (m *MockDB) StartIncBackup(ctx context.Context, mp LogPosition, dir string, ch chan error) (err error) {
	if m.incError {
		ch <- fmt.Errorf("inc backup error")
		return
	}
	binlogReader, _ := io.Pipe()
	err = m.storage.WriteStreamAll("path", "", binlogReader, false)
	ch <- err
	return
}

func (m *MockDB) FlushIncBackup() (err error) {
	if m.flushTimer != nil {
		if !m.flushTimer.Stop() {
			<-m.flushTimer.C
		}
		m.flushTimer.Reset(0)
	} else {
		return fmt.Errorf("no writes to database happened yet")
	}
	return
}

func (m *MockDB) checkBackupDirExistsAndCreate() (p string, err error) {
	if _, err := os.Stat(m.cfg.Backup.BackupDir); os.IsNotExist(err) {
		err = os.MkdirAll(m.cfg.Backup.BackupDir, os.ModePerm)
		return m.cfg.Backup.BackupDir, err
	}
	return
}

func (m *MockDB) Up(timeout time.Duration, withIP bool) (err error) {
	return
}

func (m *MockDB) deleteMariaDBDataDir() (err error) {
	cf := wait.ConditionFunc(func() (bool, error) {
		for _, d := range m.cfg.Database.Databases {
			log.Debug("deleting database: ", d)
			if err = os.RemoveAll(filepath.Join(m.cfg.Database.DataDir, d)); err != nil {
				return false, nil
			}
		}
		if err = os.RemoveAll(m.cfg.Database.DataDir); err != nil {
			return false, nil
		}
		return true, nil
	})
	if m.cfg.Database.DataDir != "" {
		return wait.Poll(1*time.Second, 30*time.Second, cf)
	}
	return
}

func (m *MockDB) GetDatabaseDiff(c1, c2 config.DatabaseConfig) (out []byte, err error) {
	return
}

func (m *MockDB) WithError(health bool, inc bool, dbError bool) {
	m.healthError = health
	m.incError = inc
	m.dbError = dbError
}
