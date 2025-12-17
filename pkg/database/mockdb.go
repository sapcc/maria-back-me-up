// SPDX-FileCopyrightText: 2021 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package database

import (
	"context"
	"fmt"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	dberror "github.com/sapcc/maria-back-me-up/pkg/error"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

type (
	// MockDB struct for testing
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

// NewMockDB create mockdb instance
func NewMockDB(c config.Config, sm *storage.Manager) (*MockDB, error) {
	return &MockDB{
		cfg:         c,
		storage:     sm,
		healthError: false,
		incError:    false,
		dbError:     false,
	}, nil
}

// CreateFullBackup implements interface
func (m *MockDB) CreateFullBackup(path string) (bp LogPosition, err error) {
	bp.Name = "test"
	bp.Pos = 3000
	if m.dbError {
		err = &dberror.DatabaseMissingError{}
		return bp, err
	}
	return bp, m.storage.WriteFolderAll(path)
}

// GetLogPosition implements interface
func (m *MockDB) GetLogPosition() LogPosition {
	return m.logPosition
}

// GetConfig implements interface
func (m *MockDB) GetConfig() config.DatabaseConfig {
	return m.cfg.Database
}

// Restore implements interface
func (m *MockDB) Restore(path string) (err error) {
	return
}

// VerifyRestore implements interface
func (m *MockDB) VerifyRestore(path string) (err error) {
	return
}

// GetCheckSumForTable implements interface
func (m *MockDB) GetCheckSumForTable(verifyTables []string, withIP bool) (cs Checksum, err error) {
	cs.TablesChecksum = make(map[string]int64)
	return
}

// HealthCheck implements interface
func (m *MockDB) HealthCheck() (status Status, err error) {
	if m.healthError {
		status.Ok = false
		return status, fmt.Errorf("db not ok")
	}
	status.Ok = true
	return
}

// StartIncBackup implements interface
func (m *MockDB) StartIncBackup(ctx context.Context, mp LogPosition, dir string, ch chan error) (err error) {
	if m.incError {
		ch <- fmt.Errorf("inc backup error")
		return
	}
	binlogChan := make(chan storage.StreamEvent, 1)
	err = m.storage.WriteStreamAll("path", "", binlogChan, false)
	ch <- err
	return
}

// FlushIncBackup implements interface
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

/*
func (m *MockDB) checkBackupDirExistsAndCreate() (p string, err error) {
	if _, err := os.Stat(m.cfg.Backup.BackupDir); os.IsNotExist(err) {
		err = os.MkdirAll(m.cfg.Backup.BackupDir, os.ModePerm)
		return m.cfg.Backup.BackupDir, err
	}
	return
}
*/

// Up implements interface
func (m *MockDB) Up(timeout time.Duration, withIP bool) (err error) {
	return
}

/*
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
*/

// GetDatabaseDiff implements interface
func (m *MockDB) GetDatabaseDiff(c1, c2 config.DatabaseConfig) (out []byte, err error) {
	return
}

// WithError implements interface
func (m *MockDB) WithError(health bool, inc bool, dbError bool) {
	m.healthError = health
	m.incError = inc
	m.dbError = dbError
}
