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

package backup

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/database"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

const backupDir = "./testDir"

func TestManagerCron(t *testing.T) {
	m, _, _ := setup(t)

	m.Start()
	c := m.cronBackup.Entries()
	if len(c) != 1 {
		t.Errorf("expected 1 cron entry, but got: %d.", len(c))
	}

	ctx := m.Stop()
	if ctx.Err() != context.Canceled {
		t.Errorf("expected cron ctx to be canceled but got: %s.", ctx.Err())
	}
	if m.cronBackup != nil {
		t.Errorf("expected cronbackup to be nil but got: %+v.", m.cronBackup)
	}
	cleanup()
}

func TestManagerBackup(t *testing.T) {
	m, _, _ := setup(t)

	if err := m.startBackup(context.TODO()); err != nil {
		t.Errorf("expected startBackup() to return nil, but got error: %s.", err.Error())
	}

	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		t.Errorf("expected backupdir %s to exist, but got err: %s.", backupDir, err.Error())
	}
	c := m.cronBackup.Entries()
	if len(c) != 1 {
		t.Errorf("expected 1 cron entry, but got: %d.", len(c))
	}
	time.Sleep(1 * time.Second)
	if m.updateSts.FullBackup["s1"] != 1 {
		t.Errorf("expected fullBackup status to be 1, but got: %d.", m.updateSts.FullBackup["s1"])
	}
	if m.updateSts.FullBackup["s2"] != 1 {
		t.Errorf("expected fullBackup status to be 1, but got: %d.", m.updateSts.FullBackup["s2"])
	}
	if m.updateSts.IncBackup["s1"] != 1 {
		t.Errorf("expected incBackup status to be 1, but got: %d.", m.updateSts.IncBackup["s1"])
	}
	if m.updateSts.IncBackup["s2"] != 1 {
		t.Errorf("expected incBackup status to be 1, but got: %d.", m.updateSts.IncBackup["s2"])
	}
	cleanup()
}

func TestManagerFullbackErrorHandling(t *testing.T) {
	m, db, _ := setup(t)
	db.WithError(true, false, false)

	if err := m.Start(); err != nil {
		t.Errorf("expected startBackup() to return nil, but got error: %s.", err.Error())
	}

	time.Sleep(1 * time.Second)
	if m.updateSts.FullBackup["s1"] != 0 {
		t.Errorf("expected fullBackup status to be 0, but got: %d.", m.updateSts.FullBackup["s1"])
	}
	if m.updateSts.FullBackup["s2"] != 0 {
		t.Errorf("expected fullBackup status to be 0, but got: %d.", m.updateSts.FullBackup["s2"])
	}

	db.WithError(false, false, true)
	m.Stop()
	if err := m.Start(); err != nil {
		t.Errorf("expected startBackup() to return nil, but got error: %s.", err.Error())
	}
	time.Sleep(1 * time.Second)
	if m.updateSts.FullBackup["s1"] != 0 {
		t.Errorf("expected fullBackup status to be 0, but got: %d.", m.updateSts.FullBackup["s1"])
	}
	if m.updateSts.FullBackup["s2"] != 0 {
		t.Errorf("expected fullBackup status to be 0, but got: %d.", m.updateSts.FullBackup["s2"])
	}

	cleanup()
}

func TestManagerIncbackErrorHandling(t *testing.T) {
	m, db, _ := setup(t)
	db.WithError(false, true, false)

	if err := m.startBackup(context.TODO()); err != nil {
		t.Errorf("expected startBackup() to return nil, but got error: %s.", err.Error())
	}

	time.Sleep(1 * time.Second)
	if m.updateSts.IncBackup["s1"] != 0 {
		t.Errorf("expected incBackup status to be 0, but got: %d.", m.updateSts.IncBackup["s1"])
	}
	if m.updateSts.IncBackup["s2"] != 0 {
		t.Errorf("expected incBackup status to be 0, but got: %d.", m.updateSts.IncBackup["s2"])
	}
	prometheus.Unregister(NewMetricsCollector(&UpdateStatus{}))
	os.Remove(backupDir)
}

func TestManagerStorageErrorHandling(t *testing.T) {
	m, _, cfg := setup(t)
	s1 := storage.NewMockStorage(cfg, "s1", "log")
	s1.WithError(true)
	s2 := storage.NewMockStorage(cfg, "s2", "log")
	m.Storage.AddStorage(s1)
	m.Storage.AddStorage(s2)

	if err := m.startBackup(context.TODO()); err != nil {
		t.Errorf("expected startBackup() to return nil, but got error: %s.", err.Error())
	}

	time.Sleep(1 * time.Second)
	if m.updateSts.FullBackup["s1"] != 0 {
		t.Errorf("expected fullBackup status to be 0, but got: %d.", m.updateSts.FullBackup["s1"])
	}
	if m.updateSts.FullBackup["s2"] != 1 {
		t.Errorf("expected fullBackup status to be 1, but got: %d.", m.updateSts.FullBackup["s2"])
	}
	time.Sleep(1 * time.Second)
	if m.updateSts.IncBackup["s1"] != 0 {
		t.Errorf("expected incBackup status to be 0, but got: %d.", m.updateSts.IncBackup["s1"])
	}
	if m.updateSts.IncBackup["s2"] != 1 {
		t.Errorf("expected incBackup status to be 1, but got: %d.", m.updateSts.IncBackup["s2"])
	}
	cleanup()
}

func TestManagerStreamingNotSupportedError(t *testing.T) {
	cfg := config.Config{
		Namespace: "test",
		Backup: config.BackupService{
			BackupDir:              backupDir,
			FullBackupCronSchedule: "*/20 * * * *",
		},
		Database: config.DatabaseConfig{
			Type: "mock",
		},
		Storages: config.StorageService{
			MariaDB: []config.MariaDBStream{
				{Name: "mariadb"},
			},
		},
	}
	s1 := storage.NewMockStorage(cfg, "s1", "log")
	sm, err := storage.NewManager(cfg.Storages, "test", "")
	sm.AddStorage(s1)

	db, err := database.NewMockDB(cfg, sm)
	if err != nil {
		return
	}

	_, err = NewManager(sm, db, nil, cfg)
	if !strings.Contains(err.Error(), "streaming backups requires a MariaDB") {
		t.Errorf("expected error streaming requires mariadb")
	}

}

func TestCheckSupportsBinlogStreaming(t *testing.T) {

	cfg := config.Config{
		Namespace: "test",
		Backup: config.BackupService{
			BackupDir:              backupDir,
			FullBackupCronSchedule: "*/20 * * * *",
		},
		Database: config.DatabaseConfig{
			Type: "mock",
		},
		Storages: config.StorageService{
			MariaDB: []config.MariaDBStream{
				{Name: "stream"},
			},
		},
	}

	db, err := database.NewMockDB(cfg, nil)
	if err != nil {
		return
	}

	testCases := []struct {
		Format            string
		AnnotateRowEvents bool
		ExpectError       bool
	}{
		{"STATEMENT", true, false},
		{"STATEMENT", false, false},
		{"ROWS", false, true},
		{"ROWS", true, false},
		{"MIXED", false, true},
		{"MIXED", true, false},
	}

	for _, test := range testCases {
		db.SetBinlogFlags(test.Format, test.AnnotateRowEvents)
		err = checkSupportsBinlogStreaming(db)
		if err != nil && !test.ExpectError {
			t.Errorf("unexpected error: %s", err.Error())
		}
		if err == nil && test.ExpectError {
			t.Errorf("expected streaming not supported error for binlog_format '%s' and binlog_annotate_row_events '%t'", test.Format, test.AnnotateRowEvents)
		}
	}
}

func setup(t *testing.T) (m *Manager, db *database.MockDB, cfg config.Config) {
	cfg = config.Config{
		Namespace: "test",
		Backup: config.BackupService{
			BackupDir:              backupDir,
			FullBackupCronSchedule: "*/20 * * * *",
		},
		Database: config.DatabaseConfig{
			Type: "mock",
		},
	}
	s1 := storage.NewMockStorage(cfg, "s1", "log")
	s2 := storage.NewMockStorage(cfg, "s2", "log")
	sm, err := storage.NewManager(cfg.Storages, "test", "")
	if err != nil {
		t.Errorf("expected storage instance, but got error: %s.", err.Error())
	}
	sm.AddStorage(s1)
	sm.AddStorage(s2)

	db, err = database.NewMockDB(cfg, sm)
	if err != nil {
		return
	}
	m, err = NewManager(sm, db, nil, cfg)
	if err != nil {
		t.Errorf("expected manager instance, but got error: %s.", err.Error())
	}
	return
}

func cleanup() {
	prometheus.Unregister(NewMetricsCollector(&UpdateStatus{}))
	os.Remove(backupDir)
}
