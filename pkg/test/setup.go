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

//Setup sets up a keppel.Configuration and database connection for a unit test.

package test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/database"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

const backupDir = "./backupDirTest"

const sourceSQLFile = "./testdata.sql"
const clearDBFile = "./testclean.sql"

//SetupOptions contains optional arguments for test.Setup().
type SetupOptions struct {
	DBType           string
	DiskStorageName  string
	WithSwiftStorage bool
	WithK8s          bool
	DumpTool         config.DumpTools
}

// Setup the manager and database for testing
func Setup(t *testing.T, opts *SetupOptions) (m *backup.Manager, cfg config.Config) {
	cfg = config.Config{
		Namespace: "test",
		SideCar:   func(b bool) *bool { return &b }(false),
		Backup: config.BackupService{
			BackupDir:              backupDir,
			FullBackupCronSchedule: "* * * * *",
		},
		Database: config.DatabaseConfig{
			Type:          opts.DBType,
			Host:          "127.0.0.1",
			Port:          3307,
			User:          "root",
			Password:      "test",
			LogNameFormat: "mysqld-bin",
			DumpTool:      opts.DumpTool,
			Databases:     []string{"service"},
			VerifyTables:  []string{"service.tasks"},
		},
	}
	if opts.DiskStorageName != "" {
		cfg.Storages.Disk = []config.Disk{{
			Name:      opts.DiskStorageName,
			BasePath:  filepath.Join(backupDir, opts.DiskStorageName),
			Retention: 1,
		}}
	}
	s, err := storage.NewManager(cfg.Storages, cfg.ServiceName, cfg.Database.LogNameFormat)
	if err != nil {
		return
	}

	db, err := database.NewDatabase(cfg, s, nil)
	if err != nil {
		t.Errorf("expected database instance, but got error: %s.", err.Error())
	}
	m, err = backup.NewManager(s, db, nil, cfg)
	if err != nil {
		t.Errorf("expected manager instance, but got error: %s.", err.Error())
	}
	if err = m.Db.Up(1*time.Minute, false); err != nil {
		t.Errorf("expected db to be up, but got error: %s.", err.Error())
	}

	// Prepare Source DB
	prepareDB(cfg.Database.Port, cfg.Database.Host, cfg.Database.User, cfg.Database.Password, sourceSQLFile)

	return
}

// Cleanup after a test
func Cleanup(t *testing.T) {
	prometheus.Unregister(backup.NewMetricsCollector(&backup.UpdateStatus{}))
	err := os.RemoveAll(backupDir)
	if err != nil {
		t.Errorf("failed to clean backupDir: %s", err.Error())
	}
}

// prepareDB executes a sql file on the db.
func prepareDB(port int, host, user, password, path string) error {

	dump, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("could not open dump file: %s", err.Error())
	}

	cmd := exec.Command(
		"mysql",
		"--port="+strconv.Itoa(port),
		"--host="+host,
		"--user="+user,
		"--password="+password,
	)
	cmd.Stdin = dump
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s error: %s", config.Mysqldump.String(), string(b))
	}
	return nil
}
