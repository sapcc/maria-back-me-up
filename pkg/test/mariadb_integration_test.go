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

package test

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/siddontang/go-mysql/client"
)

func testFullBackup(t *testing.T) {
	m, cfg := Setup(t, &SetupOptions{
		DBType:   constants.MARIADB,
		DumpTool: config.Mysqldump,
	})

	bpath := path.Join(cfg.Backup.BackupDir, "test01")
	_, err := m.Db.CreateFullBackup(bpath)
	if err != nil {
		t.Errorf("expected logpostion, but got error: %s", err.Error())
	}
	f, err := os.Open(filepath.Join(bpath, "dump.sql"))
	if err != nil {
		t.Errorf("expected dump.sql file, but got error: %s", err.Error())
	}
	fi, err := f.Stat()
	if err != nil {
		t.Errorf("expected dump.sql file stats, but got error: %s", err.Error())
	}
	if fi.Size() == 0 {
		t.Errorf("expected dump.sql file size > 0, but got: %d", fi.Size())
	}
	Cleanup(t)
}

func TestRestoreFullBackupDisk(t *testing.T) {

	m, cfg := Setup(t, &SetupOptions{
		DBType:          constants.MARIADB,
		DumpTool:        config.Mysqldump,
		WithDiskStorage: true,
	})

	// Perform Backup
	bpath := path.Join(cfg.Backup.BackupDir, "test01")
	_, err := m.Db.CreateFullBackup(bpath)
	if err != nil {
		t.Errorf("expected logpostion, but got error: %s", err.Error())
		t.FailNow()
	}

	err = m.Storage.WriteFolderAll(bpath)
	if err != nil {
		t.Errorf("could not write backup to disk storage")
	}

	// Restore full backup
	backups, err := m.Storage.GetFullBackups("disk")
	if err != nil {
		t.Errorf("failed to get backups from disk storage, error: %s", err.Error())
		t.FailNow()
	}

	path, err := m.Storage.DownloadBackup("disk", backups[0])
	if err != nil {
		t.Errorf("failed to get backup from disk, error: %s", err.Error())
		t.FailNow()
	}

	err = m.Db.Restore(path)
	if err != nil {
		t.Errorf("failed to restore the database, error: %s", err.Error())
		t.FailNow()
	}

	// Create DB client
	conn := createConnection(t, cfg)
	defer conn.Close()
	// Query from DB to see if all test entries are present
	result, err := conn.Execute("select count(*) from service.tasks;")

	if err != nil {
		t.Errorf("expected 4 entries, but got: %v", result.Resultset.Values[0][0].(int64))
	}
}

func createConnection(t *testing.T, cfg config.Config) *client.Conn {
	conn, err := client.Connect(fmt.Sprintf("%s:%v", cfg.Database.Host, cfg.Database.Port), cfg.Database.User, cfg.Database.Password, "service")
	if err != nil {
		t.Errorf("could not connect to database, error: %s", err.Error())
		t.FailNow()
	}
	return conn
}
