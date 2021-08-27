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
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
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

type testCase struct {
	WithDisk   bool
	WithStream bool
	Expected   int
}

func TestBackupRestore(t *testing.T) {
	var tests = []testCase{
		{true, false, 5},
		{false, true, 5},
		{true, true, 5},
	}

	for _, test := range tests {
		testBackupRestore(t, test)
	}
}

func testBackupRestore(t *testing.T, test testCase) {
	m, cfg := Setup(t, &SetupOptions{
		DBType:          constants.MARIADB,
		DumpTool:        config.Mysqldump,
		WithDiskStorage: test.WithDisk,
		StreamStorage:   &StreamStorageOptions{Enabled: test.WithStream},
	})

	// Perform Backup
	err := m.Start()
	if err != nil {
		t.Errorf("could not start backup: %s", err.Error())
		t.FailNow()
	}

	// Create DB client
	conn, err := createConnection(cfg.Database.User, cfg.Database.Password, cfg.Database.Host, "service", cfg.Database.Port)
	if err != nil {
		t.Errorf("could not connect to database: %s", err.Error())
		t.FailNow()
	}
	defer conn.Close()

	_, err = conn.Execute("INSERT INTO service.tasks (title, start_date, due_date, description) VALUES('task5', '2021-05-02', '2022-05-02', 'task info 5');")
	if err != nil {
		t.Errorf("failed to write to db: %s", err.Error())
	}
	time.Sleep(time.Second * 15)
	m.Stop()

	if test.WithDisk {
		serviceName := cfg.Storages.Disk[0].Name
		// Restore full backup
		backups, err := m.Storage.GetFullBackups(serviceName)
		if err != nil {
			t.Errorf("failed to get backups from disk storage, error: %s", err.Error())
			t.FailNow()
		}

		path, err := m.Storage.DownloadBackup(serviceName, backups[0])
		if err != nil {
			t.Errorf("failed to get backup from disk, error: %s", err.Error())
			t.FailNow()
		}
		if _, err = os.Stat(filepath.Join(path, "tablesChecksum.yaml")); os.IsNotExist(err) {
			if cfg.Database.VerifyTables != nil {
				t.Errorf("checksum for tables was not created")
			}
		}

		err = m.Db.Restore(path)
		if err != nil {
			t.Errorf("failed to restore the database, error: %s", err.Error())
			t.FailNow()
		}

		// Check restore success
		assertTableConsistent(t, cfg.Database.User, cfg.Database.Password, cfg.Database.Host, "service", cfg.Database.Port, 5, "tasks")
	}

	if test.WithStream {
		// Check streaming target db
		assertTableConsistent(t, "root", "streaming", "127.0.0.1", "service", 3307, 5, "tasks")
	}

	Cleanup(t)
}

func createConnection(user, password, host, db string, port int) (conn *client.Conn, err error) {
	conn, err = client.Connect(fmt.Sprintf("%s:%v", host, port), user, password, db)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func assertTableConsistent(t *testing.T, user, password, host, db string, port int, exp int64, table string) {
	conn, err := createConnection(user, password, host, db, port)
	if err != nil {
		t.Errorf("could not connect to database, error: %s", err.Error())
		t.FailNow()
	}
	defer conn.Close()
	// Query from DB to see if all test entries are present
	result, err := conn.Execute(fmt.Sprintf("select count(*) from %s.%s;", db, table))
	act := result.Resultset.Values[0][0].AsInt64()

	if err != nil || exp != act {
		t.Errorf("expected 5 entries, but got: %v", act)
	}
}
