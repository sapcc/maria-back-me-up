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
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/log"
)

func TestFilterBackup(t *testing.T) {
	m, cfg := Setup(t, &SetupOptions{
		DBType:   constants.MARIADB,
		DumpTool: config.Mysqldump,
		StreamStorage: &StreamStorageOptions{
			Enabled:   true,
			Databases: []string{"service"},
			ParseSQL:  true,
		}})

	// Perform Backup
	err := m.Start()
	if err != nil {
		t.Errorf("could not start backup: %s", err.Error())
		t.FailNow()
	}
	for i := 0; i < 15; i++ {
		stats := m.GetHealthStatus()
		if stats.FullBackup["StreamingTest"] == 1 {
			break
		} else {
			log.Info("Backup not done - waiting 1 second")
			time.Sleep(1 * time.Second)
		}
	}

	// Create DB client
	conn, err := createConnection(cfg.Database.User, cfg.Database.Password, cfg.Database.Host, "", cfg.Database.Port)
	if err != nil {
		t.Errorf("could not connect to database: %s", err.Error())
		t.FailNow()
	}
	defer conn.Close()

	_, err = conn.Execute("INSERT INTO service.tasks (title, start_date, due_date, description) VALUES('task5', '2021-05-02', '2022-05-02', 'task info 5');")
	if err != nil {
		t.Errorf("failed to write to db: %s", err.Error())
	}
	_, err = conn.Execute("INSERT INTO application.ratings (title, release_date, description, rating) VALUES('app5', '2021-05-02', 'app info 5', 4);")
	if err != nil {
		t.Errorf("failed to write to db: %s", err.Error())
	}
	time.Sleep(time.Second * 1)
	m.Stop()

	// Check streaming target db
	assertTableConsistent(t, "root", "streaming", "127.0.0.1", "service", 3307, 5, "tasks")
	assertDatabaseNotExists(t, "root", "streaming", "127.0.0.1", "application", 3307)

	Cleanup(t)
}

func assertDatabaseNotExists(t *testing.T, user, password, host, db string, port int) {
	conn, err := createConnection(user, password, host, db, port)
	if err != nil {
		if mysqlErr, ok := errors.Cause(err).(*mysql.MyError); ok {
			if mysqlErr.Code == 1049 {
				return
			}
		}
		if strings.Contains(err.Error(), "1049") {
			return
		}
		t.Errorf("unexpected error: %s", err.Error())
		return
	}
	t.Errorf("expected an error")
	defer conn.Close()
}
