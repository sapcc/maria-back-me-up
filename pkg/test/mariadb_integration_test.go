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
	"os"
	"path"
	"path/filepath"
	"testing"

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
