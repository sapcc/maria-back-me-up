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

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

func TestWriteFolder(t *testing.T) {
	disk := createTestDiskWithMockData(t, 2, 0, 7)

	folder := filepath.Join(disk.cfg.BasePath, "testdb", "backup1")

	err := os.MkdirAll(folder, os.ModePerm)
	if err != nil {
		t.Errorf("could not create backup folder: %s", err.Error())
	}

	err = os.WriteFile(filepath.Join(folder, "dump.sql"), []byte{}, os.ModePerm)
	if err != nil {
		t.Errorf("could not create backup file: %s", err.Error())
	}

	err = disk.WriteFolder(folder)
	if err != nil {
		t.Errorf("failed to write folder: %s", err.Error())
	}

	backupFile := filepath.Join(folder, "dump.tar")
	if _, err := os.Stat(backupFile); os.IsNotExist(err) {
		t.Errorf("expected folder %s does not exist", backupFile)
	}

}

func TestWriteStream(t *testing.T) {
	testDir := t.TempDir()
	testFile := "test.sql"

	disk := createTestDisk(t, testDir, 7)

	reader := strings.NewReader("hello world")
	err := disk.WriteStream(testFile, "", reader, nil, false)
	if err != nil {
		t.Errorf("failed to write stream: %s", err.Error())
	}

	act, err := os.ReadFile(filepath.Join(testDir, "testdb", testFile))
	if err != nil {
		t.Errorf("failed to read actual file: %s", err.Error())
	}
	if !strings.EqualFold(string(act), "hello world") {
		t.Errorf("expected `hello world`; actual `%s`", string(act))
	}

}

func TestWriteLastSuccessWithTags(t *testing.T) {

	testFile := "last_successful_backup"
	disk := createTestDiskWithMockData(t, 3, 0, 7)

	reader := strings.NewReader("latest backup")
	tags := map[string]string{
		"key":    "backup1",
		"binlog": "mysqld-bin.42700",
	}

	err := disk.WriteStream(testFile, "", reader, tags, false)
	if err != nil {
		t.Errorf("failed to write stream: %s", err.Error())
	}
	act, err := disk.DownloadLatestBackup()
	if err != nil {
		t.Errorf("failed to read actual file: %s", err.Error())
	}
	exp := filepath.Join(disk.cfg.BasePath, "backup1")
	if !strings.EqualFold(string(act), exp) {
		t.Errorf("expected `%s`; actual `%s`", exp, string(act))
	}

}

func TestWriteLastSuccessWithoutTags(t *testing.T) {
	testFile := "last_successful_backup"
	disk := createTestDiskWithMockData(t, 3, 0, 7)

	reader := strings.NewReader("latest backup")

	err := disk.WriteStream(testFile, "", reader, nil, false)
	if err != nil {
		if !strings.Contains(err.Error(), "expected tags were not supplied for file") {
			t.Errorf("failed to write stream: %s", err.Error())
		}
	} else {
		t.Errorf("expected an error")
	}
}

func TestListFullBackups(t *testing.T) {
	numBackups := 5

	disk := createTestDiskWithMockData(t, numBackups, 0, 7)

	backups, err := disk.ListFullBackups()
	if err != nil {
		t.Errorf("failed to list backups: %s", err.Error())
	}

	if len(backups) != numBackups {
		t.Errorf("expected %v backups, actual %v backups", numBackups, len(backups))
	}
}

func TestListFullBackupsOneMissing(t *testing.T) {
	numBackups := 5
	disk := createTestDiskWithMockData(t, numBackups, 1, 7)

	backups, err := disk.ListFullBackups()
	if err != nil {
		t.Errorf("failed to list backups: %s", err.Error())
	}

	if len(backups) != numBackups {
		t.Errorf("expected %v backups, actual %v backups", numBackups, len(backups))
	}
}

func TestListServices(t *testing.T) {
	testDir := t.TempDir()

	expServices := []string{"service1", "service2", "service3"}

	for _, s := range expServices {
		err := os.Mkdir(filepath.Join(testDir, s), os.ModePerm)
		if err != nil {
			t.Error("failed to create testfolders")
			t.FailNow()
		}
	}

	disk := createTestDisk(t, testDir, 7)

	actServices, err := disk.ListServices()

	if err != nil {
		t.Error("failed to list services")
	}

	if len(actServices) != len(expServices) {
		t.Errorf("expected # services: %v, actual # services: %v", len(expServices), len(actServices))
		t.FailNow()
	}
	for i := range expServices {
		if expServices[i] != actServices[i] {
			t.Errorf("expected service '%s' is missing", expServices[i])
			t.FailNow()
		}
	}
}

func TestListIncBackupsForSuccess(t *testing.T) {
	disk := createTestDiskWithMockData(t, 2, 0, 7)

	actBackups, err := disk.ListIncBackupsFor("testdb/backup_1")
	if err != nil {
		t.Errorf("failed retrieve incremental backups: %s", err.Error())
	}
	actIncBackups := actBackups[0].IncList
	if len(actIncBackups) != 5 { // By default the mock creates 5 incremental backup files
		t.Errorf("expected backups: 5, actual backups: %v", len(actIncBackups))
	}
}

func TestDownloadBackupFrom(t *testing.T) {
	disk := createTestDiskWithMockData(t, 2, 0, 7)

	actPath, err := disk.DownloadBackupFrom("testdb/backup_1", "mysql-bin.00001")
	if err != nil || actPath == "" {
		t.Error("failed to find binlog file")
	}

	if strings.EqualFold(actPath, filepath.Join(disk.cfg.BasePath, "testdb", "backup_1", "backup_1")) {
		t.Errorf("unexpected actual path: '%s'", actPath)
	}
}

func TestBackupRetention(t *testing.T) {
	disk := createTestDiskWithMockData(t, 5, 0, 5)

	beforeBackups, _ := disk.ListFullBackups()

	if len(beforeBackups) < 5 {
		t.Errorf("expected 5 mock backups, actual are %v", len(beforeBackups))
	}

	folder := filepath.Join(disk.cfg.BasePath, "testdb", "backup_6")

	err := os.MkdirAll(folder, os.ModePerm)
	if err != nil {
		t.Errorf("could not create backup folder: %s", err.Error())
	}

	err = os.WriteFile(filepath.Join(folder, "dump.sql"), []byte{}, os.ModePerm)
	if err != nil {
		t.Errorf("could not create backup file: %s", err.Error())
	}

	disk.WriteFolder(folder)

	afterBackups, _ := disk.ListFullBackups()

	if len(afterBackups) > disk.cfg.Retention {
		t.Errorf("expected maximum of %v backups, actual %v", disk.cfg.Retention, len(afterBackups))
	}

	sort.Sort(ByTime(afterBackups))
	if afterBackups[0].Key != "testdb/backup_2" || afterBackups[len(afterBackups)-1].Key != "testdb/backup_6" {
		t.Error("expected backups are not present")
	}

}

// creates folder structure with 'numBackups' that contain a dump.tar and 5 binlog files and additional 'numEmptyBackups' emp
func createTestDiskWithMockData(t *testing.T, numBackups int, numEmptyBackups int, retention int) (disk *Disk) {
	testDir := t.TempDir()

	err := createDummyBackups(filepath.Join(testDir, "testdb"), numBackups, numEmptyBackups)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	return createTestDisk(t, testDir, retention)
}

// createTestDisk creates an disk object with testDir as BasePath and a Retention of 5
func createTestDisk(t *testing.T, testDir string, retention int) (disk *Disk) {
	if testDir == "" {
		testDir = t.TempDir()
	}
	cfg := config.Disk{
		BasePath:  testDir,
		Retention: retention,
	}
	disk, err := NewDisk(cfg, "testdb", "mysql-bin")
	if err != nil {
		t.Error("failed to create test disk storage")
		t.FailNow()
	}
	return disk
}

// createDummyBackups creates `numberBackup` folders under `path` that each contain a "dump.tar" and 5 incremental backup files.
// Creates `numberEmptyBackup` folders which contain no files
func createDummyBackups(path string, numberBackups int, numberEmptyBackups int) error {
	for i := 1; i <= numberBackups; i++ {
		dir := filepath.Join(path, fmt.Sprintf("backup_%v", i))
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not create backup folder: %s", err.Error())
		}
		err = os.WriteFile(filepath.Join(dir, "dump.tar"), []byte{}, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not create backup file: %s", err.Error())
		}
		for i := 0; i < 5; i++ {
			err = os.WriteFile(filepath.Join(dir, fmt.Sprintf("mysql-bin.0000%v", i)), []byte{}, os.ModePerm)
			if err != nil {
				return fmt.Errorf("could not create binlog file: %s", err.Error())
			}
		}
	}
	for i := 1; i <= numberEmptyBackups; i++ {
		dir := filepath.Join(path, fmt.Sprintf("backup_%v", i))
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not create backup folder: %s", err.Error())
		}
	}
	return nil
}
