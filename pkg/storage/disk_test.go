package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

func TestWriteFolder(t *testing.T) {
	testDir := t.TempDir()
	folder := filepath.Join(testDir, "backup1")

	err := os.MkdirAll(folder, os.ModePerm)
	if err != nil {
		t.Errorf("could not create backup folder: %s", err.Error())
	}

	err = os.WriteFile(filepath.Join(folder, "dump.sql"), []byte{}, os.ModePerm)
	if err != nil {
		t.Errorf("could not create backup file: %s", err.Error())
	}

	cfg := config.Disk{
		BasePath: testDir,
	}
	disk, err := NewDisk(cfg, "testdb", "mysql-bin")

	if err != nil {
		t.Errorf("failed to create disk storage: %s", err.Error())
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

	cfg := config.Disk{
		BasePath: testDir,
	}
	disk, err := NewDisk(cfg, "testdb", "mysql-bin")
	if err != nil {
		t.Errorf("failed to create disk storage: %s", err.Error())
	}

	reader := strings.NewReader("hello world")
	err = disk.WriteStream(testFile, "", reader, nil, false)
	if err != nil {
		t.Errorf("failed to write stream: %s", err.Error())
	}

	act, err := os.ReadFile(filepath.Join(testDir, testFile))
	if err != nil {
		t.Errorf("failed to read actual file: %s", err.Error())
	}
	if !strings.EqualFold(string(act), "hello world") {
		t.Errorf("expected `hello world`; actual `%s`", string(act))
	}

}

func TestWriteStreamWithTags(t *testing.T) {
	testDir := t.TempDir()
	testFile := "last_successful_backup"

	err := createDummyBackups(testDir, 3, 0)
	if err != nil {
		t.Error(err.Error())
	}
	cfg := config.Disk{
		BasePath: testDir,
	}
	disk, err := NewDisk(cfg, "testdb", "mysql-bin")
	if err != nil {
		t.Errorf("failed to create disk storage: %s", err.Error())
	}

	reader := strings.NewReader("latest backup")
	tags := make(map[string]string)
	tags["key"] = "backup1"
	tags["binlog"] = "mysqld-bin.42700"
	err = disk.WriteStream(testFile, "", reader, tags, false)
	if err != nil {
		t.Errorf("failed to write stream: %s", err.Error())
	}
	act, err := disk.DownloadLatestBackup()
	if err != nil {
		t.Errorf("failed to read actual file: %s", err.Error())
	}
	exp := filepath.Join(testDir, "backup1")
	if !strings.EqualFold(string(act), exp) {
		t.Errorf("expected `%s`; actual `%s`", string(act), exp)
	}

}

func TestListFullBackups(t *testing.T) {
	testDir := t.TempDir()
	numBackups := 5

	err := createDummyBackups(testDir, numBackups, 0)
	if err != nil {
		t.Error(err.Error())
	}

	cfg := config.Disk{
		BasePath: testDir,
	}
	disk, err := NewDisk(cfg, "testdb", "mysql-bin")

	if err != nil {
		t.Errorf("failed to create disk storage: %s", err.Error())
	}

	backups, err := disk.ListFullBackups()
	if err != nil {
		t.Errorf("failed to list backups: %s", err.Error())
	}

	if len(backups) != numBackups {
		t.Errorf("expected %v backups, actual %v backups", numBackups, len(backups))
	}
}

func TestListFullBackupsOneMissing(t *testing.T) {
	// Setup backup dir with a list of backupdirs and files
	testDir := t.TempDir()
	numBackups := 5

	err := createDummyBackups(testDir, numBackups, 1)
	if err != nil {
		t.Error(err.Error())
	}

	// Test if all full backups are found
	cfg := config.Disk{
		BasePath: testDir,
	}
	disk, err := NewDisk(cfg, "testdb", "mysql-bin")

	if err != nil {
		t.Errorf("failed to create disk storage: %s", err.Error())
	}

	backups, err := disk.ListFullBackups()
	if err != nil {
		t.Errorf("failed to list backups: %s", err.Error())
	}

	if len(backups) != numBackups-1 {
		t.Errorf("expected %v backups, actual %v backups", numBackups-1, len(backups))
	}
}

// createDummyBackups creates `n` folders under `path` that each contain a "dump.tar" file except for `noFile` folders
func createDummyBackups(path string, n int, noFile int) error {
	for i := 1; i <= n; i++ {
		dir := filepath.Join(path, fmt.Sprintf("backup_%v", i))
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not create backup folder: %s", err.Error())
		}
		if i > noFile {
			err = os.WriteFile(filepath.Join(dir, "dump.tar"), []byte{}, os.ModePerm)
			if err != nil {
				return fmt.Errorf("could not create backup file: %s", err.Error())
			}
		}
	}
	return nil
}
