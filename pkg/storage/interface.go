package storage

import (
	"fmt"
	"io"
	"time"
)

type Storage interface {
	//WriteBytes(s3Name, f string, b []byte) (err error)
	WriteFolder(p string) (err error)
	WriteStream(name, mimeType string, body io.Reader, tags map[string]string) (err error)
	GetBackupByTimestamp(t time.Time) (path string, err error)
	DownloadLatestBackup() (path string, err error)
	ListFullBackups() (bl []Backup, err error)
	ListServices() (services []string, err error)
	ListIncBackupsFor(key string) (bl []Backup, err error)
	DownloadBackupFrom(fullBackupPath string, binlog string) (path string, err error)
	DownloadBackup(fullBackup Backup) (path string, err error)
	GetStorageServiceName() (name string)
	GetStatusError() map[string]string
	GetStatusErrorByKey(backupKey string) string
}

type Backup struct {
	Storage string
	Time    time.Time
	Key     string
	IncList []IncBackup
	Verify  []Verify
}
type IncBackup struct {
	Key          string
	LastModified time.Time
}

type NoBackupError struct {
	message string
}

func (d *NoBackupError) Error() string {
	return "No backup found for this service"
}

type StorageError struct {
	message string
	Storage string
}

func (s *StorageError) Error() string {
	return fmt.Sprintf("Storage %s error: %s", s.Storage, s.message)
}
