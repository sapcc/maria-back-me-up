package storage

import (
	"fmt"
	"io"
	"time"
)

type Storage interface {
	//WriteBytes(s3Name, f string, b []byte) (err error)
	WriteFolder(backup int, p string) (err error)
	WriteStream(backup int, name, mimeType string, body io.Reader) (err error)
	GetBackupByTimestamp(backup int, t time.Time) (path string, err error)
	DownloadLatestBackup(backup int) (path string, err error)
	ListFullBackups(backup int) (bl []Backup, err error)
	ListIncBackupsFor(backup int, key string) (bl []Backup, err error)
	DownloadBackupFrom(backup int, fullBackupPath string, binlog string) (path string, err error)
	GetRemoteStorageServices() (storages map[int]string)
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
