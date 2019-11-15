package storage

import (
	"io"
	"time"
)

type Storage interface {
	WriteBytes(f string, b []byte) (err error)
	WriteFolder(p string) (err error)
	WriteStream(name, mimeType string, body io.Reader) (err error)
	GetBackupByTimestamp(t time.Time) (path string, err error)
	DownloadLatestBackup() (path string, err error)
	ListFullBackups() (bl []Backup, err error)
	ListIncBackupsFor(key string) (bl []Backup, err error)
	DownloadBackupFrom(fullBackupPath string, binlog string) (path string, err error)
}

type NoBackupError struct {
	message string
}

func (d *NoBackupError) Error() string {
	return "No backup found for this service"
}
