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
	GetAllBackups() (bl []Backup, err error)
	DownloadBackupFrom(fullBackupPath string, binlog string) (path string, err error)
}
