package storage

import (
	"io"
	"time"
)

type Storage interface {
	WriteBytes(f string, b []byte) (err error)
	WriteFile(path string) (err error)
	WriteFolder(p string) (err error)
	WriteStream(name, mimeType string, body io.Reader) (err error)
	WriteStreamConcurrent(name, mimeType string, body io.Reader, errc chan<- *error)
	GetBackupByTimestamp(t time.Time) (path string, err error)
	GetLatestBackup() (path string, err error)
}
