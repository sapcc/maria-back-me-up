package storage

import (
	"fmt"
	"io"
	"time"
)

// Storage interface
type Storage interface {
	WriteFolder(p string) (err error)
	WriteStream(name, mimeType string, body io.Reader, tags map[string]string, dlo bool) (err error)
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

// Verify storage struct
type Verify struct {
	VerifyRestore  int    `yaml:"verify_backup"`
	VerifyChecksum int    `yaml:"verify_checksum"`
	VerifyDiff     int    `yaml:"verify_diff"`
	VerifyError    string `yaml:"verify_error"`
	Time           time.Time
}

// Backup storage struct
type Backup struct {
	Storage       string
	Time          time.Time
	Key           string
	IncList       []IncBackup
	VerifySuccess *Verify
	VerifyFail    *Verify
}

// IncBackup storage struct
type IncBackup struct {
	Key          string
	LastModified time.Time
}

// NoBackupError error when no backups are stored
type NoBackupError struct {
	message string
}

func (d *NoBackupError) Error() string {
	return "No backup found for this service"
}

// Error with storage name and error message
type Error struct {
	message string
	Storage string
}

func (s *Error) Error() string {
	return fmt.Sprintf("Storage %s error: %s", s.Storage, s.message)
}
