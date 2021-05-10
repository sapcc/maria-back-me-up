package storage

import (
	"fmt"
	"io"
	"time"
)

// Storage interface
type Storage interface {
	// WriteFolder writes a folder to the storage
	WriteFolder(p string) (err error)
	// WriteStream writes a byte stream to the storage
	WriteStream(name, mimeType string, body io.Reader, tags map[string]string, dlo bool) (err error)
	// DownloadLatestBackup downloads the latest available backup from a storage
	DownloadLatestBackup() (path string, err error)
	// GetFullBackups lists all available full backups per storage
	GetFullBackups() (bl []Backup, err error)
	// GetIncBackupsFromDump lists all available incremental backups that belong to a full backup from a storage
	GetIncBackupsFromDump(key string) (bl []Backup, err error)
	// DownloadBackupWithLogPosition downloads a specific backup until given binlog position from a storage
	DownloadBackupWithLogPosition(fullBackupPath string, binlog string) (path string, err error)
	// DownloadBackup downloads a specific backup from a storage
	DownloadBackup(b Backup) (path string, err error)
	// GetStorageServiceName lists all available storages from a service
	GetStorageServiceName() (name string)
	// GetStatusError gets the backup->error map
	GetStatusError() map[string]string
	// GetStatusErrorByKey returns error per backup
	GetStatusErrorByKey(backupKey string) string
}

// LastSuccessfulBackup name of meta file to store last successful backup
const LastSuccessfulBackupFile = "last_successful_backup"

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
