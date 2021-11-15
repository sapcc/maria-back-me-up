package storage

import (
	"fmt"
	"io"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
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
	// GetTotalIncBackupsFromDump returns current total incremental backups in this dump
	GetTotalIncBackupsFromDump(key string) (t int, err error)
}

// ChannelWriter for storages that do not support consuming io.Reader
type ChannelWriter interface {
	// WriteChannel writes the contents of the channel to the storage
	WriteChannel(name, mimeType string, body <-chan StreamEvent, tags map[string]string, dlo bool) (err error)
}

// LastSuccessfulBackupFile name of meta file that contains the information of the last successful backup
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

// ByTime implements the sort of a backup slice by time
type ByTime []Backup

func (b ByTime) Len() int           { return len(b) }
func (b ByTime) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByTime) Less(i, j int) bool { return b[i].Time.Before(b[j].Time) }

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

// StreamEvent used to send byte slices or binlog events to the storages
type StreamEvent interface {
	ToByte() []byte
}

// ByteEvent holds a slice of bytes
type ByteEvent struct {
	Value []byte
}

// ToByte returns the bytes of the event
func (b *ByteEvent) ToByte() []byte {
	return b.Value
}

// BinlogEvent holds a binlog event
type BinlogEvent struct {
	Value *replication.BinlogEvent
}

// ToByte returns the byte content of a binlog event
func (b *BinlogEvent) ToByte() []byte {
	return b.Value.RawData
}
