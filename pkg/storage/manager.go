package storage

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"reflect"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/errgroup"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sirupsen/logrus"
)

const backupIncomplete = "backup_incomplete"

// Manager which manages the different storage services
type Manager struct {
	cfg             config.StorageService
	storageServices map[string]Storage
}

func init() {
	logger = log.WithFields(logrus.Fields{"component": "storage"})
}

// NewManager creates a new manager instance
func NewManager(c config.StorageService, serviceName, binLog string) (m *Manager, err error) {
	stsvc := make(map[string]Storage)
	for _, cfg := range c.Swift {
		swift, err := NewSwift(cfg, serviceName, binLog)
		if err != nil {
			return m, err
		}
		stsvc[cfg.Name] = swift
	}
	for _, cfg := range c.S3 {
		s3, err := NewS3(cfg, serviceName, binLog)
		if err != nil {
			return m, err
		}
		stsvc[cfg.Name] = s3
	}

	for _, cfg := range c.Disk {

		disk, err := NewDisk(cfg, serviceName, binLog)
		if err != nil {
			return m, err
		}
		stsvc["disk"] = disk

	}
	m = &Manager{
		cfg:             c,
		storageServices: stsvc,
	}
	m.updateErroStatus()
	return
}

// AddStorage can add a specific storage service
func (m *Manager) AddStorage(s Storage) {
	m.storageServices[s.GetStorageServiceName()] = s
}

// GetStorageServicesKeys returns a list of all storage names
func (m *Manager) GetStorageServicesKeys() (svc []string) {
	keys := reflect.ValueOf(m.storageServices).MapKeys()
	svc = make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		svc[i] = keys[i].String()
	}
	return
}

// GetStorageServices returns all storage services handled by this manager
func (m *Manager) GetStorageServices() map[string]Storage {
	return m.storageServices
}

// WriteStreamAll writes a byte stream to all available storage services
func (m *Manager) WriteStreamAll(name, mimeType string, body io.Reader, dlo bool) (errs error) {
	var eg errgroup.Group
	readers, writer, closer := m.createIOReaders(len(m.storageServices))
	i := 0
	for _, s := range m.storageServices {
		func(i int, st Storage) {
			eg.Go(func() error {
				return st.WriteStream(name, mimeType, readers[i], nil, dlo)
			})
		}(i, s)
		i++
	}

	go func() {
		io.Copy(writer, body)
		closer.Close()
	}()

	return eg.Wait()
}

// WriteStream writes a byte stream to a specific storage service
func (m *Manager) WriteStream(storageService, name, mimeType string, body io.Reader, tags map[string]string, dlo bool) (errs error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return fmt.Errorf("unknown storage service")
	}
	return s.WriteStream(name, mimeType, body, tags, dlo)
}

// WriteFolderAll writes a folder to all storages
func (m *Manager) WriteFolderAll(path string) (errs error) {
	for k, s := range m.storageServices {
		if err := s.WriteFolder(path); err != nil {
			errs = multierror.Append(errs, &Error{message: err.Error(), Storage: k})
		}
	}

	return
}

// DownloadLatestBackup from a specific storage
func (m *Manager) DownloadLatestBackup(storageService string) (path string, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return path, fmt.Errorf("unknown storage service")
	}
	return s.DownloadLatestBackup()
}

// DownloadBackup from a specific storage
func (m *Manager) DownloadBackup(storageService string, fullBackup Backup) (path string, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return path, fmt.Errorf("unknown storage service")
	}
	return s.DownloadBackup(fullBackup)
}

// GetFullBackups lists all available full backups from a specific storage
func (m *Manager) GetFullBackups(storageService string) (bl []Backup, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return bl, fmt.Errorf("unknown storage service")
	}
	return s.GetFullBackups()
}

// GetIncBackupsFromDump lists all available incremental backups belonging to the full backup from a specific storage
func (m *Manager) GetIncBackupsFromDump(storageService, key string) (bl []Backup, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return bl, fmt.Errorf("unknown storage service")
	}
	if st := s.GetStatusErrorByKey(key); st != "" {
		return bl, fmt.Errorf("backup is incomplete, due to: %s", st)
	}

	return s.GetIncBackupsFromDump(key)
}

// DownloadBackupWithLogPosition from a specific storage and timestamp
func (m *Manager) DownloadBackupWithLogPosition(storageService, fullBackupPath string, binlog string) (path string, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return path, fmt.Errorf("unknown storage service")
	}
	return s.DownloadBackupWithLogPosition(fullBackupPath, binlog)
}

func (m *Manager) createIOReaders(count int) ([]io.Reader, io.Writer, io.Closer) {
	readers := make([]io.Reader, 0, count)
	pipeWriters := make([]io.Writer, 0, count)
	pipeClosers := make([]io.Closer, 0, count)

	for i := 0; i < count; i++ {
		pr, pw := io.Pipe()
		readers = append(readers, pr)
		pipeWriters = append(pipeWriters, pw)
		pipeClosers = append(pipeClosers, pw)
	}

	return readers, io.MultiWriter(pipeWriters...), NewIOClosers(pipeClosers)
}

func (m *Manager) updateErroStatus() {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for {
			select {
			case <-ticker.C:
				for svc, s := range m.storageServices {
					for k := range s.GetStatusError() {
						fp := path.Join(k, backupIncomplete)
						logger.Infof("Trying to save error status: %s", k)
						if err := m.WriteStream(svc, fp, "", bytes.NewReader([]byte("ERROR")), nil, false); err == nil {
							delete(s.GetStatusError(), k)
						}
					}
				}
			}
		}
	}()
}

// IOClosers holds all io closers
type IOClosers struct {
	closers []io.Closer
}

// NewIOClosers creates a list of io closers
func NewIOClosers(closers []io.Closer) *IOClosers {
	return &IOClosers{
		closers: closers,
	}
}

// Close closes all io closers
func (m *IOClosers) Close() (err error) {
	for _, c := range m.closers {
		if err = c.Close(); err != nil {
			logger.Errorf("Error closing write strream: %s", err.Error())
		}
	}
	return
}
