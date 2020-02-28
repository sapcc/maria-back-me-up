package storage

import (
	"io"
	"reflect"

	"github.com/hashicorp/go-multierror"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/errgroup"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sirupsen/logrus"
)

type Manager struct {
	cfg                         config.StorageService
	serviceName                 string
	storageServices             map[string]Storage
	verifyLastBackupFromService string
}

func init() {
	logger = log.WithFields(logrus.Fields{"component": "storage"})
}

func NewManager(c config.StorageService, sn string) (m *Manager) {
	stsvc := make(map[string]Storage, 0)
	for _, cfg := range c.Swift {
		swift, _ := NewSwift(cfg, sn)
		stsvc[cfg.Name] = swift
	}
	for _, cfg := range c.S3 {
		s3, _ := NewS3(cfg, sn)
		stsvc[cfg.Name] = s3
	}

	return &Manager{
		cfg:             c,
		serviceName:     sn,
		storageServices: stsvc,
	}
}

func (m *Manager) GetStorageServices() (svc []string) {
	keys := reflect.ValueOf(m.storageServices).MapKeys()
	svc = make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		svc[i] = keys[i].String()
	}
	return
}

func (m *Manager) WriteStream(name, mimeType string, body io.Reader) (errs error) {
	var eg errgroup.Group
	readers, writer, closer := m.createIOReaders(len(m.storageServices))
	i := 0
	for _, s := range m.storageServices {
		func(i int, st Storage) {
			eg.Go(func() error {
				return st.WriteStream(name, mimeType, readers[i])
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

func (m *Manager) WriteFile(storageService, name, mimeType string, body io.Reader) (errs error) {
	return m.storageServices[storageService].WriteStream(name, mimeType, body)
}

func (m *Manager) WriteFolder(path string) (errs error) {
	for k, s := range m.storageServices {
		if err := s.WriteFolder(path); err != nil {
			errs = multierror.Append(errs, &StorageError{message: err.Error(), Storage: k})
		}
	}

	return
}

func (m *Manager) DownloadLatestBackup(storageService string) (path string, err error) {
	if storageService == "" {
		storageService = m.cfg.DefaultStorage
	}
	return m.storageServices[storageService].DownloadLatestBackup()
}

func (m *Manager) DownloadBackup(storageService string, fullBackup Backup) (path string, err error) {
	if storageService == "" {
		storageService = m.cfg.DefaultStorage
	}
	return m.storageServices[storageService].DownloadBackup(fullBackup)
}

func (m *Manager) ListFullBackups(storageService string) (bl []Backup, err error) {
	if storageService == "" {
		storageService = m.cfg.DefaultStorage
	}
	return m.storageServices[storageService].ListFullBackups()
}

func (m *Manager) ListIncBackupsFor(storageService, key string) (bl []Backup, err error) {
	if storageService == "" {
		storageService = m.cfg.DefaultStorage
	}
	return m.storageServices[storageService].ListIncBackupsFor(key)
}

func (m *Manager) DownloadBackupFrom(storageService, fullBackupPath string, binlog string) (path string, err error) {
	if storageService == "" {
		storageService = m.cfg.DefaultStorage
	}
	return m.storageServices[storageService].DownloadBackupFrom(fullBackupPath, binlog)
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

type IOClosers struct {
	closers []io.Closer
}

func NewIOClosers(closers []io.Closer) *IOClosers {
	return &IOClosers{
		closers: closers,
	}
}

func (m *IOClosers) Close() (err error) {
	for _, c := range m.closers {
		if err = c.Close(); err != nil {
			logger.Errorf("Error closing write strream: %s", err.Error())
		}
	}
	return
}
