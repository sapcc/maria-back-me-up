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

const backupIcomplete = "backup_incomplete"

type Manager struct {
	cfg                         config.StorageService
	storageServices             map[string]Storage
	verifyLastBackupFromService string
}

func init() {
	logger = log.WithFields(logrus.Fields{"component": "storage"})
}

func NewManager(c config.StorageService, serviceName, binLog string) (m *Manager, err error) {
	stsvc := make(map[string]Storage, 0)
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
	m = &Manager{
		cfg:             c,
		storageServices: stsvc,
	}
	m.updateErroStatus()
	return
}

func (m *Manager) GetStorageServicesKeys() (svc []string) {
	keys := reflect.ValueOf(m.storageServices).MapKeys()
	svc = make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		svc[i] = keys[i].String()
	}
	return
}

func (m *Manager) GetStorageServices() map[string]Storage {
	return m.storageServices
}

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

func (m *Manager) WriteStream(storageService, name, mimeType string, body io.Reader, tags map[string]string, dlo bool) (errs error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return fmt.Errorf("unknown storage service")
	}
	return s.WriteStream(name, mimeType, body, tags, dlo)
}

func (m *Manager) WriteFolderAll(path string) (errs error) {
	for k, s := range m.storageServices {
		if err := s.WriteFolder(path); err != nil {
			errs = multierror.Append(errs, &StorageError{message: err.Error(), Storage: k})
		}
	}

	return
}

func (m *Manager) DownloadLatestBackup(storageService string) (path string, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return path, fmt.Errorf("unknown storage service")
	}
	return s.DownloadLatestBackup()
}

func (m *Manager) DownloadBackup(storageService string, fullBackup Backup) (path string, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return path, fmt.Errorf("unknown storage service")
	}
	return s.DownloadBackup(fullBackup)
}

func (m *Manager) ListFullBackups(storageService string) (bl []Backup, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return bl, fmt.Errorf("unknown storage service")
	}
	return s.ListFullBackups()
}

func (m *Manager) ListServices() (s map[string][]string, err error) {
	s = make(map[string][]string, 0)
	for n, svc := range m.storageServices {
		ls, err := svc.ListServices()
		if err != nil {
			return s, err
		}
		s[n] = ls
	}
	return
}

func (m *Manager) ListIncBackupsFor(storageService, key string) (bl []Backup, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return bl, fmt.Errorf("unknown storage service")
	}
	if st := s.GetStatusErrorByKey(key); st != "" {
		return bl, fmt.Errorf("backup is incomplete, due to: %s", st)
	}

	return s.ListIncBackupsFor(key)
}

func (m *Manager) DownloadBackupFrom(storageService, fullBackupPath string, binlog string) (path string, err error) {
	s, ok := m.storageServices[storageService]
	if !ok {
		return path, fmt.Errorf("unknown storage service")
	}
	return s.DownloadBackupFrom(fullBackupPath, binlog)
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
						fp := path.Join(k, backupIcomplete)
						logger.Infof("Trying to save error status", k)
						if err := m.WriteStream(svc, fp, "", bytes.NewReader([]byte("ERROR")), nil, false); err == nil {
							delete(s.GetStatusError(), k)
						}
					}
				}
			}
		}
	}()
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
