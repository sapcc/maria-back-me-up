// SPDX-FileCopyrightText: 2021 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"io"
	"path"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

// MockStorage struct for testing
type MockStorage struct {
	cfg           config.Config
	serviceName   string
	restoreFolder string
	binLog        string
	withError     bool
}

// NewMockStorage creates a mock storage instance
func NewMockStorage(c config.Config, serviceName, restoreFolder, binLog string) (m *MockStorage) {
	m = &MockStorage{}
	m.binLog = binLog
	m.cfg = c
	m.serviceName = serviceName
	m.withError = false
	m.restoreFolder = path.Join(restoreFolder, serviceName)

	return
}

// WithError implements interface
func (s *MockStorage) WithError(e bool) {
	s.withError = e
}

// Verify implements interface
func (s *MockStorage) Verify() bool {
	return false
}

// GetIncBackupsFromDump implements interface
func (s *MockStorage) GetIncBackupsFromDump(key string) (bl []Backup, err error) {
	return
}

// GetFullBackups implements interface
func (s *MockStorage) GetFullBackups() (bl []Backup, err error) {
	return
}

// DownloadLatestBackup implements interface
func (s *MockStorage) DownloadLatestBackup() (path string, err error) {
	return
}

// GetTotalIncBackupsFromDump implements interface
func (s *MockStorage) GetTotalIncBackupsFromDump(key string) (t int, err error) {
	return
}

// WriteStream implements interface
func (s *MockStorage) WriteStream(name, mimeType string, body io.Reader, tags map[string]string, dlo bool) (err error) {
	if s.withError {
		return &Error{message: "doh", Storage: s.serviceName}
	}
	return
}

// WriteFolder implements interface
func (s *MockStorage) WriteFolder(p string) (err error) {
	if s.withError {
		return &Error{message: "doh", Storage: s.serviceName}
	}
	return
}

// GetStorageServiceName implements interface
func (s *MockStorage) GetStorageServiceName() (name string) {
	return s.serviceName
}

// GetStatusErrorByKey implements interface
func (s *MockStorage) GetStatusErrorByKey(backupKey string) string {
	return ""
}

// GetStatusError implements interface
func (s *MockStorage) GetStatusError() (m map[string]string) {
	return m
}

// DownloadBackup implements interface
func (s *MockStorage) DownloadBackup(fullBackup Backup) (path string, err error) {
	return
}

// DownloadBackupWithLogPosition implements interface
func (s *MockStorage) DownloadBackupWithLogPosition(fullBackupPath string, binlog string) (path string, err error) {
	return
}
