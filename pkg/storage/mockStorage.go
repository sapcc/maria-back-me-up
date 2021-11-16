/**
 * Copyright 2021 SAP SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
