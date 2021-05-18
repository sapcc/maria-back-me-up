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

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

type MockStorage struct {
	cfg           config.Config
	serviceName   string
	restoreFolder string
	binLog        string
	withError     bool
}

func NewMockStorage(c config.Config, serviceName, binLog string) (m *MockStorage) {
	m = &MockStorage{}
	m.binLog = binLog
	m.cfg = c
	m.serviceName = serviceName
	m.withError = false

	return
}

func (s *MockStorage) WithError(e bool) {
	s.withError = e
}
func (s *MockStorage) ListIncBackupsFor(key string) (bl []Backup, err error) {
	return
}
func (s *MockStorage) ListServices() (services []string, err error) {
	return
}
func (s *MockStorage) ListFullBackups() (bl []Backup, err error) {
	return
}
func (s *MockStorage) DownloadLatestBackup() (path string, err error) {
	return
}
func (s *MockStorage) WriteStream(name, mimeType string, body io.Reader, tags map[string]string, dlo bool) (err error) {
	if s.withError {
		return &StorageError{message: "doh", Storage: s.serviceName}
	}
	return
}
func (s *MockStorage) WriteFolder(p string) (err error) {
	if s.withError {
		return &StorageError{message: "doh", Storage: s.serviceName}
	}
	return
}
func (s *MockStorage) GetStorageServiceName() (name string) {
	return s.serviceName
}
func (s *MockStorage) GetStatusErrorByKey(backupKey string) string {
	return ""
}
func (s *MockStorage) GetStatusError() (m map[string]string) {
	return m
}
func (s *MockStorage) DownloadBackup(fullBackup Backup) (path string, err error) {
	return
}
func (s *MockStorage) DownloadBackupFrom(fullBackupPath string, binlog string) (path string, err error) {
	return
}
