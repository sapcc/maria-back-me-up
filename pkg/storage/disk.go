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
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"gopkg.in/yaml.v2"
)

// Disk struct is ...
type Disk struct {
	name        string
	serviceName string
	binLog      string
	cfg         config.Disk
	statusError map[string]string
}

// NewDisk creates a new disk storage object
func NewDisk(cfg config.Disk, serviceName string, binLog string) (d *Disk, err error) {
	return &Disk{
		name:        cfg.Name,
		cfg:         cfg,
		serviceName: serviceName,
		binLog:      binLog,
		statusError: make(map[string]string)}, nil
}

// Verify implements interface
func (d *Disk) Verify() bool {
	if d.cfg.Verify == nil {
		return false
	}
	return *d.cfg.Verify
}

// GetStorageServiceName implements interface
func (d *Disk) GetStorageServiceName() (name string) {
	return d.name
}

// GetStatusError implements interface
func (d *Disk) GetStatusError() map[string]string {
	return d.statusError
}

// GetStatusErrorByKey implements interface
func (d *Disk) GetStatusErrorByKey(backupKey string) string {
	if st, ok := d.statusError[path.Dir(backupKey)]; ok {
		return st
	}
	return ""
}

// WriteFolder implements interface
func (d *Disk) WriteFolder(p string) (err error) {
	r, err := ZipFolderPath(p)
	if err != nil {
		return d.handleError(filepath.Join(filepath.Base(p), "dump.tar"), err)
	}

	err = d.WriteStream(path.Join(filepath.Base(p), "dump.tar"), "zip", r, nil, false)
	if err != nil {
		return err
	}
	return d.enforceBackupRetention()
}

// WriteStream implements interface.
//
// Writes a file in the base directory defined in the config.
// If the fileName is `last_successful_backup` only the tags are written to the file but not its actual content
// In all other cases the tags are ignored and the body is written to the file
func (d *Disk) WriteStream(fileName, mimeType string, body io.Reader, tags map[string]string, dlo bool) error {
	filePath := path.Join(d.cfg.BasePath, d.serviceName, fileName)

	// check if backupfolder exists, create if not
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		dir, _ := filepath.Split(filePath)
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return d.handleError(filepath.Join(d.serviceName, fileName), err)
		}
	}

	// handle special 'last_successful_backup' meta file
	if fileName == LastSuccessfulBackupFile {
		err := writeFileWithTags(filePath, tags)
		if err != nil {
			return d.handleError(filepath.Join(d.serviceName, fileName), err)
		}
		return nil
	}

	if tags != nil || len(tags) > 0 {
		log.Info(fmt.Sprintf("disk ignores tags for : %s", filepath.Join(d.serviceName, fileName)))
	}

	buffer := new(bytes.Buffer)
	_, err := buffer.ReadFrom(body)
	if err != nil {
		return d.handleError(filepath.Join(d.serviceName, fileName), fmt.Errorf("failed to read backup content: %s", err.Error()))
	}

	err = os.WriteFile(filePath, buffer.Bytes(), 0666)
	if err != nil {
		return d.handleError(filepath.Join(d.serviceName, fileName), fmt.Errorf("failed to write backup: %v", err))
	}

	return nil
}

// DownloadLatestBackup implements interface
func (d *Disk) DownloadLatestBackup() (path string, err error) {

	fileName := filepath.Join(d.cfg.BasePath, d.serviceName, LastSuccessfulBackupFile)
	tags, err := readFileWithTags(fileName)
	if err != nil {
		return "", fmt.Errorf("could not read '%s': %s", LastSuccessfulBackupFile, err.Error())
	}
	if v, ok := tags["key"]; ok {
		return filepath.Join(d.cfg.BasePath, v), nil
	}
	return path, &NoBackupError{}
}

// GetFullBackups implements interface
//
// Walks the backup basepath and list all full backups.
// Only backups which contain a dump.tar are listed
func (d *Disk) GetFullBackups() (bl []Backup, err error) {
	backupPath := filepath.Join(d.cfg.BasePath, d.serviceName)

	if _, err := os.Stat(backupPath); errors.Is(err, os.ErrNotExist) {
		return nil, &NoBackupError{fmt.Sprintf("backup directory for service '%s' does not exist", d.serviceName)}
	}

	// adds the folder that contains a 'dump.tar' file as a backup.
	// empty folders or folders without 'dump.tar' are ignored
	err = filepath.WalkDir(backupPath, func(path string, entry fs.DirEntry, err error) error {
		if !entry.IsDir() {
			fileInfo, err := entry.Info()
			if err != nil {
				return err
			}
			if strings.EqualFold(fileInfo.Name(), "dump.tar") {
				stat, _ := os.Stat(filepath.Dir(path))

				b := Backup{
					Storage: d.name,
					Time:    stat.ModTime(),
					Key:     filepath.Join(d.serviceName, filepath.Base(filepath.Dir(path))),
					IncList: make([]IncBackup, 0),
				}
				bl = append(bl, b)
			}
		}
		return nil
	})
	return bl, err
}

// GetTotalIncBackupsFromDump implements interface
func (d *Disk) GetTotalIncBackupsFromDump(key string) (t int, err error) {
	return
}

// GetIncBackupsFromDump implements interface
func (d *Disk) GetIncBackupsFromDump(key string) (bl []Backup, err error) {
	backupPath := filepath.Join(d.cfg.BasePath, key)

	info, err := os.Stat(backupPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, &NoBackupError{}
	}

	b := Backup{
		Storage: d.name,
		Time:    info.ModTime(),
		Key:     key,
	}

	incBackups := make([]IncBackup, 0)

	// find all incremental backup files in the backups folder
	err = filepath.WalkDir(backupPath, func(path string, entry fs.DirEntry, err error) error {
		if !entry.IsDir() {
			fileName := filepath.Base(path)

			// adds incremental backup for file which name contains the binlog prefix 'Disk.binLog'
			if strings.Contains(fileName, d.binLog) {
				info, err := entry.Info()
				if err != nil {
					return err
				}
				incBackup := IncBackup{
					Key:          filepath.Join(key, fileName),
					LastModified: info.ModTime(),
				}
				incBackups = append(incBackups, incBackup)
			}

			// handle verification status (success and fail)
			if strings.HasPrefix(fileName, "verify_") {
				v := Verify{}
				content, err := os.ReadFile(path)
				if err != nil {
					return d.handleError(key, fmt.Errorf("failed to read %s: %s", fileName, err.Error()))
				}

				err = yaml.Unmarshal(content, &v)
				if err != nil {
					return d.handleError(key, fmt.Errorf("failed to unmarshal %s: %s", fileName, err.Error()))
				}

				if strings.HasSuffix(fileName, "_fail") {
					b.VerifyFail = &v
				} else {
					b.VerifySuccess = &v
				}
			}

			// handle incomplete backup error file
			if strings.EqualFold(fileName, backupIncomplete) {
				v := Verify{}
				v.VerifyError = "backup incomplete!!!"
				v.Time = time.Now()
				b.VerifyFail = &v
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	b.IncList = incBackups
	bl = append(bl, b)
	return bl, nil
}

// DownloadBackupWithLogPosition implements interface
func (d *Disk) DownloadBackupWithLogPosition(fullBackupPath string, binlog string) (path string, err error) {
	if fullBackupPath == "" || binlog == "" {
		return "", &NoBackupError{}
	}

	path = filepath.Join(d.cfg.BasePath, fullBackupPath, binlog)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return "", &NoBackupError{}
	}

	return path, nil
}

// DownloadBackup implements interface
func (d *Disk) DownloadBackup(fullBackup Backup) (path string, err error) {
	backupPath := filepath.Join(d.cfg.BasePath, fullBackup.Key)
	if _, err := os.Stat(backupPath); errors.Is(err, os.ErrNotExist) {
		return "", d.handleError(fullBackup.Key, fmt.Errorf("directory for full backup is empty"))
	}
	return backupPath, nil
}

// enforceBackupRetention ensures the amount of backups does not exceed the retention
//
// Every backup folder more than the specified retention will be deleted
func (d *Disk) enforceBackupRetention() error {

	backups, err := d.GetFullBackups()
	if err != nil {
		return &NoBackupError{}
	}
	if len(backups) <= d.cfg.Retention || len(backups) == 0 {
		return nil
	}

	sort.Sort(sort.Reverse(ByTime(backups)))

	// delete all backups that are more than the retention allow, starting with the oldest
	deletions := len(backups) - d.cfg.Retention
	for range deletions {
		backupKey := backups[len(backups)-1].Key
		err := os.RemoveAll(filepath.Join(d.cfg.BasePath, backupKey))
		if err != nil {
			return d.handleError(backupKey, fmt.Errorf("failed to delete backup folder:%s", err.Error()))
		}
		log.Info(fmt.Sprintf("deleted backup '%s'", backupKey))
		backups = backups[:len(backups)-1]
	}

	return nil
}

func (d *Disk) handleError(backupKey string, err error) error {
	errS := &Error{message: "", Storage: d.name}
	errS.message = err.Error()
	if backupKey != "" && !strings.Contains(backupKey, backupIncomplete) {
		d.statusError[path.Dir(backupKey)] = err.Error()
	}
	return errS
}

// writeFileWithTags encodes the tags and writes them to the file
func writeFileWithTags(fileName string, tags map[string]string) (err error) {
	if len(tags) == 0 || tags == nil {
		return fmt.Errorf("expected tags were not supplied for file %s", filepath.Base(fileName))
	}

	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err = encoder.Encode(tags)
	if err != nil {
		return fmt.Errorf("error encoding tags: %s", err.Error())
	}
	err = os.WriteFile(fileName, buffer.Bytes(), os.ModePerm)

	if err != nil {
		return fmt.Errorf("could not write file with tags: %s", err.Error())
	}
	return nil
}

// readFileWithTags decodes the file content to a map[string]string
func readFileWithTags(fileName string) (tags map[string]string, err error) {
	reader, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	decoder := gob.NewDecoder(reader)
	err = decoder.Decode(&tags)
	if err != nil {
		return nil, fmt.Errorf("error decoding tags from file %s: %s", fileName, err.Error())
	}
	return tags, nil
}
