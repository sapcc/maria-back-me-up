package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"gopkg.in/yaml.v2"
)

type Disk struct {
	name        string
	serviceName string
	binLog      string
	cfg         config.Disk
	statusError map[string]string
}

func NewDisk(cfg config.Disk, serviceName string, binLog string) (d *Disk, err error) {
	return &Disk{name: "Disk", cfg: cfg, serviceName: serviceName, binLog: binLog}, nil
}

func (d *Disk) GetStorageServiceName() (name string) {
	return d.name
}

func (d *Disk) GetStatusError() map[string]string {
	return d.statusError
}

func (d *Disk) GetStatusErrorByKey(backupKey string) string {
	if st, ok := d.statusError[path.Dir(backupKey)]; ok {
		return st
	}
	return ""
}

func (d *Disk) WriteFolder(p string) (err error) {
	r, err := ZipFolderPath(p)
	if err != nil {
		return fmt.Errorf("error writing folder %v: %v", p, err)
	}
	return d.WriteStream(path.Join(filepath.Base(p), "dump.tar"), "zip", r, nil, false)
}

// WriteStream writes a file in the base directory set in the config.
// If the fileName is `last_successful_backup` only the tags are written to the file
// In all other cases the tags are ignored and the body is written to the file
func (d *Disk) WriteStream(fileName, mimeType string, body io.Reader, tags map[string]string, dlo bool) error {
	// TODO: delete oldest backup if older than days retention
	fileName = path.Join(d.cfg.BasePath, d.serviceName, fileName)

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		dir, _ := filepath.Split(fileName)
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return fmt.Errorf("error creating base folder: %v", err)
		}
	}

	if filepath.Base(fileName) == LastSuccessfulBackupFile {
		return writeFileWithTags(fileName, tags)
	}

	buffer := new(bytes.Buffer)
	_, err := buffer.ReadFrom(body)

	if err != nil {
		return fmt.Errorf("failed to read backup content: %s", err.Error())
	}

	err = os.WriteFile(fileName, buffer.Bytes(), 0666)

	if err != nil {
		return fmt.Errorf("error writing stream to file %v: %v", fileName, err)
	}

	return nil
}

// DownloadLatestBackup returns the path of the last backup that was verified successfully
func (d *Disk) DownloadLatestBackup() (path string, err error) {

	fileName := filepath.Join(d.cfg.BasePath, d.serviceName, LastSuccessfulBackupFile)
	tags, err := readFileWithTags(fileName)
	if err != nil {
		return "", fmt.Errorf("could not read file: %s", err.Error())
	}
	if v, ok := tags["key"]; ok {
		return filepath.Join(d.cfg.BasePath, v), nil
	}
	return path, &NoBackupError{}
}

// ListFullBackups walks the backup basepath and list all full backups.
//
// Only backups which contain a dump.tar are listed
func (d *Disk) ListFullBackups() (bl []Backup, err error) {

	err = filepath.WalkDir(d.cfg.BasePath, func(path string, entry fs.DirEntry, err error) error {
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

// ListServices returns all services which are backed up on disk
func (d *Disk) ListServices() (services []string, err error) {

	err = filepath.WalkDir(d.cfg.BasePath, func(path string, entry fs.DirEntry, err error) error {
		if entry.IsDir() {
			services = append(services, entry.Name())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return services, nil
}

func (d *Disk) ListIncBackupsFor(key string) (bl []Backup, err error) {

	backupPath := filepath.Join(d.cfg.BasePath, key)

	info, err := os.Stat(backupPath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("backup directory `%s` does not exist", backupPath)
	}

	b := Backup{
		Storage: d.name,
		Time:    info.ModTime(),
		Key:     key,
	}

	incBackups := make([]IncBackup, 1)

	err = filepath.WalkDir(backupPath, func(path string, entry fs.DirEntry, err error) error {
		if !entry.IsDir() {
			fileName := filepath.Base(path)
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

			if strings.HasPrefix(fileName, "verify_") {
				v := Verify{}
				content, err := os.ReadFile(path)
				if err != nil {
					return fmt.Errorf("could read %s: %s", fileName, err.Error())
				}

				err = yaml.Unmarshal(content, &v)
				if err != nil {
					return fmt.Errorf("failed to unmarshal %s: %s", fileName, err.Error())
				}

				if strings.HasSuffix(fileName, "_fail") {
					b.VerifyFail = &v
				} else {
					b.VerifySuccess = &v
				}
			}

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

	return bl, nil
}

// DownloadBackupFrom returns the path to the binlog file
func (d *Disk) DownloadBackupFrom(fullBackupPath string, binlog string) (path string, err error) {
	path = filepath.Join(fullBackupPath, binlog)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "", fmt.Errorf("binlog file does not exist")
	}
	return filepath.Join(fullBackupPath, binlog), nil
}

// DownloadBackup returns the folder where the backup files are written to
func (d *Disk) DownloadBackup(fullBackup Backup) (path string, err error) {
	if _, err := os.Stat(fullBackup.Key); os.IsNotExist(err) {
		return "", fmt.Errorf("directory for full backup `%s` is empty", fullBackup.Key)
	}
	return fullBackup.Key, nil
}

// writeFileWithTags encodes the tags and writes them to the file
func writeFileWithTags(fileName string, tags map[string]string) (err error) {
	if len(tags) == 0 {
		return fmt.Errorf("expected tags were not supplied for file %s", filepath.Base(fileName))
	} else {
		buffer := new(bytes.Buffer)
		encoder := gob.NewEncoder(buffer)
		err := encoder.Encode(tags)
		if err != nil {
			return fmt.Errorf("error encoding tags: %s", err.Error())
		}
		err = os.WriteFile(fileName, buffer.Bytes(), os.ModePerm)

		if err != nil {
			return fmt.Errorf("could not write file with tags: %s", err.Error())
		}
		return nil
	}
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
