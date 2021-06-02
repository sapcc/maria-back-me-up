package verification

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// Status struct holds the current info of a verifiction process
type Status struct {
	sync.RWMutex   `yaml:"-"`
	VerifyRestore  int           `yaml:"verify_backup"`
	VerifyChecksum int           `yaml:"verify_checksum"`
	VerifyDiff     int           `yaml:"verify_diff"`
	VerifyError    string        `yaml:"verify_error"`
	BackupService  string        `yaml:"backup_service"`
	StorageService string        `yaml:"storage_service"`
	logger         *logrus.Entry `yaml:"-"`
}

// NewStatus creates verification status instance
func NewStatus(backupService, storageService string) *Status {
	return &Status{
		VerifyRestore:  0,
		VerifyChecksum: 0,
		VerifyDiff:     0,
		VerifyError:    "",
		BackupService:  backupService,
		StorageService: storageService,
		logger:         logger.WithFields(logrus.Fields{"service": backupService, "storage": storageService}),
	}
}

// SetVerifyRestore updates the verify status
func (s *Status) SetVerifyRestore(i int, err error) {
	s.Lock()
	defer s.Unlock()
	if i == 0 {
		s.VerifyRestore = 0
		s.VerifyDiff = 0
		s.VerifyChecksum = 0
	} else {
		s.VerifyRestore = i
	}
	if err != nil {
		s.VerifyError = err.Error()
	}
}

// SetVerifyDiff updates the restore diff status
func (s *Status) SetVerifyDiff(i int, err error) {
	s.Lock()
	defer s.Unlock()
	s.VerifyDiff = i
	if err != nil {
		s.VerifyError = err.Error()
	}
}

// SetVerifyChecksum updates the checksum status
func (s *Status) SetVerifyChecksum(i int, err error) {
	s.Lock()
	defer s.Unlock()
	s.VerifyChecksum = i
	if err != nil {
		s.VerifyError = err.Error()
	}
}

// Reset the verify status info
func (s *Status) Reset() {
	s.Lock()
	defer s.Unlock()
	s.VerifyRestore = 0
	s.VerifyDiff = 0
	s.VerifyChecksum = 0
	s.VerifyError = ""
}

// Upload the status info to the specified storage service
func (s *Status) Upload(restoreFolder, logNameFormat, serviceName string, manager *storage.Manager) {
	s.RLock()
	out, err := yaml.Marshal(s)
	s.RUnlock()
	if err != nil {
		return
	}

	_, file := path.Split(restoreFolder)
	if s.VerifyDiff == 1 && s.VerifyRestore == 1 || s.VerifyChecksum == 1 {
		file = file + "/verify_success.yaml"
	} else {
		file = file + "/verify_fail.yaml"
	}
	s.logger.Debug("Uploading verify status to: ", file)
	err = manager.WriteStream(s.StorageService, file, "", bytes.NewReader(out), nil, false)
	if err != nil {
		s.logger.Error(fmt.Errorf("cannot upload verify status: %s", err.Error()))
	}
	if s.VerifyDiff == 1 && s.VerifyRestore == 1 {
		binlogNumber := 0
		filepath.Walk(restoreFolder, func(path string, f os.FileInfo, err error) error {
			if f.IsDir() && f.Name() == "dump" {
				return filepath.SkipDir
			}

			if !f.IsDir() && strings.Contains(f.Name(), logNameFormat) {
				bin := strings.Split(f.Name(), ".")
				binlogNbr, _ := strconv.Atoi(bin[1])
				if binlogNbr > binlogNumber {
					binlogNumber = binlogNbr
				}
			}
			return nil
		})
		tags := make(map[string]string)
		base := path.Base(restoreFolder)
		tags["key"] = path.Join(serviceName, base)
		tags["binlog"] = fmt.Sprintf("%s.%d", logNameFormat, binlogNumber)
		err = manager.WriteStream(s.StorageService, storage.LastSuccessfulBackupFile, "", strings.NewReader("latest_backup"), tags, false)
		if err != nil {
			s.logger.Error(fmt.Errorf("cannot upload verify status: %s", err.Error()))
		}
	}
}
