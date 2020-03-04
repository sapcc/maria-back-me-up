package verification

import (
	"bytes"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/storage"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

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

func NewStatus(backupService, storageService string) *Status {
	return &Status{
		VerifyRestore:  0,
		VerifyChecksum: 0,
		VerifyDiff:     0,
		VerifyError:    "",
		BackupService:  backupService,
		StorageService: storageService,
		logger:         logger.WithField("service", backupService),
	}
}

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

func (s *Status) SetVerifyDiff(i int, err error) {
	s.Lock()
	defer s.Unlock()
	s.VerifyDiff = i
	if err != nil {
		s.VerifyError = err.Error()
	}
}

func (s *Status) SetVerifyChecksum(i int, err error) {
	s.Lock()
	defer s.Unlock()
	s.VerifyChecksum = i
	if err != nil {
		s.VerifyError = err.Error()
	}
}

func (s *Status) Reset() {
	s.Lock()
	defer s.Unlock()
	s.VerifyRestore = 0
	s.VerifyDiff = 0
	s.VerifyChecksum = 0
	s.VerifyError = ""
}

func (s *Status) UploadStatus(restoreFolder string, storage *storage.Manager) {
	s.RLock()
	out, err := yaml.Marshal(s)
	s.RUnlock()
	if err != nil {
		return
	}
	u := strconv.FormatInt(time.Now().Unix(), 10)
	_, file := path.Split(restoreFolder)
	s.logger.Debug("Uploading verify status to: ", file+"/verify_"+u+".yaml")
	err = storage.WriteStream(s.StorageService, file+"/verify_"+u+".yaml", "", bytes.NewReader(out))
	if err != nil {
		s.logger.Error(fmt.Errorf("cannot upload verify status: %s", err.Error()))
	}
}
