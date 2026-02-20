// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ncw/swift"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// Swift struct is ...
type Swift struct {
	cfg           config.Swift
	connection    *swift.Connection
	serviceName   string
	restoreFolder string
	logger        *logrus.Entry `yaml:"-"`
	logBin        string
	statusError   map[string]string
}

// NewSwift creates a swift storage instance
func NewSwift(c config.Swift, serviceName, restoreFolder, logBin string) (s *Swift, err error) {
	conn := &swift.Connection{
		AuthVersion:  c.AuthVersion,
		AuthUrl:      c.AuthURL,
		UserName:     c.UserName,
		Domain:       c.UserDomainName,
		Tenant:       c.ProjectName,
		TenantDomain: c.ProjectDomainName,
		ApiKey:       c.Password,
		Region:       c.Region,
		Timeout:      time.Duration(5 * time.Hour),
	}
	if err = conn.Authenticate(); err != nil {
		return
	}

	return &Swift{
		cfg:           c,
		connection:    conn,
		serviceName:   serviceName,
		restoreFolder: path.Join(restoreFolder, c.Name),
		logger:        logger.WithField("service", serviceName),
		logBin:        logBin,
		statusError:   make(map[string]string, 0),
	}, err
}

// GetStorageServiceName implements interface
func (s *Swift) GetStorageServiceName() (storages string) {
	return s.cfg.Name
}

// GetStatusError implements interface
func (s *Swift) GetStatusError() map[string]string {
	return s.statusError
}

// Verify implements interface
func (s *Swift) Verify() bool {
	if s.cfg.Verify == nil {
		return false
	}
	return *s.cfg.Verify
}

// GetStatusErrorByKey implements interface
func (s *Swift) GetStatusErrorByKey(backupKey string) string {
	if st, ok := s.statusError[path.Dir(backupKey)]; ok {
		return st
	}
	return ""
}

// GetTotalIncBackupsFromDump implements interface
func (s *Swift) GetTotalIncBackupsFromDump(key string) (t int, err error) {
	t = 0
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: strings.ReplaceAll(key, "dump.tar", "")})
	if err != nil {
		return t, s.handleError("", err)
	}
	for _, o := range objs {
		if !strings.HasSuffix(o.Name, "/") && strings.Contains(o.Name, s.logBin) {
			t++
		}
	}
	return
}

// WriteFolder implements interface
func (s *Swift) WriteFolder(p string) (err error) {
	r, err := ZipFolderPath(p)
	if err != nil {
		return s.handleError(path.Join(filepath.Base(p), "dump.tar"), err)
	}
	size, err := FolderSize(p)
	if err != nil {
		return s.handleError(path.Join(filepath.Base(p), "dump.tar"), err)
	}
	dlo := false
	if s.cfg.SloSize == nil {
		if size > 600 {
			dlo = true
		}
	} else {
		if size > *s.cfg.SloSize {
			dlo = true
		}
	}

	return s.WriteStream(path.Join(filepath.Base(p), "dump.tar"), "zip", r, nil, dlo)
}

// WriteStream implements interface
func (s *Swift) WriteStream(name, mimeType string, body io.Reader, tags map[string]string, dlo bool) (err error) {

	t := strings.Split(name, "/")
	var expire time.Time
	if len(t) == 2 {
		// uses the full backup timestamp
		expire, _ = time.Parse(time.RFC3339, t[0])
	}
	if expire.IsZero() {
		expire = time.Now()
	}
	backupKey := path.Join(s.serviceName, name)
	headers := swift.Headers{"X-Delete-At": strconv.FormatInt(expire.AddDate(0, 0, 7).Unix(), 10)}
	for k, v := range tags {
		headers["X-Object-Meta-"+k] = v
	}
	if !dlo {
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(body)
		if err != nil {
			return s.handleError(name, err)
		}
		f, err := s.connection.ObjectCreate(s.cfg.ContainerName, backupKey, false, "", "", headers)
		if err != nil {
			return s.handleError(name, err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				logrus.Warnf("failed to close swift file: %v", err)
			}
		}()
		_, err = f.Write(buf.Bytes())
		if err != nil {
			return s.handleError(name, err)
		}
	} else {
		chunkSize := int64(200 * 1024 * 1024)
		if s.cfg.ChunkSize != nil {
			chunkSize = *s.cfg.ChunkSize
		}
		f, err := s.connection.StaticLargeObjectCreate(&swift.LargeObjectOpts{
			Container:        s.cfg.ContainerName,
			ObjectName:       backupKey,
			CheckHash:        false,
			Headers:          headers,
			SegmentContainer: s.cfg.ContainerName + "-segments",
			SegmentPrefix:    backupKey,
			ChunkSize:        chunkSize,
		})
		if err != nil {
			return s.handleError(name, err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				logrus.Warnf("failed to close swift file: %v", err)
			}
		}()
		_, err = io.Copy(f, body)
		if err != nil {
			return s.handleError(name, err)
		}
	}

	return
}

// DownloadLatestBackup implements interface
func (s *Swift) DownloadLatestBackup() (path string, err error) {
	var b bytes.Buffer
	wr := bufio.NewWriter(&b)
	headers, err := s.connection.ObjectGet(s.cfg.ContainerName, filepath.Join(s.serviceName, LastSuccessfulBackupFile), wr, true, nil)
	if err != nil {
		return
	}
	meta := headers.ObjectMetadata()
	binlog, isset := meta["binlog"]
	if !isset {
		return path, &NoBackupError{}
	}
	key, isset := meta["key"]
	if !isset {
		return path, &NoBackupError{}
	}
	return s.DownloadBackupWithLogPosition(key, binlog)
}

// GetFullBackups implements interface
func (s *Swift) GetFullBackups() (b []Backup, err error) {
	b = make([]Backup, 0)
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: s.serviceName + "/", Delimiter: 'y'})
	if err != nil {
		return b, s.handleError("", err)
	}
	for _, o := range objs {
		if strings.Contains(o.Name, "dump.tar") {
			b = append(b, Backup{
				Storage: s.cfg.Name,
				Time:    o.LastModified,
				Key:     o.Name,
				IncList: make([]IncBackup, 0),
			})
		}
	}
	return
}

// GetIncBackupsFromDump implements interface
func (s *Swift) GetIncBackupsFromDump(key string) (bl []Backup, err error) {
	b := Backup{
		Storage: s.cfg.Name,
		IncList: make([]IncBackup, 0),
		Key:     key,
	}
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: strings.ReplaceAll(key, "dump.tar", "")})
	if err != nil {
		return bl, s.handleError("", err)
	}
	for _, o := range objs {
		if strings.Contains(o.Name, "verify_") {
			v := Verify{}
			var buf bytes.Buffer
			w := bufio.NewWriter(&buf)
			if err := s.downloadStream(w, &o); err != nil {
				return bl, err
			}
			if err := yaml.Unmarshal(buf.Bytes(), &v); err != nil {
				return bl, s.handleError("", err)
			}
			v.Time = o.LastModified
			if strings.Contains(o.Name, "verify_fail") {
				b.VerifyFail = &v
			} else {
				b.VerifySuccess = &v
			}
			continue
		}
		if strings.Contains(o.Name, backupIncomplete) {
			v := Verify{}
			v.VerifyError = "backup incomplete!!!"
			v.Time = time.Now()
			b.VerifyFail = &v
		}
		if !strings.HasSuffix(o.Name, "/") && strings.Contains(o.Name, s.logBin) {
			b.IncList = append(b.IncList, IncBackup{Key: o.Name, LastModified: o.LastModified})
		}
	}
	bl = append(bl, b)
	return
}

// DownloadBackupWithLogPosition implements interface
func (s *Swift) DownloadBackupWithLogPosition(fullBackupPath string, binlog string) (path string, err error) {
	if fullBackupPath == "" || binlog == "" {
		return path, &NoBackupError{}
	}
	binlogParts := strings.Split(binlog, ".")
	untilBinlog, err := strconv.Atoi(binlogParts[1])
	if err != nil {
		return
	}
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: fullBackupPath})
	if err != nil {
		return path, s.handleError("", err)
	}
	for _, o := range objs {
		if strings.Contains(o.Name, "dump.tar") {
			if err := s.downloadFile(s.restoreFolder, &o); err != nil {
				return path, err
			}
			continue
		}
		_, file := filepath.Split(o.Name)
		nbr := strings.Split(file, ".")
		currentBinlog, err := strconv.Atoi(nbr[1])
		if err != nil {
			continue
		}
		if currentBinlog <= untilBinlog {
			if err := s.downloadFile(s.restoreFolder, &o); err != nil {
				return path, err
			}
		}
	}
	path = filepath.Join(s.restoreFolder, fullBackupPath)
	return
}

// DownloadBackup implements interface
func (s *Swift) DownloadBackup(fullBackup Backup) (path string, err error) {
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: strings.ReplaceAll(fullBackup.Key, "dump.tar", "")})
	if err != nil {
		return path, s.handleError("", err)
	}
	for _, o := range objs {
		if !strings.HasSuffix(o.Name, "/") && !strings.Contains(o.Name, "verify") {
			if err := s.downloadFile(s.restoreFolder, &o); err != nil {
				return path, err
			}
		}
	}
	path = filepath.Join(s.restoreFolder, fullBackup.Key)
	path = filepath.Dir(path)
	return
}

func (s *Swift) downloadFile(path string, obj *swift.Object) (err error) {
	err = os.MkdirAll(filepath.Join(path, filepath.Dir(obj.Name)), os.ModePerm)
	if err != nil {
		return s.handleError("", err)
	}
	file, err := os.Create(filepath.Join(path, obj.Name))
	if err != nil {
		return s.handleError("", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			logrus.Warnf("failed to close file: %v", err)
		}
	}()
	// Check if Static Large Object
	if obj.ObjectType == 1 {
		_, objs, err := s.connection.LargeObjectGetSegments(s.cfg.ContainerName, obj.Name)
		if err != nil {
			return s.handleError("", err)
		}
		for _, i := range objs {
			_, err = s.connection.ObjectGet(s.cfg.ContainerName+"-segments", i.Name, file, false, nil)
			if err != nil {
				return s.handleError("", err)
			}
		}
	} else {
		_, err = s.connection.ObjectGet(s.cfg.ContainerName, obj.Name, file, false, nil)
		if err != nil {
			return s.handleError("", err)
		}
	}

	return
}

func (s *Swift) downloadStream(w io.Writer, obj *swift.Object) (err error) {
	_, err = s.connection.ObjectGet(s.cfg.ContainerName, obj.Name, w, false, nil)
	if err != nil {
		return s.handleError("", err)
	}
	return
}

func (s *Swift) handleError(backupKey string, err error) error {
	errS := &Error{message: "", Storage: s.cfg.Name}
	errS.message = err.Error()
	if backupKey != "" && !strings.Contains(backupKey, backupIncomplete) {
		s.statusError[path.Dir(backupKey)] = err.Error()
	}
	return errS
}
