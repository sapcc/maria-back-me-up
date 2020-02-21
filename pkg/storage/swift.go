package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ncw/swift"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"gopkg.in/yaml.v2"
)

type Swift struct {
	cfg            config.Swift
	connection     *swift.Connection
	serviceName    string
	storageService string
}

func NewSwift(c config.Swift, sn string) (s *Swift, err error) {
	conn := &swift.Connection{
		AuthVersion:  c.AuthVersion,
		AuthUrl:      c.AuthUrl,
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

	return &Swift{cfg: c, connection: conn, storageService: c.Name, serviceName: sn}, err
}

func (s *Swift) GetStorageServiceName() (storages string) {
	return s.cfg.Name
}

func (s *Swift) WriteFolder(p string) (err error) {
	err = &StorageError{message: "", Storage: s.cfg.Name}
	r, err := zipFolderPath(p)
	if err != nil {
		return
	}
	return s.WriteStream(path.Join(filepath.Base(p), "dump.tar"), "zip", r)
}
func (s *Swift) WriteStream(name, mimeType string, body io.Reader) (err error) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(body)
	f, err := s.connection.ObjectCreate(s.cfg.ContainerName, path.Join(s.serviceName, name), false, "", "", swift.Headers{"X-Delete-At": strconv.FormatInt(time.Now().AddDate(0, 0, 7).Unix(), 10)})
	defer f.Close()
	if err != nil {
		return
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		return
	}
	return
	//return s.connection.ObjectPutBytes(s.cfg.ContainerName, path.Join(s.serviceName, name), buf.Bytes(), swift.Headers{"X-Delete-At": strconv.FormatInt(time.Now().AddDate(0, 0, 7).Unix(), 10)})
	//_, err = s.connection.ObjectPut(s.cfg.ContainerName, path.Join(s.serviceName, name), body, false, "", "", swift.Headers{"X-Delete-At": strconv.FormatInt(time.Now().AddDate(0, 0, 7).Unix(), 10)})
	//return
}
func (s *Swift) GetBackupByTimestamp(t time.Time) (path string, err error) {
	return
}
func (s *Swift) DownloadLatestBackup() (path string, err error) {
	var newestBackup *swift.Object
	var newestTime int64 = 0
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: s.serviceName + "/", Delimiter: 'y'})
	for _, o := range objs {
		if strings.Contains(o.Name, "dump.tar") {
			currTime := o.LastModified.Unix()
			if currTime > newestTime {
				newestTime = currTime
				newestBackup = &o
			}
		}
	}
	if newestBackup == nil {
		return path, &NoBackupError{}
	}
	objs, err = s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: strings.Replace(newestBackup.Name, "dump.tar", "", -1)})
	for _, o := range objs {
		if !strings.HasSuffix(o.Name, "/") {
			s.downloadFile(constants.RESTOREFOLDER, &o)
		}
	}
	path = filepath.Join(constants.RESTOREFOLDER, newestBackup.Name)
	path = filepath.Dir(path)
	return
}
func (s *Swift) ListFullBackups() (b []Backup, err error) {
	b = make([]Backup, 0)
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: s.serviceName + "/", Delimiter: 'y'})
	for _, o := range objs {
		if strings.Contains(o.Name, "dump.tar") {
			b = append(b, Backup{
				Storage: s.storageService,
				Time:    o.LastModified,
				Key:     o.Name,
				IncList: make([]IncBackup, 0),
				Verify:  make([]Verify, 0),
			})
		}
	}
	return
}
func (s *Swift) ListIncBackupsFor(key string) (bl []Backup, err error) {
	b := Backup{
		Storage: s.storageService,
		IncList: make([]IncBackup, 0),
		Verify:  make([]Verify, 0),
		Key:     key,
	}
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: strings.Replace(key, "dump.tar", "", -1)})
	if err != nil {
		log.Error(err.Error())
		return
	}
	for _, o := range objs {
		if strings.Contains(o.Name, "verify_") {
			v := Verify{}
			var buf bytes.Buffer
			w := bufio.NewWriter(&buf)
			s.downloadStream(w, &o)
			err = yaml.Unmarshal(buf.Bytes(), &v)
			v.Time = o.LastModified
			b.Verify = append(b.Verify, v)
			continue
		}
		if !strings.HasSuffix(o.Name, "/") && !strings.Contains(o.Name, "dump.tar") {
			b.IncList = append(b.IncList, IncBackup{Key: o.Name, LastModified: o.LastModified})
		}
	}
	bl = append(bl, b)
	return
}
func (s *Swift) DownloadBackupFrom(fullBackupPath string, binlog string) (path string, err error) {
	until := strings.Split(binlog, ".")
	objs, err := s.connection.ObjectsAll(s.cfg.ContainerName, &swift.ObjectsOpts{Prefix: fullBackupPath})
	if err != nil {
		return
	}
	for _, o := range objs {
		if strings.Contains(o.Name, "dump.tar") {
			s.downloadFile(constants.RESTOREFOLDER, &o)
			continue
		}
		_, file := filepath.Split(o.Name)
		nbr := strings.Split(file, ".")
		if nbr[1] <= until[1] {
			s.downloadFile(constants.RESTOREFOLDER, &o)
		}
	}
	path = filepath.Join(constants.RESTOREFOLDER, fullBackupPath)
	return
}

func (s *Swift) downloadFile(path string, obj *swift.Object) (err error) {
	err = os.MkdirAll(filepath.Join(path, filepath.Dir(obj.Name)), os.ModePerm)
	if err != nil {
		return
	}
	file, err := os.Create(filepath.Join(path, obj.Name))
	if err != nil {
		return fmt.Errorf("error in downloading from file: %v", err)
	}

	defer file.Close()
	// Create a downloader with the session and custom options
	s.connection.ObjectGet(s.cfg.ContainerName, obj.Name, file, false, nil)
	return
}

func (s *Swift) downloadStream(w io.Writer, obj *swift.Object) (err error) {
	_, err = s.connection.ObjectGet(s.cfg.ContainerName, obj.Name, w, false, nil)
	return
}
