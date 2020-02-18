/**
 * Copyright 2019 SAP SE
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
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

var logger *logrus.Entry

type (
	S3 struct {
		cfg         []config.S3
		sessions    []*session.Session
		serviceName string
		Storage     map[int]string
	}
	Verify struct {
		Backup int    `yaml:"verify_backup"`
		Tables int    `yaml:"verify_tables"`
		Error  string `yaml:"verify_error"`
		Time   time.Time
	}

	Backup struct {
		Storage int
		Time    time.Time
		Key     string
		IncList []s3.Object
		Verify  []Verify
	}
)

func init() {
	logger = log.WithFields(logrus.Fields{"component": "s3"})
}

func NewS3(c config.Config, sn string) (s3 *S3, err error) {
	ms := make([]*session.Session, 2)
	mc := make([]config.S3, 2)
	st := make(map[int]string, 0)
	for i, s3 := range c.S3 {
		s, err := session.NewSession(&aws.Config{
			Endpoint:         aws.String(s3.AwsEndpoint),
			Region:           aws.String(s3.Region),
			Credentials:      credentials.NewStaticCredentials(s3.AwsAccessKeyID, s3.AwsSecretAccessKey, ""),
			S3ForcePathStyle: s3.S3ForcePathStyle,
		})
		if err != nil {
			logger.Fatal(err)
		}
		ms[i] = s
		mc[i] = s3
		st[i] = s3.Name
	}

	return &S3{
		cfg:         mc,
		sessions:    ms,
		serviceName: sn,
		Storage:     st,
	}, err
}

func (s *S3) GetRemoteStorageServices() (storages map[int]string) {
	return s.Storage
}

/*
func (s *S3) WriteBytes(f string, b []byte) (err error) {
	pr, pw := io.Pipe()
	go func() {
		gw := gzip.NewWriter(pw)
		_, err := gw.Write(b)
		gw.Close()
		pw.Close()
		if err != nil {
			fmt.Errorf("WriteBytes: Failed to upload to s3. Error: %s", err.Error())
		}
	}()
	uploader := s3manager.NewUploader(s.session)
	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:                 pr,
		Bucket:               aws.String(s.cfg.BucketName),
		Key:                  aws.String(path.Join(s.serviceName, f)),
		ServerSideEncryption: aws.String("AES256"),
	})
	if err != nil {
		return fmt.Errorf("WriteBytes: Failed to upload to s3. Error: %s", err.Error())
	}
	logger.Debugln("Successfully uploaded to", result.Location)

	return
}
*/
func (s *S3) WriteFolder(backup int, p string) (err error) {
	r, err := s.zipFolderPath(p)
	if err != nil {
		return
	}
	return s.WriteStream(backup, path.Join(filepath.Base(p), "dump.tar"), "zip", r)
}

func (s *S3) WriteStream(backup int, fileName, mimeType string, body io.Reader) (err error) {
	uploader := s3manager.NewUploader(s.sessions[backup], func(u *s3manager.Uploader) {
		u.PartSize = 20 << 20 // 20MB
		u.MaxUploadParts = 10000
	})

	input := s3manager.UploadInput{
		Bucket:               aws.String(s.cfg[backup].BucketName),
		Key:                  aws.String(path.Join(s.serviceName, fileName)),
		Body:                 body,
		ServerSideEncryption: s.cfg[backup].ServerSideEncryption,
	}
	_, err = uploader.Upload(&input)
	if err != nil {
		return &StorageError{
			message: fmt.Sprintf("WriteStream: Failed to upload to storage no %s. Error: %s", s.Storage[backup], err.Error()),
			Storage: s.Storage[backup],
		}
	}
	return
}
func (s *S3) DownloadBackupFrom(backup int, fullBackupPath, binlog string) (path string, err error) {
	svc := s3.New(s.sessions[backup])
	until := strings.Split(binlog, ".")
	listRes, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg[backup].BucketName), Prefix: aws.String(fullBackupPath)})
	if err != nil {
		return
	}
	for _, listObj := range listRes.Contents {
		if err != nil {
			continue
		}
		if strings.Contains(*listObj.Key, "dump.tar") {
			s.downloadFile(backup, constants.RESTOREFOLDER, listObj)
			continue
		}
		_, file := filepath.Split(*listObj.Key)
		nbr := strings.Split(file, ".")
		if nbr[1] <= until[1] {
			s.downloadFile(backup, constants.RESTOREFOLDER, listObj)
		}
	}
	path = filepath.Join(constants.RESTOREFOLDER, fullBackupPath)
	return
}

func (s *S3) ListIncBackupsFor(backup int, key string) (bl []Backup, err error) {
	svc := s3.New(s.sessions[backup])
	b := Backup{
		Storage: backup,
		IncList: make([]s3.Object, 0),
		Verify:  make([]Verify, 0),
		Key:     key,
	}

	list, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg[backup].BucketName), Prefix: aws.String(strings.Replace(key, "dump.tar", "", -1))})
	if err != nil {
		log.Error(err.Error())
		return
	}
	for _, incObj := range list.Contents {
		if strings.Contains(*incObj.Key, "verify_") {
			v := Verify{}
			w := aws.NewWriteAtBuffer([]byte{})
			s.downloadStream(backup, w, incObj)
			err = yaml.Unmarshal(w.Bytes(), &v)
			v.Time = *incObj.LastModified
			b.Verify = append(b.Verify, v)
			continue
		}
		if !strings.HasSuffix(*incObj.Key, "/") && !strings.Contains(*incObj.Key, "dump.tar") {
			b.IncList = append(b.IncList, *incObj)
		}
	}
	bl = append(bl, b)
	return
}

func (s *S3) ListFullBackups(backup int) (bl []Backup, err error) {
	svc := s3.New(s.sessions[backup])
	listRes, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg[backup].BucketName), Prefix: aws.String(s.serviceName + "/"), Delimiter: aws.String("y")})
	if err != nil {
		return
	}

	for _, fullObj := range listRes.Contents {
		if err != nil {
			log.Error(err.Error())
			continue
		}

		if strings.Contains(*fullObj.Key, "dump.tar") {
			b := Backup{
				Storage: backup,
				Time:    *fullObj.LastModified,
				Key:     *fullObj.Key,
				IncList: make([]s3.Object, 0),
				Verify:  make([]Verify, 0),
			}
			bl = append(bl, b)
		}
	}
	return
}

func (s *S3) DownloadLatestBackup(backup int) (path string, err error) {
	var newestBackup *s3.Object
	var newestTime int64 = 0
	svc := s3.New(s.sessions[backup])
	listRes, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg[backup].BucketName), Prefix: aws.String(s.serviceName + "/"), Delimiter: aws.String("y")})
	if err != nil {
		return
	}
	for _, listObj := range listRes.Contents {
		if err != nil {
			continue
		}

		if strings.Contains(*listObj.Key, "dump.tar") {
			currTime := listObj.LastModified.Unix()
			if currTime > newestTime {
				newestTime = currTime
				newestBackup = listObj
			}
		}
	}

	if newestBackup == nil {
		return path, &NoBackupError{}
	}

	listRes, err = svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg[backup].BucketName), Prefix: aws.String(strings.Replace(*newestBackup.Key, "dump.tar", "", -1))})
	if err != nil {
		return
	}

	for _, listObj := range listRes.Contents {
		if !strings.HasSuffix(*listObj.Key, "/") {
			s.downloadFile(backup, constants.RESTOREFOLDER, listObj)
		}
	}
	path = filepath.Join(constants.RESTOREFOLDER, *newestBackup.Key)
	path = filepath.Dir(path)
	return
}

func (s *S3) GetBackupByTimestamp(backup int, t time.Time) (path string, err error) {
	var s3Backup *s3.Object
	minAge := 100.0
	svc := s3.New(s.sessions[backup])
	listRes, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg[backup].BucketName), Prefix: aws.String(s.serviceName + "/")})
	if err != nil {
		return
	}

	for _, listObj := range listRes.Contents {
		if err != nil {
			continue
		}

		if strings.Contains(*listObj.Key, "dump.tar") && t.After(*listObj.LastModified) {
			age := t.Sub(*listObj.LastModified).Minutes()
			if age < minAge {
				minAge = age
				s3Backup = listObj
			}
			continue
		}
		if t.After(*listObj.LastModified) {
			continue
		}
	}

	if s3Backup == nil {
		return path, &NoBackupError{}
	}

	listRes, err = svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg[backup].BucketName), Prefix: aws.String(strings.Replace(*s3Backup.Key, "dump.tar", "", -1))})
	if err != nil {
		return
	}

	for _, listObj := range listRes.Contents {
		if !strings.HasSuffix(*listObj.Key, "/") {
			if listObj.LastModified.Before(t) {
				s.downloadFile(backup, constants.RESTOREFOLDER, listObj)
			}
		}
	}
	path = filepath.Join(constants.RESTOREFOLDER, *s3Backup.Key)
	path = filepath.Dir(path)
	return
}

func (s *S3) downloadFile(backup int, path string, obj *s3.Object) (err error) {
	err = os.MkdirAll(filepath.Join(path, filepath.Dir(*obj.Key)), os.ModePerm)
	if err != nil {
		return
	}
	file, err := os.Create(filepath.Join(path, *obj.Key))
	if err != nil {
		return fmt.Errorf("error in downloading from file: %v", err)
	}

	defer file.Close()
	// Create a downloader with the session and custom options
	downloader := s3manager.NewDownloader(s.sessions[backup], func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
		d.Concurrency = 6
	})
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(s.cfg[backup].BucketName),
			Key:    aws.String(*obj.Key),
		})
	return
}

func (s *S3) downloadStream(backup int, w io.WriterAt, obj *s3.Object) (err error) {
	downloader := s3manager.NewDownloader(s.sessions[backup], func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
		d.Concurrency = 6
	})
	_, err = downloader.Download(w,
		&s3.GetObjectInput{
			Bucket: aws.String(s.cfg[backup].BucketName),
			Key:    aws.String(*obj.Key),
		})
	return
}

func (s *S3) zipFolderPath(pathToZip string) (pr *io.PipeReader, err error) {
	dir, err := os.Open(pathToZip)
	if err != nil {
		return
	}
	defer dir.Close()

	// get list of files
	files, err := dir.Readdir(0)
	if err != nil {
		return
	}

	pr, pw := io.Pipe()
	tarfileWriter := tar.NewWriter(pw)
	go func() {
		for _, fileInfo := range files {

			if fileInfo.IsDir() {
				continue
			}

			file, err := os.Open(dir.Name() + string(filepath.Separator) + fileInfo.Name())
			if err != nil {
				return
			}
			defer file.Close()

			// prepare the tar header
			header := new(tar.Header)
			header.Name = filepath.Base(file.Name())
			header.Size = fileInfo.Size()
			header.Mode = int64(fileInfo.Mode())
			header.ModTime = fileInfo.ModTime()

			err = tarfileWriter.WriteHeader(header)
			if err != nil {
				return
			}

			_, err = io.Copy(tarfileWriter, file)
			if err != nil {
				return
			}
		}
		tarfileWriter.Close()
		pw.Close()
	}()
	return
}
