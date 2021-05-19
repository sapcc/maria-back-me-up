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
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
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
	// S3 struct is ...
	S3 struct {
		cfg           config.S3
		session       *session.Session
		serviceName   string
		restoreFolder string
		logger        *logrus.Entry `yaml:"-"`
		binLog        string
		statusError   map[string]string
	}
)

// NewS3 creates a s3 storage instance
func NewS3(c config.S3, serviceName, binLog string) (s3 *S3, err error) {
	s, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(c.AwsEndpoint),
		Region:           aws.String(c.Region),
		Credentials:      credentials.NewStaticCredentials(c.AwsAccessKeyID, c.AwsSecretAccessKey, ""),
		S3ForcePathStyle: c.S3ForcePathStyle,
	})
	if err != nil {
		logger.Fatal(err)
	}

	return &S3{
		cfg:           c,
		session:       s,
		serviceName:   serviceName,
		restoreFolder: path.Join(constants.RESTOREFOLDER, c.Name),
		logger:        logger.WithField("service", serviceName),
		binLog:        binLog,
		statusError:   make(map[string]string, 0),
	}, err
}

// GetStorageServiceName implements interface
func (s *S3) GetStorageServiceName() (name string) {
	return s.cfg.Name
}

// GetStatusError implements interface
func (s *S3) GetStatusError() map[string]string {
	return s.statusError
}

// GetStatusErrorByKey implements interface
func (s *S3) GetStatusErrorByKey(backupKey string) string {
	if st, ok := s.statusError[path.Dir(backupKey)]; ok {
		return st
	}
	return ""
}

// WriteFolder implements interface
func (s *S3) WriteFolder(p string) (err error) {
	r, err := ZipFolderPath(p)
	if err != nil {
		return s.handleError(p, err)
	}
	return s.WriteStream(path.Join(filepath.Base(p), "dump.tar"), "zip", r, nil, false)
}

// WriteStream implements interface
func (s *S3) WriteStream(fileName, mimeType string, body io.Reader, tags map[string]string, dlo bool) error {
	uploader := s3manager.NewUploader(s.session, func(u *s3manager.Uploader) {
		u.PartSize = 20 << 20 // 20MB
		u.MaxUploadParts = 10000
	})
	var tag string
	for k, v := range tags {
		tag += fmt.Sprintf("%s=%s&", k, v)
	}

	input := s3manager.UploadInput{
		Bucket:               aws.String(s.cfg.BucketName),
		Key:                  aws.String(path.Join(s.serviceName, fileName)),
		Body:                 body,
		SSECustomerAlgorithm: s.cfg.SSECustomerAlgorithm,
		SSECustomerKey:       s.cfg.SSECustomerKey,
		Tagging:              &tag,
	}

	_, err := uploader.Upload(&input)
	if err != nil {
		return s.handleError(fileName, err)
	}
	return nil
}

// DownloadBackupWithLogPosition implements interface
func (s *S3) DownloadBackupWithLogPosition(fullBackupPath, binlog string) (path string, err error) {
	if fullBackupPath == "" || binlog == "" {
		return path, &NoBackupError{}
	}
	svc := s3.New(s.session)
	untils := strings.Split(binlog, ".")
	untilBinlog, err := strconv.Atoi(untils[1])
	listRes, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(fullBackupPath)})
	if err != nil {
		return path, s.handleError("", err)
	}
	if len(listRes.Contents) == 0 {
		return path, &NoBackupError{}
	}
	for _, listObj := range listRes.Contents {
		if err != nil {
			continue
		}
		if strings.Contains(*listObj.Key, "dump.tar") {
			if err := s.downloadFile(s.restoreFolder, listObj); err != nil {
				return path, err
			}
			continue
		}
		_, file := filepath.Split(*listObj.Key)
		nbr := strings.Split(file, ".")
		currentBinlog, err := strconv.Atoi(nbr[1])
		if err != nil {
			continue
		}
		if currentBinlog <= untilBinlog {
			if err := s.downloadFile(s.restoreFolder, listObj); err != nil {
				return path, err
			}
		}
	}
	path = filepath.Join(s.restoreFolder, fullBackupPath)
	return
}

// GetIncBackupsFromDump implements interface
func (s *S3) GetIncBackupsFromDump(key string) (bl []Backup, err error) {
	svc := s3.New(s.session)
	b := Backup{
		Storage: s.cfg.Name,
		IncList: make([]IncBackup, 0),
		Key:     key,
	}

	list, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(strings.Replace(key, "dump.tar", "", -1))})
	if err != nil {
		return bl, s.handleError("", err)
	}
	for _, incObj := range list.Contents {
		if strings.Contains(*incObj.Key, "verify_") {
			v := Verify{}
			w := aws.NewWriteAtBuffer([]byte{})
			if err := s.downloadStream(w, incObj); err != nil {
				return bl, s.handleError("", err)
			}
			err = yaml.Unmarshal(w.Bytes(), &v)
			v.Time = *incObj.LastModified
			if strings.Contains(*incObj.Key, "verify_fail") {
				b.VerifyFail = &v
			} else {
				b.VerifySuccess = &v
			}
			continue
		}
		if strings.Contains(*incObj.Key, backupIcomplete) {
			v := Verify{}
			v.VerifyError = "backup incomplete!!!"
			v.Time = time.Now()
			b.VerifyFail = &v
		}
		if !strings.HasSuffix(*incObj.Key, "/") && strings.Contains(*incObj.Key, s.binLog) {
			b.IncList = append(b.IncList, IncBackup{Key: *incObj.Key, LastModified: *incObj.LastModified})
		}
	}
	bl = append(bl, b)
	return
}

// GetFullBackups implements interface
func (s *S3) GetFullBackups() (bl []Backup, err error) {
	svc := s3.New(s.session)
	listRes, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(s.serviceName + "/"), Delimiter: aws.String("y")})
	if err != nil {
		return bl, s.handleError("", err)
	}

	for _, fullObj := range listRes.Contents {
		if err != nil {
			log.Error(err.Error())
			continue
		}

		if strings.Contains(*fullObj.Key, "dump.tar") {
			b := Backup{
				Storage: s.cfg.Name,
				Time:    *fullObj.LastModified,
				Key:     *fullObj.Key,
				IncList: make([]IncBackup, 0),
			}
			bl = append(bl, b)
		}
	}
	return
}

// DownloadBackup implements interface
func (s *S3) DownloadBackup(fullBackup Backup) (path string, err error) {
	svc := s3.New(s.session)
	listRes, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(strings.Replace(fullBackup.Key, "dump.tar", "", -1))})
	if err != nil {
		return path, s.handleError("", err)
	}

	for _, listObj := range listRes.Contents {
		if !strings.HasSuffix(*listObj.Key, "/") {
			if err := s.downloadFile(s.restoreFolder, listObj); err != nil {
				return path, s.handleError("", err)
			}
		}
	}
	path = filepath.Join(s.restoreFolder, fullBackup.Key)
	path = filepath.Dir(path)
	return
}

// DownloadLatestBackup implements interface
func (s *S3) DownloadLatestBackup() (path string, err error) {
	svc := s3.New(s.session)
	tag, err := svc.GetObjectTagging(&s3.GetObjectTaggingInput{Bucket: aws.String(s.cfg.BucketName), Key: aws.String(s.serviceName + "/last_successful_backup")})
	if err != nil {
		return path, s.handleError("", err)
	}

	if len(tag.TagSet) == 2 {
		var key string
		var binlog string
		for _, t := range tag.TagSet {
			if *t.Key == "key" {
				key = *t.Value
			}
			if *t.Key == "binlog" {
				binlog = *t.Value
			}
		}

		return s.DownloadBackupWithLogPosition(key, binlog)
	}

	return path, &NoBackupError{}
}

func (s *S3) downloadFile(path string, obj *s3.Object) error {
	if err := os.MkdirAll(filepath.Join(path, filepath.Dir(*obj.Key)), os.ModePerm); err != nil {
		return s.handleError("", err)
	}
	file, err := os.Create(filepath.Join(path, *obj.Key))
	if err != nil {
		return s.handleError("", err)
	}

	defer file.Close()
	// Create a downloader with the session and custom options
	downloader := s3manager.NewDownloader(s.session, func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
		d.Concurrency = 6
	})
	if _, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket:               aws.String(s.cfg.BucketName),
			Key:                  aws.String(*obj.Key),
			SSECustomerAlgorithm: s.cfg.SSECustomerAlgorithm,
			SSECustomerKey:       s.cfg.SSECustomerKey,
		}); err != nil {
		return s.handleError("", err)
	}
	return nil
}

func (s *S3) downloadStream(w io.WriterAt, obj *s3.Object) error {
	downloader := s3manager.NewDownloader(s.session, func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
		d.Concurrency = 6
	})
	if _, err := downloader.Download(w,
		&s3.GetObjectInput{
			Bucket:               aws.String(s.cfg.BucketName),
			Key:                  aws.String(*obj.Key),
			SSECustomerAlgorithm: s.cfg.SSECustomerAlgorithm,
			SSECustomerKey:       s.cfg.SSECustomerKey,
		}); err != nil {
		return s.handleError("", err)
	}
	return nil
}

func (s *S3) handleError(backupKey string, err error) error {
	errS := &Error{message: "", Storage: s.cfg.Name}
	errS.message = err.Error()
	if backupKey != "" && !strings.Contains(backupKey, backupIcomplete) {
		s.statusError[path.Dir(backupKey)] = err.Error()
	}
	return errS
}
