// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/log"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

var logger *logrus.Entry

type (
	// S3 struct is ...
	S3 struct {
		cfg           config.S3
		client        *s3.Client
		uploader      *transfermanager.Client
		downloader    *transfermanager.Client
		serviceName   string
		restoreFolder string
		logger        *logrus.Entry `yaml:"-"`
		binLog        string
		statusError   map[string]string
	}
)

// NewS3 creates a s3 storage instance
func NewS3(c config.S3, serviceName, restoreFolder, binLog string) (s3Storage *S3, err error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(c.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AwsAccessKeyID, c.AwsSecretAccessKey, ""),
		),
	)
	if err != nil {
		logger.Fatal(err)
	}

	clientOpts := []func(*s3.Options){
		func(o *s3.Options) {
			o.BaseEndpoint = aws.String(c.AwsEndpoint)
		},
	}
	if c.S3ForcePathStyle != nil && *c.S3ForcePathStyle {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	client := s3.NewFromConfig(cfg, clientOpts...)

	uploader := transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = 20 << 20 // 20MB
		o.Concurrency = 5
	})
	downloader := transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = 64 * 1024 * 1024 // 64MB per part
		o.Concurrency = 6
	})

	return &S3{
		cfg:           c,
		client:        client,
		uploader:      uploader,
		downloader:    downloader,
		serviceName:   serviceName,
		restoreFolder: path.Join(restoreFolder, c.Name),
		logger:        logger.WithField("service", serviceName),
		binLog:        binLog,
		statusError:   make(map[string]string, 0),
	}, err
}

// GetStorageServiceName implements interface
func (s *S3) GetStorageServiceName() (name string) {
	return s.cfg.Name
}

// Verify implements interface
func (s *S3) Verify() bool {
	if s.cfg.Verify == nil {
		return false
	}
	return *s.cfg.Verify
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
	var tag strings.Builder
	for k, v := range tags {
		fmt.Fprintf(&tag, "%s=%s&", k, v)
	}

	input := &transfermanager.UploadObjectInput{
		Bucket:               aws.String(s.cfg.BucketName),
		Key:                  aws.String(path.Join(s.serviceName, fileName)),
		Body:                 body,
		SSECustomerAlgorithm: s.cfg.SSECustomerAlgorithm,
		SSECustomerKey:       s.cfg.SSECustomerKey,
		Tagging:              aws.String(tag.String()),
	}

	_, err := s.uploader.UploadObject(context.Background(), input)
	if err != nil {
		return s.handleError(fileName, err)
	}
	return nil
}

// DownloadBackupWithLogPosition implements interface
func (s *S3) DownloadBackupWithLogPosition(fullBackupPath, binlog string) (backupPath string, err error) {
	if fullBackupPath == "" || binlog == "" {
		return backupPath, &NoBackupError{}
	}
	binlogParts := strings.Split(binlog, ".")
	untilBinlog, err := strconv.Atoi(binlogParts[1])
	if err != nil {
		return backupPath, s.handleError("", err)
	}
	listRes, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(fullBackupPath)})
	if err != nil {
		return backupPath, s.handleError("", err)
	}
	if len(listRes.Contents) == 0 {
		return backupPath, &NoBackupError{}
	}
	for _, listObj := range listRes.Contents {
		if err != nil {
			continue
		}
		if strings.Contains(*listObj.Key, "dump.tar") {
			if err := s.downloadFile(s.restoreFolder, listObj); err != nil {
				return backupPath, err
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
				return backupPath, err
			}
		}
	}
	backupPath = filepath.Join(s.restoreFolder, fullBackupPath)
	return
}

// GetTotalIncBackupsFromDump implements interface
func (s *S3) GetTotalIncBackupsFromDump(key string) (t int, err error) {
	t = 0
	list, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(strings.ReplaceAll(key, "dump.tar", ""))})
	if err != nil {
		return t, s.handleError("", err)
	}
	for _, incObj := range list.Contents {
		if !strings.HasSuffix(*incObj.Key, "/") && strings.Contains(*incObj.Key, s.binLog) {
			t++
		}
	}
	return
}

// GetIncBackupsFromDump implements interface
func (s *S3) GetIncBackupsFromDump(key string) (bl []Backup, err error) {
	b := Backup{
		Storage: s.cfg.Name,
		IncList: make([]IncBackup, 0),
		Key:     key,
	}

	list, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(strings.ReplaceAll(key, "dump.tar", ""))})
	if err != nil {
		return bl, s.handleError("", err)
	}
	for _, incObj := range list.Contents {
		if strings.Contains(*incObj.Key, "verify_") {
			v := Verify{}
			w := tmtypes.NewWriteAtBuffer([]byte{})
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
		if strings.Contains(*incObj.Key, backupIncomplete) {
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
	listRes, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(s.serviceName + "/"), Delimiter: aws.String("y")})
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
	listRes, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.BucketName), Prefix: aws.String(strings.ReplaceAll(fullBackup.Key, "dump.tar", ""))})
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
	tag, err := s.client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String(s.cfg.BucketName), Key: aws.String(filepath.Join(s.serviceName, LastSuccessfulBackupFile))})
	if err != nil {
		return path, s.handleError("", err)
	}

	if len(tag.TagSet) == 2 {
		var key string
		var binlog string
		for _, t := range tag.TagSet {
			if aws.ToString(t.Key) == "key" {
				key = aws.ToString(t.Value)
			}
			if aws.ToString(t.Key) == "binlog" {
				binlog = aws.ToString(t.Value)
			}
		}

		return s.DownloadBackupWithLogPosition(key, binlog)
	}

	return path, &NoBackupError{}
}

func (s *S3) downloadFile(path string, obj types.Object) error {
	if err := os.MkdirAll(filepath.Join(path, filepath.Dir(*obj.Key)), os.ModePerm); err != nil {
		return s.handleError("", err)
	}
	file, err := os.Create(filepath.Join(path, *obj.Key))
	if err != nil {
		return s.handleError("", err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			logger.Warnf("failed to close file: %v", err)
		}
	}()

	if _, err := s.downloader.DownloadObject(context.Background(),
		&transfermanager.DownloadObjectInput{
			Bucket:               aws.String(s.cfg.BucketName),
			Key:                  obj.Key,
			WriterAt:             file,
			SSECustomerAlgorithm: s.cfg.SSECustomerAlgorithm,
			SSECustomerKey:       s.cfg.SSECustomerKey,
		}); err != nil {
		return s.handleError("", err)
	}
	return nil
}

func (s *S3) downloadStream(w io.WriterAt, obj types.Object) error {
	if _, err := s.downloader.DownloadObject(context.Background(),
		&transfermanager.DownloadObjectInput{
			Bucket:               aws.String(s.cfg.BucketName),
			Key:                  obj.Key,
			WriterAt:             w,
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
	if backupKey != "" && !strings.Contains(backupKey, backupIncomplete) {
		s.statusError[path.Dir(backupKey)] = err.Error()
	}
	return errS
}
