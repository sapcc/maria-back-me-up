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

package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// DumpTools iota
type DumpTools int

const (
	// Mysqldump available database dump tool
	Mysqldump DumpTools = iota
	// MyDumper available database dump tool
	MyDumper
	// Dummy available database dump tool
	Dummy
)

func (m DumpTools) String() string {
	return [...]string{"mysqldump", "myDumper", "dummy"}[m]
}

// Config passed via a yaml file
type Config struct {
	ServiceName  string              `yaml:"service_name"`
	SideCar      *bool               `yaml:"sidecar"`
	Namespace    string              `yaml:"namespace"`
	Backup       BackupService       `yaml:"backup"`
	Storages     StorageService      `yaml:"storages"`
	Database     DatabaseConfig      `yaml:"database"`
	Verification VerificationService `yaml:"verification"`
}

// BackupService holds info for the backup service
type BackupService struct {
	Version                    string `yaml:"version"`
	OAuth                      OAuth  `yaml:"oauth"`
	BackupDir                  string `yaml:"backup_dir"`
	RestoreDir                 string `yaml:"restore_dir"`
	FullBackupCronSchedule     string `yaml:"full_backup_cron_schedule"`
	IncrementalBackupInMinutes int    `yaml:"incremental_backup_in_minutes"`
	PurgeBinlogAfterMinutes    int    `yaml:"purge_binlog_after_minutes"`
	EnableInitRestore          bool   `yaml:"enable_init_restore"`
	EnableRestoreOnDBFailure   bool   `yaml:"enable_restore_on_db_failure"`
	DisableBinlogPurgeOnRotate bool   `yaml:"disable_binlog_purge_on_rotate"`
	BinlogMaxReconnectAttempts int    `yaml:"binlog_max_reconnect_attempts"`
}

// DatabaseConfig holds info for the database to back up
type DatabaseConfig struct {
	Type          string    `yaml:"type"`
	DumpTool      DumpTools `yaml:"full_dump_tool"`
	Flavor        string    `yaml:"flavor"`
	LogNameFormat string    `yaml:"log_name_format"`
	Version       string    `yaml:"version"`
	Host          string    `yaml:"host"`
	Port          int       `yaml:"port"`
	User          string    `yaml:"user"`
	Password      string    `yaml:"password"`
	ServerID      int       `yaml:"server_id"`
	DataDir       string    `yaml:"data_dir"`
	Databases     []string  `yaml:"databases"`
	VerifyTables  []string  `yaml:"verify_tables"`
}

// StorageService list of available storage services
type StorageService struct {
	S3      []S3            `yaml:"s3"`
	Swift   []Swift         `yaml:"swift"`
	Disk    []Disk          `yaml:"disk"`
	MariaDB []MariaDBStream `yaml:"maria_db"`
}

// S3 hols info for the AWS S3 storage service
type S3 struct {
	Name                 string  `yaml:"name"`
	Verify               *bool   `yaml:"verify"`
	AwsAccessKeyID       string  `yaml:"aws_access_key_id"`
	AwsSecretAccessKey   string  `yaml:"aws_secret_access_key"`
	AwsEndpoint          string  `yaml:"aws_endpoint"`
	SSECustomerAlgorithm *string `yaml:"sse_customer_algorithm"`
	S3ForcePathStyle     *bool   `yaml:"s3_force_path_style"`
	SSECustomerKey       *string `yaml:"sse_customer_key"`
	Region               string  `yaml:"region"`
	BucketName           string  `yaml:"bucket_name"`
}

// Swift holds info for the OS swift storage service
type Swift struct {
	Name              string `yaml:"name"`
	Verify            *bool  `yaml:"verify"`
	AuthVersion       int    `yaml:"auth_version"`
	AuthURL           string `yaml:"auth_url"`
	UserName          string `yaml:"user_name"`
	UserDomainName    string `yaml:"user_domain_name"`
	ProjectName       string `yaml:"project_name"`
	ProjectDomainName string `yaml:"project_domain_name"`
	Password          string `yaml:"password"`
	Region            string `yaml:"region"`
	ContainerName     string `yaml:"container_name"`
	ChunkSize         *int64 `yaml:"chunk_size"` // default 200mb
	SloSize           *int64 `yaml:"slo_size"`   // default 600mb
}

// Disk holds info for the local backup storage
type Disk struct {
	Name      string `yaml:"name"`
	Verify    *bool  `yaml:"verify"`
	BasePath  string `yaml:"base_path"`
	Retention int    `yaml:"retention"`
}

// MariaDBStream holds info for the replication to another MariaDB
type MariaDBStream struct {
	Name                   string `yaml:"name"`
	Host                   string `yaml:"host"`
	Port                   int    `yaml:"port"`
	User                   string `yaml:"user"`
	Password               string `yaml:"password"`
	DumpTool               DumpTools
	Databases              []string `yaml:"databases"`
	ParseSchema            bool     `yaml:"parse_schema"`
	DumpFilterBufferSizeMB int      `yaml:"dump_filter_buffer_size_mb"`
}

// OAuth holds info for the api oauth middleware
type OAuth struct {
	SAPID       bool   `yaml:"sap_id"`
	Middleware  bool   `yaml:"middleware"`
	ProviderURL string `yaml:"provider_url"`
	RedirectURL string `yaml:"redirect_url"`
}

// VerificationService holds info for the backup verification service
type VerificationService struct {
	RunAfterIncBackups int `yaml:"run_after_inc_backups"`
	MariaDBVersion     string
	StorageServices    *[]string
}

// GetConfig returns the config struct from a yaml file
func GetConfig(opts Options) (cfg Config, err error) {
	if opts.ConfigFilePath == "" {
		return cfg, errors.New("no config file provided")
	}
	yamlBytes, err := os.ReadFile(opts.ConfigFilePath)
	if err != nil {
		return cfg, fmt.Errorf("read config file: %s", err.Error())
	}
	err = yaml.Unmarshal(yamlBytes, &cfg)
	if err != nil {
		return cfg, fmt.Errorf("parse config file: %s", err.Error())
	}

	for i := range cfg.Storages.MariaDB {
		cfg.Storages.MariaDB[i].DumpTool = cfg.Database.DumpTool
	}

	setDefaults(cfg)

	return cfg, nil
}

// setDefaults sets default values for config keys
func setDefaults(cfg Config) {
	if cfg.Backup.BinlogMaxReconnectAttempts == 0 {
		cfg.Backup.BinlogMaxReconnectAttempts = 10
	}

	for i, m := range cfg.Storages.MariaDB {
		if m.DumpFilterBufferSizeMB == 0 {
			cfg.Storages.MariaDB[i].DumpFilterBufferSizeMB = 2
		}
	}
}
