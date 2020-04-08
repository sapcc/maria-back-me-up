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
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	ServiceName         string              `yaml:"service_name"`
	SideCar             *bool               `yaml:"side_car"`
	Namespace           string              `yaml:"namespace"`
	StorageService      StorageService      `yaml:"storage_services"`
	BackupService       BackupService       `yaml:"backup_service"`
	VerificationService VerificationService `yaml:"verification_services"`
}

type BackupService struct {
	Version                    string  `yaml:"version"`
	MariaDB                    MariaDB `yaml:"maria_db"`
	OAuth                      OAuth   `yaml:"oauth"`
	BackupDir                  string  `yaml:"backup_dir"`
	FullBackupCronSchedule     string  `yaml:"full_backup_cron_schedule"`
	IncrementalBackupInMinutes int     `yaml:"incremental_backup_in_minutes"`
	EnableInitRestore          bool    `yaml:"enable_init_restore"`
	EnableRestoreOnDBFailure   bool    `yaml:"enable_restore_on_db_failure"`
	DumpTool                   *string `yaml:"full_dump_tool"`
}

type MariaDB struct {
	Flavor       string   `yaml:"flavor"`
	LogBin       string   `yaml:"log_bin"`
	Version      string   `yaml:"version"`
	Host         string   `yaml:"host"`
	Port         int      `yaml:"port"`
	User         string   `yaml:"user"`
	Password     string   `yaml:"password"`
	DataDir      string   `yaml:"data_dir"`
	Databases    []string `yaml:"databases"`
	VerifyTables []string `yaml:"verify_tables"`
}

type StorageService struct {
	S3    []S3    `yaml:"s3"`
	Swift []Swift `yaml:"swift"`
}

type S3 struct {
	Name                 string  `yaml:"name"`
	AwsAccessKeyID       string  `yaml:"aws_access_key_id"`
	AwsSecretAccessKey   string  `yaml:"aws_secret_access_key"`
	AwsEndpoint          string  `yaml:"aws_endpoint"`
	SSECustomerAlgorithm *string `yaml:"sse_customer_algorithm"`
	S3ForcePathStyle     *bool   `yaml:"s3_force_path_style"`
	SSECustomerKey       *string `yaml:"sse_customer_key"`
	Region               string  `yaml:"region"`
	BucketName           string  `yaml:"bucket_name"`
}

type Swift struct {
	Name              string `yaml:"name"`
	AuthVersion       int    `yaml:"auth_version"`
	AuthUrl           string `yaml:"auth_url"`
	UserName          string `yaml:"user_name"`
	UserDomainName    string `yaml:"user_domain_name"`
	ProjectName       string `yaml:"project_name"`
	ProjectDomainName string `yaml:"project_domain_name"`
	Password          string `yaml:"password"`
	Region            string `yaml:"region"`
	ContainerName     string `yaml:"container_name"`
}

type OAuth struct {
	Enabled     bool   `yaml:"enabled"`
	ProviderURL string `yaml:"provider_url"`
	RedirectURL string `yaml:"redirect_url"`
}

type VerificationService struct {
	IntervalInMinutes int `yaml:"interval_in_minutes"`
	MariaDBVersion    string
}

func GetConfig(opts Options) (cfg Config, err error) {
	if opts.ConfigFilePath == "" {
		return cfg, fmt.Errorf("no config file provided")
	}
	yamlBytes, err := ioutil.ReadFile(opts.ConfigFilePath)
	if err != nil {
		return cfg, fmt.Errorf("read config file: %s", err.Error())
	}
	err = yaml.Unmarshal(yamlBytes, &cfg)
	if err != nil {
		return cfg, fmt.Errorf("parse config file: %s", err.Error())
	}
	return cfg, nil
}
