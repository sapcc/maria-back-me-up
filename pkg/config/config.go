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
	Version                            string  `yaml:"version"`
	MariaDB                            MariaDB `yaml:"maria_db"`
	S3                                 S3      `yaml:"s3"`
	BackupDir                          string  `yaml:"backup_dir"`
	AutomaticRecovery                  bool    `yaml:"automatic_recovery"`
	ServiceName                        string  `yaml:"service_name"`
	Namespace                          string  `yaml:"namespace"`
	FullBackupIntervalInSeconds        int     `yaml:"full_backup_interval_in_seconds"`
	IncrementalBackupIntervalInSeconds int     `yaml:"incremental_backup_interval_in_seconds"`
}

type MariaDB struct {
	Flavor       string   `yaml:"flavor"`
	Host         string   `yaml:"host"`
	Port         int      `yaml:"port"`
	User         string   `yaml:"user"`
	Password     string   `yaml:"password"`
	DataDir      string   `yaml:"data_dir"`
	Database     string   `yaml:"database"`
	VerifyTables []string `yaml:"verify_tables"`
}

type S3 struct {
	AwsAccessKeyID     string `yaml:"aws_access_key_id"`
	AwsSecretAccessKey string `yaml:"aws_secret_access_key"`
	AwsEndpoint        string `yaml:"aws_endpoint"`
	Region             string `yaml:"region"`
	BucketName         string `yaml:"bucket_name"`
}

func GetConfig(opts Options) (cfg Config, err error) {
	if opts.ConfigFilePath == "" {
		return cfg, nil
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
