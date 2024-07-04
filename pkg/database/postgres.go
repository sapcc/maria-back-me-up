/**
 * Copyright 2024 SAP SE
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

package database

import (
	"context"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

// Postgres db is ...
type Postgres struct {
	cfg         config.Config
	storage     *storage.Manager
	logPosition LogPosition
}

// NewPostgres creates a postgres db instance
func NewPostgres(c config.Config, s *storage.Manager) (Database, error) {
	return &Postgres{
		cfg:     c,
		storage: s,
	}, nil
}

// GetLogPosition implements interface
func (p *Postgres) GetLogPosition() LogPosition {
	return p.logPosition
}

// GetConfig implements interface
func (p *Postgres) GetConfig() config.DatabaseConfig {
	return p.cfg.Database
}

// CreateFullBackup implements interface
func (p *Postgres) CreateFullBackup(path string) (bp LogPosition, err error) {
	return
}

// StartIncBackup implements interface
func (p *Postgres) StartIncBackup(ctx context.Context, bp LogPosition, dir string, ch chan error) (err error) {
	return
}

// FlushIncBackup implements interface
func (p *Postgres) FlushIncBackup() (err error) {
	return
}

// Restore implements interface
func (p *Postgres) Restore(path string) (err error) {
	return
}

// VerifyRestore implements interface
func (p *Postgres) VerifyRestore(path string) (err error) {
	return
}

// HealthCheck implements interface
func (p *Postgres) HealthCheck() (status Status, err error) {
	return
}

// GetCheckSumForTable implements interface
func (p *Postgres) GetCheckSumForTable(verifyTables []string, withIP bool) (cs Checksum, err error) {
	return
}

// GetDatabaseDiff implements interface
func (p *Postgres) GetDatabaseDiff(c1, c2 config.DatabaseConfig) (out []byte, err error) {
	return
}

// Up implements interface
func (p *Postgres) Up(timeout time.Duration, withIP bool) (err error) {
	return
}
