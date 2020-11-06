package database

import (
	"context"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

type Postgres struct {
	cfg         config.Config
	storage     *storage.Manager
	logPosition LogPosition
}

func NewPostgres(c config.Config, s *storage.Manager) (Database, error) {
	return &Postgres{
		cfg:     c,
		storage: s,
	}, nil
}

func (p *Postgres) GetLogPosition() LogPosition {
	return p.logPosition
}

func (p *Postgres) GetConfig() config.DatabaseConfig {
	return p.cfg.Database
}

func (p *Postgres) CreateFullBackup(path string) (bp LogPosition, err error) {
	return
}
func (p *Postgres) StartIncBackup(ctx context.Context, bp LogPosition, dir string, ch chan error) (err error) {
	return
}
func (p *Postgres) FlushIncBackup() (err error) {
	return
}
func (p *Postgres) Restore(path string) (err error) {
	return
}
func (p *Postgres) VerifyRestore(path string) (err error) {
	return
}
func (p *Postgres) HealthCheck() (status Status, err error) {
	return
}
func (p *Postgres) GetCheckSumForTable(verifyTables []string, withIP bool) (cs Checksum, err error) {
	return
}
func (p *Postgres) GetDatabaseDiff(c1, c2 config.DatabaseConfig) (out []byte, err error) {
	return
}
