package database

import (
	"context"
	"fmt"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/storage"
)

type (
	// Status holds database status info
	Status struct {
		Ok      bool
		Details map[string]string
	}
	// Checksum holds database table checksums
	Checksum struct {
		TablesChecksum map[string]int64 `yaml:"tables_checksum"`
	}
	// LogPosition holds database log position
	LogPosition struct {
		Format string
		Name   string
		Pos    uint32
	}
)

// Database interface for database implementation
type Database interface {
	GetLogPosition() LogPosition
	GetConfig() config.DatabaseConfig
	CreateFullBackup(path string) (lp LogPosition, err error)
	StartIncBackup(ctx context.Context, lp LogPosition, dir string, ch chan error) (err error)
	FlushIncBackup() (err error)
	Restore(path string) (err error)
	VerifyRestore(path string) (err error)
	HealthCheck() (status Status, err error)
	Up(timeout time.Duration, withIP bool) (err error)
	GetCheckSumForTable(verifyTables []string, withIP bool) (cs Checksum, err error)
	GetDatabaseDiff(c1, c2 config.DatabaseConfig) (out []byte, err error)
}

// NewDatabase creates a new database based on the chose type
func NewDatabase(c config.Config, s *storage.Manager) (Database, error) {
	if c.SideCar == nil || *c.SideCar {
		c.Database.Host = "127.0.0.1"
	}
	if c.Database.Type == constants.MARIADB {
		return NewMariaDB(c, s)
	} else if c.Database.Type == constants.POSTGRES {
		return NewPostgres(c, s)
	}

	return nil, fmt.Errorf("unsupported database")
}
