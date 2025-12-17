// SPDX-FileCopyrightText: 2021 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package database

import (
	"testing"
	"time"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

func TestMariaDBUp(t *testing.T) {
	cfg := config.Config{}

	mdb, err := NewMariaDB(cfg, nil, nil)
	if err != nil {
		t.Errorf("expected mariadb instance,but got error: %s.", err.Error())
	}

	if err = mdb.Up(1*time.Second, false); err == nil {
		t.Error("expected up to return error, but got nil instead.")
	}
}

func TestMariaDBDumpTool(t *testing.T) {
	cfg := config.Config{
		Database: config.DatabaseConfig{
			DumpTool: 2,
		},
	}

	mdb, err := NewMariaDB(cfg, nil, nil)
	if err != nil {
		t.Errorf("expected mariadb instance,but got error: %s.", err.Error())
	}
	if _, err = mdb.CreateFullBackup("path"); err == nil {
		t.Error("expected an error, but got nil")
	}
}
