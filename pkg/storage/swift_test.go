// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"testing"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

func TestNewSwiftAuthFailureIsNonFatal(t *testing.T) {
	cfg := config.Swift{
		Name:          "swift-unreachable",
		AuthVersion:   3,
		AuthURL:       "http://127.0.0.1:1/v3",
		UserName:      "user",
		Password:      "pass",
		ContainerName: "container",
	}

	s, err := NewSwift(cfg, "service", "./restore", "mysql-bin")
	if err != nil {
		t.Fatalf("NewSwift returned error when authentication failed, want nil so other storages keep working: %v", err)
	}
	if s == nil {
		t.Fatal("NewSwift returned nil Swift when authentication failed, want a usable instance that re-authenticates on use")
	}
	if got := s.GetStorageServiceName(); got != cfg.Name {
		t.Errorf("storage name: got %q, want %q", got, cfg.Name)
	}
}
