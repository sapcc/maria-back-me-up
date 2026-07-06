// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package config

import "testing"

func TestObjectLockConfig(t *testing.T) {
	cases := []struct {
		name     string
		s3       S3
		wantMode string
		wantErr  bool
	}{
		{
			name:     "disabled by default ignores stale mode and days",
			s3:       S3{Name: "s1", ObjectLockMode: "BOGUS", ObjectLockRetentionDays: -1},
			wantMode: "BOGUS",
		},
		{
			name:     "enabled defaults mode to compliance",
			s3:       S3{Name: "s1", ObjectLockEnabled: true, ObjectLockRetentionDays: 30},
			wantMode: "COMPLIANCE",
		},
		{
			name:     "enabled normalizes mode case",
			s3:       S3{Name: "s1", ObjectLockEnabled: true, ObjectLockMode: "governance", ObjectLockRetentionDays: 1},
			wantMode: "GOVERNANCE",
		},
		{
			name:    "enabled rejects unknown mode",
			s3:      S3{Name: "s1", ObjectLockEnabled: true, ObjectLockMode: "BOGUS", ObjectLockRetentionDays: 30},
			wantErr: true,
		},
		{
			name:    "enabled requires retention days",
			s3:      S3{Name: "s1", ObjectLockEnabled: true, ObjectLockMode: "COMPLIANCE"},
			wantErr: true,
		},
		{
			name:    "enabled rejects retention above 100 years",
			s3:      S3{Name: "s1", ObjectLockEnabled: true, ObjectLockMode: "COMPLIANCE", ObjectLockRetentionDays: 40000},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Storages: StorageService{S3: []S3{tc.s3}}}
			setDefaults(cfg)
			err := validate(cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("want no error, got %v", err)
			}
			if got := cfg.Storages.S3[0].ObjectLockMode; got != tc.wantMode {
				t.Errorf("mode: got %q, want %q", got, tc.wantMode)
			}
		})
	}
}
