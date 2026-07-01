// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"github.com/sapcc/maria-back-me-up/pkg/config"
)

func TestEncodeSSECustomerKey(t *testing.T) {
	b64Raw := bytes.Repeat([]byte{0xAA}, 32)
	b64Input := base64.StdEncoding.EncodeToString(b64Raw)
	b64Digest := md5.Sum(b64Raw)
	b64WantMD5 := base64.StdEncoding.EncodeToString(b64Digest[:])

	asciiInput := strings.Repeat("k", 32)
	asciiDigest := md5.Sum([]byte(asciiInput))
	asciiWantKey := base64.StdEncoding.EncodeToString([]byte(asciiInput))
	asciiWantMD5 := base64.StdEncoding.EncodeToString(asciiDigest[:])

	invalidInput := "not@valid@base64!"
	invalidDigest := md5.Sum([]byte(invalidInput))
	invalidWantKey := base64.StdEncoding.EncodeToString([]byte(invalidInput))
	invalidWantMD5 := base64.StdEncoding.EncodeToString(invalidDigest[:])

	empty := ""

	cases := []struct {
		name    string
		input   *string
		wantKey *string
		wantMD5 *string
	}{
		{name: "base64 32-byte key", input: &b64Input, wantKey: &b64Input, wantMD5: &b64WantMD5},
		{name: "32-byte ASCII falls back to raw", input: &asciiInput, wantKey: &asciiWantKey, wantMD5: &asciiWantMD5},
		{name: "invalid base64 falls back to raw", input: &invalidInput, wantKey: &invalidWantKey, wantMD5: &invalidWantMD5},
		{name: "nil", input: nil, wantKey: nil, wantMD5: nil},
		{name: "empty string", input: &empty, wantKey: nil, wantMD5: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotKey, gotMD5 := encodeSSECustomerKey(tc.input)
			if !equalStringPtr(gotKey, tc.wantKey) {
				t.Errorf("key: got %v, want %v", deref(gotKey), deref(tc.wantKey))
			}
			if !equalStringPtr(gotMD5, tc.wantMD5) {
				t.Errorf("md5: got %v, want %v", deref(gotMD5), deref(tc.wantMD5))
			}
		})
	}
}

func TestObjectLockParams(t *testing.T) {
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		cfg       config.S3
		wantMode  tmtypes.ObjectLockMode
		wantUntil *time.Time
	}{
		{
			name: "disabled ignores mode and days",
			cfg:  config.S3{ObjectLockMode: "COMPLIANCE", ObjectLockRetentionDays: 30},
		},
		{
			name:      "compliance 30 days",
			cfg:       config.S3{ObjectLockEnabled: true, ObjectLockMode: "COMPLIANCE", ObjectLockRetentionDays: 30},
			wantMode:  tmtypes.ObjectLockModeCompliance,
			wantUntil: timePtr(now.Add(30 * 24 * time.Hour)),
		},
		{
			name:      "governance 1 day",
			cfg:       config.S3{ObjectLockEnabled: true, ObjectLockMode: "GOVERNANCE", ObjectLockRetentionDays: 1},
			wantMode:  tmtypes.ObjectLockModeGovernance,
			wantUntil: timePtr(now.Add(24 * time.Hour)),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotMode, gotUntil := objectLockParams(tc.cfg, now)
			if gotMode != tc.wantMode {
				t.Errorf("mode: got %q, want %q", gotMode, tc.wantMode)
			}
			if !equalTimePtr(gotUntil, tc.wantUntil) {
				t.Errorf("retain until: got %v, want %v", gotUntil, tc.wantUntil)
			}
		})
	}
}

func TestObjectLockUsable(t *testing.T) {
	cases := []struct {
		name string
		out  *s3.GetObjectLockConfigurationOutput
		err  error
		want bool
	}{
		{
			name: "enabled bucket",
			out: &s3.GetObjectLockConfigurationOutput{
				ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled},
			},
			want: true,
		},
		{
			name: "probe error",
			err:  errors.New("ObjectLockConfigurationNotFoundError"),
		},
		{
			name: "nil output without error",
		},
		{
			name: "no lock configuration",
			out:  &s3.GetObjectLockConfigurationOutput{},
		},
		{
			name: "lock not enabled",
			out: &s3.GetObjectLockConfigurationOutput{
				ObjectLockConfiguration: &types.ObjectLockConfiguration{},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := objectLockUsable(tc.out, tc.err); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsObjectLockNotFound(t *testing.T) {
	notFound := &smithy.GenericAPIError{Code: "ObjectLockConfigurationNotFoundError"}

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "not found code", err: notFound, want: true},
		{name: "wrapped not found code", err: fmt.Errorf("probe: %w", notFound), want: true},
		{name: "other api error", err: &smithy.GenericAPIError{Code: "AccessDenied"}},
		{name: "plain error", err: errors.New("dial tcp: timeout")},
		{name: "nil", err: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isObjectLockNotFound(tc.err); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsSSECProbeRejection(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		// HEAD error responses have no body, so the SDK derives the code
		// from the HTTP status text.
		{name: "bodyless HEAD 400", err: &smithy.GenericAPIError{Code: "BadRequest", Message: "Bad Request"}, want: true},
		{name: "XML error body", err: &smithy.GenericAPIError{Code: "InvalidRequest", Message: "sse-c required"}, want: true},
		{name: "wrapped by operation error", err: &smithy.OperationError{ServiceID: "S3", OperationName: "HeadObject", Err: &smithy.GenericAPIError{Code: "BadRequest"}}, want: true},
		{name: "not found", err: &smithy.GenericAPIError{Code: "NotFound"}, want: false},
		{name: "access denied", err: &smithy.GenericAPIError{Code: "AccessDenied"}, want: false},
		{name: "plain error", err: fmt.Errorf("dial tcp: connection refused"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSSECProbeRejection(tc.err); got != tc.want {
				t.Errorf("isSSECProbeRejection(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestSequentialWriterAt(t *testing.T) {
	buf := tmtypes.NewWriteAtBuffer([]byte{})
	sw := &sequentialWriterAt{w: buf}
	for _, chunk := range []string{"hello ", "sequential ", "world"} {
		n, err := sw.Write([]byte(chunk))
		if err != nil {
			t.Fatalf("write %q: %v", chunk, err)
		}
		if n != len(chunk) {
			t.Fatalf("write %q: n = %d, want %d", chunk, n, len(chunk))
		}
	}
	if got, want := string(buf.Bytes()), "hello sequential world"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func equalTimePtr(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

func equalStringPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
