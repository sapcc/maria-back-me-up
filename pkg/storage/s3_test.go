// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"crypto/md5"
	"encoding/base64"
	"strings"
	"testing"
)

func TestEncodeSSECustomerKey(t *testing.T) {
	rawKey := strings.Repeat("k", 32) // 32-byte AES256 key
	digest := md5.Sum([]byte(rawKey))
	wantKey := base64.StdEncoding.EncodeToString([]byte(rawKey))
	wantMD5 := base64.StdEncoding.EncodeToString(digest[:])
	empty := ""

	cases := []struct {
		name    string
		input   *string
		wantKey *string
		wantMD5 *string
	}{
		{name: "raw 32-byte key", input: &rawKey, wantKey: &wantKey, wantMD5: &wantMD5},
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
