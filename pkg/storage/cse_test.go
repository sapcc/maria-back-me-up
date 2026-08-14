// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

var testAAD = []byte("svc/2026-06-15-1200/dump.tar")

func newTestCSE(t *testing.T) (*cseCipher, []byte) {
	t.Helper()
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatalf("rand: %v", err)
	}
	kekCopy := append([]byte(nil), kek...)
	c, err := newCSECipher(kek)
	if err != nil {
		t.Fatalf("newCSECipher: %v", err)
	}
	return c, kekCopy
}

func roundTrip(t *testing.T, c *cseCipher, plain []byte) {
	t.Helper()
	var ct bytes.Buffer
	rc := c.EncryptStream(bytes.NewReader(plain), testAAD)
	if _, err := io.Copy(&ct, rc); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("close encrypt pipe: %v", err)
	}
	var pt bytes.Buffer
	if err := c.DecryptStream(bytes.NewReader(ct.Bytes()), testAAD, &pt); err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(plain, pt.Bytes()) {
		t.Fatalf("plaintext mismatch: got %d bytes, want %d", pt.Len(), len(plain))
	}
}

func encryptForTest(t *testing.T, c *cseCipher, plain, aad []byte) []byte {
	t.Helper()
	var ct bytes.Buffer
	rc := c.EncryptStream(bytes.NewReader(plain), aad)
	if _, err := io.Copy(&ct, rc); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("close encrypt pipe: %v", err)
	}
	return ct.Bytes()
}

func TestCSE_RoundTrip_Empty(t *testing.T) {
	c, _ := newTestCSE(t)
	roundTrip(t, c, nil)
}

func TestCSE_RoundTrip_Small(t *testing.T) {
	c, _ := newTestCSE(t)
	plain := make([]byte, 1024)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, plain)
}

func TestCSE_RoundTrip_ExactlyOneFrame(t *testing.T) {
	c, _ := newTestCSE(t)
	plain := make([]byte, cseSegmentSize)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, plain)
}

func TestCSE_RoundTrip_JustOverOneFrame(t *testing.T) {
	c, _ := newTestCSE(t)
	plain := make([]byte, cseSegmentSize+1)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, plain)
}

func TestCSE_RoundTrip_MultiFrame(t *testing.T) {
	c, _ := newTestCSE(t)
	plain := make([]byte, 5*cseSegmentSize)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, c, plain)
}

func TestCSE_Tamper_FlipMidStream(t *testing.T) {
	c, _ := newTestCSE(t)
	plain := make([]byte, 2*cseSegmentSize)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	ct := encryptForTest(t, c, plain, testAAD)
	corrupted := append([]byte(nil), ct...)
	corrupted[cseSegmentSize+cseSegmentSize/2] ^= 0x01
	if err := c.DecryptStream(bytes.NewReader(corrupted), testAAD, io.Discard); err == nil {
		t.Fatal("expected decrypt error after mid-stream tamper")
	}
}

func TestCSE_Tamper_FlipHeader(t *testing.T) {
	c, _ := newTestCSE(t)
	plain := make([]byte, 64*1024)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	ct := encryptForTest(t, c, plain, testAAD)
	corrupted := append([]byte(nil), ct...)
	corrupted[10] ^= 0x01
	if err := c.DecryptStream(bytes.NewReader(corrupted), testAAD, io.Discard); err == nil {
		t.Fatal("expected decrypt error after header tamper")
	}
}

func TestCSE_Truncation_DropLastFrame(t *testing.T) {
	c, _ := newTestCSE(t)
	plain := make([]byte, 2*cseSegmentSize+128)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	ct := encryptForTest(t, c, plain, testAAD)
	truncated := ct[:len(ct)-200]
	if err := c.DecryptStream(bytes.NewReader(truncated), testAAD, io.Discard); err == nil {
		t.Fatal("expected decrypt error after truncation")
	}
}

func TestCSE_WrongKEK(t *testing.T) {
	a, _ := newTestCSE(t)
	b, _ := newTestCSE(t)
	plain := make([]byte, 4096)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	ct := encryptForTest(t, a, plain, testAAD)
	if err := b.DecryptStream(bytes.NewReader(ct), testAAD, io.Discard); err == nil {
		t.Fatal("expected decrypt error with wrong KEK")
	}
}

func TestCSE_AADMismatch_FailsDecrypt(t *testing.T) {
	c, _ := newTestCSE(t)
	plain := make([]byte, 4096)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	ct := encryptForTest(t, c, plain, []byte("svc/A/dump.tar"))
	if err := c.DecryptStream(bytes.NewReader(ct), []byte("svc/B/dump.tar"), io.Discard); err == nil {
		t.Fatal("expected decrypt error when AAD does not match")
	}
}

func TestCSE_LoadKey_Base64(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(key)), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadCSEKey(path)
	if err != nil {
		t.Fatalf("loadCSEKey: %v", err)
	}
	if !bytes.Equal(got, key) {
		t.Fatal("loaded key does not match")
	}
}

func TestCSE_LoadKey_Raw32(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadCSEKey(path)
	if err != nil {
		t.Fatalf("loadCSEKey: %v", err)
	}
	if !bytes.Equal(got, key) {
		t.Fatal("loaded key does not match")
	}
}

func TestCSE_LoadKey_TrimsWhitespace(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "key")
	encoded := base64.StdEncoding.EncodeToString(key)
	if err := os.WriteFile(path, []byte("\n  "+encoded+"  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := loadCSEKey(path)
	if err != nil {
		t.Fatalf("loadCSEKey: %v", err)
	}
	if !bytes.Equal(got, key) {
		t.Fatal("loaded key does not match")
	}
}

func TestCSE_LoadKey_RejectShort(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, []byte("too-short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCSEKey(path); err == nil {
		t.Fatal("expected error for short key")
	}
}

func TestCSE_LoadKey_MissingFile(t *testing.T) {
	if _, err := loadCSEKey(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

// writeTempKey writes a fresh raw 32-byte KEK to a file under t.TempDir and
// returns its path.
func writeTempKey(t *testing.T, name string) string {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, key, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCSERegistry_RoutesByName(t *testing.T) {
	old := writeTempKey(t, "old")
	new := writeTempKey(t, "new")
	r, err := newCSERegistry("new", []config.CSEKey{
		{Name: "old", File: old},
		{Name: "new", File: new},
	})
	if err != nil {
		t.Fatalf("newCSERegistry: %v", err)
	}
	if _, name := r.activeCipher(); name != "new" {
		t.Fatalf("active = %q, want new", name)
	}
	plain := []byte("hello rotation")
	oldC, err := r.cipherByName("old")
	if err != nil {
		t.Fatalf("cipherByName old: %v", err)
	}
	ct := encryptForTest(t, oldC, plain, testAAD)

	// Decrypting with the active cipher must fail; only the named cipher works.
	activeC, _ := r.activeCipher()
	if err := activeC.DecryptStream(bytes.NewReader(ct), testAAD, io.Discard); err == nil {
		t.Fatal("expected wrong-cipher decrypt to fail")
	}
	var pt bytes.Buffer
	if err := oldC.DecryptStream(bytes.NewReader(ct), testAAD, &pt); err != nil {
		t.Fatalf("old decrypt: %v", err)
	}
	if !bytes.Equal(plain, pt.Bytes()) {
		t.Fatal("plaintext mismatch")
	}
}

func TestCSERegistry_RejectsBadConfig(t *testing.T) {
	good := writeTempKey(t, "good")
	cases := []struct {
		name   string
		active string
		keys   []config.CSEKey
	}{
		{"empty active", "", []config.CSEKey{{Name: "k", File: good}}},
		{"no keys", "k", nil},
		{"unknown active", "missing", []config.CSEKey{{Name: "k", File: good}}},
		{"empty name", "k", []config.CSEKey{{Name: "", File: good}}},
		{"empty file", "k", []config.CSEKey{{Name: "k", File: ""}}},
		{"duplicate name", "k", []config.CSEKey{{Name: "k", File: good}, {Name: "k", File: good}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newCSERegistry(tc.active, tc.keys); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestCSERegistry_UnknownKeyName(t *testing.T) {
	good := writeTempKey(t, "good")
	r, err := newCSERegistry("k", []config.CSEKey{{Name: "k", File: good}})
	if err != nil {
		t.Fatalf("newCSERegistry: %v", err)
	}
	if _, err := r.cipherByName("missing"); err == nil {
		t.Fatal("expected error for unknown key name")
	}
}
