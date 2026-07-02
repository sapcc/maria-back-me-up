// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"github.com/tink-crypto/tink-go/v2/insecuresecretdataaccess"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/secretdata"
	"github.com/tink-crypto/tink-go/v2/streamingaead"
	"github.com/tink-crypto/tink-go/v2/streamingaead/aesgcmhkdf"
	"github.com/tink-crypto/tink-go/v2/tink"

	"github.com/sapcc/maria-back-me-up/pkg/config"
)

// CSEKeyMetaHeader marks an object as client-side encrypted; S3 stores it as
// `x-amz-meta-cse-key`. The value is the KEK name, kept for diagnostics only —
// decryption picks the key by trial across the configured keyset.
const CSEKeyMetaHeader = "cse-key"

const cseSegmentSize = 1 << 20 // matches Tink AES256_GCM_HKDF_1MB

type cseCipher struct {
	saead tink.StreamingAEAD
}

// cseRegistry holds one Tink keyset built from all configured KEKs: the
// active key encrypts, decryption tries every key. Nil when CSE is disabled.
type cseRegistry struct {
	active string
	cipher *cseCipher
}

func newCSERegistry(activeName string, keys []config.CSEKey) (*cseRegistry, error) {
	if activeName == "" {
		return nil, fmt.Errorf("cse: cse_active_key is required when cse_keys is configured")
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("cse: cse_keys must list at least the active key %q", activeName)
	}
	mgr := keyset.NewManager()
	seen := make(map[string]struct{}, len(keys))
	var activeID uint32
	activeFound := false
	for _, k := range keys {
		if k.Name == "" {
			return nil, fmt.Errorf("cse: cse_keys entry has empty name")
		}
		if k.File == "" {
			return nil, fmt.Errorf("cse: cse_keys entry %q has empty file", k.Name)
		}
		if _, dup := seen[k.Name]; dup {
			return nil, fmt.Errorf("cse: duplicate key name %q in cse_keys", k.Name)
		}
		seen[k.Name] = struct{}{}
		kek, err := loadCSEKey(k.File)
		if err != nil {
			return nil, err
		}
		key, err := newCSEKeyFromKEK(kek)
		if err != nil {
			return nil, fmt.Errorf("cse: key %q: %w", k.Name, err)
		}
		id, err := mgr.AddKey(key)
		if err != nil {
			return nil, fmt.Errorf("cse: add key %q: %w", k.Name, err)
		}
		if k.Name == activeName {
			activeID = id
			activeFound = true
		}
	}
	if !activeFound {
		return nil, fmt.Errorf("cse: active key %q not found in cse_keys", activeName)
	}
	cipher, err := cipherFromManager(mgr, activeID)
	if err != nil {
		return nil, err
	}
	return &cseRegistry{active: activeName, cipher: cipher}, nil
}

func (r *cseRegistry) activeCipher() (*cseCipher, string) {
	return r.cipher, r.active
}

func loadCSEKey(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cse: read key file %q: %w", path, err)
	}
	defer zeroBytes(raw)
	// A 32-byte file is the raw key; no trimming, since random bytes can be
	// whitespace at the boundary. Trimming only applies to longer files
	// (typically base64 with a trailing newline).
	if len(raw) == 32 {
		return append([]byte(nil), raw...), nil
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 32 {
		return append([]byte(nil), trimmed...), nil
	}
	// Decode into a fixed buffer to avoid an intermediate string copy of the
	// key (DecodeString would allocate one). 64 bytes is well above the 33
	// DecodedLen needs for any 44-char padded base64 of a 32-byte key.
	var dec [64]byte
	defer zeroBytes(dec[:])
	if n, err := base64.StdEncoding.Decode(dec[:], trimmed); err == nil && n == 32 {
		return append([]byte(nil), dec[:n]...), nil
	}
	return nil, fmt.Errorf("cse: key in %q must be 32 raw bytes or base64 of 32 bytes", path)
}

// newCSEKeyFromKEK consumes kek: it is cloned into Tink-owned secret data and
// our copy is zeroed.
func newCSEKeyFromKEK(kek []byte) (*aesgcmhkdf.Key, error) {
	if len(kek) != 32 {
		return nil, fmt.Errorf("cse: KEK must be 32 bytes, got %d", len(kek))
	}
	params, err := aesgcmhkdf.NewParameters(aesgcmhkdf.ParametersOpts{
		KeySizeInBytes:        32,
		DerivedKeySizeInBytes: 32,
		HKDFHashType:          aesgcmhkdf.SHA256,
		SegmentSizeInBytes:    cseSegmentSize,
	})
	if err != nil {
		return nil, fmt.Errorf("cse: build parameters: %w", err)
	}
	keyBytes := secretdata.NewBytesFromData(kek, insecuresecretdataaccess.Token{})
	zeroBytes(kek)
	k, err := aesgcmhkdf.NewKey(params, keyBytes)
	if err != nil {
		return nil, fmt.Errorf("cse: build key: %w", err)
	}
	return k, nil
}

// cipherFromManager finalizes the keyset: encrypts with the primary key,
// trial-decrypts with any key in the set.
func cipherFromManager(mgr *keyset.Manager, primaryID uint32) (*cseCipher, error) {
	if err := mgr.SetPrimary(primaryID); err != nil {
		return nil, fmt.Errorf("cse: set primary: %w", err)
	}
	handle, err := mgr.Handle()
	if err != nil {
		return nil, fmt.Errorf("cse: build handle: %w", err)
	}
	saead, err := streamingaead.New(handle)
	if err != nil {
		return nil, fmt.Errorf("cse: build primitive: %w", err)
	}
	return &cseCipher{saead: saead}, nil
}

func newCSECipher(kek []byte) (*cseCipher, error) {
	k, err := newCSEKeyFromKEK(kek)
	if err != nil {
		return nil, err
	}
	mgr := keyset.NewManager()
	id, err := mgr.AddKey(k)
	if err != nil {
		return nil, fmt.Errorf("cse: add key: %w", err)
	}
	return cipherFromManager(mgr, id)
}

func zeroBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// EncryptStream returns a ReadCloser the caller MUST close on every exit
// path; otherwise the writer goroutine blocks forever waiting for the pipe
// reader. AAD is bound to the object's logical path so a swapped ciphertext
// (different key, same KEK) fails to decrypt instead of silently restoring.
func (c *cseCipher) EncryptStream(plain io.Reader, aad []byte) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		ew, err := c.saead.NewEncryptingWriter(pw, aad)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(ew, plain); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.CloseWithError(ew.Close())
	}()
	return pr
}

func (c *cseCipher) DecryptStream(cipher io.Reader, aad []byte, dst io.Writer) error {
	dr, err := c.saead.NewDecryptingReader(cipher, aad)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, dr); err != nil {
		return err
	}
	return nil
}
