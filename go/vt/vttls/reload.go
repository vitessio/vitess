/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vttls

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// clientFilesKey identifies the files that a client config is built
// from, other than its CRL, which ClientConfig reads on every call.
// cert and key are either both set or both empty.
type clientFilesKey struct {
	cert, key, ca string
}

// clientFiles is what was loaded from the files of a clientFilesKey,
// all from one read of them.
type clientFiles struct {
	certificates []tls.Certificate
	roots        *x509.CertPool
	// caCertificates are the certificates in the CA file, which the
	// CRLs may be signed by.
	caCertificates []*x509.Certificate
	digest         [sha256.Size]byte
}

// clientFileContents are the contents of the files of a clientFilesKey.
type clientFileContents struct {
	cert, key, ca []byte
}

var (
	// cachedClientFiles maps each clientFilesKey that ClientConfig
	// loaded to its *clientFiles. A load that fails is not cached, so
	// the next call tries again.
	cachedClientFiles sync.Map
	// loadingClientFiles maps each clientFilesKey to the *sync.Mutex
	// that its first load holds, so that it is read once.
	loadingClientFiles sync.Map
	// watchedCRLs maps the CRL files that ClientConfig read to the
	// SHA-256 digest of their contents when last seen.
	watchedCRLs sync.Map

	reloadMu              sync.Mutex
	cachedFilesGeneration atomic.Uint64
)

// CachedFilesGeneration returns a number that changes whenever
// ReloadCachedFiles finds that a file changed. A config built by
// ClientConfig before it changed may hold what the files held before.
func CachedFilesGeneration() uint64 {
	return cachedFilesGeneration.Load()
}

// loadClientFiles returns what the files of k hold, read on the first
// call for k and again by every ReloadCachedFiles that finds them
// changed.
func loadClientFiles(k clientFilesKey) (*clientFiles, error) {
	if f, ok := cachedClientFiles.Load(k); ok {
		return f.(*clientFiles), nil
	}
	mu, _ := loadingClientFiles.LoadOrStore(k, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()
	if f, ok := cachedClientFiles.Load(k); ok {
		return f.(*clientFiles), nil
	}

	contents, err := k.read()
	if err != nil {
		return nil, err
	}
	f, err := contents.parse(k)
	if err != nil {
		return nil, err
	}
	cachedClientFiles.Store(k, f)
	return f, nil
}

func (k clientFilesKey) read() (clientFileContents, error) {
	var (
		c   clientFileContents
		err error
	)
	if k.cert != "" {
		if c.cert, err = os.ReadFile(k.cert); err != nil {
			return c, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to load tls certificate, cert %s, key: %s: %v", k.cert, k.key, err)
		}
		if c.key, err = os.ReadFile(k.key); err != nil {
			return c, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to load tls certificate, cert %s, key: %s: %v", k.cert, k.key, err)
		}
	}
	if k.ca != "" {
		if c.ca, err = os.ReadFile(k.ca); err != nil {
			return c, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read ca file: %s", k.ca)
		}
	}
	return c, nil
}

func (c clientFileContents) digest() [sha256.Size]byte {
	h := sha256.New()
	for _, content := range [][]byte{c.cert, c.key, c.ca} {
		// Length-prefixed, so that no two sets of files digest alike.
		_ = binary.Write(h, binary.BigEndian, uint64(len(content)))
		h.Write(content)
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func (c clientFileContents) parse(k clientFilesKey) (*clientFiles, error) {
	f := &clientFiles{digest: c.digest()}
	if k.cert != "" {
		crt, err := tls.X509KeyPair(c.cert, c.key)
		if err != nil {
			return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to load tls certificate, cert %s, key: %s: %v", k.cert, k.key, err)
		}
		f.certificates = []tls.Certificate{crt}
	}
	if k.ca != "" {
		f.roots = x509.NewCertPool()
		if !f.roots.AppendCertsFromPEM(c.ca) {
			return nil, vterrors.Errorf(vtrpc.Code_UNKNOWN, "failed to append certificates from ca file: %s", k.ca)
		}
		f.caCertificates = parseX509Certificates(c.ca)
	}
	return f, nil
}

// watchCRL records the digest of the contents of the CRL file crl that
// a config was just built from, so that ReloadCachedFiles counts any
// other contents it finds there as a change.
func watchCRL(crl string, body []byte) {
	watchedCRLs.Store(crl, sha256.Sum256(body))
}

// ReloadCachedFiles reads again the certificate, key, CA and CRL files
// of the configs that ClientConfig built, so that the configs it
// builds from then on use what the files now hold. The files of a
// config are loaded together, from one read: when any of them cannot
// be loaded, the config keeps all that was loaded before, and the
// error is returned. A CRL file that cannot be read or parsed is
// reported, and does not count as a change; ClientConfig fails while
// it stays that way. ReloadCachedFiles reports whether any file
// changed.
func ReloadCachedFiles() (changed bool, err error) {
	reloadMu.Lock()
	defer reloadMu.Unlock()

	var errs []error
	cachedClientFiles.Range(func(k, cached any) bool {
		key := k.(clientFilesKey)
		contents, err := key.read()
		if err == nil && contents.digest() == cached.(*clientFiles).digest {
			return true
		}
		var fresh *clientFiles
		if err == nil {
			fresh, err = contents.parse(key)
		}
		if err != nil {
			errs = append(errs, err)
			return true
		}
		cachedClientFiles.Store(k, fresh)
		changed = true
		return true
	})

	watchedCRLs.Range(func(crl, seen any) bool {
		body, err := os.ReadFile(crl.(string))
		if err == nil {
			_, err = parseCRLSet(crl.(string), body)
		}
		if err != nil {
			errs = append(errs, vterrors.Wrapf(err, "failed to load crl file: %s", crl))
			return true
		}
		if digest := sha256.Sum256(body); digest != seen.([sha256.Size]byte) {
			// A config built meanwhile may have stored its own
			// digest, which a later reload then compares with.
			watchedCRLs.CompareAndSwap(crl, seen, digest)
			changed = true
		}
		return true
	})

	if changed {
		cachedFilesGeneration.Add(1)
	}
	return changed, vterrors.Aggregate(errs)
}
