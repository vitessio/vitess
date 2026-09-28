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
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/vt/vterrors"
)

// keyPairFiles are the files a cached certificate was loaded from. ca
// is set only for a certificate combined with its CA chain.
type keyPairFiles struct {
	ca, cert, key string
}

var (
	// cachedKeyPairs and cachedCombinedKeyPairs map the keys of
	// tlsCertificates and combinedTLSCertificates to the files their
	// certificates were loaded from.
	cachedKeyPairs, cachedCombinedKeyPairs sync.Map
	// watchedCRLs maps the CRL files ClientConfig used to the SHA-256
	// digest of their contents when last seen.
	watchedCRLs sync.Map

	reloadMu              sync.Mutex
	cachedFilesGeneration atomic.Uint64

	cachedFilesInUse     = make(chan struct{})
	cachedFilesInUseOnce sync.Once
)

// CachedFilesInUse returns a channel that is closed once ClientConfig
// or ServerConfig first loads a file, from when on ReloadCachedFiles
// has files to read again.
func CachedFilesInUse() <-chan struct{} {
	return cachedFilesInUse
}

func markCachedFilesInUse() {
	cachedFilesInUseOnce.Do(func() { close(cachedFilesInUse) })
}

// CachedFilesGeneration returns a number that changes whenever
// ReloadCachedFiles finds that a file changed. A config built by
// ClientConfig before it changed may hold what the files held before.
func CachedFilesGeneration() uint64 {
	return cachedFilesGeneration.Load()
}

// watchCRL has ReloadCachedFiles watch the CRL file crl for changes.
// ClientConfig reads the CRL on every call, so the file is only
// watched, for CachedFilesGeneration, not cached.
func watchCRL(crl string) {
	if _, ok := watchedCRLs.Load(crl); ok {
		return
	}
	digest, err := fileDigest(crl)
	if err != nil {
		return
	}
	watchedCRLs.LoadOrStore(crl, digest)
	markCachedFilesInUse()
}

// ReloadCachedFiles reads again the certificate, key, CA and CRL files
// that ClientConfig and ServerConfig loaded, so that the configs they
// build from then on use what the files now hold. A file that cannot
// be loaded keeps what was loaded from it before, and its error is
// returned. It reports whether any file changed.
func ReloadCachedFiles() (changed bool, err error) {
	reloadMu.Lock()
	defer reloadMu.Unlock()

	var errs []error
	reloadKeyPairs := func(cache, files *sync.Map, read func(keyPairFiles) (*[]tls.Certificate, error)) {
		files.Range(func(id, value any) bool {
			fresh, err := read(value.(keyPairFiles))
			if err != nil {
				errs = append(errs, err)
				return true
			}
			if cached, ok := cache.Load(id); !ok || !sameCertificates(*cached.(*[]tls.Certificate), *fresh) {
				cache.Store(id, fresh)
				changed = true
			}
			return true
		})
	}
	reloadKeyPairs(&tlsCertificates, &cachedKeyPairs, func(f keyPairFiles) (*[]tls.Certificate, error) {
		return readTLSCertificate(f.cert, f.key)
	})
	reloadKeyPairs(&combinedTLSCertificates, &cachedCombinedKeyPairs, func(f keyPairFiles) (*[]tls.Certificate, error) {
		return readAndCombineTLSCertificates(f.ca, f.cert, f.key)
	})

	certPools.Range(func(ca, cached any) bool {
		fresh, err := readx509CertPool(ca.(string))
		if err != nil {
			errs = append(errs, err)
			return true
		}
		if !fresh.Equal(cached.(*x509.CertPool)) {
			certPools.Store(ca, fresh)
			changed = true
		}
		return true
	})
	caCertificates.Range(func(ca, cached any) bool {
		fresh, err := readx509Certificates(ca.(string))
		if err != nil {
			errs = append(errs, err)
			return true
		}
		if !slices.EqualFunc(fresh, cached.([]*x509.Certificate), (*x509.Certificate).Equal) {
			caCertificates.Store(ca, fresh)
			changed = true
		}
		return true
	})
	watchedCRLs.Range(func(crl, seen any) bool {
		digest, err := fileDigest(crl.(string))
		if err != nil {
			errs = append(errs, vterrors.Wrapf(err, "failed to read crl file: %s", crl))
			return true
		}
		if digest != seen.([sha256.Size]byte) {
			watchedCRLs.Store(crl, digest)
			changed = true
		}
		return true
	})

	if changed {
		cachedFilesGeneration.Add(1)
	}
	return changed, vterrors.Aggregate(errs)
}

// sameCertificates reports whether a and b hold the same certificate
// chains. A key cannot change without its certificate.
func sameCertificates(a, b []tls.Certificate) bool {
	return slices.EqualFunc(a, b, func(a, b tls.Certificate) bool {
		return slices.EqualFunc(a.Certificate, b.Certificate, bytes.Equal)
	})
}

func fileDigest(path string) ([sha256.Size]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(b), nil
}
