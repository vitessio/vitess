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
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
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
	// digest of their contents when last seen valid.
	watchedCRLs sync.Map
	// lastValidCRLs maps the CRL files ClientConfig used to the CRLs
	// last loaded from them, and staleCRLWarnings holds the files and
	// errors that it held those CRLs against peers for.
	lastValidCRLs, staleCRLWarnings sync.Map

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

// maxReloadAttempts bounds how many times ReloadCachedFiles reads the
// files again looking for a consistent view of them.
const maxReloadAttempts = 5

// reloadTestHook, when set, runs on every attempt ReloadCachedFiles
// makes to read the files, after it digested them and before it reads
// them, so that a test can change the files in between. Nil outside
// tests.
var reloadTestHook func()

// ReloadCachedFiles reads again the certificate, key, CA and CRL files
// that ClientConfig and ServerConfig loaded, so that the configs they
// build from then on use what the files now hold. It reports whether
// any file changed.
//
// The files are digested before and after they are read, and read
// again until both digests agree, so that what it publishes comes from
// one version of the files: when their directory is swapped for a new
// one halfway through, as Kubernetes updates a secret, a client does
// not end up with its old key pair and the new CA. When the files keep
// changing, nothing is published. A file that cannot be loaded keeps
// what was loaded from it before, and its error is returned.
func ReloadCachedFiles() (changed bool, err error) {
	reloadMu.Lock()
	defer reloadMu.Unlock()

	entries := snapshotCachedFiles()
	paths := entries.paths()
	for attempt := 1; ; attempt++ {
		before := digestFiles(paths)
		if reloadTestHook != nil {
			reloadTestHook()
		}
		updates, errs := entries.read()
		if maps.Equal(before, digestFiles(paths)) {
			for _, u := range updates {
				u.cache.Store(u.key, u.value)
			}
			if len(updates) > 0 {
				cachedFilesGeneration.Add(1)
			}
			return len(updates) > 0, vterrors.Aggregate(errs)
		}
		if attempt == maxReloadAttempts {
			return false, vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "the TLS files kept changing across %d attempts to read them consistently; none was reloaded", maxReloadAttempts)
		}
	}
}

// cachedFiles are the entries of the caches that ReloadCachedFiles
// reloads, and the files they were loaded from.
type cachedFiles struct {
	keyPairs, combinedKeyPairs map[any]keyPairFiles
	certPools                  map[string]*x509.CertPool
	caCertificates             map[string][]*x509.Certificate
	crls                       map[string][sha256.Size]byte
}

func snapshotCachedFiles() cachedFiles {
	entries := cachedFiles{
		keyPairs:         make(map[any]keyPairFiles),
		combinedKeyPairs: make(map[any]keyPairFiles),
		certPools:        make(map[string]*x509.CertPool),
		caCertificates:   make(map[string][]*x509.Certificate),
		crls:             make(map[string][sha256.Size]byte),
	}
	cachedKeyPairs.Range(func(id, files any) bool {
		entries.keyPairs[id] = files.(keyPairFiles)
		return true
	})
	cachedCombinedKeyPairs.Range(func(id, files any) bool {
		entries.combinedKeyPairs[id] = files.(keyPairFiles)
		return true
	})
	certPools.Range(func(ca, pool any) bool {
		entries.certPools[ca.(string)] = pool.(*x509.CertPool)
		return true
	})
	caCertificates.Range(func(ca, certificates any) bool {
		entries.caCertificates[ca.(string)] = certificates.([]*x509.Certificate)
		return true
	})
	watchedCRLs.Range(func(crl, digest any) bool {
		entries.crls[crl.(string)] = digest.([sha256.Size]byte)
		return true
	})
	return entries
}

// paths returns the files the entries were loaded from.
func (c cachedFiles) paths() []string {
	var paths []string
	for _, files := range c.keyPairs {
		paths = append(paths, files.cert, files.key)
	}
	for _, files := range c.combinedKeyPairs {
		paths = append(paths, files.ca, files.cert, files.key)
	}
	for ca := range c.certPools {
		paths = append(paths, ca)
	}
	for ca := range c.caCertificates {
		paths = append(paths, ca)
	}
	for crl := range c.crls {
		paths = append(paths, crl)
	}
	slices.Sort(paths)
	return slices.Compact(paths)
}

// cacheUpdate is an entry that ReloadCachedFiles stores once it has
// read every file consistently.
type cacheUpdate struct {
	cache      *sync.Map
	key, value any
}

// read reads the files of the entries, and returns the updates of
// those whose files changed, without applying them.
func (c cachedFiles) read() (updates []cacheUpdate, errs []error) {
	readKeyPairs := func(cache *sync.Map, entries map[any]keyPairFiles, read func(keyPairFiles) (*[]tls.Certificate, error)) {
		for id, files := range entries {
			fresh, err := read(files)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if cached, ok := cache.Load(id); !ok || !sameCertificates(*cached.(*[]tls.Certificate), *fresh) {
				updates = append(updates, cacheUpdate{cache: cache, key: id, value: fresh})
			}
		}
	}
	readKeyPairs(&tlsCertificates, c.keyPairs, func(f keyPairFiles) (*[]tls.Certificate, error) {
		return readTLSCertificate(f.cert, f.key)
	})
	readKeyPairs(&combinedTLSCertificates, c.combinedKeyPairs, func(f keyPairFiles) (*[]tls.Certificate, error) {
		return readAndCombineTLSCertificates(f.ca, f.cert, f.key)
	})
	for ca, cached := range c.certPools {
		fresh, err := readx509CertPool(ca)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !fresh.Equal(cached) {
			updates = append(updates, cacheUpdate{cache: &certPools, key: ca, value: fresh})
		}
	}
	for ca, cached := range c.caCertificates {
		fresh, err := readx509Certificates(ca)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !slices.EqualFunc(fresh, cached, (*x509.Certificate).Equal) {
			updates = append(updates, cacheUpdate{cache: &caCertificates, key: ca, value: fresh})
		}
	}
	for crl, seen := range c.crls {
		digest, err := fileDigest(crl)
		if err != nil {
			errs = append(errs, vterrors.Wrapf(err, "failed to read crl file: %s", crl))
			continue
		}
		if digest == seen {
			continue
		}
		if _, err := loadCRLSet(crl); err != nil {
			errs = append(errs, vterrors.Wrapf(err, "cannot use the CRL file %s; the CRLs last loaded from it stay in use", crl))
			continue
		}
		updates = append(updates, cacheUpdate{cache: &watchedCRLs, key: crl, value: digest})
	}
	return updates, errs
}

// fileState is a file's digest, or that it could not be read.
type fileState struct {
	digest   [sha256.Size]byte
	readable bool
}

func digestFiles(paths []string) map[string]fileState {
	states := make(map[string]fileState, len(paths))
	for _, path := range paths {
		digest, err := fileDigest(path)
		states[path] = fileState{digest: digest, readable: err == nil}
	}
	return states
}

// clientCRLChecker is newCRLChecker for ClientConfig, which reads the
// CRL file on every call. When the file cannot be read or used, for
// instance while it is being replaced, the CRLs last loaded from it
// are held against the peer instead, as a server whose reload fails
// keeps its previous CRLs, rather than every new connection failing.
// Only the first load of a file must succeed.
func clientCRLChecker(crl, ca string) (*crlChecker, error) {
	var issuers []*x509.Certificate
	if ca != "" {
		var err error
		if issuers, err = loadx509Certificates(ca); err != nil {
			return nil, err
		}
	}
	crls, err := loadCRLSet(crl)
	if err == nil {
		var checker *crlChecker
		if checker, err = newCRLCheckerFrom(crls, issuers); err == nil {
			lastValidCRLs.Store(crl, crls)
			return checker, nil
		}
	}
	last, ok := lastValidCRLs.Load(crl)
	if !ok {
		return nil, err
	}
	checker, lastErr := newCRLCheckerFrom(last.([]*x509.RevocationList), issuers)
	if lastErr != nil {
		return nil, err
	}
	if _, warned := staleCRLWarnings.LoadOrStore(crl+"\x00"+err.Error(), struct{}{}); !warned {
		log.Warn(fmt.Sprintf("Cannot use the CRL file %s, so the CRLs last loaded from it stay in use: %v", crl, err))
	}
	return checker, nil
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
