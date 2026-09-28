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
	"strings"
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
	// watchedCRLs maps the CRL files ClientConfig used, with the CA
	// file it used them with, to the SHA-256 digest of their contents
	// when last seen valid under that CA.
	watchedCRLs sync.Map
	// lastValidCRLs maps the CRL files ClientConfig used to the CRLs
	// last loaded from them, and staleCRLWarnings holds the files and
	// errors that it held those CRLs against peers for.
	lastValidCRLs, staleCRLWarnings sync.Map

	reloadMu              sync.Mutex
	cachedFilesGeneration atomic.Uint64
	// publishMu keeps ClientConfig and ServerConfig from reading the
	// caches while ReloadCachedFiles updates them, so that a config
	// never combines files from before and after a reload.
	publishMu sync.RWMutex

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

// crlWatch is a CRL file that ClientConfig used, and the CA file it
// used it with, empty for none.
type crlWatch struct {
	crl, ca string
}

// watchCRL has ReloadCachedFiles watch the CRL file of w for changes,
// starting from digest, the digest of the contents ClientConfig
// loaded from it. ClientConfig reads the CRL on every call, so the
// file is only watched, for CachedFilesGeneration, not cached.
func watchCRL(w crlWatch, digest [sha256.Size]byte) {
	if _, loaded := watchedCRLs.LoadOrStore(w, digest); !loaded {
		markCachedFilesInUse()
	}
}

// maxReloadAttempts bounds how many times ReloadCachedFiles reads the
// files again looking for a consistent view of them.
const maxReloadAttempts = 5

// reloadTestHook, when set, runs on every attempt ReloadCachedFiles
// makes to read the files, after it digested them and before it reads
// them, so that a test can change the files in between. publishTestHook,
// when set, runs after ReloadCachedFiles updated the first cache entry
// of a reload and before the others. Both are nil outside tests.
var reloadTestHook, publishTestHook func()

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
// changing, nothing is published.
//
// A file that cannot be loaded keeps what was loaded from it before,
// and its error is returned. So do the files that a configuration built
// from it was built from along with it, and theirs in turn, so that no
// configuration is left with some of its files reloaded and not the
// others: with the new CA but its old key pair, or with a new CA its
// CRL is not valid under. Configurations that share no file reload
// independently.
func ReloadCachedFiles() (changed bool, err error) {
	reloadMu.Lock()
	defer reloadMu.Unlock()

	for attempt := 1; ; attempt++ {
		// The snapshot is taken while no ClientConfig or ServerConfig
		// is building a config, so that it holds every configuration
		// whose entries it holds. One that registers after it may use
		// its entries together in a way it does not know about, so the
		// reload is then read again rather than published.
		publishMu.Lock()
		registered := fileSetsRegistered.Load()
		entries := snapshotCachedFiles()
		publishMu.Unlock()

		paths := entries.paths()
		before := digestFiles(paths)
		if reloadTestHook != nil {
			reloadTestHook()
		}
		results := entries.read()
		if maps.Equal(before, digestFiles(paths)) {
			updates, errs := entries.withhold(results)
			if publish(updates, registered) {
				return len(updates) > 0, vterrors.Aggregate(errs)
			}
		}
		if attempt == maxReloadAttempts {
			return false, vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "the TLS files, or the configurations built from them, kept changing across %d attempts to read them consistently; none was reloaded", maxReloadAttempts)
		}
	}
}

// publish applies updates to the caches, all at once to ClientConfig
// and ServerConfig, unless a configuration registered since registered
// was the count of registrations, in which case it reports false.
func publish(updates []cacheUpdate, registered uint64) bool {
	if len(updates) == 0 {
		return true
	}
	publishMu.Lock()
	defer publishMu.Unlock()
	if fileSetsRegistered.Load() != registered {
		return false
	}
	for i, u := range updates {
		u.cache.Store(u.key, u.value)
		if i == 0 && publishTestHook != nil {
			publishTestHook()
		}
	}
	cachedFilesGeneration.Add(1)
	return true
}

// cachedFiles are the entries of the caches that ReloadCachedFiles
// reloads, and the files they were loaded from.
type cachedFiles struct {
	keyPairs, combinedKeyPairs map[any]keyPairFiles
	certPools                  map[string]*x509.CertPool
	caCertificates             map[string][]*x509.Certificate
	crls                       map[crlWatch][sha256.Size]byte
	// fileSets are the entries each configuration was built from.
	fileSets [][]entryID
}

func snapshotCachedFiles() cachedFiles {
	entries := cachedFiles{
		keyPairs:         make(map[any]keyPairFiles),
		combinedKeyPairs: make(map[any]keyPairFiles),
		certPools:        make(map[string]*x509.CertPool),
		caCertificates:   make(map[string][]*x509.Certificate),
		crls:             make(map[crlWatch][sha256.Size]byte),
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
	watchedCRLs.Range(func(w, digest any) bool {
		entries.crls[w.(crlWatch)] = digest.([sha256.Size]byte)
		return true
	})
	fileSets.Range(func(_, set any) bool {
		entries.fileSets = append(entries.fileSets, set.([]entryID))
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
	for w := range c.crls {
		paths = append(paths, w.crl)
		if w.ca != "" {
			paths = append(paths, w.ca)
		}
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

// entryID names a cache entry, as the files it is loaded from.
type entryID string

func keyPairEntry(id any) entryID { return entryID(fmt.Sprintf("key pair %v", id)) }
func combinedKeyPairEntry(id any) entryID {
	return entryID(fmt.Sprintf("key pair with CA chain %v", id))
}
func caPoolEntry(ca string) entryID { return entryID("CA pool " + ca) }
func caCertificatesEntry(ca string) entryID {
	return entryID("CA certificates " + ca)
}
func crlEntry(w crlWatch) entryID { return entryID(fmt.Sprintf("CRL %s under CA %s", w.crl, w.ca)) }

// fileSets maps the entries a configuration was built from, joined, to
// those entries, and fileSetsRegistered counts them. See
// ReloadCachedFiles.
var (
	fileSets           sync.Map
	fileSetsRegistered atomic.Uint64
)

// registerFileSet records that a configuration was built from the
// entries of set.
func registerFileSet(set ...entryID) {
	if len(set) > 1 {
		key := make([]string, len(set))
		for i, id := range set {
			key[i] = string(id)
		}
		if _, loaded := fileSets.LoadOrStore(strings.Join(key, "\x00"), set); !loaded {
			fileSetsRegistered.Add(1)
		}
	}
}

// entryResult is what reading the files of a cache entry again gave:
// the update of an entry whose files changed, or the error of one that
// could not be loaded. The entries whose files did not change have no
// result.
type entryResult struct {
	update *cacheUpdate
	err    error
}

// read reads the files of the entries, and returns the result of each
// entry whose files changed or failed to load, without applying them.
func (c cachedFiles) read() map[entryID]entryResult {
	results := make(map[entryID]entryResult)
	readKeyPairs := func(cache *sync.Map, entries map[any]keyPairFiles, name func(any) entryID, read func(keyPairFiles) (*[]tls.Certificate, error)) {
		for id, files := range entries {
			fresh, err := read(files)
			if err != nil {
				results[name(id)] = entryResult{err: err}
				continue
			}
			if cached, ok := cache.Load(id); !ok || !sameCertificates(*cached.(*[]tls.Certificate), *fresh) {
				results[name(id)] = entryResult{update: &cacheUpdate{cache: cache, key: id, value: fresh}}
			}
		}
	}
	readKeyPairs(&tlsCertificates, c.keyPairs, keyPairEntry, func(f keyPairFiles) (*[]tls.Certificate, error) {
		return readTLSCertificate(f.cert, f.key)
	})
	readKeyPairs(&combinedTLSCertificates, c.combinedKeyPairs, combinedKeyPairEntry, func(f keyPairFiles) (*[]tls.Certificate, error) {
		return readAndCombineTLSCertificates(f.ca, f.cert, f.key)
	})
	for ca, cached := range c.certPools {
		fresh, err := readx509CertPool(ca)
		if err != nil {
			results[caPoolEntry(ca)] = entryResult{err: err}
			continue
		}
		if !fresh.Equal(cached) {
			results[caPoolEntry(ca)] = entryResult{update: &cacheUpdate{cache: &certPools, key: ca, value: fresh}}
		}
	}
	// The CA certificates that CRLs are validated under: the ones
	// this read loads, or else the ones in the cache.
	readCAs := make(map[string][]*x509.Certificate)
	for ca, cached := range c.caCertificates {
		fresh, err := readx509Certificates(ca)
		if err != nil {
			results[caCertificatesEntry(ca)] = entryResult{err: err}
			continue
		}
		if !slices.EqualFunc(fresh, cached, (*x509.Certificate).Equal) {
			results[caCertificatesEntry(ca)] = entryResult{update: &cacheUpdate{cache: &caCertificates, key: ca, value: fresh}}
			readCAs[ca] = fresh
		}
	}
	for w, seen := range c.crls {
		body, err := os.ReadFile(w.crl)
		if err != nil {
			results[crlEntry(w)] = entryResult{err: vterrors.Wrapf(err, "failed to read crl file: %s", w.crl)}
			continue
		}
		digest := sha256.Sum256(body)
		issuers, caChanged := readCAs[w.ca]
		if digest == seen && !caChanged {
			continue
		}
		if w.ca != "" && !caChanged {
			if issuers, err = loadx509Certificates(w.ca); err != nil {
				results[crlEntry(w)] = entryResult{err: err}
				continue
			}
		}
		// Validated as ClientConfig will use it, since a CRL that its
		// CA does not validate would have ClientConfig fall back to the
		// CRLs last loaded from the file.
		crls, err := parseCRLSet(w.crl, body)
		if err == nil {
			_, err = newCRLCheckerFrom(crls, issuers)
		}
		if err != nil {
			results[crlEntry(w)] = entryResult{err: vterrors.Wrapf(err, "cannot use the CRL file %s under the CA file %s; the CRLs last loaded from it stay in use", w.crl, w.ca)}
			continue
		}
		if digest != seen {
			results[crlEntry(w)] = entryResult{update: &cacheUpdate{cache: &watchedCRLs, key: w, value: digest}}
		}
	}
	return results
}

// withhold returns the updates of results to publish, and the errors
// to report. An entry that failed to load holds back the updates of
// every entry that it shares a configuration with, directly or through
// other entries, so that each configuration keeps all its files from
// before the reload or takes all of them from after it.
func (c cachedFiles) withhold(results map[entryID]entryResult) ([]cacheUpdate, []error) {
	group := make(map[entryID]entryID)
	var find func(entryID) entryID
	find = func(id entryID) entryID {
		parent, ok := group[id]
		if !ok || parent == id {
			return id
		}
		root := find(parent)
		group[id] = root
		return root
	}
	for _, set := range c.fileSets {
		for _, id := range set[1:] {
			if a, b := find(set[0]), find(id); a != b {
				group[b] = a
			}
		}
	}

	failed := make(map[entryID]bool)
	var errs []error
	for id, result := range results {
		if result.err != nil {
			failed[find(id)] = true
			errs = append(errs, result.err)
		}
	}
	var updates []cacheUpdate
	var heldBack []string
	for id, result := range results {
		if result.update == nil {
			continue
		}
		if failed[find(id)] {
			heldBack = append(heldBack, string(id))
			continue
		}
		updates = append(updates, *result.update)
	}
	if len(heldBack) > 0 {
		slices.Sort(heldBack)
		errs = append(errs, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "not reloaded either, since a configuration uses them along with a file that failed to load: %s", strings.Join(heldBack, ", ")))
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
	// The CRL is read once, so that the digest the file is watched
	// from is the digest of the CRLs checked against.
	body, err := os.ReadFile(crl)
	if err == nil {
		var crls []*x509.RevocationList
		if crls, err = parseCRLSet(crl, body); err == nil {
			var checker *crlChecker
			if checker, err = newCRLCheckerFrom(crls, issuers); err == nil {
				lastValidCRLs.Store(crl, crls)
				watchCRL(crlWatch{crl: crl, ca: ca}, sha256.Sum256(body))
				return checker, nil
			}
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
