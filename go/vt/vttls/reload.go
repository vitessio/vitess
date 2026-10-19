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
	// watchedCRLs holds the CRL files ClientConfig used, with the CA
	// file it used them with, as crlWatch keys. The digest of the
	// contents the configs were built from is kept in their
	// crlFallback.
	watchedCRLs sync.Map
	// lastValidCRLs maps the CRL files ClientConfig used, with the CA
	// file it used them with, to their *crlFallback, and
	// staleCRLWarnings holds each of those whose last valid CRLs it
	// held against peers for, with the error it warned about then: the
	// warning repeats when the failure changes, or when the file breaks
	// again after having worked.
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
// has files to read again. A ServerConfig load arms it too, rather
// than leaving a config built with it holding files that nothing
// reloads: the Vitess servers read their files anew through
// ReadServerConfig, so what a reload then updates is only the cache
// that ServerConfig, kept for its other callers, serves from.
func CachedFilesInUse() <-chan struct{} {
	return cachedFilesInUse
}

func markCachedFilesInUse() {
	cachedFilesInUseOnce.Do(func() { close(cachedFilesInUse) })
}

// CachedFilesGeneration returns a number that changes whenever
// ReloadCachedFiles finds that a file changed, and whenever ClientConfig
// builds a config from a CRL that differs from the one the configs
// before it were built from. A config built by ClientConfig before it
// changed may hold what the files held before.
func CachedFilesGeneration() uint64 {
	return cachedFilesGeneration.Load()
}

// crlWatch is a CRL file that ClientConfig used, and the CA file it
// used it with, empty for none.
type crlWatch struct {
	crl, ca string
}

// watchCRL has ReloadCachedFiles watch the CRL file of w for changes
// from what the configs ClientConfig built were built from, see
// crlFallback.observe. ClientConfig reads the CRL on every call, so the
// file is only watched, for CachedFilesGeneration, not cached.
func watchCRL(w crlWatch) {
	if _, loaded := watchedCRLs.LoadOrStore(w, struct{}{}); !loaded {
		markCachedFilesInUse()
	}
}

// crlFallback holds the CRLs last loaded from a CRL file under a CA
// file, by ClientConfig or ReloadCachedFiles, which ClientConfig falls
// back to when the file cannot be read or used under that CA. They are
// kept for each CA, since a CRL file that one CA validates may hold a
// CRL that another cannot, and the CRLs of one must not take the place
// of those that another can use. The reads of the file are
// numbered in the order they are made, and CRLs replace only those
// from an earlier read, so that a read validated slowly, or published
// by a reload after a later read, does not leave older CRLs in place
// of newer ones.
type crlFallback struct {
	// readMu serializes the reads of the file, so that their numbers
	// follow the order they are made in, and guards reads, which counts
	// them. mu guards the rest, and is never held while the file is
	// read, so that a read stalled on the file system holds back
	// neither store nor load, which are called under publishMu.
	readMu sync.Mutex
	reads  uint64

	mu sync.Mutex
	// crlsRead is the number of the read that crls came from, zero for
	// none.
	crlsRead uint64
	crls     []*x509.RevocationList
	// issuers holds, by DER encoding, the issuers beyond the CA file
	// that the verified chains of peers brought and that the CRLs bound
	// to, at most maxBoundIssuers of them, see clientCRLCheckerFrom.
	issuers map[string]*x509.Certificate
	// digest is the SHA-256 digest of the file's contents that the
	// configs built since CachedFilesGeneration last changed were built
	// from, as of the read numbered digestRead, zero for none, see
	// observe.
	digest     [sha256.Size]byte
	digestRead uint64
}

func crlFallbackOf(w crlWatch) *crlFallback {
	fallback, _ := lastValidCRLs.LoadOrStore(w, &crlFallback{})
	return fallback.(*crlFallback)
}

// read returns what the file holds, and the number of this read.
func (f *crlFallback) read(crl string) ([]byte, uint64, error) {
	f.readMu.Lock()
	defer f.readMu.Unlock()
	f.reads++
	if crlFileReadTestHook != nil {
		crlFileReadTestHook()
	}
	body, err := os.ReadFile(crl)
	return body, f.reads, err
}

// store keeps crls, from the read numbered read, unless the CRLs of a
// later read are kept already.
func (f *crlFallback) store(crls []*x509.RevocationList, read uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if read > f.crlsRead {
		f.crls, f.crlsRead = crls, read
	}
}

func (f *crlFallback) load() ([]*x509.RevocationList, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.crls, f.crlsRead > 0
}

// observe records that a config is built from the file's contents
// whose digest is digest, as of the read numbered read, and reports
// whether those differ from the contents that the configs built before
// it were built from, in which case CachedFilesGeneration has to change
// so that those are built again: when a config is built while the file
// briefly holds other contents, and the contents before are back by
// the next reload, that reload finds no change from the digest first
// recorded, yet the config holds the other contents. The digest of a
// later read than the one recorded replaces it. The first recorded
// digest invalidates nothing.
func (f *crlFallback) observe(digest [sha256.Size]byte, read uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.digestRead == 0 {
		f.digest, f.digestRead = digest, read
		return false
	}
	if read > f.digestRead {
		changed := digest != f.digest
		f.digest, f.digestRead = digest, read
		return changed
	}
	return digest != f.digest
}

func (f *crlFallback) observed() [sha256.Size]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.digest
}

// newerThan returns the CRLs kept, when they come from a read made
// after the read numbered read.
func (f *crlFallback) newerThan(read uint64) ([]*x509.RevocationList, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.crls, f.crlsRead > read
}

// learnIssuer records issuer, which a verified chain brought, unless
// maxBoundIssuers are recorded already.
func (f *crlFallback) learnIssuer(issuer *x509.Certificate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, known := f.issuers[string(issuer.Raw)]; known || len(f.issuers) >= maxBoundIssuers {
		return
	}
	if f.issuers == nil {
		f.issuers = make(map[string]*x509.Certificate)
	}
	f.issuers[string(issuer.Raw)] = issuer
}

func (f *crlFallback) learnedIssuers() []*x509.Certificate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Collect(maps.Values(f.issuers))
}

// clientCRLCheckerFrom is newCRLCheckerFrom for the CRLs of a client's
// CRL file, which are also checked against the issuers beyond the CA
// file that verified chains brought before, see crlFallback. The CRLs
// of such an issuer, an intermediate under a CA file that holds the
// root alone for one, are otherwise only checked when a chain next
// brings it, so that a replacement whose signature does not verify
// would take the place of the CRLs to fall back to, and fail every
// handshake through that issuer even once the file is gone. The
// bindings this makes are kept in the checker, within its bound, so
// that its handshakes through those issuers do not check the CRLs'
// signatures again; ClientConfig builds a checker for every MySQL
// connection. The checker it returns records the issuers that chains
// bring from then on.
func clientCRLCheckerFrom(crls []*x509.RevocationList, issuers []*x509.Certificate, fallback *crlFallback) (*crlChecker, error) {
	checker, err := newCRLCheckerFrom(crls, issuers)
	if err != nil {
		return nil, err
	}
	for _, issuer := range fallback.learnedIssuers() {
		if _, configured := checker.configured[string(issuer.Raw)]; configured {
			continue
		}
		issuerCRLs, err := checker.bindCRLs(issuer)
		if err != nil {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "the CRLs cannot be applied under the CA certificate %s that a verified chain of a peer brought: %v", issuer.Subject.CommonName, err)
		}
		if checker.reserveBinding() {
			if _, loaded := checker.bound.LoadOrStore(string(issuer.Raw), crlBinding{crls: issuerCRLs}); loaded {
				checker.boundIssuers.Add(-1)
			}
		}
	}
	checker.onIssuer = fallback.learnIssuer
	return checker, nil
}

// maxReloadAttempts bounds how many times ReloadCachedFiles reads the
// files again looking for a consistent view of them.
const maxReloadAttempts = 5

// reloadTestHook, when set, runs on every attempt ReloadCachedFiles
// makes to read the files, after it digested them and before it reads
// them, so that a test can change the files in between. publishTestHook,
// when set, runs after ReloadCachedFiles updated the first cache entry
// of a reload and before the others. crlReadTestHook, when set, runs
// after ClientConfig read a CRL file and before it takes publishMu to
// validate it. All
// are nil outside tests.
var reloadTestHook, publishTestHook, crlReadTestHook func()

// crlFileReadTestHook, when set, runs in every read of a CRL file that
// a crlFallback numbers, before the file is read. Nil outside tests.
var crlFileReadTestHook func()

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
//
// A read of a CRL file that hangs, as on an unresponsive NFS mount,
// holds up the reload, and the reloads after it, until it returns:
// reloads are serialized, and a CRL file's reads are too, see
// crlFallback. It does not hold up the configs that ClientConfig builds
// from other files, nor the servers' reloads.
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
		if u.apply != nil {
			u.apply()
		} else {
			u.cache.Store(u.key, u.value)
		}
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
	watchedCRLs.Range(func(w, _ any) bool {
		entries.crls[w.(crlWatch)] = crlFallbackOf(w.(crlWatch)).observed()
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
	// apply, when set, applies the update instead of storing value.
	apply func()
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

func crlEntry(w crlWatch) entryID {
	if w.ca == "" {
		return entryID("CRL " + w.crl)
	}
	return entryID("CRL " + w.crl + " under CA " + w.ca)
}

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
// the updates of an entry whose files changed, or the error of one that
// could not be loaded. The entries whose files did not change have no
// result.
type entryResult struct {
	updates []cacheUpdate
	err     error
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
				results[name(id)] = entryResult{updates: []cacheUpdate{{cache: cache, key: id, value: fresh}}}
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
			results[caPoolEntry(ca)] = entryResult{updates: []cacheUpdate{{cache: &certPools, key: ca, value: fresh}}}
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
			results[caCertificatesEntry(ca)] = entryResult{updates: []cacheUpdate{{cache: &caCertificates, key: ca, value: fresh}}}
			readCAs[ca] = fresh
		}
	}
	for w, seen := range c.crls {
		fallback := crlFallbackOf(w)
		body, read, err := fallback.read(w.crl)
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
			_, err = clientCRLCheckerFrom(crls, issuers, fallback)
		}
		if err != nil {
			results[crlEntry(w)] = entryResult{err: vterrors.Wrapf(err, "cannot use the CRL file %s under the CA file %s; the CRLs last loaded from it stay in use", w.crl, w.ca)}
			continue
		}
		// The CRLs are also what ClientConfig falls back to from then
		// on, rather than the ones it last loaded itself, which a
		// config built while the file is briefly unreadable would
		// otherwise keep, with nothing left to reload once it is back.
		if digest != seen {
			results[crlEntry(w)] = entryResult{updates: []cacheUpdate{{apply: func() {
				fallback.store(crls, read)
				fallback.observe(digest, read)
			}}}}
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
		if result.updates == nil {
			continue
		}
		if failed[find(id)] {
			heldBack = append(heldBack, string(id))
			continue
		}
		updates = append(updates, result.updates...)
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

// clientCRLRead is a read of a CRL file that ClientConfig makes before
// it takes publishMu, so that a read stalled on a slow or unresponsive
// file system holds back neither the publish of a reload nor the
// configs built after it. A read that a later one overtook, such as
// the read of a reload that published since, is not built from, see
// clientCRLChecker.
type clientCRLRead struct {
	w        crlWatch
	fallback *crlFallback
	body     []byte
	// read is the number of the read, see crlFallback.
	read uint64
	err  error
}

func readClientCRL(crl, ca string) clientCRLRead {
	w := crlWatch{crl: crl, ca: ca}
	fallback := crlFallbackOf(w)
	body, read, err := fallback.read(crl)
	if err == nil && crlReadTestHook != nil {
		crlReadTestHook()
	}
	return clientCRLRead{w: w, fallback: fallback, body: body, read: read, err: err}
}

// clientCRLChecker is newCRLChecker for ClientConfig, which reads the
// CRL file on every call, see readClientCRL. When the file cannot be
// read or used, for instance while it is being replaced, the CRLs last
// loaded from it are held against the peer instead, as a server whose
// reload fails keeps its previous CRLs, rather than every new
// connection failing. Only the first load of a file under each CA must
// succeed.
func clientCRLChecker(r clientCRLRead) (*crlChecker, error) {
	var issuers []*x509.Certificate
	if r.w.ca != "" {
		var err error
		if issuers, err = loadx509Certificates(r.w.ca); err != nil {
			return nil, err
		}
	}
	// A read overtaken by a later one, whose CRLs are kept, is not
	// built from: a reload may have published a new CA along with
	// those CRLs, and the CRLs of the older read, from the old CA, would
	// then apply to no certificate of the new CA, letting through the
	// ones that the newer CRLs revoke. The newer CRLs were validated
	// under the CA the config is built with, or published along with
	// it.
	if newer, overtaken := r.fallback.newerThan(r.read); overtaken {
		return clientCRLCheckerFrom(newer, issuers, r.fallback)
	}
	// The CRL is read once, so that the digest the file is watched
	// from is the digest of the CRLs checked against.
	err := r.err
	if err == nil {
		var crls []*x509.RevocationList
		if crls, err = parseCRLSet(r.w.crl, r.body); err == nil {
			var checker *crlChecker
			if checker, err = clientCRLCheckerFrom(crls, issuers, r.fallback); err == nil {
				r.fallback.store(crls, r.read)
				if r.fallback.observe(sha256.Sum256(r.body), r.read) {
					cachedFilesGeneration.Add(1)
				}
				watchCRL(r.w)
				staleCRLWarnings.Delete(r.w)
				return checker, nil
			}
		}
	}
	last, ok := r.fallback.load()
	if !ok {
		return nil, err
	}
	checker, lastErr := clientCRLCheckerFrom(last, issuers, r.fallback)
	if lastErr != nil {
		return nil, err
	}
	if warned, warnedFor := staleCRLWarnings.Load(r.w); !warnedFor || warned.(string) != err.Error() {
		staleCRLWarnings.Store(r.w, err.Error())
		log.Warn(fmt.Sprintf("Cannot use the CRL file %s under the CA file %s, so the CRLs last loaded from it stay in use: %v", r.w.crl, r.w.ca, err))
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
