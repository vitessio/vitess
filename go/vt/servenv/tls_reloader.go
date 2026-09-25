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

package servenv

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"
)

var (
	tlsReloadSuccessTimestamp = stats.NewGaugesWithSingleLabel("TLSReloadSuccessTimestamp", "Unix time of the last successful load of a server's TLS files", "Server")
	tlsReloadErrors           = stats.NewCountersWithSingleLabel("TLSReloadErrors", "Number of failed reloads of a server's TLS files", "Server")
	tlsCertNotAfter           = stats.NewGaugesWithSingleLabel("TLSCertNotAfter", "Unix time at which the certificate a server presents expires", "Server")
)

// TLSServerFiles are the files a server's TLS config is built from.
// Empty paths are unused.
type TLSServerFiles struct {
	Cert, Key, CA, CRL, ServerCA string
}

// TLSReloader keeps a server's TLS config in step with its files. It
// builds a new config whenever it is told to, on SIGHUP or on a
// timer, and hands it to a sink, which stores it where the server's
// handshakes load it from. A config is never changed once handed
// over. A reload that fails leaves the previous config in place.
type TLSReloader struct {
	name          string
	files         TLSServerFiles
	minTLSVersion uint16
	sink          func(*tls.Config)

	mu sync.Mutex
	// fingerprint digests the files the current config was built
	// from, and caFingerprint the CA file alone.
	fingerprint, caFingerprint [sha256.Size]byte
	// ticketKeys are the session ticket keys every config gets once
	// the CA file changed, nil before that. See Reload.
	ticketKeys [][32]byte

	cancel context.CancelFunc
	done   chan struct{}
}

// NewTLSReloader loads the TLS config of the server that name
// identifies in logs and metrics from files and hands it to sink.
func NewTLSReloader(name string, files TLSServerFiles, minTLSVersion uint16, sink func(*tls.Config)) (*TLSReloader, error) {
	r := &TLSReloader{
		name:          name,
		files:         files,
		minTLSVersion: minTLSVersion,
		sink:          sink,
	}
	if err := r.Reload(true); err != nil {
		return nil, err
	}
	return r, nil
}

// Start reloads the config on every signal received on signals, and
// every interval if it is positive, when the files changed since the
// last load, until ctx is done or Stop is called. signals may be nil
// when the config reloads on a timer alone. It must be called at most
// once.
func (r *TLSReloader) Start(ctx context.Context, signals <-chan os.Signal, interval time.Duration) {
	ctx, r.cancel = context.WithCancel(ctx)
	r.done = make(chan struct{})

	go func() {
		defer close(r.done)
		var tick <-chan time.Time
		if interval > 0 {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			tick = ticker.C
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-signals:
				_ = r.Reload(true)
			case <-tick:
				_ = r.Reload(false)
			}
		}
	}()
}

// StartOnSIGHUP is Start with the process's SIGHUPs as the signals.
func (r *TLSReloader) StartOnSIGHUP(interval time.Duration) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP)
	r.Start(context.Background(), signals, interval)
	go func() {
		<-r.done
		signal.Stop(signals)
	}()
}

// Stop ends what Start started and waits for it to finish.
func (r *TLSReloader) Stop() {
	if r.cancel == nil {
		return
	}
	r.cancel()
	<-r.done
}

// Reload builds a new config from the files and hands it to the sink.
// Unless force is set, it does nothing when the files are the same as
// the ones the current config was built from.
//
// When the CA file changed, the new config gets fresh session ticket
// keys, which every later config keeps. On resumption Go does not
// verify the client's certificate against the CAs again, so a session
// established under a CA that has since been removed would otherwise
// still resume.
func (r *TLSReloader) Reload(force bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// The files are digested before they are loaded, so that files
	// changing in between are loaded again on the next tick.
	fingerprint, caFingerprint, err := r.digest()
	if err != nil {
		return r.failed(err)
	}
	if !force && fingerprint == r.fingerprint {
		return nil
	}

	config, err := vttls.ReadServerConfig(r.files.Cert, r.files.Key, r.files.CA, r.files.CRL, r.files.ServerCA, r.minTLSVersion)
	if err != nil {
		return r.failed(err)
	}

	var zero [sha256.Size]byte
	if r.caFingerprint != zero && caFingerprint != r.caFingerprint {
		var key [32]byte
		if _, err := rand.Read(key[:]); err != nil {
			return r.failed(err)
		}
		r.ticketKeys = [][32]byte{key}
	}
	if r.ticketKeys != nil {
		config.SetSessionTicketKeys(r.ticketKeys)
	}

	r.sink(config)
	r.fingerprint, r.caFingerprint = fingerprint, caFingerprint

	now := time.Now()
	tlsReloadSuccessTimestamp.Set(r.name, now.Unix())
	if leaf := leafCertificate(config); leaf != nil {
		tlsCertNotAfter.Set(r.name, leaf.NotAfter.Unix())
		log.Info(fmt.Sprintf("Loaded the %s server's TLS config: certificate %q, serial %s, valid until %s", r.name, leaf.Subject, leaf.SerialNumber, leaf.NotAfter.Format(time.RFC3339)))
	}
	return nil
}

func (r *TLSReloader) failed(err error) error {
	tlsReloadErrors.Add(r.name, 1)
	err = vterrors.Wrapf(err, "cannot load the %s server's TLS config from cert %s, key %s, ca %s, crl %s, server-ca %s; the previous config stays in place", r.name, r.files.Cert, r.files.Key, r.files.CA, r.files.CRL, r.files.ServerCA)
	log.Error(err.Error())
	return err
}

// digest returns the SHA-256 digest of the files, and of the CA file
// alone.
func (r *TLSReloader) digest() (all, ca [sha256.Size]byte, err error) {
	entries := []struct {
		path string
		ca   bool
	}{
		{path: r.files.Cert},
		{path: r.files.Key},
		{path: r.files.CA, ca: true},
		{path: r.files.CRL},
		{path: r.files.ServerCA},
	}
	h := sha256.New()
	for _, entry := range entries {
		var content []byte
		if entry.path != "" {
			if content, err = os.ReadFile(entry.path); err != nil {
				return all, ca, err
			}
		}
		if entry.ca {
			ca = sha256.Sum256(content)
		}
		// Length-prefixed, so that no two sets of files digest alike.
		_ = binary.Write(h, binary.BigEndian, uint64(len(content)))
		h.Write(content)
	}
	copy(all[:], h.Sum(nil))
	return all, ca, nil
}

// leafCertificate returns the certificate that config presents.
func leafCertificate(config *tls.Config) *x509.Certificate {
	if len(config.Certificates) == 0 {
		return nil
	}
	cert := config.Certificates[0]
	if cert.Leaf != nil {
		return cert.Leaf
	}
	if len(cert.Certificate) == 0 {
		return nil
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil
	}
	return leaf
}
