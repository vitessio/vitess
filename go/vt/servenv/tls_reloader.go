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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"
)

var (
	tlsReloadSuccessTimestamp = stats.NewGaugesWithSingleLabel("TLSReloadSuccessTimestamp", "Unix time of the last successful load of the TLS files of a server, or of the process's clients", "Server")
	tlsReloadErrors           = stats.NewCountersWithSingleLabel("TLSReloadErrors", "Number of failed reloads of the TLS files of a server, or of the process's clients", "Server")
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
	// from.
	fingerprint [sha256.Size]byte
	// config is the current config, and caPEM the contents of the CA
	// file it was built from, which ReloadCRL validates the CRLs under.
	config *tls.Config
	caPEM  []byte

	// testHook, when set, runs on every attempt Reload makes to read
	// the files, between the pre-load digest and the config load, so
	// a test can change the files in that window. Nil outside tests.
	testHook func()

	cancel context.CancelFunc
	done   chan struct{}
}

// maxReadAttempts bounds how many times Reload re-reads the files
// looking for a digest that matches what it actually loaded, before
// giving up and keeping the previous config.
const maxReadAttempts = 5

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
// last load, until ctx is done or Stop is called. A signal reloads
// every file when signalReloadsAll is set, and the CRL alone, with
// ReloadCRL, otherwise. signals may be nil when the config reloads on
// a timer alone. It must be called at most once.
func (r *TLSReloader) Start(ctx context.Context, signals <-chan os.Signal, signalReloadsAll bool, interval time.Duration) {
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
				if signalReloadsAll {
					_ = r.Reload(true)
				} else {
					_ = r.ReloadCRL()
				}
			case <-tick:
				_ = r.Reload(false)
			}
		}
	}()
}

// StartOnProcessSignals is Start with the process's SIGHUPs as the
// signals when onSIGHUP is set, and none otherwise. It starts nothing
// when there is neither a signal nor an interval to reload on.
func (r *TLSReloader) StartOnProcessSignals(onSIGHUP bool, interval time.Duration) {
	if !onSIGHUP {
		if interval > 0 {
			r.Start(context.Background(), nil, true, interval)
		}
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP)
	r.Start(context.Background(), signals, true, interval)
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
func (r *TLSReloader) Reload(force bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		fingerprint [sha256.Size]byte
		caPEM       []byte
		config      *tls.Config
	)
	// The files are digested before they are loaded, and again after,
	// so that a file changing mid-load is caught rather than paired
	// with a fingerprint of what it contained before the change, which
	// would have the next reload skip the change. The read is retried,
	// re-digesting first, until a pre- and post-load digest agree.
	for attempt := 1; ; attempt++ {
		var err error
		if fingerprint, caPEM, err = r.digest(); err != nil {
			return r.failed(err)
		}
		if !force && attempt == 1 && fingerprint == r.fingerprint {
			return nil
		}
		if r.testHook != nil {
			r.testHook()
		}

		if config, err = vttls.ServerConfig(r.files.Cert, r.files.Key, r.files.CA, r.files.CRL, r.files.ServerCA, r.minTLSVersion); err != nil {
			return r.failed(err)
		}

		after, _, err := r.digest()
		if err != nil {
			return r.failed(err)
		}
		if after == fingerprint {
			break
		}
		if attempt == maxReadAttempts {
			return r.failed(vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "the files kept changing across %d attempts to read them consistently", maxReadAttempts))
		}
	}

	r.sink(config)
	r.fingerprint = fingerprint
	r.config, r.caPEM = config, caPEM

	now := time.Now()
	tlsReloadSuccessTimestamp.Set(r.name, now.Unix())
	if leaf := leafCertificate(config); leaf != nil {
		tlsCertNotAfter.Set(r.name, leaf.NotAfter.Unix())
		log.Info("Loaded a server's TLS config",
			slog.String("server", r.name),
			slog.String("subject", leaf.Subject.String()),
			slog.String("serial", leaf.SerialNumber.String()),
			slog.Time("not_after", leaf.NotAfter))
	}
	return nil
}

// ReloadCRL hands the sink the current config with the CRLs that the
// CRL file holds now, validated under the CA file the config was built
// from. The certificate, key and CA stay as they were loaded. It does
// nothing for a server without a CRL file.
func (r *TLSReloader) ReloadCRL() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.files.CRL == "" {
		return nil
	}
	config, err := vttls.ServerConfigWithCRL(r.config, r.caPEM, r.files.CRL)
	if err != nil {
		return r.failed(err)
	}
	r.sink(config)
	r.config = config
	tlsReloadSuccessTimestamp.Set(r.name, time.Now().Unix())
	log.Info("Reloaded a server's CRL", slog.String("server", r.name), slog.String("crl", r.files.CRL))
	return nil
}

func (r *TLSReloader) failed(err error) error {
	tlsReloadErrors.Add(r.name, 1)
	err = vterrors.Wrapf(err, "cannot load the %s server's TLS config from cert %s, key %s, ca %s, crl %s, server-ca %s; the previous config stays in place", r.name, r.files.Cert, r.files.Key, r.files.CA, r.files.CRL, r.files.ServerCA)
	log.Error("Cannot load a server's TLS config; the previous config stays in place", slog.String("server", r.name), slog.Any("error", err))
	return err
}

// digest returns the SHA-256 digest of the files, and the contents of
// the CA file.
func (r *TLSReloader) digest() (digest [sha256.Size]byte, caPEM []byte, err error) {
	h := sha256.New()
	for _, path := range []string{r.files.Cert, r.files.Key, r.files.CA, r.files.CRL, r.files.ServerCA} {
		var content []byte
		if path != "" {
			if content, err = os.ReadFile(path); err != nil {
				return digest, nil, err
			}
		}
		if path == r.files.CA {
			caPEM = content
		}
		// Length-prefixed, so that no two sets of files digest alike.
		_ = binary.Write(h, binary.BigEndian, uint64(len(content)))
		h.Write(content)
	}
	copy(digest[:], h.Sum(nil))
	return digest, caPEM, nil
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
