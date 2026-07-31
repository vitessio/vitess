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

package mysqlctl

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
)

// shutdownFlockPollInterval is how often a shutdown attempt re-tries the
// interprocess shutdown lock while another process holds it.
const shutdownFlockPollInterval = 250 * time.Millisecond

// shutdownFlockPath returns the per-instance interprocess shutdown lock file,
// kept next to the instance's pid file so that shutdown attempts from
// different processes (e.g. two concurrent mysqlctl CLI invocations for the
// same tablet) contend on the same path.
func shutdownFlockPath(cnf *Mycnf) string {
	return filepath.Join(filepath.Dir(cnf.PidFile), "mysqld_shutdown.flock")
}

// acquireShutdownFlock takes -- or confirms this Mysqld already holds -- the
// per-instance interprocess shutdown lock. The in-process shutdown gate only
// serializes attempts sharing one Mysqld object; a fresh process (e.g. a
// second mysqlctl CLI invocation) builds its own object, and without
// cross-process serialization one failed attempt's background replica-state
// restoration could reset the durability fence beneath another process's
// shutdown. The lock is deliberately held until Close so it also covers that
// pending restoration, which Close waits out; a process that exits or
// crashes releases its lock through the OS. Cross-process waiters therefore
// wait a prior attempt's restoration out -- bounded by their own ctx --
// rather than take it over the way same-object retries do.
//
// Lock SETUP failures -- opening or locking the file for any reason other
// than contention -- degrade to proceeding without cross-process
// serialization rather than failing the shutdown: the crash-safety machinery
// is best effort and must never veto a shutdown (e.g. on a read-only
// directory or a filesystem without flock support). Contention keeps its
// meaning: waiting for another process's attempt is real serialization,
// bounded by the caller's ctx.
func (mysqld *Mysqld) acquireShutdownFlock(ctx context.Context, cnf *Mycnf) error {
	// The gate lets exactly one goroutine per object run the lock loop;
	// same-object concurrency is then serialized by the shutdown gate.
	mysqld.shutdownFlockGateOnce.Do(func() {
		mysqld.shutdownFlockGateCh = make(chan struct{}, 1)
	})
	select {
	case mysqld.shutdownFlockGateCh <- struct{}{}:
	case <-ctx.Done():
		return vterrors.Wrap(ctx.Err(), "shutdown cancelled while waiting for a concurrent mysqld shutdown in another process")
	}
	defer func() { <-mysqld.shutdownFlockGateCh }()

	mysqld.shutdownFlockMu.Lock()
	held := mysqld.shutdownFlock != nil
	mysqld.shutdownFlockMu.Unlock()
	if held {
		return nil
	}

	path := shutdownFlockPath(cnf)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		log.Warn(
			"cannot open the interprocess shutdown lock file; proceeding without cross-process shutdown serialization",
			slog.String("lock_file", path),
			slog.Any("error", err),
		)
		return nil
	}
	logged := false
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			mysqld.shutdownFlockMu.Lock()
			mysqld.shutdownFlock = f
			mysqld.shutdownFlockMu.Unlock()
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			f.Close()
			log.Warn(
				"cannot lock the interprocess shutdown lock file; proceeding without cross-process shutdown serialization",
				slog.String("lock_file", path),
				slog.Any("error", err),
			)
			return nil
		}
		if !logged {
			log.Warn(
				"waiting for a concurrent mysqld shutdown in another process",
				slog.String("lock_file", path),
			)
			logged = true
		}
		select {
		case <-ctx.Done():
			f.Close()
			return vterrors.Wrap(ctx.Err(), "shutdown cancelled while waiting for a concurrent mysqld shutdown in another process")
		case <-time.After(shutdownFlockPollInterval):
		}
	}
}

// releaseShutdownFlock releases the interprocess shutdown lock if this Mysqld
// holds it. Closing the file descriptor releases the flock.
func (mysqld *Mysqld) releaseShutdownFlock() {
	mysqld.shutdownFlockMu.Lock()
	defer mysqld.shutdownFlockMu.Unlock()
	if mysqld.shutdownFlock != nil {
		mysqld.shutdownFlock.Close()
		mysqld.shutdownFlock = nil
	}
}
