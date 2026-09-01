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
	"sync"
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

// acquireShutdownFlock takes -- or adds a reference to, when this Mysqld
// already holds it -- the per-instance interprocess shutdown lock, returning
// the release of that reference. The in-process shutdown gate only
// serializes attempts sharing one Mysqld object; a fresh process (e.g. a
// second mysqlctl CLI invocation) builds its own object, and without
// cross-process serialization one failed attempt's background replica-state
// restoration could reset the durability fence beneath another process's
// shutdown. The lock is reference-counted rather than held for the object's
// lifetime: the attempt holds a reference until it returns, a restoration it
// arms holds another until that completes (see armReplicaRestore), and the
// last drop releases the lock -- so a long-lived owner such as mysqlctld
// does not block every other process's shutdown between attempts, and the
// next attempt reacquires the lock fresh. A process that exits or crashes
// releases its lock through the OS. Cross-process waiters therefore wait a
// prior attempt and its restoration out -- bounded by their own ctx --
// rather than take it over the way same-object retries do.
//
// Lock SETUP failures -- opening or locking the file for any reason other
// than contention -- degrade to proceeding without cross-process
// serialization rather than failing the shutdown: the crash-safety machinery
// is best effort and must never veto a shutdown (e.g. on a read-only
// directory or a filesystem without flock support). The returned release is
// then a no-op. Contention keeps its meaning: waiting for another process's
// attempt is real serialization, bounded by the caller's ctx.
func (mysqld *Mysqld) acquireShutdownFlock(ctx context.Context, cnf *Mycnf) (release func(), err error) {
	// The gate lets exactly one goroutine per object run the lock loop;
	// same-object concurrency is then serialized by the shutdown gate.
	mysqld.shutdownFlockGateOnce.Do(func() {
		mysqld.shutdownFlockGateCh = make(chan struct{}, 1)
	})
	select {
	case mysqld.shutdownFlockGateCh <- struct{}{}:
	case <-ctx.Done():
		return nil, vterrors.Wrap(ctx.Err(), "shutdown cancelled while waiting for a concurrent mysqld shutdown in another process")
	}
	defer func() { <-mysqld.shutdownFlockGateCh }()

	mysqld.shutdownFlockMu.Lock()
	if f := mysqld.shutdownFlock; f != nil {
		mysqld.shutdownFlockRefs++
		mysqld.shutdownFlockMu.Unlock()
		return mysqld.shutdownFlockRelease(f), nil
	}
	mysqld.shutdownFlockMu.Unlock()

	path := shutdownFlockPath(cnf)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		log.Warn(
			"cannot open the interprocess shutdown lock file; proceeding without cross-process shutdown serialization",
			slog.String("lock_file", path),
			slog.Any("error", err),
		)
		return func() {}, nil
	}
	logged := false
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			mysqld.shutdownFlockMu.Lock()
			mysqld.shutdownFlock = f
			mysqld.shutdownFlockRefs = 1
			mysqld.shutdownFlockMu.Unlock()
			return mysqld.shutdownFlockRelease(f), nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			f.Close()
			log.Warn(
				"cannot lock the interprocess shutdown lock file; proceeding without cross-process shutdown serialization",
				slog.String("lock_file", path),
				slog.Any("error", err),
			)
			return func() {}, nil
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
			return nil, vterrors.Wrap(ctx.Err(), "shutdown cancelled while waiting for a concurrent mysqld shutdown in another process")
		case <-time.After(shutdownFlockPollInterval):
		}
	}
}

// retainShutdownFlock adds a reference to the interprocess shutdown lock when
// this Mysqld holds it, returning the release of that reference; when the
// lock is not held (never acquired, or its setup degraded), the returned
// release is a no-op. armReplicaRestore uses it so a pending restoration
// keeps the lock held after the arming attempt drops its own reference.
func (mysqld *Mysqld) retainShutdownFlock() func() {
	mysqld.shutdownFlockMu.Lock()
	defer mysqld.shutdownFlockMu.Unlock()
	f := mysqld.shutdownFlock
	if f == nil {
		return func() {}
	}
	mysqld.shutdownFlockRefs++
	return mysqld.shutdownFlockRelease(f)
}

// shutdownFlockRelease returns a once-only release of one reference to the
// interprocess shutdown lock. It is bound to the lock file the reference was
// taken against: after a force release (Close's backstop), a straggling
// holder's release must not touch a lock a later attempt reacquired. Closing
// the file descriptor releases the flock.
func (mysqld *Mysqld) shutdownFlockRelease(f *os.File) func() {
	return sync.OnceFunc(func() {
		mysqld.shutdownFlockMu.Lock()
		defer mysqld.shutdownFlockMu.Unlock()
		if mysqld.shutdownFlock != f {
			return
		}
		mysqld.shutdownFlockRefs--
		if mysqld.shutdownFlockRefs > 0 {
			return
		}
		mysqld.shutdownFlockRefs = 0
		mysqld.shutdownFlock.Close()
		mysqld.shutdownFlock = nil
	})
}

// releaseShutdownFlock force-releases the interprocess shutdown lock if this
// Mysqld still holds it, regardless of outstanding references. It is Close's
// backstop: the references are normally all dropped by then, and any that is
// not belongs to a restoration Close already timed out waiting for.
func (mysqld *Mysqld) releaseShutdownFlock() {
	mysqld.shutdownFlockMu.Lock()
	defer mysqld.shutdownFlockMu.Unlock()
	mysqld.shutdownFlockRefs = 0
	if mysqld.shutdownFlock != nil {
		mysqld.shutdownFlock.Close()
		mysqld.shutdownFlock = nil
	}
}
