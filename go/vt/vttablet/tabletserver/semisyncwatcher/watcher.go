/*
Copyright 2025 The Vitess Authors.

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

package semisyncwatcher

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const (
	semiSyncWaitSessionsRead = "SHOW STATUS LIKE 'Rpl_semi_sync_%_wait_sessions'"
	semiSyncRecoverWrite     = "INSERT INTO semisync_recover (ts) VALUES (NOW())"
	semiSyncRecoverClear     = "DELETE FROM semisync_recover"
)

// Watcher is a watcher that checks if the primary tablet
// is blocked on a semi-sync ack from the replica.
// If the semi-sync ACK is lost in the network,
// it is possible that the primary is indefinitely stuck,
// blocking PRS. The watcher looks for this situation and manufactures a write
// periodically to unblock the primary.
type Watcher struct {
	// env is used to get the connection parameters.
	env tabletenv.Env
	// ticks is the ticker on which we'll check
	// if the primary is blocked on semi-sync ACKs or not.
	ticks *timer.Timer
	// clearTicks is the ticker to clear the data in
	// the semisync_recover table.
	clearTicks *timer.Timer

	// mu protects the fields below.
	mu      sync.Mutex
	appPool *dbconnpool.ConnectionPool
	isOpen  bool
	// isWriting stores if the watcher is currently writing to the DB.
	// We don't want two different threads initiating writes, so we use this
	// for synchronization.
	isWriting bool
	// isBlocked stores if the primary is blocked on semi-sync ack.
	isBlocked bool
	// waiters stores the list of waiters that are waiting for the primary to be unblocked.
	waiters []chan any
}

// NewWatcher creates a new Watcher.
func NewWatcher(env tabletenv.Env) *Watcher {
	// TODO (@GuptaManan100): Parameterize the watch interval.
	watchInterval := 30 * time.Second
	return &Watcher{
		env:   env,
		ticks: timer.NewTimer(watchInterval),
		// We clear the data every day. We can make it configurable in the future,
		// but this seams fine for now.
		clearTicks: timer.NewTimer(24 * time.Hour),
		appPool:    dbconnpool.NewConnectionPool("SemiSyncWatcherAppPool", env.Exporter(), 20, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
		waiters:    make([]chan any, 0),
	}
}

// Open starts the watcher.
func (w *Watcher) Open() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isOpen {
		// If we are already open, then there is nothing to do
		return
	}
	// Set the watcher to be open.
	w.isOpen = true
	log.Info("SemiSync Watcher: opening")

	// This function could be running from within a unit test scope, in which case we use
	// mock pools that are already open. This is why we test for the pool being open.
	if !w.appPool.IsOpen() {
		w.appPool.Open(w.env.Config().DB.AppWithDB())
	}
	w.clearTicks.Start(w.clearAllData)
	w.ticks.Start(w.checkAndFixSemiSyncBlocked)
}

// Close stops the watcher.
func (w *Watcher) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.isOpen {
		// If we are already closed, then there is nothing to do
		return
	}
	w.isOpen = false
	log.Info("SemiSync Watcher: closing")
	w.clearTicks.Stop()
	w.ticks.Stop()
	w.appPool.Close()
}

// checkAndFixSemiSyncBlocked checks if the primary is blocked on semi-sync ack
// and manufactures a write to unblock the primary. This function is safe to
// be called multiple times in parallel.
func (w *Watcher) checkAndFixSemiSyncBlocked() {
	// Check if semi-sync is blocked or not
	isBlocked, err := w.isSemiSyncBlocked(context.Background())
	if err != nil {
		// If we are unable to determine whether the primary is blocked or not,
		// then we can just abort the function and try again later.
		log.Errorf("SemiSync Watcher: failed to check if primary is blocked on semi-sync: %v", err)
		return
	}
	// Set the isBlocked state.
	w.setIsBlocked(isBlocked)
	if isBlocked {
		// If we are blocked, then we want to start the writes.
		// That function is re-entrant. If we are already writing, then it will just return.
		w.startWrites()
	}
}

// isSemiSyncBlocked checks if the primary is blocked on semi-sync.
func (w *Watcher) isSemiSyncBlocked(ctx context.Context) (bool, error) {
	// Get a connection from the pool
	conn, err := w.appPool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Recycle()

	// Execute the query to check if the primary is blocked on semi-sync.
	res, err := conn.Conn.ExecuteFetch(semiSyncWaitSessionsRead, 1, false)
	if err != nil {
		return false, err
	}
	// If we have no rows, then the primary doesn't have semi-sync enabled.
	// It then follows, that the primary isn't blocked :)
	if len(res.Rows) == 0 {
		return false, nil
	}

	// Read the status value and check if it is non-zero.
	if len(res.Rows) != 1 || len(res.Rows[0]) != 2 {
		return false, errors.New("unexpected number of rows received")
	}
	value, err := res.Rows[0][1].ToInt()
	return value != 0, err
}

// waitUntilSemiSyncUnblocked waits until the primary is not blocked
// on semi-sync.
func (w *Watcher) waitUntilSemiSyncUnblocked() {
	// run one iteration of checking if semi-sync is blocked or not.
	w.checkAndFixSemiSyncBlocked()
	if !w.stillBlocked() {
		// If we find that the primary isn't blocked, we're good,
		// we don't need to wait for anything.
		return
	}
	// The primary is blocked. We need to wait for it to be unblocked.
	ch := w.addWaiter()
	<-ch
}

// stillBlocked returns true if the watcher should continue writing to the DB
// because the watcher is still open, and the primary is still blocked.
func (w *Watcher) stillBlocked() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isOpen && w.isBlocked
}

// checkAndSetIsWriting checks if the watcher is already writing to the DB.
// If it is not, then it sets the isWriting field and signals the caller.
func (w *Watcher) checkAndSetIsWriting() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isWriting {
		return false
	}
	w.isWriting = true
	return true
}

// clearIsWriting clears the isWriting field.
func (w *Watcher) clearIsWriting() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.isWriting = false
}

// startWrites starts writing to the DB.
// It is re-entrant and will return if we are already writing.
func (w *Watcher) startWrites() {
	// If we are already writing, then we can just return.
	if !w.checkAndSetIsWriting() {
		return
	}
	// We defer the clear of the isWriting field.
	defer w.clearIsWriting()

	// We start writing to the DB with a backoff.
	backoff := 1 * time.Second
	maxBackoff := 1 * time.Minute
	// Check if we need to continue writing or not.
	for w.stillBlocked() {
		go w.write()
		<-time.After(backoff)
		backoff = time.Duration(math.Min(float64(backoff*2), float64(maxBackoff)))
	}
}

// write writes a heartbeat to unblock semi-sync being stuck.
func (w *Watcher) write() {
	// Get a connection from the pool
	conn, err := w.appPool.Get(context.Background())
	if err != nil {
		return
	}
	defer conn.Recycle()
	_, _ = conn.Conn.ExecuteFetch(semiSyncRecoverWrite, 0, false)
}

// setIsBlocked sets the isBlocked field.
func (w *Watcher) setIsBlocked(val bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.isBlocked = val
	if !val {
		// If we are unblocked, then we need to signal all the waiters.
		for _, ch := range w.waiters {
			close(ch)
		}
		// We also empty the list of current waiters.
		w.waiters = nil
	}
}

// clearAllData clears all the data in the table so that it never
// consumes too much space on the MySQL instance.
func (w *Watcher) clearAllData() {
	// Get a connection from the pool
	conn, err := w.appPool.Get(context.Background())
	if err != nil {
		log.Errorf("SemiSync Watcher: failed to clear semisync_recovery table: %v", err)
		return
	}
	defer conn.Recycle()
	_, err = conn.Conn.ExecuteFetch(semiSyncRecoverClear, 0, false)
	if err != nil {
		log.Errorf("SemiSync Watcher: failed to clear semisync_recovery table: %v", err)
	}
}

// addWaiter adds a waiter to the list of waiters
// that will be unblocked when the primary is no longer blocked.
func (w *Watcher) addWaiter() chan any {
	w.mu.Lock()
	defer w.mu.Unlock()
	ch := make(chan any, 1)
	w.waiters = append(w.waiters, ch)
	return ch
}
