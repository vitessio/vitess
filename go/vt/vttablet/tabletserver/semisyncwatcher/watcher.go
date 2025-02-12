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
)

// Watcher is a watcher that checks if the primary tablet
// is blocked on a semi-sync ack from the replica.
// If the semi-sync ACK is lost in the network,
// it is possible that the primary is indefinitely stuck,
// blocking PRS. The watcher looks for this situation and manufactures a write
// periodically to unblock the primary.
type Watcher struct {
	ticks *timer.Timer
	env   tabletenv.Env

	mu        sync.Mutex
	appPool   *dbconnpool.ConnectionPool
	isOpen    bool
	isWriting bool
	isBlocked bool
}

// NewWatcher creates a new Watcher.
func NewWatcher(env tabletenv.Env) *Watcher {
	// TODO (@GuptaManan100): Parameterize the watch interval.
	watchInterval := 30 * time.Second
	return &Watcher{
		env:     env,
		ticks:   timer.NewTimer(watchInterval),
		appPool: dbconnpool.NewConnectionPool("SemiSyncWatcherAppPool", env.Exporter(), 20, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
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
	// We reset the state when the watcher starts.
	w.isBlocked = false
	log.Info("SemiSync Watcher: opening")

	// This function could be running from within a unit test scope, in which case we use
	// mock pools that are already open. This is why we test for the pool being open.
	if !w.appPool.IsOpen() {
		w.appPool.Open(w.env.Config().DB.AppWithDB())
	}
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
	w.ticks.Stop()
	w.appPool.Close()
}

// checkAndFixSemiSyncBlocked checks if the primary is blocked on semi-sync ack
// and manufactures a write to unblock the primary.
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
	w.checkAndFixSemiSyncBlocked()
	// TODO: Complete the function.
}

// continueWrites returns true if the watcher should continue writing to the DB.
// It checks if the watcher is still open and if the primary is still blocked.
func (w *Watcher) continueWrites() bool {
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
	for !w.continueWrites() {
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
	// TODO: Fix the write in question.
	_, _ = conn.Conn.ExecuteFetch("INSERT INTO heartbeat (timestamp) VALUES (NOW())", 1, false)
}

// setIsBlocked sets the isBlocked field.
func (w *Watcher) setIsBlocked(val bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.isBlocked = val
}
