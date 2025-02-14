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

package semisyncmonitor

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
	semiSyncRecoverClear     = "TRUNCATE TABLE semisync_recover"
)

// Monitor is a monitor that checks if the primary tablet
// is blocked on a semi-sync ack from the replica.
// If the semi-sync ACK is lost in the network,
// it is possible that the primary is indefinitely stuck,
// blocking PRS. The monitor looks for this situation and manufactures a write
// periodically to unblock the primary.
type Monitor struct {
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
	// isWriting stores if the monitor is currently writing to the DB.
	// We don't want two different threads initiating writes, so we use this
	// for synchronization.
	isWriting bool
	// isBlocked stores if the primary is blocked on semi-sync ack.
	isBlocked bool
	// waiters stores the list of waiters that are waiting for the primary to be unblocked.
	waiters []chan any
}

// NewMonitor creates a new Monitor.
func NewMonitor(env tabletenv.Env) *Monitor {
	// TODO (@GuptaManan100): Parameterize the watch interval.
	watchInterval := 30 * time.Second
	return &Monitor{
		env:   env,
		ticks: timer.NewTimer(watchInterval),
		// We clear the data every day. We can make it configurable in the future,
		// but this seams fine for now.
		clearTicks: timer.NewTimer(24 * time.Hour),
		appPool:    dbconnpool.NewConnectionPool("SemiSyncMonitorAppPool", env.Exporter(), 20, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
		waiters:    make([]chan any, 0),
	}
}

// Open starts the monitor.
func (m *Monitor) Open() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isOpen {
		// If we are already open, then there is nothing to do
		return
	}
	// Set the monitor to be open.
	m.isOpen = true
	log.Info("SemiSync Monitor: opening")

	// This function could be running from within a unit test scope, in which case we use
	// mock pools that are already open. This is why we test for the pool being open.
	if !m.appPool.IsOpen() {
		m.appPool.Open(m.env.Config().DB.AppWithDB())
	}
	m.clearTicks.Start(m.clearAllData)
	m.ticks.Start(m.checkAndFixSemiSyncBlocked)
}

// Close stops the monitor.
func (m *Monitor) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isOpen {
		// If we are already closed, then there is nothing to do
		return
	}
	m.isOpen = false
	log.Info("SemiSync Monitor: closing")
	m.clearTicks.Stop()
	m.ticks.Stop()
	m.appPool.Close()
}

// checkAndFixSemiSyncBlocked checks if the primary is blocked on semi-sync ack
// and manufactures a write to unblock the primary. This function is safe to
// be called multiple times in parallel.
func (m *Monitor) checkAndFixSemiSyncBlocked() {
	// Check if semi-sync is blocked or not
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	isBlocked, err := m.isSemiSyncBlocked(ctx)
	if err != nil {
		// If we are unable to determine whether the primary is blocked or not,
		// then we can just abort the function and try again later.
		log.Errorf("SemiSync Monitor: failed to check if primary is blocked on semi-sync: %v", err)
		return
	}
	// Set the isBlocked state.
	m.setIsBlocked(isBlocked)
	if isBlocked {
		// If we are blocked, then we want to start the writes.
		// That function is re-entrant. If we are already writing, then it will just return.
		m.startWrites()
	}
}

// isSemiSyncBlocked checks if the primary is blocked on semi-sync.
func (m *Monitor) isSemiSyncBlocked(ctx context.Context) (bool, error) {
	// Get a connection from the pool
	conn, err := m.appPool.Get(ctx)
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
func (m *Monitor) waitUntilSemiSyncUnblocked() {
	// run one iteration of checking if semi-sync is blocked or not.
	m.checkAndFixSemiSyncBlocked()
	if !m.stillBlocked() {
		// If we find that the primary isn't blocked, we're good,
		// we don't need to wait for anything.
		return
	}
	// The primary is blocked. We need to wait for it to be unblocked.
	ch := m.addWaiter()
	<-ch
}

// stillBlocked returns true if the monitor should continue writing to the DB
// because the monitor is still open, and the primary is still blocked.
func (m *Monitor) stillBlocked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isOpen && m.isBlocked
}

// checkAndSetIsWriting checks if the monitor is already writing to the DB.
// If it is not, then it sets the isWriting field and signals the caller.
func (m *Monitor) checkAndSetIsWriting() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isWriting {
		return false
	}
	m.isWriting = true
	return true
}

// clearIsWriting clears the isWriting field.
func (m *Monitor) clearIsWriting() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isWriting = false
}

// startWrites starts writing to the DB.
// It is re-entrant and will return if we are already writing.
func (m *Monitor) startWrites() {
	// If we are already writing, then we can just return.
	if !m.checkAndSetIsWriting() {
		return
	}
	// We defer the clear of the isWriting field.
	defer m.clearIsWriting()

	// We start writing to the DB with a backoff.
	backoff := 1 * time.Second
	maxBackoff := 1 * time.Minute
	// Check if we need to continue writing or not.
	for m.stillBlocked() {
		// We do the writes in a go-routine because if the network disruption
		// is somewhat long-lived, then the writes themselves can also block.
		// By doing them in a go-routine we give the system more time to recover while
		// exponentially backing off. We will eventually run out of the connections in the pool
		// at which point, there would be nothing that we can do.
		go m.write()
		<-time.After(backoff)
		backoff = time.Duration(math.Min(float64(backoff*2), float64(maxBackoff)))
	}
}

// write writes a heartbeat to unblock semi-sync being stuck.
func (m *Monitor) write() {
	// Get a connection from the pool
	conn, err := m.appPool.Get(context.Background())
	if err != nil {
		log.Errorf("SemiSync Monitor: failed to write to semisync_recovery table: %v", err)
		return
	}
	defer conn.Recycle()
	_, err = conn.Conn.ExecuteFetch(semiSyncRecoverWrite, 0, false)
	if err != nil {
		log.Errorf("SemiSync Monitor: failed to write to semisync_recovery table: %v", err)
	}
}

// setIsBlocked sets the isBlocked field.
func (m *Monitor) setIsBlocked(val bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isBlocked = val
	if !val {
		// If we are unblocked, then we need to signal all the waiters.
		for _, ch := range m.waiters {
			close(ch)
		}
		// We also empty the list of current waiters.
		m.waiters = nil
	}
}

// clearAllData clears all the data in the table so that it never
// consumes too much space on the MySQL instance.
func (m *Monitor) clearAllData() {
	// Get a connection from the pool
	conn, err := m.appPool.Get(context.Background())
	if err != nil {
		log.Errorf("SemiSync Monitor: failed to clear semisync_recovery table: %v", err)
		return
	}
	defer conn.Recycle()
	_, err = conn.Conn.ExecuteFetch(semiSyncRecoverClear, 0, false)
	if err != nil {
		log.Errorf("SemiSync Monitor: failed to clear semisync_recovery table: %v", err)
	}
}

// addWaiter adds a waiter to the list of waiters
// that will be unblocked when the primary is no longer blocked.
func (m *Monitor) addWaiter() chan any {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan any)
	m.waiters = append(m.waiters, ch)
	return ch
}
