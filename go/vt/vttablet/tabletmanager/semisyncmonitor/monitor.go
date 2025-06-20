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
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const (
	semiSyncWaitSessionsRead = "select variable_value from performance_schema.global_status where regexp_like(variable_name, 'Rpl_semi_sync_(source|master)_wait_sessions')"
	semiSyncHeartbeatWrite   = "INSERT INTO %s.semisync_heartbeat (ts) VALUES (NOW())"
	semiSyncHeartbeatClear   = "TRUNCATE TABLE %s.semisync_heartbeat"
	maxWritesPermitted       = 15
	clearTimerDuration       = 24 * time.Hour
)

var (
	// waitBetweenWrites is the time to wait between consecutive writes.
	// This is a variable instead of a constant only to be tweaked in tests.
	waitBetweenWrites = 1 * time.Second
)

// Monitor is a monitor that checks if the primary tablet
// is blocked on a semi-sync ack from the replica.
// If the semi-sync ACK is lost in the network,
// it is possible that the primary is indefinitely stuck,
// blocking PRS. The monitor looks for this situation and manufactures a write
// periodically to unblock the primary.
type Monitor struct {
	// config is used to get the connection parameters.
	config *tabletenv.TabletConfig
	// ticks is the ticker on which we'll check
	// if the primary is blocked on semi-sync ACKs or not.
	ticks *timer.Timer
	// clearTicks is the ticker to clear the data in
	// the semisync_heartbeat table.
	clearTicks *timer.Timer

	// timerMu protects operations on the timers to prevent deadlocks
	// This must be acquired before mu if both are needed.
	timerMu sync.Mutex

	// mu protects the fields below.
	mu      sync.Mutex
	appPool *dbconnpool.ConnectionPool
	isOpen  bool
	// isWriting stores if the monitor is currently writing to the DB.
	// We don't want two different threads initiating writes, so we use this
	// for synchronization.
	isWriting bool
	// inProgressWriteCount is the number of writes currently in progress.
	// The writes from the monitor themselves might get blocked and hence a count for them is required.
	// After enough writes are blocked, we want to notify VTOrc to run an ERS.
	inProgressWriteCount int
	// isBlocked stores if the primary is blocked on semi-sync ack.
	isBlocked bool
	// waiters stores the list of waiters that are waiting for the primary to be unblocked.
	waiters []chan struct{}
	// writesBlockedGauge is a gauge tracking the number of writes the monitor is blocked on.
	writesBlockedGauge *stats.Gauge
	// errorCount is the number of errors that the semi-sync monitor ran into.
	// We ignore some of the errors, so the counter is a good way to track how many errors we have seen.
	errorCount *stats.Counter
}

// NewMonitor creates a new Monitor.
func NewMonitor(config *tabletenv.TabletConfig, exporter *servenv.Exporter) *Monitor {
	return &Monitor{
		config: config,
		ticks:  timer.NewTimer(config.SemiSyncMonitor.Interval),
		// We clear the data every day. We can make it configurable in the future,
		// but this seams fine for now.
		clearTicks:         timer.NewTimer(clearTimerDuration),
		writesBlockedGauge: exporter.NewGauge("SemiSyncMonitorWritesBlocked", "Number of writes blocked in the semi-sync monitor"),
		errorCount:         exporter.NewCounter("SemiSyncMonitorErrorCount", "Number of errors encountered by the semi-sync monitor"),
		appPool:            dbconnpool.NewConnectionPool("SemiSyncMonitorAppPool", exporter, maxWritesPermitted+5, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
		waiters:            make([]chan struct{}, 0),
	}
}

// CreateTestSemiSyncMonitor created a monitor for testing.
// It takes an optional fake db.
func CreateTestSemiSyncMonitor(db *fakesqldb.DB, exporter *servenv.Exporter) *Monitor {
	var dbc *dbconfigs.DBConfigs
	if db != nil {
		params := db.ConnParams()
		cp := *params
		dbc = dbconfigs.NewTestDBConfigs(cp, cp, "")
	}
	return NewMonitor(&tabletenv.TabletConfig{
		DB: dbc,
		SemiSyncMonitor: tabletenv.SemiSyncMonitorConfig{
			Interval: 1 * time.Second,
		},
	}, exporter)
}

// Open starts the monitor.
func (m *Monitor) Open() {
	// First acquire the timer mutex to prevent deadlock - we acquire in a consistent order
	m.timerMu.Lock()
	defer m.timerMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// The check for config being nil is only requried for tests.
	if m.isOpen || m.config == nil || m.config.DB == nil {
		// If we are already open, then there is nothing to do
		return
	}
	// Set the monitor to be open.
	m.isOpen = true
	log.Info("SemiSync Monitor: opening")

	// This function could be running from within a unit test scope, in which case we use
	// mock pools that are already open. This is why we test for the pool being open.
	if !m.appPool.IsOpen() {
		m.appPool.Open(m.config.DB.AppWithDB())
	}
	m.clearTicks.Start(m.clearAllData)
	m.ticks.Start(m.checkAndFixSemiSyncBlocked)
}

// Close stops the monitor.
func (m *Monitor) Close() {
	// First acquire the timer mutex to prevent deadlock - we acquire in a consistent order
	m.timerMu.Lock()
	defer m.timerMu.Unlock()
	log.Info("SemiSync Monitor: closing")
	// We close the ticks before we acquire the mu mutex to prevent deadlock.
	// The timer Close should not be called while holding a mutex that the function
	// the timer runs also acquires.
	m.clearTicks.Stop()
	m.ticks.Stop()
	// Acquire the mu mutex to update the isOpen field.
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isOpen = false
	m.appPool.Close()
}

// checkAndFixSemiSyncBlocked checks if the primary is blocked on semi-sync ack
// and manufactures a write to unblock the primary. This function is safe to
// be called multiple times in parallel.
func (m *Monitor) checkAndFixSemiSyncBlocked() {
	// Check if semi-sync is blocked or not
	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()
	isBlocked, err := m.isSemiSyncBlocked(ctx)
	if err != nil {
		m.errorCount.Add(1)
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
		// We start it in a go-routine, because we want to continue to check for when
		// we get unblocked.
		go m.startWrites()
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
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		return false, fmt.Errorf("unexpected number of rows received - %v", res.Rows)
	}
	value, err := res.Rows[0][0].ToCastInt64()
	return value != 0, err
}

// isClosed returns if the monitor is currently closed or not.
func (m *Monitor) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.isOpen
}

// WaitUntilSemiSyncUnblocked waits until the primary is not blocked
// on semi-sync or until the context expires.
func (m *Monitor) WaitUntilSemiSyncUnblocked(ctx context.Context) error {
	// SemiSyncMonitor is closed, which means semi-sync is not enabled.
	// We don't have anything to wait for.
	if m.isClosed() {
		return nil
	}
	// run one iteration of checking if semi-sync is blocked or not.
	m.checkAndFixSemiSyncBlocked()
	if !m.stillBlocked() {
		// If we find that the primary isn't blocked, we're good,
		// we don't need to wait for anything.
		log.Infof("Primary not blocked on semi-sync ACKs")
		return nil
	}
	log.Infof("Waiting for semi-sync to be unblocked")
	// The primary is blocked. We need to wait for it to be unblocked
	// or the context to expire.
	ch := m.addWaiter()
	select {
	case <-ch:
		log.Infof("Finished waiting for semi-sync to be unblocked")
		return nil
	case <-ctx.Done():
		log.Infof("Error while waiting for semi-sync to be unblocked - %s", ctx.Err().Error())
		return ctx.Err()
	}
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

	// Check if we need to continue writing or not.
	for m.stillBlocked() {
		// We do the writes in a go-routine because if the network disruption
		// is somewhat long-lived, then the writes themselves can also block.
		// By doing them in a go-routine we give the system more time to recover while
		// exponentially backing off. We will not do more than maxWritesPermitted writes and once
		// all maxWritesPermitted writes are blocked, we'll wait for VTOrc to run an ERS.
		go m.write()
		time.Sleep(waitBetweenWrites)
	}
}

// incrementWriteCount tries to increment the write count. It
// also checks that the write count value should not exceed
// the maximum value configured. It returns whether it was able
// to increment the value or not.
func (m *Monitor) incrementWriteCount() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inProgressWriteCount == maxWritesPermitted {
		return false
	}
	m.inProgressWriteCount++
	m.writesBlockedGauge.Set(int64(m.inProgressWriteCount))
	return true
}

// AllWritesBlocked returns if maxWritesPermitted number of writes
// are already outstanding.
func (m *Monitor) AllWritesBlocked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isOpen && m.inProgressWriteCount == maxWritesPermitted
}

// decrementWriteCount decrements the write count.
func (m *Monitor) decrementWriteCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inProgressWriteCount--
	m.writesBlockedGauge.Set(int64(m.inProgressWriteCount))
}

// write writes a heartbeat to unblock semi-sync being stuck.
func (m *Monitor) write() {
	shouldWrite := m.incrementWriteCount()
	if !shouldWrite {
		return
	}
	defer m.decrementWriteCount()
	// Get a connection from the pool
	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()
	conn, err := m.appPool.Get(ctx)
	if err != nil {
		m.errorCount.Add(1)
		log.Errorf("SemiSync Monitor: failed to get a connection when writing to semisync_heartbeat table: %v", err)
		return
	}
	_, err = conn.Conn.ExecuteFetch(m.bindSideCarDBName(semiSyncHeartbeatWrite), 0, false)
	conn.Recycle()
	if err != nil {
		m.errorCount.Add(1)
		log.Errorf("SemiSync Monitor: failed to write to semisync_heartbeat table: %v", err)
	} else {
		// One of the writes went through without an error.
		// This means that we aren't blocked on semi-sync anymore.
		m.setIsBlocked(false)
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
		m.errorCount.Add(1)
		log.Errorf("SemiSync Monitor: failed get a connection to clear semisync_heartbeat table: %v", err)
		return
	}
	defer conn.Recycle()
	_, err = conn.Conn.ExecuteFetch(m.bindSideCarDBName(semiSyncHeartbeatClear), 0, false)
	if err != nil {
		m.errorCount.Add(1)
		log.Errorf("SemiSync Monitor: failed to clear semisync_heartbeat table: %v", err)
	}
}

// addWaiter adds a waiter to the list of waiters
// that will be unblocked when the primary is no longer blocked.
func (m *Monitor) addWaiter() chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan struct{})
	m.waiters = append(m.waiters, ch)
	return ch
}

// bindSideCarDBName binds the sidecar db name to the query.
func (m *Monitor) bindSideCarDBName(query string) string {
	return sqlparser.BuildParsedQuery(query, sidecar.GetIdentifier()).Query
}
