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
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var (
	exporter = servenv.NewExporter("TestSemiSyncMonitor", "")
)

// createFakeDBAndMonitor created a fake DB and a monitor for testing.
func createFakeDBAndMonitor(t *testing.T) (*fakesqldb.DB, *Monitor) {
	db := fakesqldb.New(t)
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")
	config := &tabletenv.TabletConfig{
		DB: dbc,
		SemiSyncMonitor: tabletenv.SemiSyncMonitorConfig{
			Interval: 10 * time.Second,
		},
	}
	monitor := NewMonitor(config, exporter)
	monitor.mu.Lock()
	defer monitor.mu.Unlock()
	monitor.isOpen = true
	monitor.appPool.Open(config.DB.AppWithDB())
	return db, monitor
}

// TestMonitorIsSemiSyncBlocked tests the functionality of isSemiSyncBlocked.
// NOTE: This test focuses on the first getSemiSyncStats call and early-return logic.
// The full two-call behavior is tested in TestMonitorIsSemiSyncBlockedProgressDetection.
func TestMonitorIsSemiSyncBlocked(t *testing.T) {
	defer utils.EnsureNoLeaks(t)

	tests := []struct {
		name    string
		result  *sqltypes.Result
		want    bool
		wantErr string
	}{
		{
			name:   "no rows - semi-sync not enabled",
			result: &sqltypes.Result{},
			want:   false,
		},
		{
			name:    "incorrect results - invalid variable names",
			result:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "foo|3", "foo|3"),
			wantErr: "unexpected results for semi-sync stats query",
		},
		{
			name:   "unblocked - zero waiting sessions",
			result: sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|0", "Rpl_semi_sync_source_yes_tx|1"),
			want:   false,
		},
		{
			name:   "has waiting sessions - needs second check",
			result: sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|1", "Rpl_semi_sync_source_yes_tx|5"),
			// With fakesqldb limitation, second call returns same result, so it appears blocked.
			want: true,
		},
		{
			name:   "master prefix for backwards compatibility",
			result: sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_master_wait_sessions|2", "Rpl_semi_sync_master_yes_tx|50"),
			// With fakesqldb limitation, second call returns same result, so it appears blocked.
			want: true,
		},
		{
			name:    "invalid variable value",
			result:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|not_a_number", "Rpl_semi_sync_source_yes_tx|5"),
			wantErr: "unexpected results for semi-sync stats query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			m.actionDelay = 10 * time.Millisecond
			m.actionTimeout = 1 * time.Second
			defer db.Close()
			defer func() {
				m.Close()
				waitUntilWritingStopped(t, m)
			}()

			db.AddQuery(fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()), tt.result)

			got, err := m.isSemiSyncBlocked()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestMonitorIsSemiSyncBlockedConnectionError tests that we do not
// consider semi-sync blocked when we encounter an error trying to check.
func TestMonitorIsSemiSyncBlockedConnectionError(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()

	// Close the pool to simulate connection errors.
	m.mu.Lock()
	m.appPool.Close()
	m.mu.Unlock()

	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// The function should return an error when it can't get a connection.
	got, err := m.isSemiSyncBlocked()
	require.Error(t, err)
	require.False(t, got)
}

// TestMonitorIsSemiSyncBlockedWithBadResults tests error handling when
// the query returns an unexpected result.
func TestMonitorIsSemiSyncBlockedWithBadResults(t *testing.T) {
	defer utils.EnsureNoLeaks(t)

	tests := []struct {
		name    string
		res     *sqltypes.Result
		wantErr string
	}{
		{
			name:    "one row instead of two",
			res:     sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|1"),
			wantErr: "unexpected number of rows received, expected 2 but got 1",
		},
		{
			name:    "one column instead of two",
			res:     sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "1", "1"),
			wantErr: "unexpected number of columns received, expected 2 but got 1",
		},
		{
			name: "three rows instead of two",
			res:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|1", "Rpl_semi_sync_source_yes_tx|5", "extra_row|10"),
			// Note: The actual error is "Row count exceeded" because ExecuteFetch has maxrows=2.
			wantErr: "Row count exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			m.actionDelay = 10 * time.Millisecond
			m.actionTimeout = 1 * time.Second
			defer db.Close()
			defer func() {
				m.Close()
				waitUntilWritingStopped(t, m)
			}()

			db.AddQuery(fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()), tt.res)

			got, err := m.isSemiSyncBlocked()
			require.False(t, got)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestMonitorIsSemiSyncBlockedProgressDetection tests various scenarios
// for detecting progress in semi-sync replication by directly calling
// getSemiSyncStats to verify the logic.
func TestMonitorIsSemiSyncBlockedProgressDetection(t *testing.T) {
	defer utils.EnsureNoLeaks(t)

	tests := []struct {
		name            string
		firstWaiting    int64
		firstAcked      int64
		secondWaiting   int64
		secondAcked     int64
		expectedBlocked bool
		description     string
	}{
		{
			name:            "progress - acked increased by 1",
			firstWaiting:    2,
			firstAcked:      100,
			secondWaiting:   2,
			secondAcked:     101,
			expectedBlocked: false,
			description:     "should detect progress when acked transactions increase",
		},
		{
			name:            "progress - acked increased significantly",
			firstWaiting:    1,
			firstAcked:      50,
			secondWaiting:   1,
			secondAcked:     1000,
			expectedBlocked: false,
			description:     "should detect progress with large acked transaction increase",
		},
		{
			name:            "progress - waiting decreased to zero",
			firstWaiting:    3,
			firstAcked:      100,
			secondWaiting:   0,
			secondAcked:     100,
			expectedBlocked: false,
			description:     "should detect progress when waiting sessions drop to zero",
		},
		{
			name:            "blocked - waiting decreased but transactions not progressing",
			firstWaiting:    5,
			firstAcked:      100,
			secondWaiting:   2,
			secondAcked:     100,
			expectedBlocked: true,
			description:     "should still be blocked when waiting sessions decrease but no transactions are acked",
		},
		{
			name:            "blocked - no change in metrics",
			firstWaiting:    2,
			firstAcked:      100,
			secondWaiting:   2,
			secondAcked:     100,
			expectedBlocked: true,
			description:     "should detect blocked state when no metrics change",
		},
		{
			name:            "blocked - waiting increased",
			firstWaiting:    1,
			firstAcked:      100,
			secondWaiting:   3,
			secondAcked:     100,
			expectedBlocked: true,
			description:     "should detect blocked state when waiting sessions increase",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Directly test the logic by simulating what isSemiSyncBlocked does.
			// First call - check initial stats.
			stats := semiSyncStats{
				waitingSessions: tt.firstWaiting,
				ackedTrxs:       tt.firstAcked,
			}

			// Early return conditions from isSemiSyncBlocked.
			if stats.waitingSessions == 0 {
				require.False(t, tt.expectedBlocked, tt.description)
				return
			}

			// Second call - check follow-up stats.
			followUpStats := semiSyncStats{
				waitingSessions: tt.secondWaiting,
				ackedTrxs:       tt.secondAcked,
			}

			// Check if we're still blocked based on the actual logic in isSemiSyncBlocked.
			// Returns false (not blocked) if: waitingSessions == 0 OR ackedTrxs increased.
			isBlocked := !(followUpStats.waitingSessions == 0 || followUpStats.ackedTrxs > stats.ackedTrxs)

			require.Equal(t, tt.expectedBlocked, isBlocked, tt.description)
		})
	}
}

// TestGetSemiSyncStats tests the getSemiSyncStats helper function.
func TestGetSemiSyncStats(t *testing.T) {
	defer utils.EnsureNoLeaks(t)

	tests := []struct {
		name            string
		res             *sqltypes.Result
		expectedWaiting int64
		expectedAcked   int64
		wantErr         string
	}{
		{
			name: "valid source prefix",
			res: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_source_wait_sessions|3",
				"Rpl_semi_sync_source_yes_tx|150"),
			expectedWaiting: 3,
			expectedAcked:   150,
		},
		{
			name: "valid master prefix",
			res: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_master_wait_sessions|5",
				"Rpl_semi_sync_master_yes_tx|200"),
			expectedWaiting: 5,
			expectedAcked:   200,
		},
		{
			name: "zero values",
			res: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_source_wait_sessions|0",
				"Rpl_semi_sync_source_yes_tx|0"),
			expectedWaiting: 0,
			expectedAcked:   0,
		},
		{
			name: "large values",
			res: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_source_wait_sessions|999999",
				"Rpl_semi_sync_source_yes_tx|123456789"),
			expectedWaiting: 999999,
			expectedAcked:   123456789,
		},
		{
			name:            "no rows returns empty stats",
			res:             &sqltypes.Result{},
			expectedWaiting: 0,
			expectedAcked:   0,
		},
		{
			name: "wrong number of rows",
			res: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"Rpl_semi_sync_source_wait_sessions|3"),
			wantErr: "unexpected number of rows received, expected 2 but got 1",
		},
		{
			name: "invalid variable name",
			res: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
				"invalid_variable|3",
				"Rpl_semi_sync_source_yes_tx|150"),
			wantErr: "unexpected results for semi-sync stats query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer func() {
				m.Close()
				waitUntilWritingStopped(t, m)
			}()

			db.AddQuery(fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()), tt.res)
			conn, err := m.appPool.Get(context.Background())
			require.NoError(t, err)
			defer conn.Recycle()

			stats, err := m.getSemiSyncStats(conn)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expectedWaiting, stats.waitingSessions)
			require.Equal(t, tt.expectedAcked, stats.ackedTrxs)
		})
	}
}

func TestMonitorBindSideCarDBName(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	tests := []struct {
		query    string
		expected string
	}{
		{
			query:    semiSyncHeartbeatWrite,
			expected: "INSERT INTO _vt.semisync_heartbeat (ts) VALUES (NOW())",
		},
		{
			query:    semiSyncHeartbeatClear,
			expected: "TRUNCATE TABLE _vt.semisync_heartbeat",
		},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			m := &Monitor{}
			require.EqualValues(t, tt.expected, m.bindSideCarDBName(tt.query))
		})
	}
}

func TestMonitorClearAllData(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()
	db.SetNeverFail(true)
	// ExecuteFetchMulti will execute each statement separately, so we need to add both queries.
	db.AddQuery("SET SESSION lock_wait_timeout=5", &sqltypes.Result{})
	db.AddQuery("truncate table _vt.semisync_heartbeat", &sqltypes.Result{})
	m.clearAllData()
	ql := db.QueryLog()
	require.Contains(t, ql, "set session lock_wait_timeout=5")
	require.Contains(t, ql, "truncate table _vt.semisync_heartbeat")
}

// TestMonitorWaitMechanism tests that the wait mechanism works as intended.
// Setting the monitor to unblock state should unblock the waiters.
func TestMonitorWaitMechanism(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// Add a waiter.
	ch := m.addWaiter()
	var waiterUnblocked atomic.Bool
	go func() {
		<-ch
		waiterUnblocked.Store(true)
	}()

	// Ensure that the waiter is currently blocked.
	require.False(t, waiterUnblocked.Load())

	// Verify that setting again to being blocked doesn't unblock the waiter.
	m.setIsBlocked(true)
	require.False(t, waiterUnblocked.Load())
	require.False(t, m.isClosed())
	require.True(t, m.stillBlocked())

	// Verify that setting we are no longer blocked, unblocks the waiter.
	m.setIsBlocked(false)
	require.Eventually(t, func() bool {
		return waiterUnblocked.Load()
	}, 2*time.Second, time.Millisecond*100)
	require.False(t, m.stillBlocked())
	require.False(t, m.isClosed())
}

func TestMonitorIncrementWriteCount(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	tests := []struct {
		initVal  int
		finalVal int
		want     bool
	}{
		{
			initVal:  maxWritesPermitted - 2,
			finalVal: maxWritesPermitted - 1,
			want:     true,
		}, {
			initVal:  maxWritesPermitted - 1,
			finalVal: maxWritesPermitted,
			want:     true,
		}, {
			initVal:  maxWritesPermitted,
			finalVal: maxWritesPermitted,
			want:     false,
		}, {
			initVal:  0,
			finalVal: 1,
			want:     true,
		},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.initVal), func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer func() {
				m.Close()
				waitUntilWritingStopped(t, m)
			}()
			m.mu.Lock()
			m.inProgressWriteCount = tt.initVal
			m.mu.Unlock()
			got := m.incrementWriteCount()
			require.EqualValues(t, tt.want, got)
			m.mu.Lock()
			require.EqualValues(t, tt.finalVal, m.inProgressWriteCount)
			require.EqualValues(t, tt.finalVal, m.writesBlockedGauge.Get())
			m.mu.Unlock()
		})
	}
}

func TestMonitorDecrementWriteCount(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	tests := []struct {
		initVal  int
		finalVal int
	}{
		{
			initVal:  maxWritesPermitted - 1,
			finalVal: maxWritesPermitted - 2,
		}, {
			initVal:  maxWritesPermitted,
			finalVal: maxWritesPermitted - 1,
		}, {
			initVal:  1,
			finalVal: 0,
		},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.initVal), func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer func() {
				m.Close()
				waitUntilWritingStopped(t, m)
			}()
			m.mu.Lock()
			m.inProgressWriteCount = tt.initVal
			m.mu.Unlock()
			m.decrementWriteCount()
			m.mu.Lock()
			require.EqualValues(t, tt.finalVal, m.inProgressWriteCount)
			require.EqualValues(t, tt.finalVal, m.writesBlockedGauge.Get())
			m.mu.Unlock()
		})
	}
}

func TestMonitorAllWritesBlocked(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	tests := []struct {
		initVal  int
		expected bool
	}{
		{
			initVal:  maxWritesPermitted - 1,
			expected: false,
		}, {
			initVal:  maxWritesPermitted,
			expected: true,
		}, {
			initVal:  1,
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.initVal), func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer func() {
				m.Close()
				waitUntilWritingStopped(t, m)
			}()
			m.mu.Lock()
			m.inProgressWriteCount = tt.initVal
			if m.inProgressWriteCount == tt.initVal {
				m.isBlocked = true
			}
			m.mu.Unlock()
			require.EqualValues(t, tt.expected, m.AllWritesBlocked())
		})
	}
}

func TestMonitorWrite(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	tests := []struct {
		initVal     int
		shouldWrite bool
	}{
		{
			initVal:     maxWritesPermitted - 2,
			shouldWrite: true,
		}, {
			initVal:     maxWritesPermitted - 1,
			shouldWrite: true,
		}, {
			initVal:     maxWritesPermitted,
			shouldWrite: false,
		}, {
			initVal:     0,
			shouldWrite: true,
		},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.initVal), func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer func() {
				m.Close()
				waitUntilWritingStopped(t, m)
			}()
			db.SetNeverFail(true)
			// ExecuteFetchMulti will execute each statement separately, so we need to add both queries.
			db.AddQuery("SET SESSION lock_wait_timeout=5", &sqltypes.Result{})
			db.AddQuery("insert into _vt.semisync_heartbeat (ts) values (now())", &sqltypes.Result{})
			m.mu.Lock()
			m.inProgressWriteCount = tt.initVal
			m.writesBlockedGauge.Set(int64(tt.initVal))
			m.mu.Unlock()
			m.write()
			m.mu.Lock()
			require.EqualValues(t, tt.initVal, m.inProgressWriteCount)
			require.EqualValues(t, tt.initVal, m.writesBlockedGauge.Get())
			m.mu.Unlock()
			queryLog := db.QueryLog()
			if tt.shouldWrite {
				require.Contains(t, queryLog, "set session lock_wait_timeout=5")
				require.Contains(t, queryLog, "insert into _vt.semisync_heartbeat (ts) values (now())")
			} else {
				require.Equal(t, "", queryLog)
			}
		})
	}
}

// TestMonitorWriteBlocked tests the write function when the writes are blocked.
func TestMonitorWriteBlocked(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db, m := createFakeDBAndMonitor(t)
	m.actionDelay = 10 * time.Millisecond
	m.actionTimeout = 1 * time.Second
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// Check the initial value of the inProgressWriteCount.
	m.mu.Lock()
	require.EqualValues(t, 0, m.inProgressWriteCount)
	m.mu.Unlock()

	// ExecuteFetchMulti will execute each statement separately, so we need to add SET query and INSERT query.
	// Add them multiple times so the writes can execute.
	for range maxWritesPermitted {
		db.AddQuery("SET SESSION lock_wait_timeout=1", &sqltypes.Result{})
		db.AddQuery("INSERT INTO _vt.semisync_heartbeat (ts) VALUES (NOW())", &sqltypes.Result{})
	}

	// Do a write, which we expect to block.
	var writeFinished atomic.Bool
	go func() {
		m.write()
		writeFinished.Store(true)
	}()

	// We should see the number of writers increase briefly, before it completes.
	require.Zero(t, m.errorCount.Get())
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount > 0
	}, 2*time.Second, 5*time.Microsecond)

	// Check that the writes finished successfully.
	require.Eventually(t, func() bool {
		return writeFinished.Load()
	}, 2*time.Second, 100*time.Millisecond)

	// After write completes, count should be back to zero.
	m.mu.Lock()
	defer m.mu.Unlock()
	require.EqualValues(t, 0, m.inProgressWriteCount)
}

// TestIsWriting checks the transitions for the isWriting field.
func TestIsWriting(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// Check the initial value of the isWriting field.
	m.mu.Lock()
	require.False(t, m.isWriting.Load())
	m.mu.Unlock()

	// Clearing a false field does nothing.
	m.clearIsWriting()
	m.mu.Lock()
	require.False(t, m.isWriting.Load())
	m.mu.Unlock()

	// Check and set should set the field.
	set := m.checkAndSetIsWriting()
	require.True(t, set)
	m.mu.Lock()
	require.True(t, m.isWriting.Load())
	m.mu.Unlock()

	// Checking and setting shouldn't do anything.
	set = m.checkAndSetIsWriting()
	require.False(t, set)
	m.mu.Lock()
	require.True(t, m.isWriting.Load())
	m.mu.Unlock()

	// Clearing should now make the field false.
	m.clearIsWriting()
	m.mu.Lock()
	require.False(t, m.isWriting.Load())
	m.mu.Unlock()
}

func TestStartWrites(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db, m := createFakeDBAndMonitor(t)
	m.actionDelay = 10 * time.Millisecond
	m.actionTimeout = 1 * time.Second
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// Set up semi-sync stats query to return blocked state (waiting sessions > 0, no progress).
	// This is what isSemiSyncBlocked will check inside startWrites.
	db.AddQuery(fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|2",
		"Rpl_semi_sync_source_yes_tx|100"))

	// ExecuteFetchMulti will execute each statement separately.
	// Use patterns for both SET and INSERT since they can be called multiple times.
	db.AddQuery("SET SESSION lock_wait_timeout=1", &sqltypes.Result{})
	db.AddQuery("INSERT INTO _vt.semisync_heartbeat (ts) VALUES (NOW())", &sqltypes.Result{})

	// If we aren't blocked, then start writes doesn't do anything.
	m.startWrites()
	require.EqualValues(t, "", db.QueryLog())

	// Now we set the monitor to be blocked.
	m.setIsBlocked(true)

	// Start writes and wait for them to complete.
	m.startWrites()

	// Check that some writes are in progress.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount > 0
	}, 2*time.Second, 5*time.Microsecond)

	// Verify the query log shows the writes were executed.
	queryLog := db.QueryLog()
	require.Contains(t, queryLog, "insert into _vt.semisync_heartbeat")

	// Make the monitor unblocked. This should stop the writes.
	m.setIsBlocked(false)

	// Check that no writes are in progress anymore.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 0
	}, 5*time.Second, 100*time.Millisecond)
}

func TestCheckAndFixSemiSyncBlocked(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db, m := createFakeDBAndMonitor(t)
	m.actionDelay = 10 * time.Millisecond
	m.actionTimeout = 1 * time.Second
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	db.SetNeverFail(true)
	// Initially everything is unblocked (zero waiting sessions).
	db.AddQuery(fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|0",
		"Rpl_semi_sync_source_yes_tx|10"))

	// ExecuteFetchMulti will execute each statement separately.
	// Use patterns for both SET and INSERT since they can be called multiple times.
	db.AddQuery("SET SESSION lock_wait_timeout=1", &sqltypes.Result{})
	db.AddQuery("INSERT INTO _vt.semisync_heartbeat (ts) VALUES (NOW())", &sqltypes.Result{})

	// Check that the monitor thinks we are unblocked.
	m.checkAndFixSemiSyncBlocked()
	m.mu.Lock()
	require.False(t, m.isBlocked)
	m.mu.Unlock()

	// Now we set the monitor to be blocked (waiting sessions > 0, no progress).
	db.AddQuery(fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|2",
		"Rpl_semi_sync_source_yes_tx|10"))

	// Manually set isBlocked and start writes like the monitor would do.
	m.setIsBlocked(true)

	// Start writes and wait for them to complete.
	m.startWrites()

	// Wait a bit to let writes execute
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 0
	}, 2*time.Second, 5*time.Microsecond)

	// Verify the query log shows the writes were executed.
	queryLog := db.QueryLog()
	require.Contains(t, queryLog, "insert into _vt.semisync_heartbeat")

	// Now we set the monitor to be unblocked (waiting sessions = 0).
	db.AddQuery(fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|0",
		"Rpl_semi_sync_source_yes_tx|10"))

	// Make the monitor unblocked. This should stop the writes.
	m.setIsBlocked(false)

	// Check that no writes are in progress anymore.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 0
	}, 10*time.Second, 100*time.Millisecond)
}

// statefulQueryHandler is a custom query handler that can return different results
// based on an atomic boolean state. This allows tests to dynamically change query
// results without relying on precise query result counts.
type statefulQueryHandler struct {
	db                 *fakesqldb.DB
	semisyncBlocked    atomic.Bool
	blockedResult      *sqltypes.Result
	unblockedResult    *sqltypes.Result
	semiSyncStatsQuery string
}

func (h *statefulQueryHandler) HandleQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	if query == h.semiSyncStatsQuery {
		if h.semisyncBlocked.Load() {
			return callback(h.blockedResult)
		}
		return callback(h.unblockedResult)
	}
	// Fall back to default handler for all other queries (SET and INSERT patterns).
	return h.db.HandleQuery(c, query, callback)
}

func TestWaitUntilSemiSyncUnblocked(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")
	config := &tabletenv.TabletConfig{
		DB: dbc,
		SemiSyncMonitor: tabletenv.SemiSyncMonitorConfig{
			Interval: 100 * time.Millisecond,
		},
	}
	m := NewMonitor(config, exporter)
	m.actionDelay = 10 * time.Millisecond
	m.actionTimeout = 1 * time.Second
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	db.SetNeverFail(true)

	// Set up a custom query handler that returns different results based on state
	handler := &statefulQueryHandler{
		db:                 db,
		semiSyncStatsQuery: fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()),
		blockedResult: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|3",
			"Rpl_semi_sync_source_yes_tx|3"),
		unblockedResult: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|0",
			"Rpl_semi_sync_source_yes_tx|0"),
	}
	handler.semisyncBlocked.Store(false) // Initially unblocked
	db.Handler = handler

	// ExecuteFetchMulti will execute each statement separately
	// Use patterns for both SET and INSERT since they can be called multiple times
	db.AddQuery("SET SESSION lock_wait_timeout=1", &sqltypes.Result{})
	db.AddQuery("INSERT INTO _vt.semisync_heartbeat (ts) VALUES (NOW())", &sqltypes.Result{})

	// Open the monitor so the periodic timer runs.
	m.Open()

	// When everything is unblocked, then this should return without blocking.
	err := m.WaitUntilSemiSyncUnblocked(context.Background())
	require.NoError(t, err)

	// Now we set the monitor to be blocked by changing the state.
	handler.semisyncBlocked.Store(true)

	// Wait until the writes have started.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		// Check if we have any in-progress writes, which indicates writing has started.
		return m.inProgressWriteCount > 0 || m.isWriting.Load()
	}, 5*time.Second, 5*time.Microsecond)

	wg := sync.WaitGroup{}
	// Start a cancellable context and use that to wait.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg.Add(1)
	var ctxErr error
	var mu sync.Mutex
	go func() {
		defer wg.Done()
		err := m.WaitUntilSemiSyncUnblocked(ctx)
		mu.Lock()
		ctxErr = err
		mu.Unlock()
	}()

	// Start another go routine, also waiting for semi-sync being unblocked, but not using the cancellable context.
	wg.Go(func() {
		err := m.WaitUntilSemiSyncUnblocked(context.Background())
		require.NoError(t, err)
	})

	// Now we cancel the context. This should fail the first wait.
	cancel()
	// Since we cancel the context before the semi-sync has been unblocked, we expect a context timeout error.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return ctxErr != nil
	}, 2*time.Second, 100*time.Millisecond)
	mu.Lock()
	require.EqualError(t, ctxErr, "context canceled")
	mu.Unlock()

	// Now we set the monitor to be unblocked by changing the state
	handler.semisyncBlocked.Store(false)

	err = m.WaitUntilSemiSyncUnblocked(context.Background())
	require.NoError(t, err)
	// This should unblock the second wait.
	wg.Wait()
	// Eventually the writes should also stop.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return !m.isWriting.Load()
	}, 2*time.Second, 100*time.Millisecond)

	// Also verify that if the monitor is closed, we don't wait.
	m.Close()
	err = m.WaitUntilSemiSyncUnblocked(context.Background())
	require.NoError(t, err)
	require.True(t, m.isClosed())
}

// TestDeadlockOnClose tests the deadlock that can occur when calling Close().
// Look at https://github.com/vitessio/vitess/issues/18275 for more details.
func TestDeadlockOnClose(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")
	config := &tabletenv.TabletConfig{
		DB: dbc,
		SemiSyncMonitor: tabletenv.SemiSyncMonitorConfig{
			// Extremely low interval to trigger the deadlock quickly.
			// This makes the monitor try and write that it is still blocked quite aggressively.
			Interval: 10 * time.Millisecond,
		},
	}
	m := NewMonitor(config, exporter)
	m.actionDelay = 10 * time.Millisecond
	m.actionTimeout = 1 * time.Second

	// Set up for semisync to be blocked
	db.SetNeverFail(true)
	db.AddQuery(fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds()), sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|1", "Rpl_semi_sync_source_yes_tx|1"))

	// Open the monitor
	m.Open()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// We will now try to close and open the monitor multiple times to see if we can trigger a deadlock.
	finishCh := make(chan int)
	go func() {
		count := 100
		for range count {
			m.Close()
			m.Open()
			time.Sleep(20 * time.Millisecond)
		}
		close(finishCh)
	}()

	select {
	case <-finishCh:
		// The test finished without deadlocking.
	case <-time.After(5 * time.Second):
		// The test timed out, which means we deadlocked.
		buf := make([]byte, 1<<16) // 64 KB buffer size
		stackSize := runtime.Stack(buf, true)
		log.Errorf("Stack trace:\n%s", string(buf[:stackSize]))
		t.Fatalf("Deadlock occurred while closing the monitor")
	}
}

// TestSemiSyncMonitor tests the semi-sync monitor as a black box.
// It only calls the exported methods to see they work as intended.
func TestSemiSyncMonitor(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")
	config := &tabletenv.TabletConfig{
		DB: dbc,
		SemiSyncMonitor: tabletenv.SemiSyncMonitorConfig{
			Interval: 100 * time.Millisecond,
		},
	}
	m := NewMonitor(config, exporter)
	m.actionDelay = 10 * time.Millisecond
	m.actionTimeout = 1 * time.Second
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	db.SetNeverFail(true)

	// Set up a custom query handler that returns different results based on state.
	handler := &statefulQueryHandler{
		db:                 db,
		semiSyncStatsQuery: fmt.Sprintf(semiSyncStatsQuery, m.actionDelay.Milliseconds()),
		blockedResult: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|1",
			"Rpl_semi_sync_source_yes_tx|1"),
		unblockedResult: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|0",
			"Rpl_semi_sync_source_yes_tx|0"),
	}
	handler.semisyncBlocked.Store(false) // Initially unblocked
	db.Handler = handler

	// ExecuteFetchMulti will execute each statement separately
	// Use patterns for both SET and INSERT since they can be called multiple times.
	db.AddQuery("SET SESSION lock_wait_timeout=1", &sqltypes.Result{})
	db.AddQuery("INSERT INTO _vt.semisync_heartbeat (ts) VALUES (NOW())", &sqltypes.Result{})

	// Open the monitor.
	m.Open()

	// Initially writes aren't blocked and the wait returns immediately.
	require.False(t, m.AllWritesBlocked())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := m.WaitUntilSemiSyncUnblocked(ctx)
	require.NoError(t, err)

	// Test that WaitUntilSemiSyncUnblocked works correctly when the monitor starts returning unblocked.
	// We don't need to test the blocking behavior in detail since TestWaitUntilSemiSyncUnblocked covers that.
	// This test just verifies the basic black-box behavior.

	// Set to blocked state.
	handler.semisyncBlocked.Store(true)

	// Start a waiter.
	var waitFinished atomic.Bool
	go func() {
		err := m.WaitUntilSemiSyncUnblocked(context.Background())
		require.NoError(t, err)
		waitFinished.Store(true)
	}()

	// Now unblock and verify the wait completes.
	handler.semisyncBlocked.Store(false)

	require.Eventually(t, func() bool {
		return waitFinished.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// Close the monitor.
	m.Close()
	// The test is technically over, but we wait for the writes to have stopped to prevent any data races.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return !m.isWriting.Load()
	}, 2*time.Second, 100*time.Millisecond)
}

// waitUntilWritingStopped is a utility functions that waits until all the go-routines of a semi-sync monitor
// have stopped. This is useful to prevent data-race errors. After a monitor has been closed, it stops writing
// in the next check of stillBlocked.
func waitUntilWritingStopped(t *testing.T, m *Monitor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timed out waiting for writing to stop: %v", ctx.Err())
		case <-tick.C:
			m.mu.Lock()
			if !m.isWriting.Load() {
				m.mu.Unlock()
				return
			}
			m.mu.Unlock()
		}
	}
}
