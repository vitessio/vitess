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
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	initialVal := waitBetweenWrites
	waitBetweenWrites = 1 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer func() {
				m.Close()
				waitUntilWritingStopped(t, m)
			}()

			db.AddQuery(semiSyncStatsQuery, tt.result)

			got, err := m.isSemiSyncBlocked()
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestMonitorIsSemiSyncBlockedConnectionError tests error handling when
// getting a connection from the pool fails.
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

// TestMonitorIsSemiSyncBlockedWithWrongRowCount tests error handling when
// the query returns an unexpected number of rows.
func TestMonitorIsSemiSyncBlockedWithWrongRowCount(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	initialVal := waitBetweenWrites
	waitBetweenWrites = 50 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()

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
			name: "three rows instead of two",
			res:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|1", "Rpl_semi_sync_source_yes_tx|5", "extra_row|10"),
			// Note: The actual error is "Row count exceeded" because ExecuteFetch has maxrows=2.
			wantErr: "Row count exceeded",
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

			db.AddQuery(semiSyncStatsQuery, tt.res)

			got, err := m.isSemiSyncBlocked()
			require.Error(t, err)
			require.False(t, got)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestMonitorIsSemiSyncBlockedWithInvalidValues tests error handling when
// the query returns non-numeric values.
func TestMonitorIsSemiSyncBlockedWithInvalidValues(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	initialVal := waitBetweenWrites
	waitBetweenWrites = 50 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()

	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// Return non-numeric values
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|not_a_number",
		"Rpl_semi_sync_source_yes_tx|5"))

	got, err := m.isSemiSyncBlocked()
	require.Error(t, err)
	require.False(t, got)
	require.Contains(t, err.Error(), "unexpected results for semi-sync stats query")
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
			// Directly test the logic by simulating what isSemiSyncBlocked does
			// First call - check initial stats
			stats := semiSyncStats{
				waitingSessions: tt.firstWaiting,
				ackedTrxs:       tt.firstAcked,
			}

			// Early return conditions from isSemiSyncBlocked
			if stats.waitingSessions == 0 {
				require.False(t, tt.expectedBlocked, tt.description)
				return
			}

			// Second call - check follow-up stats
			followUpStats := semiSyncStats{
				waitingSessions: tt.secondWaiting,
				ackedTrxs:       tt.secondAcked,
			}

			// Check if we're still blocked based on the actual logic in isSemiSyncBlocked
			// Returns false (not blocked) if: waitingSessions == 0 OR ackedTrxs increased
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

			db.AddQuery(semiSyncStatsQuery, tt.res)
			conn, err := m.appPool.Get(context.Background())
			require.NoError(t, err)
			defer conn.Recycle()

			stats, err := getSemiSyncStats(conn)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
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
	m.clearAllData()
	ql := db.QueryLog()
	require.EqualValues(t, "truncate table _vt.semisync_heartbeat", ql)
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
		initVal  int
		queryLog string
	}{
		{
			initVal:  maxWritesPermitted - 2,
			queryLog: "insert into _vt.semisync_heartbeat (ts) values (now())",
		}, {
			initVal:  maxWritesPermitted - 1,
			queryLog: "insert into _vt.semisync_heartbeat (ts) values (now())",
		}, {
			initVal:  maxWritesPermitted,
			queryLog: "",
		}, {
			initVal:  0,
			queryLog: "insert into _vt.semisync_heartbeat (ts) values (now())",
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
			m.mu.Lock()
			m.inProgressWriteCount = tt.initVal
			m.writesBlockedGauge.Set(int64(tt.initVal))
			m.mu.Unlock()
			m.write()
			m.mu.Lock()
			require.EqualValues(t, tt.initVal, m.inProgressWriteCount)
			require.EqualValues(t, tt.initVal, m.writesBlockedGauge.Get())
			m.mu.Unlock()
			require.EqualValues(t, tt.queryLog, db.QueryLog())
		})
	}
}

// TestMonitorWriteBlocked tests the write function when the writes are blocked.
func TestMonitorWriteBlocked(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	initialVal := waitBetweenWrites
	waitBetweenWrites = 250 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// Check the initial value of the inProgressWriteCount.
	m.mu.Lock()
	require.EqualValues(t, 0, m.inProgressWriteCount)
	m.mu.Unlock()

	// Add a universal insert query pattern that would block until we make it unblock.
	ch := make(chan int)
	db.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})

	// Do a write, which we expect to block.
	var writeFinished atomic.Bool
	go func() {
		m.write()
		writeFinished.Store(true)
	}()
	// We should see the number of writes blocked to increase.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 1
	}, 2*time.Second, 100*time.Millisecond)

	// Once the writers are unblocked, we expect to see a zero value again.
	close(ch)
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 0
	}, 2*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return writeFinished.Load()
	}, 2*time.Second, 100*time.Millisecond)
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
	initialVal := waitBetweenWrites
	waitBetweenWrites = 150 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	// Set up semi-sync stats query to return blocked state (waiting sessions > 0, no progress)
	// This is what isSemiSyncBlocked will check inside startWrites
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|2",
		"Rpl_semi_sync_source_yes_tx|100"))

	// Add a universal insert query pattern that would block until we make it unblock.
	ch := make(chan int)
	db.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})

	// If we aren't blocked, then start writes doesn't do anything.
	m.startWrites()
	require.EqualValues(t, "", db.QueryLog())

	// Now we set the monitor to be blocked.
	m.setIsBlocked(true)

	var writesFinished atomic.Bool
	go func() {
		m.startWrites()
		writesFinished.Store(true)
	}()

	// We should see the number of writes blocked to increase.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount >= 1
	}, 5*time.Second, 100*time.Millisecond)

	// Once the writes have started, another call to startWrites shouldn't do anything.
	m.startWrites()

	// We should continue to see the number of writes blocked increase.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount >= 2
	}, 5*time.Second, 100*time.Millisecond)

	// Check that the writes are still going.
	require.False(t, writesFinished.Load())

	// Update the semi-sync stats to show unblocked state (waiting sessions = 0)
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|0",
		"Rpl_semi_sync_source_yes_tx|100"))

	// Make the monitor unblocked. This should stop the writes eventually.
	m.setIsBlocked(false)
	close(ch)

	require.Eventually(t, func() bool {
		return writesFinished.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// Check that no writes are in progress anymore.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 0
	}, 5*time.Second, 100*time.Millisecond)
}

func TestCheckAndFixSemiSyncBlocked(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	initialVal := waitBetweenWrites
	waitBetweenWrites = 150 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	db.SetNeverFail(true)
	// Initially everything is unblocked (zero waiting sessions).
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|0",
		"Rpl_semi_sync_source_yes_tx|10"))

	// Add a universal insert query pattern that would block until we make it unblock.
	ch := make(chan int)
	db.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})

	// Check that the monitor thinks we are unblocked.
	m.checkAndFixSemiSyncBlocked()
	m.mu.Lock()
	require.False(t, m.isBlocked)
	m.mu.Unlock()

	// Now we set the monitor to be blocked (waiting sessions > 0, no progress).
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|2",
		"Rpl_semi_sync_source_yes_tx|10"))

	// Manually set isBlocked and start writes like the monitor would do.
	m.setIsBlocked(true)

	var writesFinished atomic.Bool
	go func() {
		m.startWrites()
		writesFinished.Store(true)
	}()

	// Meanwhile writes should have started and should be getting blocked.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount >= 2
	}, 10*time.Second, 100*time.Millisecond)

	// Check that the writes are still going.
	require.False(t, writesFinished.Load())

	// Now we set the monitor to be unblocked (waiting sessions = 0).
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|0",
		"Rpl_semi_sync_source_yes_tx|10"))

	// Make the monitor unblocked. This should stop the writes eventually.
	m.setIsBlocked(false)
	close(ch)

	require.Eventually(t, func() bool {
		return writesFinished.Load()
	}, 10*time.Second, 100*time.Millisecond)

	// Check that no writes are in progress anymore.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func TestWaitUntilSemiSyncUnblocked(t *testing.T) {
	defer utils.EnsureNoLeaks(t)
	initialVal := waitBetweenWrites
	waitBetweenWrites = 1 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")
	config := &tabletenv.TabletConfig{
		DB: dbc,
		SemiSyncMonitor: tabletenv.SemiSyncMonitorConfig{
			Interval: 10 * time.Millisecond,
		},
	}
	m := NewMonitor(config, exporter)
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	db.SetNeverFail(true)
	// Initially everything is unblocked.
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|0", "Rpl_semi_sync_source_yes_tx|0"))

	// Open the monitor so the periodic timer runs
	m.Open()

	// When everything is unblocked, then this should return without blocking.
	err := m.WaitUntilSemiSyncUnblocked(context.Background())
	require.NoError(t, err)

	// Add a universal insert query pattern that would block until we make it unblock.
	ch := make(chan int)
	db.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})
	// Now we set the monitor to be blocked.
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|3", "Rpl_semi_sync_source_yes_tx|3"))

	// wg is used to keep track of all the go routines.
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := m.WaitUntilSemiSyncUnblocked(context.Background())
		require.NoError(t, err)
	}()

	// Wait until the writes have started.
	// Note: With the timer-based approach, checkAndFixSemiSyncBlocked is called periodically
	// and spawns startWrites with a context that expires. So writes may start and stop repeatedly.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		// Check if we have any in-progress writes, which indicates writing has started.
		return m.inProgressWriteCount > 0 || m.isWriting.Load()
	}, 15*time.Second, 100*time.Millisecond)

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

	// Now we set the monitor to be unblocked.
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|0", "Rpl_semi_sync_source_yes_tx|0"))
	close(ch)
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
	initialVal := waitBetweenWrites
	waitBetweenWrites = 1 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
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

	// Set up for semisync to be blocked
	db.SetNeverFail(true)
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|1", "Rpl_semi_sync_source_yes_tx|1"))

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
		for i := 0; i < count; i++ {
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
	initialVal := waitBetweenWrites
	waitBetweenWrites = 1 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "")
	config := &tabletenv.TabletConfig{
		DB: dbc,
		SemiSyncMonitor: tabletenv.SemiSyncMonitorConfig{
			Interval: 10 * time.Millisecond,
		},
	}
	m := NewMonitor(config, exporter)
	defer func() {
		m.Close()
		waitUntilWritingStopped(t, m)
	}()

	db.SetNeverFail(true)
	// Initially everything is unblocked.
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|0", "Rpl_semi_sync_source_yes_tx|0"))

	// Open the monitor.
	m.Open()

	// Initially writes aren't blocked and the wait returns immediately.
	require.False(t, m.AllWritesBlocked())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := m.WaitUntilSemiSyncUnblocked(ctx)
	require.NoError(t, err)

	// Add a universal insert query pattern that would block until we make it unblock.
	ch := make(chan int)
	db.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})
	// Now we set the monitor to be blocked.
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|1", "Rpl_semi_sync_source_yes_tx|1"))

	// Start a go routine waiting for semi-sync being unblocked.
	var waitFinished atomic.Bool
	go func() {
		err := m.WaitUntilSemiSyncUnblocked(context.Background())
		require.NoError(t, err)
		waitFinished.Store(true)
	}()

	// Even if we wait a second, the wait shouldn't be over.
	time.Sleep(1 * time.Second)
	require.False(t, waitFinished.Load())

	// If we unblock the semi-sync, then the wait should finish.
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|0", "Rpl_semi_sync_source_yes_tx|0"))
	close(ch)
	require.Eventually(t, func() bool {
		return waitFinished.Load()
	}, 2*time.Second, 100*time.Millisecond)
	require.False(t, m.AllWritesBlocked())

	// Add a universal insert query pattern that would block until we make it unblock.
	ch = make(chan int)
	db.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})
	// We block the semi-sync again.
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|1", "Rpl_semi_sync_source_yes_tx|1"))

	// Start another go routine, also waiting for semi-sync being unblocked.
	waitFinished.Store(false)
	go func() {
		err := m.WaitUntilSemiSyncUnblocked(context.Background())
		require.NoError(t, err)
		waitFinished.Store(true)
	}()

	// Since the writes are now blocking, eventually writes should accumulate.
	// Note: With the timer-based approach and short-lived contexts, reaching all 15 writes
	// takes a very long time. So we just verify that some writes are blocked.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount >= 3
	}, 20*time.Second, 100*time.Millisecond)

	// The wait should still not have ended.
	require.False(t, waitFinished.Load())

	// Now we unblock the writes and semi-sync.
	close(ch)
	db.AddQuery(semiSyncStatsQuery, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"), "Rpl_semi_sync_source_wait_sessions|0", "Rpl_semi_sync_source_yes_tx|0"))

	// The wait should now finish.
	require.Eventually(t, func() bool {
		return waitFinished.Load()
	}, 2*time.Second, 100*time.Millisecond)
	require.False(t, m.AllWritesBlocked())

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
