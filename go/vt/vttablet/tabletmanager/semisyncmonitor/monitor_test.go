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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
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
func TestMonitorIsSemiSyncBlocked(t *testing.T) {
	tests := []struct {
		name    string
		res     *sqltypes.Result
		want    bool
		wantErr string
	}{
		{
			name: "no rows",
			res:  &sqltypes.Result{},
			want: false,
		},
		{
			name:    "incorrect number of rows",
			res:     sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "1", "1"),
			wantErr: "Row count exceeded 1",
		},
		{
			name:    "incorrect number of fields",
			res:     sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value|a", "varchar|varchar"), "1|2"),
			wantErr: `unexpected number of rows received - [[VARCHAR("1") VARCHAR("2")]]`,
		},
		{
			name: "Unblocked",
			res:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "0"),
			want: false,
		},
		{
			name: "Blocked",
			res:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "1"),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer m.Close()
			db.AddQuery(semiSyncWaitSessionsRead, tt.res)
			got, err := m.isSemiSyncBlocked(context.Background())
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tt.want, got)
		})
	}
}

func TestMonitorBindSideCarDBName(t *testing.T) {
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
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer m.Close()
	db.SetNeverFail(true)
	m.clearAllData()
	ql := db.QueryLog()
	require.EqualValues(t, "truncate table _vt.semisync_heartbeat", ql)
}

// TestMonitorWaitMechanism tests that the wait mechanism works as intended.
// Setting the monitor to unblock state should unblock the waiters.
func TestMonitorWaitMechanism(t *testing.T) {
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer m.Close()

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
		t.Run(fmt.Sprintf("%d", tt.initVal), func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer m.Close()
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
		t.Run(fmt.Sprintf("%d", tt.initVal), func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer m.Close()
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
		t.Run(fmt.Sprintf("%d", tt.initVal), func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer m.Close()
			m.mu.Lock()
			m.inProgressWriteCount = tt.initVal
			m.mu.Unlock()
			require.EqualValues(t, tt.expected, m.AllWritesBlocked())
		})
	}
}

func TestMonitorWrite(t *testing.T) {
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
		t.Run(fmt.Sprintf("%d", tt.initVal), func(t *testing.T) {
			db, m := createFakeDBAndMonitor(t)
			defer db.Close()
			defer m.Close()
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
	initialVal := waitBetweenWrites
	waitBetweenWrites = 250 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer m.Close()

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
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer m.Close()

	// Check the initial value of the isWriting field.
	m.mu.Lock()
	require.False(t, m.isWriting)
	m.mu.Unlock()

	// Clearing a false field does nothing.
	m.clearIsWriting()
	m.mu.Lock()
	require.False(t, m.isWriting)
	m.mu.Unlock()

	// Check and set should set the field.
	set := m.checkAndSetIsWriting()
	require.True(t, set)
	m.mu.Lock()
	require.True(t, m.isWriting)
	m.mu.Unlock()

	// Checking and setting shouldn't do anything.
	set = m.checkAndSetIsWriting()
	require.False(t, set)
	m.mu.Lock()
	require.True(t, m.isWriting)
	m.mu.Unlock()

	// Clearing should now make the field false.
	m.clearIsWriting()
	m.mu.Lock()
	require.False(t, m.isWriting)
	m.mu.Unlock()
}

func TestStartWrites(t *testing.T) {
	initialVal := waitBetweenWrites
	waitBetweenWrites = 250 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer m.Close()

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
	}, 2*time.Second, 100*time.Millisecond)

	// Once the writes have started, another call to startWrites shouldn't do anything
	m.startWrites()

	// We should continue to see the number of writes blocked increase.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount >= 2
	}, 2*time.Second, 100*time.Millisecond)

	// Check that the writes are still going.
	require.False(t, writesFinished.Load())

	// Make the monitor unblocked. This should stop the writes eventually.
	m.setIsBlocked(false)
	close(ch)

	require.Eventually(t, func() bool {
		return writesFinished.Load()
	}, 2*time.Second, 100*time.Millisecond)

	// Check that no writes are in progress anymore.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 0
	}, 2*time.Second, 100*time.Millisecond)
}

func TestCheckAndFixSemiSyncBlocked(t *testing.T) {
	initialVal := waitBetweenWrites
	waitBetweenWrites = 250 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer m.Close()

	// Initially everything is unblocked.
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "0"))
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

	// Now we set the monitor to be blocked.
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "2"))
	m.checkAndFixSemiSyncBlocked()

	m.mu.Lock()
	require.True(t, m.isBlocked)
	m.mu.Unlock()

	// Checking again shouldn't make a difference.
	m.checkAndFixSemiSyncBlocked()
	m.mu.Lock()
	require.True(t, m.isBlocked)
	m.mu.Unlock()

	// Meanwhile writes should have started and should be getting blocked.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount >= 2
	}, 2*time.Second, 100*time.Millisecond)

	// Now we set the monitor to be unblocked.
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "0"))
	close(ch)
	m.checkAndFixSemiSyncBlocked()

	// We expect the writes to clear out and also the monitor should think its unblocked.
	m.mu.Lock()
	require.False(t, m.isBlocked)
	m.mu.Unlock()
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.inProgressWriteCount == 0 && m.isWriting == false
	}, 2*time.Second, 100*time.Millisecond)
}

func TestWaitUntilSemiSyncUnblocked(t *testing.T) {
	initialVal := waitBetweenWrites
	waitBetweenWrites = 250 * time.Millisecond
	defer func() {
		waitBetweenWrites = initialVal
	}()
	db, m := createFakeDBAndMonitor(t)
	defer db.Close()
	defer m.Close()

	db.SetNeverFail(true)
	// Initially everything is unblocked.
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "0"))

	// When everything is unblocked, then this should return without blocking.
	err := m.WaitUntilSemiSyncUnblocked(context.Background())
	require.NoError(t, err)

	// Add a universal insert query pattern that would block until we make it unblock.
	ch := make(chan int)
	db.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})
	// Now we set the monitor to be blocked.
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "3"))

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
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.isWriting
	}, 2*time.Second, 100*time.Millisecond)

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
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "0"))
	close(ch)
	err = m.WaitUntilSemiSyncUnblocked(context.Background())
	require.NoError(t, err)
	// This should unblock the second wait.
	wg.Wait()
	// Eventually the writes should also stop.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return !m.isWriting
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
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "1"))

	// Open the monitor
	m.Open()
	defer m.Close()

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
		t.Fatalf("Deadlock occurred while closing the monitor")
	}
}

// TestSemiSyncMonitor tests the semi-sync monitor as a black box.
// It only calls the exported methods to see they work as intended.
func TestSemiSyncMonitor(t *testing.T) {
	initialVal := waitBetweenWrites
	waitBetweenWrites = 250 * time.Millisecond
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
			Interval: 1 * time.Second,
		},
	}
	m := NewMonitor(config, exporter)
	defer m.Close()

	db.SetNeverFail(true)
	// Initially everything is unblocked.
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "0"))

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
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "1"))

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
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "0"))
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
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "1"))

	// Start another go routine, also waiting for semi-sync being unblocked.
	waitFinished.Store(false)
	go func() {
		err := m.WaitUntilSemiSyncUnblocked(context.Background())
		require.NoError(t, err)
		waitFinished.Store(true)
	}()

	// Since the writes are now blocking, eventually all the writes should block.
	require.Eventually(t, func() bool {
		return m.AllWritesBlocked()
	}, 10*time.Second, 100*time.Millisecond)

	// The wait should still not have ended.
	require.False(t, waitFinished.Load())

	// Now we unblock the writes and semi-sync.
	close(ch)
	db.AddQuery(semiSyncWaitSessionsRead, sqltypes.MakeTestResult(sqltypes.MakeTestFields("variable_value", "varchar"), "0"))

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
		return !m.isWriting
	}, 2*time.Second, 100*time.Millisecond)
}
