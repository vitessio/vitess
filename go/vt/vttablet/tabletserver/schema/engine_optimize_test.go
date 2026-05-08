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

package schema

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
)

// TestShouldAttemptGtidExecutedOptimizeLocked exercises the three gates:
// primary/non-primary, tablet-role stability cooldown, and per-tablet 24h
// throttle.
func TestShouldAttemptGtidExecutedOptimizeLocked(t *testing.T) {
	now := time.Now()

	t.Run("serving primary tablet skips", func(t *testing.T) {
		se := &Engine{isServingPrimary: true}
		se.isPrimaryTablet.Store(true)
		assert.False(t, se.shouldAttemptGtidExecutedOptimizeLocked(now))
	})

	t.Run("primary-not-serving tablet skips", func(t *testing.T) {
		// Regression for the unservePrimary() path where isServingPrimary
		// is false but the tablet is still of type PRIMARY: OPTIMIZE must
		// not run on it.
		se := &Engine{isServingPrimary: false}
		se.isPrimaryTablet.Store(true)
		assert.False(t, se.shouldAttemptGtidExecutedOptimizeLocked(now))
	})

	t.Run("within stability cooldown skips", func(t *testing.T) {
		se := &Engine{tabletTypeLastChangedAt: now.Add(-1 * time.Minute)}
		assert.False(t, se.shouldAttemptGtidExecutedOptimizeLocked(now))
	})

	t.Run("past stability cooldown proceeds", func(t *testing.T) {
		se := &Engine{tabletTypeLastChangedAt: now.Add(-10 * time.Minute)}
		assert.True(t, se.shouldAttemptGtidExecutedOptimizeLocked(now))
	})

	t.Run("within 24h throttle skips", func(t *testing.T) {
		se := &Engine{gtidExecutedOptimizeLastAt: now.Add(-1 * time.Hour)}
		assert.False(t, se.shouldAttemptGtidExecutedOptimizeLocked(now))
	})

	t.Run("past 24h throttle proceeds", func(t *testing.T) {
		se := &Engine{gtidExecutedOptimizeLastAt: now.Add(-25 * time.Hour)}
		assert.True(t, se.shouldAttemptGtidExecutedOptimizeLocked(now))
	})

	t.Run("fresh engine waits for cooldown", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()

		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		assert.False(t, se.shouldAttemptGtidExecutedOptimizeLocked(time.Now()))
	})
}

// TestDisableSuperReadOnly covers the SRO toggle helper's branches:
// already-off, on-and-flipped-with-restore, and unknown-system-variable
// (MariaDB / older MySQL).
func TestDisableSuperReadOnly(t *testing.T) {
	selectSuperReadOnlyQuery := "SELECT @@global.super_read_only"

	t.Run("already off returns noop restore", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"0",
		))
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)
		defer conn.Close()

		restore, err := se.disableSuperReadOnly(t.Context(), conn)
		require.NoError(t, err)
		require.NotNil(t, restore)
		restore() // no-op, must not send a SET GLOBAL
		assert.Equal(t, 1, db.GetQueryCalledNum(selectSuperReadOnlyQuery))
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"))
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
	})

	t.Run("on is flipped and restored via a fresh connection", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"1",
		))
		db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
		db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)
		defer conn.Close()

		restore, err := se.disableSuperReadOnly(t.Context(), conn)
		require.NoError(t, err)
		require.NotNil(t, restore)
		// After disable-flip, we expect the SELECT + OFF to have fired, but
		// not yet the ON restore.
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"))
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
		restore()
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
	})

	t.Run("restore still runs even if OPTIMIZE conn has been closed", func(t *testing.T) {
		// Regression for the timeout path: executeFetchCtx closes the
		// OPTIMIZE conn after issuing KILL. restoreSuperReadOnly must not
		// depend on that handle — it opens a fresh DBA connection.
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"1",
		))
		db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
		db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)

		restore, err := se.disableSuperReadOnly(t.Context(), conn)
		require.NoError(t, err)
		// Simulate executeFetchCtx having closed the conn after KILL.
		conn.Close()

		// Restore must still succeed (fresh DBA connection).
		restore()
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
			"SRO restore must use a fresh connection so it survives the original conn being closed")
	})

	t.Run("unknown system variable returns noop restore", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddRejectedQuery(selectSuperReadOnlyQuery,
			sqlerror.NewSQLError(sqlerror.ERUnknownSystemVariable, "", "Unknown system variable 'super_read_only'"))
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)
		defer conn.Close()

		restore, err := se.disableSuperReadOnly(t.Context(), conn)
		require.NoError(t, err, "MariaDB-style unknown-variable error must not surface as an error")
		require.NotNil(t, restore)
		restore() // no-op
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"))
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
	})

	t.Run("restore skipped if tablet was promoted to primary", func(t *testing.T) {
		// Regression for the PRS/ERS-during-OPTIMIZE race: if a tablet is
		// promoted while OPTIMIZE is in flight, the promotion itself turns
		// super_read_only OFF. The deferred restore must NOT stomp that back
		// ON or a freshly-promoted primary ends up read-only.
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"1",
		))
		db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)
		defer conn.Close()

		restore, err := se.disableSuperReadOnly(t.Context(), conn)
		require.NoError(t, err)

		// Simulate PRS/ERS landing while OPTIMIZE was in flight.
		se.MakePrimary(true)

		restore()
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
			"restore must skip SET GLOBAL super_read_only = 'ON' after promotion")
	})

	t.Run("restore skips if promoted while reopening the DBA connection", func(t *testing.T) {
		// Regression for the middle promotion re-check: a PRS/ERS can land
		// between the initial check and the SET ... ON while we are still
		// establishing the fresh DBA connection. SetConnDelay forces that
		// connection open to take ~200ms; a goroutine flips the tablet to
		// PRIMARY ~50ms in, so by the time the middle check runs after the
		// handshake completes it observes the promotion and bails out
		// before SET ... ON.
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"1",
		))
		db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
		db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)
		defer conn.Close()

		restore, err := se.disableSuperReadOnly(t.Context(), conn)
		require.NoError(t, err)

		db.SetConnDelay(200 * time.Millisecond)
		promoted := make(chan struct{})
		go func() {
			defer close(promoted)
			time.Sleep(50 * time.Millisecond)
			se.MakePrimary(true)
		}()

		restore()
		<-promoted
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
			"restore must skip SET ... ON when promotion lands during the fresh DBA conn open")
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"),
			"restore must not issue a compensating SET ... OFF since SET ... ON never ran")
	})

	t.Run("restore disables super_read_only again if promotion races with restore", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"1",
		))
		db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
		onResult := db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)
		defer conn.Close()

		restore, err := se.disableSuperReadOnly(t.Context(), conn)
		require.NoError(t, err)

		onResult.BeforeFunc = func() {
			se.MakePrimary(true)
		}

		restore()
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
		assert.Equal(t, 2, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"),
			"restore must turn super_read_only back OFF if the tablet is promoted during the restore SET")
	})

	t.Run("timeout while disabling restores best effort", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"1",
		))

		const disableQuery = "SET GLOBAL super_read_only = 'OFF'"
		db.AddQuery(disableQuery, &sqltypes.Result{})
		disableStarted := make(chan struct{})
		releaseDisable := make(chan struct{})
		var releaseDisableOnce sync.Once
		t.Cleanup(func() {
			releaseDisableOnce.Do(func() {
				close(releaseDisable)
			})
		})
		var disableStartedOnce sync.Once
		db.SetBeforeFunc(disableQuery, func() {
			disableStartedOnce.Do(func() {
				close(disableStarted)
			})
			<-releaseDisable
		})

		var killSeen atomic.Int32
		db.AddQueryPatternWithCallback(`(?i)^kill [0-9]+$`, &sqltypes.Result{}, func(string) {
			killSeen.Add(1)
		})
		db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})

		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		type disableResult struct {
			restore func()
			err     error
		}
		done := make(chan disableResult, 1)
		go func() {
			restore, err := se.disableSuperReadOnly(ctx, conn)
			done <- disableResult{restore: restore, err: err}
		}()

		require.Eventually(t, func() bool {
			select {
			case <-disableStarted:
				return true
			default:
				return false
			}
		}, 5*time.Second, 10*time.Millisecond, "SET GLOBAL super_read_only = 'OFF' should start")
		assert.Eventually(t, func() bool { return killSeen.Load() >= 1 },
			5*time.Second, 10*time.Millisecond, "disable timeout should interrupt the in-flight SET GLOBAL super_read_only = 'OFF'")

		releaseDisableOnce.Do(func() {
			close(releaseDisable)
		})
		res := <-done
		require.ErrorIs(t, res.err, context.DeadlineExceeded)
		assert.NotNil(t, res.restore)
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
			"disable timeout has unknown MySQL-side result, so it must best-effort restore super_read_only")
	})

	t.Run("client-side error while disabling restores best effort", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"1",
		))
		db.AddRejectedQuery("SET GLOBAL super_read_only = 'OFF'",
			sqlerror.NewSQLError(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "lost connection after SET"))
		db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})

		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
		conn := newTestDBConnection(t, db)
		defer conn.Close()

		restore, err := se.disableSuperReadOnly(t.Context(), conn)
		require.Error(t, err)
		require.ErrorContains(t, err, "disabling super_read_only")
		require.NotNil(t, restore)
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
			"client-side errors have unknown MySQL-side result, so disable must best-effort restore super_read_only")
	})
}

// TestRestoreSuperReadOnlyHonorsContextTimeout verifies that the restore path
// bounds the SET GLOBAL super_read_only = 'ON' itself, not just the fresh DBA
// connection open. Without this, a hung MySQL query could leave a non-primary
// tablet writeable indefinitely.
func TestRestoreSuperReadOnlyHonorsContextTimeout(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	const restoreQuery = "SET GLOBAL super_read_only = 'ON'"
	db.AddQuery(restoreQuery, &sqltypes.Result{})
	release := make(chan struct{})
	db.SetBeforeFunc(restoreQuery, func() {
		<-release
	})

	var killSeen atomic.Int32
	db.AddQueryPatternWithCallback(`(?i)^kill [0-9]+$`, &sqltypes.Result{}, func(string) {
		killSeen.Add(1)
	})

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		se.restoreSuperReadOnlyWithContext(ctx)
		close(done)
	}()

	assert.Eventually(t, func() bool { return killSeen.Load() >= 1 },
		5*time.Second, 10*time.Millisecond, "restore must issue KILL when SET GLOBAL super_read_only = 'ON' times out")
	close(release)
	assert.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "restore must unwind after timing out the SET GLOBAL super_read_only = 'ON'")
}

// TestRunOptimizeGtidExecutedSuccess exercises the full goroutine body on
// the happy path: SRO is on, it is flipped to off, OPTIMIZE runs, SRO is
// restored to on. Running the function synchronously from the test (instead
// of as a goroutine) lets us assert state deterministically.
func TestRunOptimizeGtidExecutedSuccess(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	// SRO toggling.
	db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("super_read_only", "int64"), "1"))
	db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
	db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
	// The actual OPTIMIZE.
	db.AddQuery("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed", &sqltypes.Result{})

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	// Push tabletTypeLastChangedAt into the past so the stability cooldown
	// does not trip the re-check.
	se.mu.Lock()
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.mu.Unlock()

	se.runOptimizeGtidExecuted(200 * 1024 * 1024)

	assert.Equal(t, 1, db.GetQueryCalledNum("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed"))
	assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"))
	assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
}

// TestRunOptimizeGtidExecutedSkipsOnTabletTypeFlip verifies that if the
// tablet is no longer REPLICA/RDONLY by the time the goroutine re-checks,
// it bails out without sending OPTIMIZE or touching super_read_only.
func TestRunOptimizeGtidExecutedSkipsOnTabletTypeFlip(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	// Reject OPTIMIZE so a rogue execution fails loudly.
	db.AddRejectedQuery("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed",
		sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "OPTIMIZE must not be executed when tablet is PRIMARY"))

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.MakePrimary(true)
	// Pre-stamp so the goroutine's cleanup path has something to reset.
	se.mu.Lock()
	se.gtidExecutedOptimizeLastAt = time.Now()
	se.mu.Unlock()

	se.runOptimizeGtidExecuted(200 * 1024 * 1024)

	// gtidExecutedOptimizeLastAt is cleared on the not-eligible path so we
	// retry on the next reload rather than waiting 24h.
	se.mu.Lock()
	assert.True(t, se.gtidExecutedOptimizeLastAt.IsZero())
	se.mu.Unlock()
	assert.Equal(t, 0, db.GetQueryCalledNum("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed"))
}

// TestRunOptimizeGtidExecutedOptimizeFailureRestoresSRO verifies that even
// when OPTIMIZE itself fails, the deferred super_read_only restore still
// runs. This is the key correctness property that protects the tablet from
// being left writeable.
func TestRunOptimizeGtidExecutedOptimizeFailureRestoresSRO(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("super_read_only", "int64"), "1"))
	db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
	db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
	db.AddRejectedQuery("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed",
		sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "simulated OPTIMIZE failure"))

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.mu.Lock()
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.gtidExecutedOptimizeLastAt = time.Now()
	se.mu.Unlock()

	se.runOptimizeGtidExecuted(200 * 1024 * 1024)

	// SRO must be restored even on failure.
	assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
	// The failure path stamps the last-run time to ~now so a
	// persistently-failing OPTIMIZE respects the 24h throttle rather than
	// retrying on every schema reload (each retry would flip super_read_only
	// briefly).
	se.mu.Lock()
	assert.False(t, se.gtidExecutedOptimizeLastAt.IsZero())
	se.mu.Unlock()
}

func TestRunOptimizeGtidExecutedSkipsIfPromotedAfterDisablingSuperReadOnly(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("super_read_only", "int64"), "1"))
	offResult := db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
	db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
	db.AddQuery("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed", &sqltypes.Result{})

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.mu.Lock()
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.gtidExecutedOptimizeLastAt = time.Now()
	se.mu.Unlock()

	offResult.BeforeFunc = func() {
		se.MakePrimary(true)
	}

	se.runOptimizeGtidExecuted(200 * 1024 * 1024)

	assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"))
	assert.Equal(t, 0, db.GetQueryCalledNum("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed"),
		"OPTIMIZE must not run once the tablet has been promoted to PRIMARY")
	assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
		"restore must not re-enable super_read_only on a promoted PRIMARY")
	se.mu.Lock()
	assert.True(t, se.gtidExecutedOptimizeLastAt.IsZero())
	se.mu.Unlock()
}

func TestCloseCancelsInFlightOptimize(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("super_read_only", "int64"), "1"))
	db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
	db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
	db.AddQuery("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed", &sqltypes.Result{})

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseQuery := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseQuery)
	db.SetBeforeFunc("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed", func() {
		<-release
	})

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.mu.Lock()
	se.isOpen = true
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.tables = map[string]*Table{"dual": NewTable("dual", NoType)}
	se.notifiers = make(map[string]notifier)
	se.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		se.runOptimizeGtidExecuted(200 * 1024 * 1024)
	}()

	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed") == 1
	}, 30*time.Second, 10*time.Millisecond)

	se.Close()

	assert.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond)

	releaseQuery()
	assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
		"SRO restore must run even when Engine.Close() has cancelled backgroundCtx; leaving a non-primary tablet with super_read_only=OFF is a correctness hazard")
}

// TestUpdateUserTableFreeSpaceDisabled verifies that when the feature is
// disabled we issue no query and reset any gauge labels published while
// it was previously enabled.
func TestUpdateUserTableFreeSpaceDisabled(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	// Seed the "previously active" state as if a prior enabled run had
	// set labels on the gauge.
	se.tableDataFreeBytes.Set("t_old", 12345)
	se.freeSpaceMetricsActive = true

	// With the feature off, updateUserTableFreeSpaceLocked must not call
	// the DB at all. To assert that, we install an unconditionally-rejecting
	// query pattern that matches DATA_FREE queries.
	db.RejectQueryPattern(".*data_free.*", "updateUserTableFreeSpaceLocked must not query when feature is disabled")

	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.updateUserTableFreeSpaceLocked(ctx, conn.Conn, false)
	se.mu.Unlock()
	conn.Recycle()

	// Stale label should be cleared via ResetAll; freeSpaceMetricsActive
	// should be flipped back to false.
	assert.Equal(t, int64(0), se.tableDataFreeBytes.Counts()["t_old"],
		"gauge label for a table we previously surfaced must be reset when the feature is disabled")
	assert.False(t, se.freeSpaceMetricsActive)
}

// TestUpdateUserTableFreeSpaceDisabledNoOpWhenAlreadyInactive verifies that
// calling updateUserTableFreeSpaceLocked with enabled=false when no labels
// were ever published is a clean no-op (no DB work, no ResetAll).
func TestUpdateUserTableFreeSpaceDisabledNoOpWhenAlreadyInactive(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	require.False(t, se.freeSpaceMetricsActive)

	// No gauge labels seeded; no DB query expected.
	db.RejectQueryPattern(".*data_free.*", "updateUserTableFreeSpaceLocked must not query when feature is disabled")

	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.updateUserTableFreeSpaceLocked(ctx, conn.Conn, false)
	se.mu.Unlock()
	conn.Recycle()

	assert.False(t, se.freeSpaceMetricsActive)
}

// TestUpdateUserTableFreeSpaceEnabled verifies the enabled-feature flow:
// the DATA_FREE query runs and returned rows populate the gauge for every
// user table. Operators apply their own thresholds in their monitoring
// system.
func TestUpdateUserTableFreeSpaceEnabled(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	// PRIMARY so the OPTIMIZE spawn step is skipped cleanly — we're only
	// asserting the visibility side here.
	se.MakePrimary(true)
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	dataFreeQuery := "select table_name, data_free from information_schema.TABLES where table_schema = database() and table_type = 'BASE TABLE'"

	db.AddQuery(dataFreeQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|data_free", "varchar|uint64"),
		"t_a|600",
		"t_b|800",
		"t_c|0",
	))
	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.updateUserTableFreeSpaceLocked(ctx, conn.Conn, true)
	se.mu.Unlock()
	conn.Recycle()

	// Every user table — including ones with zero DATA_FREE — gets a label
	// so operators can compute free-space ratios in their monitoring system.
	assert.Equal(t, int64(600), se.tableDataFreeBytes.Counts()["t_a"])
	assert.Equal(t, int64(800), se.tableDataFreeBytes.Counts()["t_b"])
	assert.Equal(t, int64(0), se.tableDataFreeBytes.Counts()["t_c"])
	assert.True(t, se.freeSpaceMetricsActive)
}

// TestUpdateUserTableFreeSpaceIgnoresMaxTableCount verifies that the
// visibility query is not coupled to the DDL guard for creating tables.
func TestUpdateUserTableFreeSpaceIgnoresMaxTableCount(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.MakePrimary(true)
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	originalMaxTableCount := MaxTableCount()
	SetMaxTableCount(1)
	t.Cleanup(func() { SetMaxTableCount(originalMaxTableCount) })

	dataFreeQuery := "select table_name, data_free from information_schema.TABLES where table_schema = database() and table_type = 'BASE TABLE'"
	db.AddQuery(dataFreeQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|data_free", "varchar|uint64"),
		"t_a|600",
		"t_b|800",
	))

	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.updateUserTableFreeSpaceLocked(ctx, conn.Conn, true)
	se.mu.Unlock()
	conn.Recycle()

	assert.Equal(t, int64(600), se.tableDataFreeBytes.Counts()["t_a"])
	assert.Equal(t, int64(800), se.tableDataFreeBytes.Counts()["t_b"])
}

// TestMaybeOptimizeGtidExecutedSmallDataFreeSkips verifies that when
// mysql.gtid_executed's DATA_FREE is under the 128 MiB threshold, we
// neither spawn the OPTIMIZE goroutine nor stamp the last-run time.
func TestMaybeOptimizeGtidExecutedSmallDataFreeSkips(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(
		"select data_free from information_schema.TABLES where table_schema = 'mysql' and table_name = 'gtid_executed'",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("data_free", "uint64"), "1024"),
	)
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.mu.Lock()
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.mu.Unlock()
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.maybeOptimizeGtidExecutedLocked(ctx, conn.Conn)
	se.mu.Unlock()
	conn.Recycle()

	// DATA_FREE is well under threshold so no spawn: last-run is still zero
	// and no OPTIMIZE was dispatched.
	se.mu.Lock()
	assert.True(t, se.gtidExecutedOptimizeLastAt.IsZero())
	se.mu.Unlock()
	assert.Equal(t, 0, db.GetQueryCalledNum("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed"))
}

// TestMaybeOptimizeGtidExecutedLargeDataFreeSpawns verifies the full happy
// path: when DATA_FREE is over threshold and all gates pass, we stamp the
// last-run time and spawn the goroutine (which we then wait for).
func TestMaybeOptimizeGtidExecutedLargeDataFreeSpawns(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	// DATA_FREE > 128 MiB threshold.
	db.AddQuery(
		"select data_free from information_schema.TABLES where table_schema = 'mysql' and table_name = 'gtid_executed'",
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("data_free", "uint64"), "209715200"),
	)
	// Queries the goroutine will issue.
	db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("super_read_only", "int64"), "1"))
	db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
	db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
	db.AddQuery("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed", &sqltypes.Result{})

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.mu.Lock()
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.mu.Unlock()
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.maybeOptimizeGtidExecutedLocked(ctx, conn.Conn)
	se.mu.Unlock()
	conn.Recycle()

	// Wait for the goroutine to finish OPTIMIZing.
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum("OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed") == 1
	}, 30*time.Second, 10*time.Millisecond)

	// Stamp must be set (goroutine didn't reset on failure).
	se.mu.Lock()
	defer se.mu.Unlock()
	assert.False(t, se.gtidExecutedOptimizeLastAt.IsZero())
}

// TestMaybeOptimizeGtidExecutedIneligibleTabletSkips verifies that a
// PRIMARY tablet short-circuits without even running the DATA_FREE query.
func TestMaybeOptimizeGtidExecutedIneligibleTabletSkips(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	// If the DATA_FREE query is issued, it errors out so the failure is
	// loud.
	db.AddRejectedQuery(
		"select data_free from information_schema.TABLES where table_schema = 'mysql' and table_name = 'gtid_executed'",
		sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "must not be queried on PRIMARY"),
	)
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.MakePrimary(true)
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	// Must not panic or trigger the rejected query.
	se.mu.Lock()
	se.maybeOptimizeGtidExecutedLocked(ctx, conn.Conn)
	se.mu.Unlock()
	conn.Recycle()
}

// TestExecuteFetchCtxKillsOnDeadline verifies the context-aware
// ExecuteFetch wrapper: when ctx expires before the query returns, it
// issues KILL <conn_id> from a separate connection to abort the in-flight
// query. Without this, the 60s OPTIMIZE timeout would not actually be
// enforceable (ExecuteFetch itself is not context-aware).
//
// fakesqldb does not model "KILL X aborts a query running on conn X", so
// this test asserts the observable contract: after ctx expiration, a KILL
// is issued and executeFetchCtx unwinds (rather than hanging).
func TestExecuteFetchCtxKillsOnDeadline(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	const blockingQuery = "OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed"
	db.AddQuery(blockingQuery, &sqltypes.Result{})
	release := make(chan struct{})
	db.SetBeforeFunc(blockingQuery, func() {
		<-release
	})

	var killSeen atomic.Int32
	db.AddQueryPatternWithCallback(`(?i)^kill [0-9]+$`, &sqltypes.Result{}, func(q string) {
		killSeen.Add(1)
	})

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)

	conn, err := dbconnpool.NewDBConnection(t.Context(), dbconfigs.New(db.ConnParams()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = se.executeFetchCtx(ctx, conn, blockingQuery, 1, false)
		close(done)
	}()

	assert.Eventually(t, func() bool { return killSeen.Load() >= 1 },
		5*time.Second, 10*time.Millisecond, "expected KILL to be issued after ctx deadline")

	// Unblock the fakesqldb BeforeFunc so the underlying ExecuteFetch can
	// unwind (fakesqldb does not honour the KILL semantically).
	close(release)
	assert.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "executeFetchCtx must unwind after KILL+release")
}

// TestExecuteFetchCtxFastFailOnExpiredCtx verifies the fast-fail path:
// when ctx is already done at entry, executeFetchCtx returns immediately
// without issuing the query.
func TestExecuteFetchCtxFastFailOnExpiredCtx(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddRejectedQuery("sentinel", sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "must not run"))

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	conn, err := dbconnpool.NewDBConnection(t.Context(), dbconfigs.New(db.ConnParams()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // already done

	_, err = se.executeFetchCtx(ctx, conn, "sentinel", 1, false)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, db.GetQueryCalledNum("sentinel"),
		"executeFetchCtx must not issue query when ctx is already done")
}

// TestMakeNonPrimaryResetsSequenceInfo covers the sequence-reset branch of
// MakeNonPrimary so coverage reflects the full function body.
func TestMakeNonPrimaryResetsSequenceInfo(t *testing.T) {
	se := &Engine{
		tables: map[string]*Table{
			"seq_tbl": {SequenceInfo: &SequenceInfo{NextVal: 5, LastVal: 10}},
			"reg_tbl": {},
		},
	}
	se.MakeNonPrimary()
	assert.Equal(t, int64(0), se.tables["seq_tbl"].SequenceInfo.NextVal,
		"sequence info must be reset when leaving primaryship")
	assert.Equal(t, int64(0), se.tables["seq_tbl"].SequenceInfo.LastVal)
}

// TestMaybeOptimizeGtidExecutedDataFreeQueryError covers the DATA_FREE
// read-error branch.
func TestMaybeOptimizeGtidExecutedDataFreeQueryError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddRejectedQuery(
		"select data_free from information_schema.TABLES where table_schema = 'mysql' and table_name = 'gtid_executed'",
		sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "simulated"),
	)
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.mu.Lock()
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.mu.Unlock()
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.maybeOptimizeGtidExecutedLocked(ctx, conn.Conn)
	se.mu.Unlock()
	conn.Recycle()

	// On error we do not stamp last-run and we do not spawn.
	se.mu.Lock()
	defer se.mu.Unlock()
	assert.True(t, se.gtidExecutedOptimizeLastAt.IsZero())
}

// TestMaybeOptimizeGtidExecutedEmptyResult covers the "table not present"
// branch (e.g. before mysql.gtid_executed has been created).
func TestMaybeOptimizeGtidExecutedEmptyResult(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery(
		"select data_free from information_schema.TABLES where table_schema = 'mysql' and table_name = 'gtid_executed'",
		&sqltypes.Result{Fields: sqltypes.MakeTestFields("data_free", "uint64")},
	)
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.mu.Lock()
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.mu.Unlock()
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.maybeOptimizeGtidExecutedLocked(ctx, conn.Conn)
	se.mu.Unlock()
	conn.Recycle()

	se.mu.Lock()
	defer se.mu.Unlock()
	assert.True(t, se.gtidExecutedOptimizeLastAt.IsZero())
}

// TestDisableSuperReadOnlyReadError covers the branch where the SELECT
// fails with a non-ERUnknownSystemVariable error: we must propagate the
// error so the caller can skip OPTIMIZE.
func TestDisableSuperReadOnlyReadError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddRejectedQuery("SELECT @@global.super_read_only",
		sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "simulated read failure"))
	conn := newTestDBConnection(t, db)
	defer conn.Close()

	se := &Engine{}
	restore, err := se.disableSuperReadOnly(t.Context(), conn)
	require.Error(t, err)
	require.ErrorContains(t, err, "reading @@global.super_read_only")
	// The returned restore is still a no-op and must not panic.
	require.NotNil(t, restore)
	restore()
}

// TestDisableSuperReadOnlyEmptyResult covers the branch where the SELECT
// succeeds but returns no rows. We treat this as a no-op.
func TestDisableSuperReadOnlyEmptyResult(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT @@global.super_read_only",
		&sqltypes.Result{Fields: sqltypes.MakeTestFields("super_read_only", "int64")},
	)
	conn := newTestDBConnection(t, db)
	defer conn.Close()

	se := &Engine{}
	restore, err := se.disableSuperReadOnly(t.Context(), conn)
	require.NoError(t, err)
	require.NotNil(t, restore)
	restore()
	assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"))
	assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
}

// TestDisableSuperReadOnlyFlipError covers the branch where SELECT says
// super_read_only is on, but flipping it OFF fails.
func TestDisableSuperReadOnlyFlipError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("super_read_only", "int64"), "1"))
	db.AddRejectedQuery("SET GLOBAL super_read_only = 'OFF'",
		sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "simulated flip failure"))
	conn := newTestDBConnection(t, db)
	defer conn.Close()

	se := &Engine{}
	restore, err := se.disableSuperReadOnly(t.Context(), conn)
	require.Error(t, err)
	require.ErrorContains(t, err, "disabling super_read_only")
	require.NotNil(t, restore)
	restore() // no-op because we never actually flipped
	assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
}

// newTestDBConnection opens a direct DB connection against the provided
// fakesqldb for use with the SRO-toggle helper.
func newTestDBConnection(t *testing.T, db *fakesqldb.DB) *dbconnpool.DBConnection {
	t.Helper()
	params := db.ConnParams()
	cp := *params
	connector := dbconfigs.New(&cp)
	conn, err := dbconnpool.NewDBConnection(t.Context(), connector)
	require.NoError(t, err)
	return conn
}
