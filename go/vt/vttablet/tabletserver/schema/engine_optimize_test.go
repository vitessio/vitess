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

	t.Run("primary promotion in progress skips", func(t *testing.T) {
		se := &Engine{tabletTypeLastChangedAt: now.Add(-10 * time.Minute)}
		se.BeginPrimaryPromotion()
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

// TestGatingThroughMakePrimaryAndMakeNonPrimaryAPI pins the contract that
// the tabletTypeStabilityCooldown gating is driven by the public
// MakePrimary/MakeNonPrimary API — specifically that each transition
// stamps tabletTypeLastChangedAt so the eligibility check correctly
// blocks OPTIMIZE during the cooldown window. A regression in
// MakePrimary or MakeNonPrimary that forgot to stamp the timestamp
// would let OPTIMIZE fire during a fresh tablet-type transition, which
// is exactly what the cooldown is designed to prevent.
//
// Each subtest captures the stamp before the API call, advances the
// engine's perceived "before" clock by backdating to a known offset,
// invokes the API, and asserts the stamp advanced past the backdate.
// This catches a regression that drops the stamping entirely (which a
// simple WithinDuration check would miss in a fast-running test).
func TestGatingThroughMakePrimaryAndMakeNonPrimaryAPI(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)

	readStamp := func() time.Time {
		se.mu.Lock()
		defer se.mu.Unlock()
		return se.tabletTypeLastChangedAt
	}
	backdateStamp := func(to time.Time) {
		se.mu.Lock()
		defer se.mu.Unlock()
		se.tabletTypeLastChangedAt = to
	}
	checkEligibility := func() bool {
		se.mu.Lock()
		defer se.mu.Unlock()
		return se.shouldAttemptGtidExecutedOptimizeLocked(time.Now())
	}

	t.Run("MakePrimary stamps timestamp and blocks OPTIMIZE", func(t *testing.T) {
		// Backdate to a known sentinel so we can prove the stamp advanced.
		sentinel := time.Now().Add(-7 * 24 * time.Hour)
		backdateStamp(sentinel)
		se.MakePrimary(true)
		assert.True(t, se.isPrimaryTablet.Load(), "MakePrimary must set isPrimaryTablet")
		assert.True(t, readStamp().After(sentinel),
			"MakePrimary must advance tabletTypeLastChangedAt past the sentinel — dropping the stamp would leave it at sentinel")
		assert.False(t, checkEligibility(), "PRIMARY tablet must not be eligible for OPTIMIZE")
	})

	t.Run("MakeNonPrimary stamps timestamp on a real transition", func(t *testing.T) {
		// Already PRIMARY from previous subtest. Backdate, then transition.
		sentinel := time.Now().Add(-7 * 24 * time.Hour)
		backdateStamp(sentinel)
		se.MakeNonPrimary()
		assert.False(t, se.isPrimaryTablet.Load(), "MakeNonPrimary must clear isPrimaryTablet")
		assert.True(t, readStamp().After(sentinel),
			"MakeNonPrimary on a real PRIMARY→non-PRIMARY transition must advance tabletTypeLastChangedAt")
		assert.False(t, checkEligibility(),
			"non-primary tablet within stability cooldown must not be eligible")
	})

	t.Run("simulated cooldown elapse makes the same engine eligible", func(t *testing.T) {
		// Backdate the stamp past the cooldown without touching the API,
		// to prove the eligibility check actually reads what the API
		// stamped. If the API stamped a different field, this backdate
		// would not affect eligibility.
		backdateStamp(time.Now().Add(-2 * tabletTypeStabilityCooldown))
		assert.True(t, checkEligibility(),
			"past cooldown the same engine must become eligible — confirms the API-stamped field is what gating reads")
	})

	t.Run("MakeNonPrimary on an already-non-primary engine does NOT re-stamp", func(t *testing.T) {
		// This pins the deliberate "only on real transitions" behavior at
		// engine.go MakeNonPrimary line ~434. A future regression that
		// stamps unconditionally would reset the cooldown on every reload
		// tick for an idle replica, which the cooldown is designed to
		// avoid.
		backdated := time.Now().Add(-2 * tabletTypeStabilityCooldown)
		backdateStamp(backdated)
		require.False(t, se.isPrimaryTablet.Load(), "test precondition: already non-primary")
		se.MakeNonPrimary()
		assert.Equal(t, backdated.UnixNano(), readStamp().UnixNano(),
			"MakeNonPrimary on an already-non-primary engine must not re-stamp")
	})
}

// TestWaitForPrimaryPromotion verifies that restore waits for the promotion
// outcome separately from the shorter super_read_only statement timeout.
func TestWaitForPrimaryPromotion(t *testing.T) {
	t.Run("waits for aborted promotion", func(t *testing.T) {
		se := &Engine{}
		se.BeginPrimaryPromotion()

		done := make(chan bool, 1)
		go func() {
			done <- se.waitForPrimaryPromotion(t.Context(), 200*time.Millisecond) == nil
		}()

		require.Never(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, 50*time.Millisecond, 10*time.Millisecond)

		se.AbortPrimaryPromotion()

		require.Eventually(t, func() bool {
			select {
			case ok := <-done:
				return ok
			default:
				return false
			}
		}, 30*time.Second, 10*time.Millisecond)
		assert.False(t, se.isPrimaryTablet.Load())
	})

	t.Run("bounds unresolved promotion", func(t *testing.T) {
		se := &Engine{}
		se.BeginPrimaryPromotion()

		require.ErrorIs(t, se.waitForPrimaryPromotion(t.Context(), 20*time.Millisecond), context.DeadlineExceeded)
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
		db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("read_only", "int64"),
			"1",
		))
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
		db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("read_only", "int64"),
			"1",
		))
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
		//
		// TODO: this is a race-window simulation (50ms sleep vs 200ms conn
		// delay). On a heavily-loaded CI runner the 50ms sleep can fire
		// after the 200ms delay completes, missing the window. Replacing
		// with a deterministic synchronization primitive (e.g., a barrier
		// signalled from a fakesqldb conn-open hook) would eliminate the
		// flake risk — left as a follow-up because it requires a fakesqldb
		// extension that doesn't exist today.
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

	t.Run("restore waits for in-progress promotion to abort", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("read_only", "int64"),
			"1",
		))
		db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)

		se.BeginPrimaryPromotion()

		done := make(chan struct{})
		go func() {
			defer close(done)
			se.restoreSuperReadOnlyWithContext(t.Context())
		}()

		require.Never(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, 100*time.Millisecond, 10*time.Millisecond,
			"restore must wait for the promotion outcome before deciding whether to leave super_read_only off")
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))

		se.AbortPrimaryPromotion()

		require.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, 30*time.Second, 10*time.Millisecond)
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
			"aborted promotion leaves the tablet non-primary, so restore must re-enable super_read_only")
	})

	t.Run("restore waits for in-progress promotion to succeed", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddRejectedQuery("SET GLOBAL super_read_only = 'ON'",
			sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "restore must not re-enable super_read_only after promotion succeeds"))
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)

		se.BeginPrimaryPromotion()

		done := make(chan struct{})
		go func() {
			defer close(done)
			se.restoreSuperReadOnlyWithContext(t.Context())
		}()

		require.Never(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, 100*time.Millisecond, 10*time.Millisecond,
			"restore must wait for the promotion outcome before deciding whether to restore")

		se.MakePrimary(true)

		require.Eventually(t, func() bool {
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, 30*time.Second, 10*time.Millisecond)
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
			"successful promotion must keep super_read_only off")
	})

	t.Run("restore skips when MySQL is already read-write", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("read_only", "int64"),
			"0",
		))
		db.AddRejectedQuery("SET GLOBAL super_read_only = 'ON'",
			sqlerror.NewSQLError(sqlerror.ERUnknownError, "", "restore must not re-enable super_read_only on read-write MySQL"))
		se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)

		se.restoreSuperReadOnlyWithContext(t.Context())
		assert.Equal(t, 0, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"),
			"restore must skip SET ... ON when MySQL is already read-write")
	})

	t.Run("restore disables super_read_only again if promotion races with restore", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery(selectSuperReadOnlyQuery, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("super_read_only", "int64"),
			"1",
		))
		db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
		db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("read_only", "int64"),
			"1",
		))
		onResult := db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})
		db.AddQuery("SET GLOBAL read_only = 'OFF'", &sqltypes.Result{})
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
			"rescue must clear super_read_only first; MySQL refuses SET read_only = 'OFF' while super_read_only is ON")
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL read_only = 'OFF'"),
			"restore must make the promoted primary fully writable if the promotion races with SET ... ON")
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
		db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("read_only", "int64"),
			"1",
		))
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
		}, 30*time.Second, 10*time.Millisecond, "SET GLOBAL super_read_only = 'OFF' should start")
		assert.Eventually(t, func() bool { return killSeen.Load() >= 1 },
			30*time.Second, 10*time.Millisecond, "disable timeout should interrupt the in-flight SET GLOBAL super_read_only = 'OFF'")

		releaseDisableOnce.Do(func() {
			close(releaseDisable)
		})
		res := <-done
		require.Error(t, res.err)
		assert.ErrorContains(t, res.err, context.DeadlineExceeded.Error())
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
		db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("read_only", "int64"),
			"1",
		))
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
	db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("read_only", "int64"),
		"1",
	))
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
		30*time.Second, 10*time.Millisecond, "restore must issue KILL when SET GLOBAL super_read_only = 'ON' times out")
	close(release)
	assert.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "restore must unwind after timing out the SET GLOBAL super_read_only = 'ON'")
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
	db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("read_only", "int64"),
		"1",
	))
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
	db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("read_only", "int64"),
		"1",
	))
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

// TestRunOptimizeGtidExecutedHonorsOverallTimeout pins the contract that
// runOptimizeGtidExecuted actually enforces gtidExecutedOptimizeTimeout on
// the OPTIMIZE call. We shrink the timeout via the test-overridable var
// (restored via t.Cleanup), block fakesqldb's OPTIMIZE longer than the
// override, then assert:
//   - the function returns within a bounded wall-clock time (the override),
//   - SRO restore (SET GLOBAL super_read_only = 'ON') still ran so the
//     tablet is not left writeable, and
//   - the failure-path stamp gtidExecutedOptimizeLastAt is set so we don't
//     retry on every reload.
//
// A regression that swaps backgroundTimeoutContext for context.Background
// or otherwise drops the timeout wrap would let the OPTIMIZE block
// indefinitely; the t.Fatal in the wait below would then fire.
func TestRunOptimizeGtidExecutedHonorsOverallTimeout(t *testing.T) {
	const overrideTimeout = 200 * time.Millisecond
	original := gtidExecutedOptimizeTimeout
	gtidExecutedOptimizeTimeout = overrideTimeout
	t.Cleanup(func() { gtidExecutedOptimizeTimeout = original })

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("super_read_only", "int64"), "1"))
	db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("read_only", "int64"), "1"))
	db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})

	const optimizeQuery = "OPTIMIZE NO_WRITE_TO_BINLOG TABLE mysql.gtid_executed"
	db.AddQuery(optimizeQuery, &sqltypes.Result{})
	release := make(chan struct{})
	t.Cleanup(func() {
		// Best-effort: unblock fakesqldb if the test fails mid-flight so
		// we don't leak a stuck goroutine into the rest of the suite.
		select {
		case <-release:
		default:
			close(release)
		}
	})
	db.SetBeforeFunc(optimizeQuery, func() {
		<-release
	})
	var killSeen atomic.Int32
	db.AddQueryPatternWithCallback(`(?i)^kill [0-9]+$`, &sqltypes.Result{}, func(q string) {
		killSeen.Add(1)
	})

	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.mu.Lock()
	se.tabletTypeLastChangedAt = time.Now().Add(-1 * time.Hour)
	se.mu.Unlock()

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		se.runOptimizeGtidExecuted(200 * 1024 * 1024)
	}()

	// Wait for KILL — proof that executeFetchCtx entered the cleanup
	// path because the override timeout expired. Bound this generously
	// (CI-resilient) but well above the override.
	assert.Eventually(t, func() bool { return killSeen.Load() >= 1 },
		30*time.Second, 10*time.Millisecond,
		"KILL must fire after the override timeout expires — proves runOptimizeGtidExecuted honours the var")

	// Unblock fakesqldb so the underlying ExecuteFetch can unwind.
	close(release)

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runOptimizeGtidExecuted did not return within bounded time — timeout enforcement regressed")
	}

	elapsed := time.Since(start)
	// Sanity: the override + post-close grace (5s) + a little slack.
	// If we observe wall-clock approaching the original 60s constant, the
	// override was ignored (variable shadowing or a refactor that captured
	// the constant by value at init).
	assert.Less(t, elapsed, 30*time.Second,
		"runOptimizeGtidExecuted exceeded bounded wall-clock; the timeout override was not honoured")

	assert.GreaterOrEqual(t, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"), 1,
		"SRO must be restored even when OPTIMIZE times out")

	se.mu.Lock()
	defer se.mu.Unlock()
	assert.False(t, se.gtidExecutedOptimizeLastAt.IsZero(),
		"on OPTIMIZE failure (including timeout) the last-run stamp must be set so we don't retry every reload")
}

func TestCloseCancelsInFlightOptimize(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("super_read_only", "int64"), "1"))
	db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("read_only", "int64"),
		"1",
	))
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
	// set labels on both gauges.
	se.tableDataFreeBytes.Set("t_old", 12345)
	se.tableAllocatedBytes.Set("t_old", 67890)
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

	// Stale labels should be cleared via ResetAll on both gauges;
	// freeSpaceMetricsActive should be flipped back to false.
	assert.Equal(t, int64(0), se.tableDataFreeBytes.Counts()["t_old"],
		"DATA_FREE gauge label for a table we previously surfaced must be reset when the feature is disabled")
	assert.Equal(t, int64(0), se.tableAllocatedBytes.Counts()["t_old"],
		"ALLOCATED gauge label for a table we previously surfaced must be reset when the feature is disabled")
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

	dataFreeQuery := "select table_name, ifnull(data_free, 0), ifnull(data_length, 0) + ifnull(index_length, 0) + ifnull(data_free, 0) from information_schema.TABLES where table_schema = database() and table_type = 'BASE TABLE'"

	db.AddQuery(dataFreeQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|data_free|allocated", "varchar|uint64|uint64"),
		"t_a|600|2000",
		"t_b|800|1600",
		"t_c|0|4096",
	))
	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.updateUserTableFreeSpaceLocked(ctx, conn.Conn, true)
	se.mu.Unlock()
	conn.Recycle()

	// Every user table — including ones with zero DATA_FREE — gets labels
	// on both gauges so operators can compute free-space ratios as
	// data_free / allocated in their monitoring system.
	dataFree := se.tableDataFreeBytes.Counts()
	allocated := se.tableAllocatedBytes.Counts()
	assert.Equal(t, int64(600), dataFree["t_a"])
	assert.Equal(t, int64(800), dataFree["t_b"])
	assert.Equal(t, int64(0), dataFree["t_c"])
	assert.Equal(t, int64(2000), allocated["t_a"])
	assert.Equal(t, int64(1600), allocated["t_b"])
	assert.Equal(t, int64(4096), allocated["t_c"])
	assert.True(t, se.freeSpaceMetricsActive)
}

// TestUpdateUserTableFreeSpaceResetsMissingBaseTableLabels verifies that a
// previously-published table label is cleared when the table no longer appears
// in the base-table DATA_FREE query, e.g. after being replaced by a view with
// the same name.
func TestUpdateUserTableFreeSpaceResetsMissingBaseTableLabels(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	se := newEngine(10*time.Second, 10*time.Second, 0, db, nil)
	se.conns.Open(se.cp, se.cp, se.cp)
	defer se.conns.Close()

	se.tableDataFreeBytes.Set("became_view", 12345)
	se.tableAllocatedBytes.Set("became_view", 67890)
	se.freeSpaceMetricsActive = true

	dataFreeQuery := "select table_name, ifnull(data_free, 0), ifnull(data_length, 0) + ifnull(index_length, 0) + ifnull(data_free, 0) from information_schema.TABLES where table_schema = database() and table_type = 'BASE TABLE'"
	db.AddQuery(dataFreeQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|data_free|allocated", "varchar|uint64|uint64"),
		"still_table|600|2000",
	))
	ctx := t.Context()
	conn, err := se.conns.Get(ctx, nil)
	require.NoError(t, err)
	se.mu.Lock()
	se.updateUserTableFreeSpaceLocked(ctx, conn.Conn, true)
	se.mu.Unlock()
	conn.Recycle()

	dataFree := se.tableDataFreeBytes.Counts()
	allocated := se.tableAllocatedBytes.Counts()
	assert.Equal(t, int64(0), dataFree["became_view"],
		"stale DATA_FREE label must be reset when the name is no longer a base table")
	assert.Equal(t, int64(0), allocated["became_view"],
		"stale ALLOCATED label must be reset when the name is no longer a base table")
	assert.Equal(t, int64(600), dataFree["still_table"])
	assert.Equal(t, int64(2000), allocated["still_table"])
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

	dataFreeQuery := "select table_name, ifnull(data_free, 0), ifnull(data_length, 0) + ifnull(index_length, 0) + ifnull(data_free, 0) from information_schema.TABLES where table_schema = database() and table_type = 'BASE TABLE'"
	db.AddQuery(dataFreeQuery, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("table_name|data_free|allocated", "varchar|uint64|uint64"),
		"t_a|600|2000",
		"t_b|800|1600",
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
	assert.Equal(t, int64(2000), se.tableAllocatedBytes.Counts()["t_a"])
	assert.Equal(t, int64(1600), se.tableAllocatedBytes.Counts()["t_b"])
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
	db.AddQuery("SELECT @@global.read_only", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("read_only", "int64"),
		"1",
	))
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
		30*time.Second, 10*time.Millisecond, "expected KILL to be issued after ctx deadline")

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
	}, 30*time.Second, 10*time.Millisecond, "executeFetchCtx must unwind after KILL+release")
}

// TestExecuteFetchCtxPreservesUnderlyingExecErrOnTimeout pins that when ctx
// expires and the in-flight ExecuteFetch subsequently returns with a non-nil
// error (e.g. "EOF" from the conn being closed by executeFetchCtx itself, or
// any server-side reason that arrives during the post-close grace window),
// the returned error preserves the underlying execErr message AND remains
// ctx.DeadlineExceeded-classifiable via errors.Is. Without this, operators
// lose the post-mortem detail of what MySQL actually reported when the
// query was interrupted.
func TestExecuteFetchCtxPreservesUnderlyingExecErrOnTimeout(t *testing.T) {
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

	type execResult struct {
		err error
	}
	resultCh := make(chan execResult, 1)
	go func() {
		_, err := se.executeFetchCtx(ctx, conn, blockingQuery, 1, false)
		resultCh <- execResult{err: err}
	}()

	// Wait until executeFetchCtx has demonstrably entered the cleanup path
	// by observing the issued KILL — that proves the ctx.Done branch (not
	// the lucky <-done branch) was taken and we're now in the post-close
	// grace select. Only then unblock fakesqldb so the underlying
	// ExecuteFetch can unwind with whatever error it sees from the closed
	// connection. This avoids the race where unblocking too early lets
	// the original goroutine return successfully and the outer select
	// pseudo-randomly picks <-done first.
	assert.Eventually(t, func() bool { return killSeen.Load() >= 1 },
		30*time.Second, 10*time.Millisecond, "KILL must fire before we unblock the underlying query")
	close(release)

	var got execResult
	select {
	case got = <-resultCh:
	case <-time.After(30 * time.Second):
		t.Fatal("executeFetchCtx did not unwind after ctx expiry + release")
	}

	require.Error(t, got.err)
	require.ErrorIs(t, got.err, context.DeadlineExceeded,
		"returned error must remain classifiable as ctx deadline exceeded")
	// The underlying execErr message must survive in the error chain for
	// post-mortem debuggability. The actual underlying error text depends
	// on how fakesqldb / the MySQL conn driver reports the close, but it
	// MUST NOT equal just ctx.Err()'s text — that would mean we dropped
	// the underlying detail.
	assert.NotEqual(t, context.DeadlineExceeded.Error(), got.err.Error(),
		"returned error %q must wrap the underlying execErr, not just return ctx.Err() bare", got.err.Error())
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
