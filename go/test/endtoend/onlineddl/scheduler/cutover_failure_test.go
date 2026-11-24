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

package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/schema"
)

// TestCutOverFailureReleasesLocksPromptly reproduces a cut-over that fails
// after the RENAME has been issued and is blocked behind the cut-over's own
// LOCK TABLES, and measures how long the original table stays locked after
// the cut-over has given up.
//
// Failure injection: once the cut-over has passed "post-sentry pos reached",
// an external transaction takes a row lock on this migration's row in
// _vt.vreplication. The vplayer can then no longer persist its position, so
// the post-lock waitForPos times out with "failed waiting for pos after
// locking" while the RENAME is still in flight.
func TestCutOverFailureReleasesLocksPromptly(t *testing.T) {
	shards = clusterInstance.Keyspaces[0].Shards
	require.Len(t, shards, 1)
	throttler.EnableLagThrottlerAndWaitForStatus(t, clusterInstance)

	// This test uses its own table so that it does not interfere with the
	// tables used by the other tests in this package.
	const (
		ddlStrategy = "vitess"
		tableName   = "t_cutover_test"
		createTable = `CREATE TABLE t_cutover_test (id bigint(20) not null, hint_col varchar(64) not null default 'just-created', PRIMARY KEY (id)) ENGINE=InnoDB`
		alterTable  = `ALTER TABLE t_cutover_test ENGINE=InnoDB`
		dropTable   = `DROP TABLE IF EXISTS t_cutover_test`
	)

	t.Run("create table", func(t *testing.T) {
		uuid := testOnlineDDLStatement(t, &testOnlineDDLStatementParams{ddlStatement: createTable, ddlStrategy: ddlStrategy, executeStrategy: "vtgate"})
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, uuid, schema.OnlineDDLStatusComplete)
		_, err := onlineddl.VtgateExecQuery(t.Context(), &vtParams, "insert ignore into t_cutover_test values (1, 'new_row')")
		require.NoError(t, err)
	})
	// Teardown is registered before the ALTER is submitted, so the table is
	// dropped even if the submission itself fails. It runs as a defer rather
	// than t.Cleanup so that t.Context() is still live for the helpers.
	var uuid string
	defer func() {
		if uuid != "" {
			// If the test failed before the ALTER completed, cancel it so that the
			// DROP below does not queue behind it. On a completed migration this is
			// a no-op.
			_, _ = onlineddl.VtgateExecQuery(t.Context(), &vtParams, fmt.Sprintf("alter vitess_migration '%s' cancel", uuid))
		}
		dropUUID := testOnlineDDLStatement(t, &testOnlineDDLStatementParams{ddlStatement: dropTable, ddlStrategy: ddlStrategy, executeStrategy: "vtgate"})
		onlineddl.CheckMigrationStatus(t, &vtParams, shards, dropUUID, schema.OnlineDDLStatusComplete)
	}()
	uuid = testOnlineDDLStatement(t, &testOnlineDDLStatementParams{ddlStatement: alterTable, ddlStrategy: ddlStrategy + " --postpone-completion", executeStrategy: "vtgate", skipWait: true})
	onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, normalWaitTime, schema.OnlineDDLStatusRunning)
	waitForReadyToComplete(t, uuid, true)

	newConn := func() *mysql.Conn {
		conn, err := primaryTablet.VttabletProcess.TabletConn(keyspaceName, true)
		require.NoError(t, err)
		return conn
	}
	stageConn := newConn()
	defer stageConn.Close()
	stallConn := newConn()
	defer stallConn.Close()
	probeConn := newConn()
	defer probeConn.Close()
	barrierConn := newConn()
	defer barrierConn.Close()

	readStage := func() (string, error) {
		rs, err := stageConn.ExecuteFetch(fmt.Sprintf("select stage from _vt.schema_migrations where migration_uuid='%s'", uuid), 1, false)
		if err != nil {
			return "", err
		}
		if len(rs.Rows) != 1 {
			return "", fmt.Errorf("expected 1 row for migration %s, got %d", uuid, len(rs.Rows))
		}
		return rs.Rows[0][0].ToString(), nil
	}
	// pollStage waits until the migration stage satisfies pred, and returns the
	// matching stage and the local time it was first observed. It uses assert
	// rather than require so that it is safe to call from a goroutine; callers
	// on the test goroutine check ok.
	pollStage := func(pred func(string) bool, timeout time.Duration) (stage string, seen time.Time, ok bool) {
		ok = assert.Eventually(t, func() bool {
			s, err := readStage()
			if err != nil || !pred(s) {
				return false
			}
			stage, seen = s, time.Now()
			return true
		}, timeout, 5*time.Millisecond, "waiting for migration stage")
		return stage, seen, ok
	}
	hasAnyPrefix := func(prefixes ...string) func(string) bool {
		return func(stage string) bool {
			for _, p := range prefixes {
				if strings.HasPrefix(stage, p) {
					return true
				}
			}
			return false
		}
	}

	// Barrier: a READ lock on the table from an external connection makes the
	// cut-over's own LOCK TABLES ... WRITE block at the "locking tables" stage.
	// It is released only once the vreplication row lock below is held, so the
	// post-lock position is always read after vplayer can no longer persist
	// its position. Without the barrier the stall could race with vplayer.
	_, err := barrierConn.ExecuteFetch(fmt.Sprintf("lock tables %s read", tableName), 0, false)
	require.NoError(t, err)

	// Stall vplayer position updates as soon as the cut-over is past the sentry
	// wait, so that the post-lock waitForPos cannot be satisfied.
	type stallResult struct {
		stage string
		err   error
	}
	stalled := make(chan stallResult, 1)
	go func() {
		stage, _, ok := pollStage(hasAnyPrefix(
			"post-sentry pos reached",
			"buffering queries",
			"graceful wait for buffering",
			"locking tables",
			"renaming tables",
			"waiting for RENAME to block",
			"RENAME found",
		), extendedWaitTime)
		var err error
		if !ok {
			err = errors.New("timed out waiting for the cut-over to pass the sentry wait")
		}
		if err == nil {
			_, err = stallConn.ExecuteFetch("begin", 0, false)
		}
		if err == nil {
			_, err = stallConn.ExecuteFetch(fmt.Sprintf("select id from _vt.vreplication where workflow='%s' for update", uuid), 1, false)
		}
		if err == nil {
			// Row lock held: let the cut-over proceed to LOCK TABLES.
			_, err = barrierConn.ExecuteFetch("unlock tables", 0, false)
		}
		stalled <- stallResult{stage: stage, err: err}
	}()

	onlineddl.CheckCompleteMigration(t, &vtParams, shards, uuid, true)
	var stall stallResult
	select {
	case stall = <-stalled:
	case <-time.After(extendedWaitTime):
		require.Fail(t, "timed out stalling vplayer position updates")
	}
	require.NoError(t, stall.err)
	t.Logf("vplayer position updates stalled at stage %q", stall.stage)

	// LOCK TABLES is held and the RENAME is in flight from "RENAME found" on.
	// Probe the original table from a third connection: this read blocks until
	// the cut-over's LOCK TABLES is released.
	lockedStage, _, ok := pollStage(hasAnyPrefix("RENAME found", "reading post-lock pos", "waiting for post-lock pos"), extendedWaitTime)
	require.True(t, ok)
	t.Logf("tables locked, stage %q", lockedStage)
	type probeResult struct {
		at  time.Time
		err error
	}
	probeStart := time.Now()
	probeReleased := make(chan probeResult, 1)
	go func() {
		_, err := probeConn.ExecuteFetch(fmt.Sprintf("select id from %s limit 1", tableName), 1, false)
		probeReleased <- probeResult{at: time.Now(), err: err}
	}()

	// The cut-over gives up here; this stage is written before the function
	// returns and its deferred cleanup runs.
	_, failedAt, ok := pollStage(hasAnyPrefix("timeout while waiting for post-lock pos"), extendedWaitTime)
	require.True(t, ok)
	var probe probeResult
	select {
	case probe = <-probeReleased:
	case <-time.After(extendedWaitTime):
		require.Fail(t, "probe is still blocked: the table was not unlocked after the cut-over gave up")
	}
	require.NoError(t, probe.err)
	heldAfterFailure := probe.at.Sub(failedAt)
	t.Logf("probe blocked for %v in total; original table stayed locked for %v after the cut-over gave up", probe.at.Sub(probeStart), heldAfterFailure)

	// Let vreplication catch up; the scheduler retries the cut-over on its own.
	_, err = stallConn.ExecuteFetch("rollback", 0, false)
	require.NoError(t, err)
	status := onlineddl.WaitForMigrationStatus(t, &vtParams, shards, uuid, extendedWaitTime, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
	require.Equal(t, schema.OnlineDDLStatusComplete, status)

	// The probe must still have been blocked when the cut-over gave up (it was
	// issued about one CutOverThreshold earlier); the negative slack only covers
	// the stage-polling granularity. Otherwise the test would not have exercised
	// the condition in its name.
	require.Greater(t, heldAfterFailure, -time.Second, "probe was released before the cut-over gave up; the table lock was not held")
	assert.Less(t, heldAfterFailure, time.Second, "original table stayed locked for %v after the cut-over gave up", heldAfterFailure)
}
