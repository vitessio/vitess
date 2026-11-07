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

package onlineddl

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	logTestUUID      = "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	logTestOtherUUID = "2cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
)

func newLogTestExecutor(execQuery func(ctx context.Context, query string) (*sqltypes.Result, error)) *Executor {
	e := &Executor{
		vreplicationLastError:     map[string]*vterrors.LastError{},
		vreplicationPendingCancel: map[string]string{},
		vreplicationProgress:      map[string]vreplStreamProgress{},
		tabletAlias:               &topodatapb.TabletAlias{Cell: "cell", Uid: 1},
		ticks:                     timer.NewTimer(time.Hour),
		execQuery:                 execQuery,
	}
	e.isOpen.Store(1)
	return e
}

// newFakeDBEnv returns a tablet environment whose database is a fake that
// fails any query it hasn't been given.
func newFakeDBEnv(t *testing.T) (*fakesqldb.DB, tabletenv.Env) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	params := db.ConnParams()
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbconfigs.NewTestDBConfigs(*params, *params, params.DbName)
	return db, tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "ExecutorTest")
}

// assertNoRecordMentions checks that no log message contains s.
func assertNoRecordMentions(t *testing.T, logs *logtest.Logs, s string) {
	t.Helper()
	for _, record := range logs.Records(t) {
		assert.NotContains(t, record["msg"], s)
	}
}

// TestMonitorStaleMigrationsLogsEachStaleMigration checks that every stale
// migration is logged with its own identifiers and staleness.
func TestMonitorStaleMigrationsLogsEachStaleMigration(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	e := newLogTestExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
		switch {
		case strings.Contains(query, "liveness_timestamp <"):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|stale_minutes", "varchar|int64"),
				logTestUUID+"|15", logTestOtherUUID+"|25"), nil
		case strings.Contains(query, logTestUUID):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|mysql_table|liveness_timestamp", "varchar|varchar|varchar"),
				logTestUUID+"|t1|2026-10-28 10:00:00"), nil
		case strings.Contains(query, logTestOtherUUID):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|mysql_table|liveness_timestamp", "varchar|varchar|varchar"),
				logTestOtherUUID+"|t2|2026-10-28 09:50:00"), nil
		}
		return &sqltypes.Result{}, nil
	})

	require.NoError(t, e.monitorStaleMigrations(t.Context()))

	var got []map[string]any
	for _, record := range logs.Records(t) {
		if record["msg"] == "monitorStaleMigrations: stale migration found running but indicates no liveness" {
			got = append(got, record)
		}
	}
	require.Len(t, got, 2)
	for i, want := range []struct {
		uuid, table, liveness string
		staleMinutes          float64
	}{
		{logTestUUID, "t1", "2026-10-28 10:00:00", 15},
		{logTestOtherUUID, "t2", "2026-10-28 09:50:00", 25},
	} {
		assert.Equal(t, "WARN", got[i]["level"])
		assert.Equal(t, want.uuid, got[i]["uuid"])
		assert.Equal(t, want.table, got[i]["table"])
		assert.Equal(t, want.staleMinutes, got[i]["stale_minutes"])
		assert.Equal(t, want.liveness, got[i]["liveness_timestamp"])
		assert.Equal(t, "cell-0000000001", got[i]["tablet"])
	}
}

// TestScheduleNextMigrationLogsError checks that scheduling a migration logs
// an error only when updating its status fails.
func TestScheduleNextMigrationLogsError(t *testing.T) {
	for _, tcase := range []struct {
		name      string
		updateErr error
	}{
		{name: "success"},
		{name: "failure", updateErr: vterrors.New(vtrpcpb.Code_UNAVAILABLE, "backend unavailable")},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			logs := logtest.Capture(t, slog.LevelInfo)
			e := newLogTestExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
				if query == sqlSelectQueuedMigrations {
					return sqltypes.MakeTestResult(
						sqltypes.MakeTestFields("migration_uuid|postpone_launch|postpone_completion|ready_to_complete|is_immediate_operation", "varchar|int64|int64|int64|int64"),
						logTestUUID+"|0|0|1|0"), nil
				}
				return &sqltypes.Result{RowsAffected: 1}, tcase.updateErr
			})

			err := e.scheduleNextMigration(t.Context())
			assert.Equal(t, tcase.updateErr, err)

			record := logs.Record(t, "Executor.scheduleNextMigration: scheduling migration")
			assert.Equal(t, "INFO", record["level"])
			assert.Equal(t, logTestUUID, record["uuid"])
			if tcase.updateErr == nil {
				assert.NotContains(t, record, "error")
				assert.NotContains(t, record, "error_code")
			} else {
				assert.Contains(t, record["error"], "backend unavailable")
				assert.Equal(t, "UNAVAILABLE", record["error_code"])
			}
		})
	}
}

// TestSubmitMigrationLogsStatementAsQuery checks that a submitted statement is
// logged, truncated, as the query attribute rather than in the message.
func TestSubmitMigrationLogsStatementAsQuery(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	venv := vtenv.NewTestEnv()
	stmt, err := venv.Parser().Parse("alter table zq7submittable add column zq7column int not null default 0")
	require.NoError(t, err)
	e := newLogTestExecutor(nil)
	e.env = tabletenv.NewEnv(venv, tabletenv.NewDefaultConfig(), "ExecutorTest")

	// The statement carries no Online DDL comments, so submitting it fails
	// once it has been logged.
	_, err = e.SubmitMigration(t.Context(), stmt)
	require.Error(t, err)

	record := logs.Record(t, "SubmitMigration: request to submit migration with statement")
	assert.Equal(t, "ALTER TABLE `zq7submittable` ADD COLUMN `zq7column...", record["query"])
	assertNoRecordMentions(t, logs, "zq7")
}

// TestOnMigrationCheckTickNamesFailedSteps checks that each failing step of
// the scheduler tick logs its own name, with the error as attributes.
func TestOnMigrationCheckTickNamesFailedSteps(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	e := newLogTestExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
		return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "backend unavailable")
	})
	_, e.env = newFakeDBEnv(t)
	e.keyspace = "ks"
	e.tabletTypeFunc = func() topodatapb.TabletType { return topodatapb.TabletType_PRIMARY }
	e.lagThrottler = throttle.NewThrottler(e.env, nil, nil, e.tabletAlias, nil, e.tabletTypeFunc, "TestPool")
	e.reviewedRunningMigrationsFlag = true

	e.onMigrationCheckTick()

	var steps []string
	for _, record := range logs.Records(t) {
		msg, _ := record["msg"].(string)
		step, ok := strings.CutPrefix(msg, "Executor.onMigrationCheckTick(): ")
		if !ok {
			continue
		}
		steps = append(steps, step)
		assert.Equal(t, "ERROR", record["level"], step)
		assert.Contains(t, record["error"], "backend unavailable", step)
		assert.Equal(t, "UNAVAILABLE", record["error_code"], step)
	}
	// cancelMigrations only runs after a successful review.
	assert.Equal(t, []string{
		"retryTabletFailureMigrations failed",
		"reviewQueuedMigrations failed",
		"scheduleNextMigration failed",
		"reviewInOrderMigrations failed",
		"runNextMigration failed",
		"reviewRunningMigrations failed",
		"monitorStaleMigrations failed",
		"reviewStaleMigrations failed",
		"gcArtifacts failed",
	}, steps)
}

// TestKillTableLockHoldersLogsQueries checks that a query found in the
// processlist is logged, capped at 100 characters, under query, and that one
// that can't be parsed is logged with the parse error.
func TestKillTableLockHoldersLogsQueries(t *testing.T) {
	db, env := newFakeDBEnv(t)
	e := newLogTestExecutor(nil)
	e.env = env

	long := "select * from zq7table where zq7column = '" + strings.Repeat("x", 120) + "'"
	processlist, err := sqlparser.ParseAndBind(sqlFindProcessByInfo, sqltypes.StringBindVariable("%zq7table%"))
	require.NoError(t, err)
	db.AddQuery(processlist, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|info", "int64|varchar"),
		"7|"+long,
		"8|zq7table is not sql",
	))
	db.AddQuery("KILL QUERY 7", &sqltypes.Result{})
	logs := logtest.Capture(t, slog.LevelInfo)

	// The fake database doesn't answer the lock queries that follow, so the
	// call fails after the processlist lines are logged.
	_ = e.killTableLockHoldersAndAccessors(t.Context(), logTestUUID, "zq7table")

	record := logs.Record(t, "killTableLockHoldersAndAccessors: killing query")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, logTestUUID, record["uuid"])
	assert.Equal(t, float64(7), record["connection_id"])
	assert.Equal(t, long[:100], record["query"])
	assert.NotContains(t, logs.String(), long[100:])

	record = logs.Record(t, "unable to parse processlist Info query")
	assert.Equal(t, "zq7table is not sql", record["query"])
	assert.Contains(t, record["error"], "syntax error")
}
