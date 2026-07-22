/*
Copyright 2021 The Vitess Authors.

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

/*
Functionality of this Executor is tested in go/test/endtoend/onlineddl/...
*/

package onlineddl

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestShouldCutOverAccordingToBackoff(t *testing.T) {
	tcases := []struct {
		name string

		shouldForceCutOverIndicator bool
		forceCutOverAfter           time.Duration
		sinceReadyToComplete        time.Duration
		sinceLastCutoverAttempt     time.Duration
		cutoverAttempts             int64

		expectShouldCutOver      bool
		expectShouldForceCutOver bool
	}{
		{
			name:                "no reason why not, normal cutover",
			expectShouldCutOver: true,
		},
		{
			name:                "backoff",
			cutoverAttempts:     1,
			expectShouldCutOver: false,
		},
		{
			name:                "more backoff",
			cutoverAttempts:     3,
			expectShouldCutOver: false,
		},
		{
			name:                    "more backoff, since last cutover",
			cutoverAttempts:         3,
			sinceLastCutoverAttempt: time.Second,
			expectShouldCutOver:     false,
		},
		{
			name:                    "no backoff, long since last cutover",
			cutoverAttempts:         3,
			sinceLastCutoverAttempt: time.Hour,
			expectShouldCutOver:     true,
		},
		{
			name:                    "many attempts, long since last cutover",
			cutoverAttempts:         3000,
			sinceLastCutoverAttempt: time.Hour,
			expectShouldCutOver:     true,
		},
		{
			name:                        "force cutover",
			shouldForceCutOverIndicator: true,
			expectShouldCutOver:         true,
			expectShouldForceCutOver:    true,
		},
		{
			name:                        "force cutover overrides backoff",
			cutoverAttempts:             3,
			shouldForceCutOverIndicator: true,
			expectShouldCutOver:         true,
			expectShouldForceCutOver:    true,
		},
		{
			name:                     "backoff; cutover-after not in effect yet",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "backoff; cutover-after still not in effect yet",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     time.Millisecond,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "zero since ready",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     0,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "zero since read, zero cut-over-after",
			cutoverAttempts:          3,
			forceCutOverAfter:        0,
			sinceReadyToComplete:     0,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "microsecond",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Microsecond,
			sinceReadyToComplete:     time.Millisecond,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
		{
			name:                     "2 milliseconds, not ready",
			cutoverAttempts:          3,
			forceCutOverAfter:        2 * time.Millisecond,
			sinceReadyToComplete:     time.Millisecond,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "microsecond, ready irrespective of sinceReadyToComplete",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Millisecond,
			sinceReadyToComplete:     time.Microsecond,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
		{
			name:                     "cutover-after overrides backoff",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     time.Second * 2,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
		{
			name:                     "cutover-after overrides backoff, realistic value",
			cutoverAttempts:          300,
			sinceLastCutoverAttempt:  time.Minute,
			forceCutOverAfter:        time.Hour,
			sinceReadyToComplete:     time.Hour * 2,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			shouldCutOver, shouldForceCutOver := shouldCutOverAccordingToBackoff(
				tcase.shouldForceCutOverIndicator,
				tcase.forceCutOverAfter,
				tcase.sinceReadyToComplete,
				tcase.sinceLastCutoverAttempt,
				tcase.cutoverAttempts,
			)
			assert.Equal(t, tcase.expectShouldCutOver, shouldCutOver)
			assert.Equal(t, tcase.expectShouldForceCutOver, shouldForceCutOver)
		})
	}
}

func TestSafeMigrationCutOverThreshold(t *testing.T) {
	require.NotZero(t, defaultCutOverThreshold)
	require.GreaterOrEqual(t, defaultCutOverThreshold, minCutOverThreshold)
	require.LessOrEqual(t, defaultCutOverThreshold, maxCutOverThreshold)

	tcases := []struct {
		threshold time.Duration
		expect    time.Duration
		isErr     bool
	}{
		{
			threshold: 0,
			expect:    defaultCutOverThreshold,
		},
		{
			threshold: 2 * time.Second,
			expect:    defaultCutOverThreshold,
			isErr:     true,
		},
		{
			threshold: 75 * time.Second,
			expect:    defaultCutOverThreshold,
			isErr:     true,
		},
		{
			threshold: defaultCutOverThreshold,
			expect:    defaultCutOverThreshold,
		},
		{
			threshold: 5 * time.Second,
			expect:    5 * time.Second,
		},
		{
			threshold: 15 * time.Second,
			expect:    15 * time.Second,
		},
		{
			threshold: 25 * time.Second,
			expect:    25 * time.Second,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.threshold.String(), func(t *testing.T) {
			threshold, err := safeMigrationCutOverThreshold(tcase.threshold)
			if tcase.isErr {
				require.Error(t, err)
				require.Equal(t, defaultCutOverThreshold, tcase.expect)
				// And keep testing, because we then also expect the threshold to be the default
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tcase.expect, threshold)
		})
	}
}

func TestGetInOrderCompletionPendingCount(t *testing.T) {
	const ctx = "ctx-same"
	onlineDDL := &schema.OnlineDDL{UUID: t.Name(), MigrationContext: ctx}
	pm := func(uuid, migrationContext string) pendingMigration {
		return pendingMigration{uuid: uuid, migrationContext: migrationContext}
	}
	{
		require.Zero(t, getInOrderCompletionPendingCount(onlineDDL, nil))
	}
	{
		require.Zero(t, getInOrderCompletionPendingCount(onlineDDL, []pendingMigration{}))
	}
	{
		pendingMigrations := []pendingMigration{pm(t.Name(), ctx)}
		require.Zero(t, getInOrderCompletionPendingCount(onlineDDL, pendingMigrations))
	}
	{
		pendingMigrations := []pendingMigration{pm("a", ctx), pm("b", ctx), pm("c", ctx), pm(t.Name(), ctx), pm("x", ctx)}
		require.Equal(t, uint64(3), getInOrderCompletionPendingCount(onlineDDL, pendingMigrations))
	}
	{
		// migrations from a different context do not count
		pendingMigrations := []pendingMigration{pm("a", "ctx-other"), pm("b", ctx), pm(t.Name(), ctx), pm("x", ctx)}
		require.Equal(t, uint64(1), getInOrderCompletionPendingCount(onlineDDL, pendingMigrations))
	}
}

func TestInitDBConnectionLockWaitTimeout(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	connector := dbconfigs.NewTestDBConfigs(*params, *params, params.DbName).DbaWithDB()
	conn, err := dbconnpool.NewDBConnection(t.Context(), connector)
	require.NoError(t, err)
	defer conn.Close()

	db.AddQuery("set @lock_wait_timeout=@@session.lock_wait_timeout", &sqltypes.Result{})
	db.AddQuery("set @@session.lock_wait_timeout=5", &sqltypes.Result{})
	db.AddQuery("set @@session.lock_wait_timeout=@lock_wait_timeout", &sqltypes.Result{})

	executor := &Executor{}
	deferFunc, err := executor.initDBConnectionLockWaitTimeout(conn, 5*time.Second)
	require.NoError(t, err)
	queryLog := db.QueryLog()
	assert.Contains(t, queryLog, "set @lock_wait_timeout=@@session.lock_wait_timeout")
	assert.Contains(t, queryLog, "set @@session.lock_wait_timeout=5")

	deferFunc()
	assert.Contains(t, db.QueryLog(), "set @@session.lock_wait_timeout=@lock_wait_timeout")
}

// TestInitMigrationSessionVariables verifies requested values are applied in order and prior values are restored in reverse order.
func TestInitMigrationSessionVariables(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	connector := dbconfigs.NewTestDBConfigs(*params, *params, params.DbName).DbaWithDB()
	conn, err := dbconnpool.NewDBConnection(t.Context(), connector)
	require.NoError(t, err)
	defer conn.Close()

	db.AddQuery("set @vt_onlineddl_session_variable_0=@@session.innodb_strict_mode", &sqltypes.Result{})
	db.AddQuery("set @@session.innodb_strict_mode=X'6f6666'", &sqltypes.Result{})
	db.AddQuery("set @vt_onlineddl_session_variable_1=@@session.sql_mode", &sqltypes.Result{})
	db.AddQuery("set @@session.sql_mode=X'414e5349'", &sqltypes.Result{})
	db.AddQuery("set @@session.sql_mode=@vt_onlineddl_session_variable_1", &sqltypes.Result{})
	db.AddQuery("set @@session.innodb_strict_mode=@vt_onlineddl_session_variable_0", &sqltypes.Result{})

	executor := &Executor{}
	onlineDDL := &schema.OnlineDDL{
		Strategy: schema.DDLStrategyOnline,
		Options:  "--session-variable innodb_strict_mode=off --session-variable sql_mode=ANSI",
	}
	deferFunc, err := executor.initMigrationSessionVariables(t.Context(), onlineDDL, conn)
	require.NoError(t, err)
	queryLog := db.QueryLog()
	assert.Contains(t, queryLog, "set @@session.innodb_strict_mode=x'6f6666'")
	assert.Contains(t, queryLog, "set @@session.sql_mode=x'414e5349'")

	deferFunc()
	queryLog = db.QueryLog()
	assert.Contains(t, queryLog, "set @@session.sql_mode=@vt_onlineddl_session_variable_1")
	assert.Contains(t, queryLog, "set @@session.innodb_strict_mode=@vt_onlineddl_session_variable_0")
}

// TestMigrationSessionVariablesAreSetBeforeDDL verifies migration DDL observes the requested session state.
func TestMigrationSessionVariablesAreSetBeforeDDL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	connector := dbconfigs.NewTestDBConfigs(*params, *params, params.DbName).DbaWithDB()
	conn, err := dbconnpool.NewDBConnection(t.Context(), connector)
	require.NoError(t, err)
	defer conn.Close()

	db.AddQuery("set @vt_onlineddl_session_variable_0=@@session.innodb_strict_mode", &sqltypes.Result{})
	db.AddQuery("set @@session.innodb_strict_mode=X'6f6666'", &sqltypes.Result{})
	db.AddQuery("set @@session.innodb_strict_mode=@vt_onlineddl_session_variable_0", &sqltypes.Result{})
	db.AddQuery("create table _vrepl_shadow (id int primary key)", &sqltypes.Result{})

	executor := &Executor{}
	onlineDDL := &schema.OnlineDDL{
		Strategy: schema.DDLStrategyOnline,
		Options:  "--session-variable innodb_strict_mode=off",
	}
	restoreSessionVariablesFunc, err := executor.initMigrationSessionVariables(t.Context(), onlineDDL, conn)
	require.NoError(t, err)
	defer restoreSessionVariablesFunc()

	_, err = conn.ExecuteFetch("create table _vrepl_shadow (id int primary key)", 0, false)
	require.NoError(t, err)

	got := strings.Split(db.QueryLog(), ";")
	sessionVariableIdx := -1
	createIdx := -1
	for i, q := range got {
		q = strings.TrimSpace(strings.ToLower(q))
		if strings.Contains(q, "innodb_strict_mode=x'6f6666'") {
			sessionVariableIdx = i
		}
		if strings.Contains(q, "create table _vrepl_shadow") {
			createIdx = i
		}
	}
	require.NotEqual(t, -1, sessionVariableIdx)
	require.NotEqual(t, -1, createIdx)
	assert.Less(t, sessionVariableIdx, createIdx, "session variables must be set before shadow CREATE/ALTER DDL")
}

// TestMigrationSessionVariableFailurePreventsDDL verifies a failed session assignment aborts before migration DDL executes.
func TestMigrationSessionVariableFailurePreventsDDL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	connector := dbconfigs.NewTestDBConfigs(*params, *params, params.DbName).DbaWithDB()
	conn, err := dbconnpool.NewDBConnection(t.Context(), connector)
	require.NoError(t, err)
	defer conn.Close()

	db.AddQuery("set @vt_onlineddl_session_variable_0=@@session.sql_mode", &sqltypes.Result{})
	db.AddRejectedQuery("set @@session.sql_mode=X'414e5349'", errors.New("cannot set session variable"))
	db.AddQuery("set @@session.sql_mode=@vt_onlineddl_session_variable_0", &sqltypes.Result{})
	db.AddQuery("create table _vrepl_shadow (id int primary key)", &sqltypes.Result{})

	executor := &Executor{}
	onlineDDL := &schema.OnlineDDL{
		Strategy: schema.DDLStrategyOnline,
		Options:  "--session-variable sql_mode=ANSI",
	}
	restoreSessionVariablesFunc, err := executor.initMigrationSessionVariables(t.Context(), onlineDDL, conn)
	defer restoreSessionVariablesFunc()
	require.ErrorContains(t, err, "cannot set session variable")
	assert.NotContains(t, db.QueryLog(), "create table _vrepl_shadow")
}

// TestAlterViewSessionVariableFailurePreventsDDL verifies online view DDL initializes session state on its dedicated connection.
func TestAlterViewSessionVariableFailurePreventsDDL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbconfigs.NewTestDBConfigs(*params, *params, params.DbName)

	db.AddQuery("set @vt_onlineddl_session_variable_0=@@session.sql_mode", &sqltypes.Result{})
	db.AddRejectedQuery("set @@session.sql_mode=X'414e5349'", errors.New("cannot set session variable"))
	db.AddQuery("set @@session.sql_mode=@vt_onlineddl_session_variable_0", &sqltypes.Result{})

	executor := &Executor{
		env: tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "ExecutorTest"),
	}
	onlineDDL := &schema.OnlineDDL{
		SQL:      "alter view test_view as select 1",
		Strategy: schema.DDLStrategyOnline,
		Options:  "--session-variable sql_mode=ANSI",
	}
	err := executor.executeAlterViewOnline(t.Context(), onlineDDL)
	require.ErrorContains(t, err, "cannot set session variable")
	assert.NotContains(t, strings.ToLower(db.QueryLog()), "create or replace view")
}

func TestExecuteDirectlySetsLockWaitTimeout(t *testing.T) {
	ctx := t.Context()
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	connector := dbconfigs.NewTestDBConfigs(*params, *params, params.DbName).DbaWithDB()
	conn, err := dbconnpool.NewDBConnection(ctx, connector)
	require.NoError(t, err)
	defer conn.Close()

	db.AddQuery("set @lock_wait_timeout=@@session.lock_wait_timeout", &sqltypes.Result{})
	db.AddQuery("set @@session.lock_wait_timeout=5", &sqltypes.Result{})
	db.AddQuery("set @@session.lock_wait_timeout=@lock_wait_timeout", &sqltypes.Result{})
	db.AddQuery("create table test_lock_wait(id int)", &sqltypes.Result{})

	venv := vtenv.NewTestEnv()
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbconfigs.NewTestDBConfigs(*params, *params, params.DbName)
	protocolName := t.Name()
	resetProtocol := tmclienttest.SetProtocol(t.Name(), protocolName)
	defer resetProtocol()
	tmclient.RegisterTabletManagerClientFactory(protocolName, func() tmclient.TabletManagerClient {
		return &fakeTabletManagerClient{}
	})
	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	ts := memorytopo.NewServer(ctx, "cell")
	err = ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias:    alias,
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	})
	require.NoError(t, err)
	executor := &Executor{
		env:         tabletenv.NewEnv(venv, cfg, "ExecutorTest"),
		ts:          ts,
		tabletAlias: alias,
		execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
			return &sqltypes.Result{}, nil
		},
		ticks: timer.NewTimer(migrationCheckInterval),
	}

	onlineDDL := &schema.OnlineDDL{SQL: "create table test_lock_wait(id int)", CutOverThreshold: 5 * time.Second, UUID: "uuid"}
	_, err = executor.executeDirectly(ctx, onlineDDL)
	require.NoError(t, err)

	queryLog := db.QueryLog()
	assert.Contains(t, queryLog, "set @lock_wait_timeout=@@session.lock_wait_timeout")
	assert.Contains(t, queryLog, "set @@session.lock_wait_timeout=5")
	assert.Contains(t, queryLog, "set @@session.lock_wait_timeout=@lock_wait_timeout")
}

type fakeTabletManagerClient struct {
	tmclient.TabletManagerClient
}

func (fakeTabletManagerClient) Close() {}

func (fakeTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	return nil
}

func TestMigrationMetricsIncrement(t *testing.T) {
	tcases := []struct {
		name     string
		testFunc func()
		verify   func(before int64, after int64) bool
	}{
		{
			name: "startedMigrations increments correctly",
			testFunc: func() {
				startedMigrations.Add(1)
			},
			verify: func(before int64, after int64) bool {
				return after == before+1
			},
		},
		{
			name: "successfulMigrations increments correctly",
			testFunc: func() {
				successfulMigrations.Add(1)
			},
			verify: func(before int64, after int64) bool {
				return after == before+1
			},
		},
		{
			name: "failedMigrations increments correctly",
			testFunc: func() {
				failedMigrations.Add(1)
			},
			verify: func(before int64, after int64) bool {
				return after == before+1
			},
		},
	}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			var before, after int64

			switch tcase.name {
			case "startedMigrations increments correctly":
				before = startedMigrations.Get()
				tcase.testFunc()
				after = startedMigrations.Get()
			case "successfulMigrations increments correctly":
				before = successfulMigrations.Get()
				tcase.testFunc()
				after = successfulMigrations.Get()
			case "failedMigrations increments correctly":
				before = failedMigrations.Get()
				tcase.testFunc()
				after = failedMigrations.Get()
			}

			assert.True(t, tcase.verify(before, after), "metric should increment correctly: before=%d, after=%d", before, after)
		})
	}
}

func TestMigrationStatusTransitionsUpdateMetrics(t *testing.T) {
	tcases := []struct {
		name          string
		status        schema.OnlineDDLStatus
		expectStarted int64
		expectSuccess int64
		expectFailed  int64
	}{
		{
			name:          "running status updates started metric",
			status:        schema.OnlineDDLStatusRunning,
			expectStarted: 1,
		},
		{
			name:          "complete status updates successful metric",
			status:        schema.OnlineDDLStatusComplete,
			expectSuccess: 1,
		},
		{
			name:         "failed status updates failed metric",
			status:       schema.OnlineDDLStatusFailed,
			expectFailed: 1,
		},
	}

	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			startedBefore := startedMigrations.Get()
			successBefore := successfulMigrations.Get()
			failedBefore := failedMigrations.Get()

			switch tcase.status {
			case schema.OnlineDDLStatusRunning:
				startedMigrations.Add(1)
			case schema.OnlineDDLStatusComplete:
				successfulMigrations.Add(1)
			case schema.OnlineDDLStatusFailed:
				failedMigrations.Add(1)
			}

			assert.Equal(t, startedBefore+tcase.expectStarted, startedMigrations.Get(), "startedMigrations")
			assert.Equal(t, successBefore+tcase.expectSuccess, successfulMigrations.Get(), "successfulMigrations")
			assert.Equal(t, failedBefore+tcase.expectFailed, failedMigrations.Get(), "failedMigrations")
		})
	}
}
