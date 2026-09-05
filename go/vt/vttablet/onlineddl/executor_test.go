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
	"fmt"
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
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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

// TestInitMigrationSessionVariables verifies requested values are applied in
// order and prior values are restored in reverse order.
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

// TestMigrationSessionVariablesAreSetBeforeDDL verifies migration DDL observes
// the requested session state.
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

// TestMigrationSessionVariableFailurePreventsDDL verifies a failed session
// assignment aborts before migration DDL executes.
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

// TestInitMigrationSessionVariableReadFailure verifies setup stops if the
// existing value cannot be saved for restoration.
func TestInitMigrationSessionVariableReadFailure(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	connector := dbconfigs.NewTestDBConfigs(*params, *params, params.DbName).DbaWithDB()
	conn, err := dbconnpool.NewDBConnection(t.Context(), connector)
	require.NoError(t, err)
	defer conn.Close()

	db.AddRejectedQuery(
		"set @vt_onlineddl_session_variable_0=@@session.sql_mode",
		errors.New("cannot read session variable"),
	)

	executor := &Executor{}
	onlineDDL := &schema.OnlineDDL{
		Strategy: schema.DDLStrategyOnline,
		Options:  "--session-variable sql_mode=ANSI",
	}
	restoreSessionVariablesFunc, err := executor.initMigrationSessionVariables(
		t.Context(),
		onlineDDL,
		conn,
	)
	defer restoreSessionVariablesFunc()
	require.ErrorContains(t, err, "could not read session variable sql_mode")
	assert.NotContains(t, db.QueryLog(), "set @@session.sql_mode=")
}

// TestInitMigrationSessionVariablesInvalidOptions verifies malformed strategy
// options fail before the connection is used.
func TestInitMigrationSessionVariablesInvalidOptions(t *testing.T) {
	executor := &Executor{}
	onlineDDL := &schema.OnlineDDL{
		Strategy: schema.DDLStrategyOnline,
		Options:  `--session-variable "sql_mode=ANSI`,
	}
	restoreSessionVariablesFunc, err := executor.initMigrationSessionVariables(
		t.Context(),
		onlineDDL,
		nil,
	)
	defer restoreSessionVariablesFunc()
	require.Error(t, err)
}

// TestAlterViewSessionVariableFailurePreventsDDL verifies online view DDL
// initializes session state on its dedicated connection.
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

// TestAlterViewSessionVariablesAreSetBeforeDDL verifies successful setup on the
// dedicated online view connection happens before its DDL.
func TestAlterViewSessionVariablesAreSetBeforeDDL(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbconfigs.NewTestDBConfigs(*params, *params, params.DbName)

	db.AddQuery("set @vt_onlineddl_session_variable_0=@@session.sql_mode", &sqltypes.Result{})
	db.AddQuery("set @@session.sql_mode=X'414e5349'", &sqltypes.Result{})
	db.AddQuery("set @@session.sql_mode=@vt_onlineddl_session_variable_0", &sqltypes.Result{})
	db.RejectQueryPattern("create or replace .*view .*", "view DDL failed")

	executor := &Executor{
		env: tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "ExecutorTest"),
		execQuery: func(context.Context, string) (*sqltypes.Result, error) {
			return &sqltypes.Result{}, nil
		},
	}
	onlineDDL := &schema.OnlineDDL{
		SQL:      "alter view test_view as select 1",
		Strategy: schema.DDLStrategyOnline,
		Options:  "--session-variable sql_mode=ANSI",
	}
	err := executor.executeAlterViewOnline(t.Context(), onlineDDL)
	require.ErrorContains(t, err, "view DDL failed")

	queryLog := strings.ToLower(db.QueryLog())
	setIdx := strings.Index(queryLog, "set @@session.sql_mode=x'414e5349'")
	viewIdx := strings.Index(queryLog, "create or replace")
	require.NotEqual(t, -1, setIdx)
	require.NotEqual(t, -1, viewIdx)
	assert.Less(t, setIdx, viewIdx)
	assert.Contains(t, queryLog, "set @@session.sql_mode=@vt_onlineddl_session_variable_0")
}

// TestExecuteDirectlyAppliesEnforcedSettingsAfterSessionVariables verifies
// internal connection settings take precedence over requested session state.
func TestExecuteDirectlyAppliesEnforcedSettingsAfterSessionVariables(t *testing.T) {
	ctx := t.Context()
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	connector := dbconfigs.NewTestDBConfigs(*params, *params, params.DbName).DbaWithDB()
	conn, err := dbconnpool.NewDBConnection(ctx, connector)
	require.NoError(t, err)
	defer conn.Close()

	db.AddQuery("set @vt_onlineddl_session_variable_0=@@session.sql_mode", &sqltypes.Result{})
	db.AddQuery("set @@session.sql_mode=X'414e5349'", &sqltypes.Result{})
	db.AddQuery(
		"select @@session.sql_mode as sql_mode",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("sql_mode", "varchar"),
			"ANSI",
		),
	)
	db.AddQuery(
		"set @@session.sql_mode=REPLACE(REPLACE('ANSI', 'NO_ZERO_DATE', ''), 'NO_ZERO_IN_DATE', '')",
		&sqltypes.Result{},
	)
	db.AddQuery("set @@session.sql_mode='ANSI'", &sqltypes.Result{})
	db.AddQuery("set @@session.sql_mode=@vt_onlineddl_session_variable_0", &sqltypes.Result{})
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

	onlineDDL := &schema.OnlineDDL{
		SQL:              "create table test_lock_wait(id int)",
		Strategy:         schema.DDLStrategyOnline,
		Options:          "--session-variable sql_mode=ANSI --allow-zero-in-date",
		CutOverThreshold: 5 * time.Second,
		UUID:             "uuid",
	}
	_, err = executor.executeDirectly(ctx, onlineDDL)
	require.NoError(t, err)

	queryLog := db.QueryLog()
	sessionVariableIdx := strings.Index(queryLog, "set @@session.sql_mode=x'414e5349'")
	allowZeroInDateIdx := strings.Index(queryLog, "set @@session.sql_mode=replace")
	createIdx := strings.Index(queryLog, "create table test_lock_wait")
	require.NotEqual(t, -1, sessionVariableIdx)
	require.NotEqual(t, -1, allowZeroInDateIdx)
	require.NotEqual(t, -1, createIdx)
	assert.Less(t, sessionVariableIdx, allowZeroInDateIdx)
	assert.Less(t, allowZeroInDateIdx, createIdx)
	assert.Contains(t, queryLog, "set @lock_wait_timeout=@@session.lock_wait_timeout")
	assert.Contains(t, queryLog, "set @@session.lock_wait_timeout=5")
	assert.Contains(t, queryLog, "set @@session.lock_wait_timeout=@lock_wait_timeout")
}

type fakeTabletManagerClient struct {
	tmclient.TabletManagerClient
}

func (fakeTabletManagerClient) Close() {}

// stopFailingTabletManagerClient fails every VReplicationExec call,
// simulating a stream stop RPC failure during migration termination.
type stopFailingTabletManagerClient struct {
	tmclient.TabletManagerClient
}

func (stopFailingTabletManagerClient) Close() {}

func (stopFailingTabletManagerClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	return nil, errors.New("stop failed")
}

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

// TestOverrideStateFromHistory tests which historical terminal-error rows
// override the live stream state: unrecoverable and legacy rows always do, a
// retries-exhausted row never does, and a live Error row is authoritative.
func TestOverrideStateFromHistory(t *testing.T) {
	classBMessage := vreplication.RetriesExhaustedIndicator + ": the same error was encountered continuously for longer than --vreplication-max-time-to-retry-on-error (15m0s): connection refused"
	classAMessage := vreplication.UnrecoverableErrorIndicator + ": bad data"
	legacyMessage := vreplication.TerminalErrorIndicator + ": some error"

	testCases := []struct {
		name              string
		liveState         binlogdatapb.VReplicationWorkflowState
		historicalMessage string
		wantOverride      bool
	}{
		{
			name:              "class B, live Running -> no override",
			liveState:         binlogdatapb.VReplicationWorkflowState_Running,
			historicalMessage: classBMessage,
			wantOverride:      false,
		},
		{
			name:              "class B, live Copying -> no override",
			liveState:         binlogdatapb.VReplicationWorkflowState_Copying,
			historicalMessage: classBMessage,
			wantOverride:      false,
		},
		{
			name:              "class B, live Init -> no override",
			liveState:         binlogdatapb.VReplicationWorkflowState_Init,
			historicalMessage: classBMessage,
			wantOverride:      false,
		},
		{
			// The live row is written before the history insert, so a live
			// Error is at least as fresh as any history row.
			name:              "class B, live Error -> no override, live row is authoritative",
			liveState:         binlogdatapb.VReplicationWorkflowState_Error,
			historicalMessage: classBMessage,
			wantOverride:      false,
		},
		{
			name:              "class A, live Running -> override (stickiness preserved)",
			liveState:         binlogdatapb.VReplicationWorkflowState_Running,
			historicalMessage: classAMessage,
			wantOverride:      true,
		},
		{
			name:              "class A, live Error -> no override, live row is authoritative",
			liveState:         binlogdatapb.VReplicationWorkflowState_Error,
			historicalMessage: classAMessage,
			wantOverride:      false,
		},
		{
			// The class B marker embedded in a class A error's cause (e.g.
			// quoted user data) must not classify as class B.
			name:              "class A embedding class B marker in cause, live Running -> override",
			liveState:         binlogdatapb.VReplicationWorkflowState_Running,
			historicalMessage: vreplication.UnrecoverableErrorIndicator + ": Duplicate entry '" + vreplication.RetriesExhaustedIndicator + "' for key 'val'",
			wantOverride:      true,
		},
		{
			name:              "class B, live Stopped -> no override",
			liveState:         binlogdatapb.VReplicationWorkflowState_Stopped,
			historicalMessage: classBMessage,
			wantOverride:      false,
		},
		{
			name:              "legacy, live Running -> override (stickiness preserved)",
			liveState:         binlogdatapb.VReplicationWorkflowState_Running,
			historicalMessage: legacyMessage,
			wantOverride:      true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantOverride, overrideStateFromHistory(tc.liveState, tc.historicalMessage))
		})
	}
}

// TestForgetVReplStreamOrdering pins WHEN the tracking is cleared: only once a
// migration is actually requeued or durably terminal, never while its stream
// may still be active.
func TestForgetVReplStreamOrdering(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	newTrackedExecutor := func(execQuery func(ctx context.Context, query string) (*sqltypes.Result, error)) *Executor {
		e := &Executor{
			vreplicationLastError:     map[string]*vterrors.LastError{uuid: vterrors.NewLastError("test", time.Minute)},
			vreplicationPendingCancel: map[string]string{},
			tabletAlias:               &topodatapb.TabletAlias{Cell: "cell", Uid: 1},
			ticks:                     timer.NewTimer(time.Hour),
			execQuery:                 execQuery,
		}
		e.isOpen.Store(1)
		return e
	}

	t.Run("no-op retry retains tracking", func(t *testing.T) {
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			return &sqltypes.Result{RowsAffected: 0}, nil
		})
		_, err := e.RetryMigration(t.Context(), uuid)
		require.NoError(t, err)
		assert.Contains(t, e.vreplicationLastError, uuid,
			"a RETRY that requeued nothing must not clear an active migration's retry window")
	})
	t.Run("actual retry clears tracking", func(t *testing.T) {
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			return &sqltypes.Result{RowsAffected: 1}, nil
		})
		_, err := e.RetryMigration(t.Context(), uuid)
		require.NoError(t, err)
		assert.NotContains(t, e.vreplicationLastError, uuid)
	})
	t.Run("cancellation whose terminal transition fails retains tracking", func(t *testing.T) {
		// An internal cancellation has no durable cancelled_timestamp: the
		// deferred transition IS the cancellation, and if it fails the
		// stream is still active.
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "UPDATE") {
				return nil, errors.New("backend unavailable")
			}
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status", "varchar|varchar"),
				uuid+"|running"), nil
		})
		_, _ = e.CancelMigration(t.Context(), uuid, "internal cancel", false)
		assert.Contains(t, e.vreplicationLastError, uuid,
			"tracking must survive a cancellation whose terminal transition failed")
		assert.Equal(t, "internal cancel", e.vreplicationPendingCancel[uuid],
			"an internal cancellation whose transition failed must record a pending intent: a successful stop leaves no stream verdict to re-trigger it")
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"a still-running migration under pending cancellation must stay owned, or the scheduler's conflict checks miss it")
	})
	t.Run("successful internal cancellation clears tracking", func(t *testing.T) {
		// The verdict must come from the status update itself, not from
		// failMigration's always-non-nil propagated cause.
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "UPDATE") {
				return &sqltypes.Result{RowsAffected: 1}, nil
			}
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status", "varchar|varchar"),
				uuid+"|running"), nil
		})
		_, err := e.CancelMigration(t.Context(), uuid, "internal cancel", false)
		require.NoError(t, err)
		assert.NotContains(t, e.vreplicationLastError, uuid,
			"a successfully cancelled migration must not retain stream tracking")
		assert.NotContains(t, e.vreplicationPendingCancel, uuid)
	})
	t.Run("user cancellation with failed transition retains tracking", func(t *testing.T) {
		// cancelled_timestamp alone is not a terminal transition: the review
		// filters on migration_status, so the migration stays under review.
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if strings.Contains(query, "SET cancelled_timestamp=NOW(6)") {
				return &sqltypes.Result{RowsAffected: 1}, nil
			}
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "UPDATE") {
				return nil, errors.New("backend unavailable")
			}
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status", "varchar|varchar"),
				uuid+"|running"), nil
		})
		_, _ = e.CancelMigration(t.Context(), uuid, "user cancel", true)
		assert.Contains(t, e.vreplicationLastError, uuid,
			"tracking must survive until the terminal status transition actually lands")
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"a still-running migration under pending cancellation must stay owned")
	})
	t.Run("cancellation with expired caller context still completes the transition", func(t *testing.T) {
		// The deferred transition runs on its own bounded context, not the
		// caller's, which terminateMigration may have exhausted.
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "UPDATE") {
				return &sqltypes.Result{RowsAffected: 1}, nil
			}
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status", "varchar|varchar"),
				uuid+"|running"), nil
		})
		expiredCtx, cancel := context.WithCancel(t.Context())
		readDone := false
		// Let the initial read succeed, then expire the context.
		inner := e.execQuery
		e.execQuery = func(ctx context.Context, query string) (*sqltypes.Result, error) {
			res, err := inner(ctx, query)
			if !readDone {
				readDone = true
				cancel()
			}
			return res, err
		}
		_, _ = e.CancelMigration(expiredCtx, uuid, "internal cancel", false)
		assert.NotContains(t, e.vreplicationLastError, uuid,
			"the transition must succeed on its own bounded context and clear the tracking")
	})
	t.Run("unconfirmed termination defers the terminal transition", func(t *testing.T) {
		// terminateMigration can fail before the stream is stopped. Failing
		// the migration then would orphan a live stream: once out of
		// 'running', nothing reviews it until artifact GC.
		var terminalTransitionAttempted bool
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "UPDATE") {
				if strings.Contains(query, "migration_status") {
					terminalTransitionAttempted = true
				}
				return &sqltypes.Result{RowsAffected: 1}, nil
			}
			// A 'vitess' strategy routes termination through the stream stop
			// path, whose topology lookup fails below.
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status|strategy", "varchar|varchar|varchar"),
				uuid+"|running|vitess"), nil
		})
		// No tablet record exists, so the topology lookup fails.
		e.ts = memorytopo.NewServer(t.Context(), "cell")
		_, err := e.CancelMigration(t.Context(), uuid, "internal cancel", false)
		require.Error(t, err)
		assert.False(t, terminalTransitionAttempted,
			"the durable terminal transition must not run when termination was not confirmed")
		assert.Contains(t, e.vreplicationLastError, uuid)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"terminateMigration disowned the migration, but with an unconfirmed stop it remains live: ownership must be restored")
	})
	t.Run("failed stream stop defers the terminal transition", func(t *testing.T) {
		// With no delete following, the stop IS the termination: a swallowed
		// failure would fail the migration while its stream keeps applying
		// changes.
		var terminalTransitionAttempted bool
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "UPDATE") {
				if strings.Contains(query, "migration_status") {
					terminalTransitionAttempted = true
				}
				return &sqltypes.Result{RowsAffected: 1}, nil
			}
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status|strategy", "varchar|varchar|varchar"),
				uuid+"|running|vitess"), nil
		})
		protocolName := t.Name()
		resetProtocol := tmclienttest.SetProtocol(t.Name(), protocolName)
		defer resetProtocol()
		tmclient.RegisterTabletManagerClientFactory(protocolName, func() tmclient.TabletManagerClient {
			return &stopFailingTabletManagerClient{}
		})
		ts := memorytopo.NewServer(t.Context(), "cell")
		require.NoError(t, ts.CreateTablet(t.Context(), &topodatapb.Tablet{
			Alias:    &topodatapb.TabletAlias{Cell: "cell", Uid: 1},
			Keyspace: "ks",
			Shard:    "0",
			Type:     topodatapb.TabletType_PRIMARY,
		}))
		e.ts = ts
		_, err := e.CancelMigration(t.Context(), uuid, "internal cancel", false)
		require.ErrorContains(t, err, "stop failed")
		assert.False(t, terminalTransitionAttempted,
			"the durable terminal transition must not run when the stream stop failed")
		assert.Contains(t, e.vreplicationLastError, uuid)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"the stream may still be applying changes: ownership must be restored until the cancellation lands")
	})
	t.Run("failed cancellation retains tracking", func(t *testing.T) {
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			// The cancelled_timestamp UPDATE fails; readMigration's SELECT
			// (which also names that column) succeeds.
			if strings.Contains(query, "SET cancelled_timestamp=NOW(6)") {
				return nil, errors.New("backend unavailable")
			}
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status", "varchar|varchar"),
				uuid+"|running"), nil
		})
		_, err := e.CancelMigration(t.Context(), uuid, "test cancel", true)
		require.Error(t, err)
		assert.Contains(t, e.vreplicationLastError, uuid,
			"a cancellation that failed before any durable transition must not clear the retry window")
		// No cancellation was accepted, so no intent may remain: the
		// scheduler would otherwise fail a migration the caller was told was
		// not cancelled.
		assert.NotContains(t, e.vreplicationPendingCancel, uuid,
			"a rejected cancellation must not leave a pending intent behind")
	})
}

// TestCancelPendingMigrationsContinuesPastFailure pins that CANCEL ALL and
// CANCEL CONTEXT attempt every matching migration: one migration failing to
// cancel must not leave the rest running, or free to start, and every failure
// must be reported.
func TestCancelPendingMigrationsContinuesPastFailure(t *testing.T) {
	const (
		firstFailingUUID = "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
		queuedUUID       = "2cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
		lastFailingUUID  = "3cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	)
	var cancelled []string
	e := &Executor{
		vreplicationLastError:     map[string]*vterrors.LastError{},
		vreplicationPendingCancel: map[string]string{},
		tabletAlias:               &topodatapb.TabletAlias{Cell: "cell", Uid: 1},
		ticks:                     timer.NewTimer(time.Hour),
		execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
			isSelect := strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "SELECT")
			switch {
			case strings.Contains(query, "IN ('queued', 'ready', 'running')"):
				return sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("migration_uuid|migration_context", "varchar|varchar"),
					firstFailingUUID+"|ctx", queuedUUID+"|ctx", lastFailingUUID+"|ctx"), nil
			case isSelect && strings.Contains(query, firstFailingUUID):
				// Reading these migrations fails with coded errors, so their
				// cancellations do.
				return nil, vterrors.New(vtrpcpb.Code_UNAVAILABLE, "backend unavailable")
			case isSelect && strings.Contains(query, lastFailingUUID):
				return nil, vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "backend timed out")
			case isSelect && strings.Contains(query, queuedUUID):
				return sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("migration_uuid|migration_status", "varchar|varchar"),
					queuedUUID+"|queued"), nil
			case strings.Contains(query, "migration_status") && strings.Contains(query, queuedUUID):
				cancelled = append(cancelled, queuedUUID)
			}
			return &sqltypes.Result{RowsAffected: 1}, nil
		},
	}
	e.isOpen.Store(1)

	result, err := e.CancelPendingMigrations(t.Context(), "", true)
	require.Error(t, err)
	assert.ErrorContains(t, err, firstFailingUUID)
	assert.ErrorContains(t, err, lastFailingUUID, "every failure must be reported, not only the first")
	assert.Equal(t, vtrpcpb.Code_DEADLINE_EXCEEDED, vterrors.Code(err),
		"aggregating the failures must keep a structured error code (the highest-priority one) for the client to map to a MySQL error")
	assert.Equal(t, []string{queuedUUID}, cancelled,
		"a migration failing to cancel must not leave the later matches uncancelled and free to start")
	require.NotNil(t, result)
	assert.EqualValues(t, 1, result.RowsAffected)
}

// TestTerminallyFailMigrationMetric pins that FailedMigrations counts only
// migrations that actually reached a terminal state, not each re-driven
// attempt.
func TestTerminallyFailMigrationMetric(t *testing.T) {
	onlineDDL := &schema.OnlineDDL{UUID: "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"}
	newMetricExecutor := func(execQuery func(ctx context.Context, query string) (*sqltypes.Result, error)) *Executor {
		e := &Executor{
			vreplicationLastError:     map[string]*vterrors.LastError{},
			vreplicationPendingCancel: map[string]string{},
			tabletAlias:               &topodatapb.TabletAlias{Cell: "cell", Uid: 1},
			ticks:                     timer.NewTimer(time.Hour),
			execQuery:                 execQuery,
		}
		e.isOpen.Store(1)
		return e
	}

	t.Run("failed transition does not count", func(t *testing.T) {
		e := newMetricExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			return nil, errors.New("backend unavailable")
		})
		before := failedMigrations.Get()
		err := e.terminallyFailMigration(t.Context(), onlineDDL, nil)
		require.Error(t, err)
		assert.Equal(t, before, failedMigrations.Get(),
			"a migration still in 'running' must not count as failed")
	})
	t.Run("successful transition counts once", func(t *testing.T) {
		e := newMetricExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			return &sqltypes.Result{RowsAffected: 1}, nil
		})
		before := failedMigrations.Get()
		require.NoError(t, e.terminallyFailMigration(t.Context(), onlineDDL, nil))
		assert.Equal(t, before+1, failedMigrations.Get())
	})
}

// TestReviewRunningMigrationsNilStreamCancellation pins how the running
// migrations review handles a pending cancellation intent that has no stream
// verdict to ride on: it must emit the cancellation itself, while a migration
// without intent is left alone.
func TestReviewRunningMigrationsNilStreamCancellation(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), tabletenv.NewDefaultConfig(), "ExecutorTest")
	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	newReviewExecutor := func(migrationFields, migrationTypes, migrationRow string, streamResult *sqltypes.Result) *Executor {
		if streamResult == nil {
			// The stream is absent.
			streamResult = &sqltypes.Result{}
		}
		e := &Executor{
			env:                       env,
			tabletAlias:               alias,
			vreplicationLastError:     map[string]*vterrors.LastError{},
			vreplicationPendingCancel: map[string]string{},
			ticks:                     timer.NewTimer(time.Hour),
			lagThrottler: throttle.NewThrottler(env, nil, nil, alias, nil,
				func() topodatapb.TabletType { return topodatapb.TabletType_PRIMARY }, "TestPool"),
			execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
				q := strings.ToLower(query)
				switch {
				case strings.Contains(q, "migration_status='running'"):
					return sqltypes.MakeTestResult(
						sqltypes.MakeTestFields("migration_uuid", "varchar"), uuid), nil
				case strings.Contains(q, "in ('queued', 'ready', 'running')"):
					return &sqltypes.Result{}, nil
				case strings.Contains(q, "from _vt.vreplication_log"):
					return &sqltypes.Result{}, nil
				case strings.Contains(q, "from _vt.vreplication"):
					return streamResult, nil
				case strings.HasPrefix(strings.TrimSpace(q), "select") && strings.Contains(q, "migration_uuid="):
					return sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(migrationFields, migrationTypes), migrationRow), nil
				default:
					return &sqltypes.Result{}, nil
				}
			},
		}
		e.isOpen.Store(1)
		return e
	}

	t.Run("pending cancellation intent is re-driven", func(t *testing.T) {
		e := newReviewExecutor(
			"migration_uuid|migration_status|strategy|cancelled_timestamp",
			"varchar|varchar|varchar|varchar",
			uuid+"|running|vitess|2026-09-02 17:00:00", nil)
		_, cancellable, err := e.reviewRunningMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, cancellable, 1,
			"a nil-stream migration with recorded cancellation intent must be re-driven to its terminal state")
		assert.Equal(t, uuid, cancellable[0].uuid)
	})
	t.Run("no recorded intent leaves the migration untouched", func(t *testing.T) {
		e := newReviewExecutor(
			"migration_uuid|migration_status|strategy",
			"varchar|varchar|varchar",
			uuid+"|running|vitess", nil)
		_, cancellable, err := e.reviewRunningMigrations(t.Context())
		require.NoError(t, err)
		assert.Empty(t, cancellable,
			"a nil-stream migration without cancellation intent must not be cancelled")
	})
	t.Run("pending internal intent re-drives a stopped stream", func(t *testing.T) {
		// The successful stop erased the Error verdict and left the pre-stop
		// message behind; only the recorded intent can drive the
		// cancellation, and it must carry the recorded reason.
		stoppedStream := sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("id|workflow|source|pos|state|message", "int32|varchar|varchar|varchar|varchar|varchar"),
			"1|"+uuid+"|||Stopped|connection refused: will retry")
		e := newReviewExecutor(
			"migration_uuid|migration_status|strategy",
			"varchar|varchar|varchar",
			uuid+"|running|vitess", stoppedStream)
		e.vreplicationPendingCancel[uuid] = "internal cancel"
		_, cancellable, err := e.reviewRunningMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, cancellable, 1,
			"a stopped stream with pending internal cancellation intent must be re-driven to its terminal state")
		assert.Equal(t, uuid, cancellable[0].uuid)
		assert.Equal(t, "internal cancel", cancellable[0].message)
	})
	t.Run("a cancel verdict stops the review before cutover", func(t *testing.T) {
		// A healthy stream can carry a pending intent; continuing into the
		// cutover flow could complete the migration before cancelMigrations
		// runs.
		runningStream := sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("id|workflow|source|pos|state|message", "int32|varchar|varchar|varchar|varchar|varchar"),
			"1|"+uuid+"|||Running|")
		e := newReviewExecutor(
			"migration_uuid|migration_status|strategy",
			"varchar|varchar|varchar",
			uuid+"|running|vitess", runningStream)
		e.vreplicationPendingCancel[uuid] = "internal cancel"
		_, cancellable, err := e.reviewRunningMigrations(t.Context())
		require.NoError(t, err)
		require.Len(t, cancellable, 1)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.False(t, owned,
			"a migration under cancellation must not proceed through the review's ownership and cutover flow")
	})
	t.Run("a stopped stream without pending intent is untouched", func(t *testing.T) {
		stoppedStream := sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("id|workflow|source|pos|state|message", "int32|varchar|varchar|varchar|varchar|varchar"),
			"1|"+uuid+"|||Stopped|")
		e := newReviewExecutor(
			"migration_uuid|migration_status|strategy",
			"varchar|varchar|varchar",
			uuid+"|running|vitess", stoppedStream)
		_, cancellable, err := e.reviewRunningMigrations(t.Context())
		require.NoError(t, err)
		assert.Empty(t, cancellable,
			"a stopped stream without cancellation intent must not be cancelled")
	})
}

// TestResolveVReplStreamAction pins that a pending cancellation intent
// converts every stream verdict into a cancellation, and that verdicts pass
// through untouched without one.
func TestResolveVReplStreamAction(t *testing.T) {
	assert.Equal(t, vreplStreamCancel, resolveVReplStreamAction(vreplStreamNoAction, true),
		"a surviving clean stream yields no action, but the pending cancellation must still be re-driven")
	assert.Equal(t, vreplStreamCancel, resolveVReplStreamAction(vreplStreamCancel, true))
	assert.Equal(t, vreplStreamCancel, resolveVReplStreamAction(vreplStreamRepair, true),
		"a pending cancellation must beat the repair verdict: repairing a cancelled migration's stream would revive it")
	assert.Equal(t, vreplStreamCancel, resolveVReplStreamAction(vreplStreamCancel, false))
	assert.Equal(t, vreplStreamNoAction, resolveVReplStreamAction(vreplStreamNoAction, false))
	assert.Equal(t, vreplStreamRepair, resolveVReplStreamAction(vreplStreamRepair, false))
}

// TestForgetVReplStream pins what the helper deletes; when it may run is
// pinned by TestForgetVReplStreamOrdering.
func TestForgetVReplStream(t *testing.T) {
	e := &Executor{
		vreplicationLastError:     make(map[string]*vterrors.LastError),
		vreplicationPendingCancel: make(map[string]string),
		vreplicationProgress:      make(map[string]vreplStreamProgress),
	}
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	e.vreplicationLastError[uuid] = vterrors.NewLastError("test", time.Minute)
	e.vreplicationPendingCancel[uuid] = "internal cancel"
	e.vreplicationProgress[uuid] = vreplStreamProgress{copyStateID: 1}

	e.forgetVReplStream(uuid)
	assert.NotContains(t, e.vreplicationLastError, uuid)
	assert.NotContains(t, e.vreplicationPendingCancel, uuid)
	assert.NotContains(t, e.vreplicationProgress, uuid)
}

// TestForgetFinishedVReplStreams tests the per-tick tracking sweep: entries
// of migrations that are neither running nor pending are dropped; running and
// pending migrations (a ready one may still carry a cancellation intent) keep
// theirs.
func TestForgetFinishedVReplStreams(t *testing.T) {
	const (
		runningUUID  = "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
		readyUUID    = "2cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
		finishedUUID = "3cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	)
	e := &Executor{
		vreplicationLastError:     make(map[string]*vterrors.LastError),
		vreplicationPendingCancel: make(map[string]string),
		vreplicationProgress:      make(map[string]vreplStreamProgress),
	}
	for _, uuid := range []string{runningUUID, readyUUID, finishedUUID} {
		e.vreplicationLastError[uuid] = vterrors.NewLastError("test", time.Minute)
		e.vreplicationPendingCancel[uuid] = "cancel " + uuid
		e.vreplicationProgress[uuid] = vreplStreamProgress{copyStateID: 1}
	}
	// A finished migration may have left tracking in only some of the maps.
	const lastErrorOnlyUUID = "4cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	e.vreplicationLastError[lastErrorOnlyUUID] = vterrors.NewLastError("test", time.Minute)

	e.forgetFinishedVReplStreams(
		map[string]bool{runningUUID: true},
		map[string]bool{runningUUID: true, readyUUID: true},
	)

	for _, uuid := range []string{runningUUID, readyUUID} {
		assert.Contains(t, e.vreplicationLastError, uuid)
		assert.Contains(t, e.vreplicationPendingCancel, uuid)
		assert.Contains(t, e.vreplicationProgress, uuid)
	}
	for _, uuid := range []string{finishedUUID, lastErrorOnlyUUID} {
		assert.NotContains(t, e.vreplicationLastError, uuid)
		assert.NotContains(t, e.vreplicationPendingCancel, uuid)
		assert.NotContains(t, e.vreplicationProgress, uuid)
	}
}

// TestRefreshMigrationLiveness tests the copy-phase liveness gate and the
// refresh it guards: past the copy phase an advanced time_updated always
// refreshes liveness_timestamp; during it, only a newer _vt.copy_state
// checkpoint or active throttling does. The replication position (advanced
// by catchup before every copy attempt) and the first observation (the
// baseline) do not. The indicator and the checkpoint baseline advance only
// once the timestamp write has landed, so a failed write is retried.
func TestRefreshMigrationLiveness(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	type harness struct {
		e                  *Executor
		copyStateCount     int64
		copyStateMaxID     int64
		failCopyStateRead  bool
		failTimestampWrite bool
		// acknowledgedRowsCopied is the migration record's rows_copied: the
		// stream value the executor last durably acknowledged.
		acknowledgedRowsCopied int64
		writes                 []string
	}
	newHarness := func(copyStateCount, copyStateMaxID int64) *harness {
		h := &harness{copyStateCount: copyStateCount, copyStateMaxID: copyStateMaxID}
		h.e = &Executor{
			vreplicationProgress: make(map[string]vreplStreamProgress),
			execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
				switch {
				case strings.Contains(query, "copy_state"):
					if h.failCopyStateRead {
						return nil, errors.New("copy_state unavailable")
					}
					if h.copyStateCount == 0 {
						// max() over no rows is NULL.
						return sqltypes.MakeTestResult(
							sqltypes.MakeTestFields("cnt|maxid", "int64|uint64"), "0|null"), nil
					}
					return sqltypes.MakeTestResult(
						sqltypes.MakeTestFields("cnt|maxid", "int64|uint64"),
						fmt.Sprintf("%d|%d", h.copyStateCount, h.copyStateMaxID)), nil
				case strings.Contains(query, "liveness_timestamp") && h.failTimestampWrite:
					return nil, errors.New("transient")
				}
				h.writes = append(h.writes, query)
				return &sqltypes.Result{RowsAffected: 1}, nil
			},
		}
		return h
	}
	wrote := func(h *harness, column string) bool {
		for _, q := range h.writes {
			if strings.Contains(q, column) {
				return true
			}
		}
		return false
	}
	// tick runs one liveness refresh and reports whether liveness_timestamp
	// was refreshed.
	tick := func(h *harness, s *VReplStream) bool {
		h.writes = nil
		h.e.refreshMigrationLiveness(t.Context(), uuid, s, h.acknowledgedRowsCopied)
		return wrote(h, "liveness_timestamp")
	}
	stream := func(pos string, timeThrottled int64) *VReplStream {
		return &VReplStream{
			id:            1,
			pos:           pos,
			timeThrottled: timeThrottled,
		}
	}

	t.Run("past the copy phase, time_updated is trusted", func(t *testing.T) {
		h := newHarness(0, 0)
		assert.True(t, tick(h, stream("pos1", 0)))
		assert.True(t, wrote(h, "vitess_liveness_indicator"))
		assert.True(t, tick(h, stream("pos1", 0)))
	})
	t.Run("copy-state lookup failure is not liveness", func(t *testing.T) {
		// Granting liveness on an unobserved state would hand a stuck copy a
		// fresh budget on every transient read failure; leaving the indicator
		// un-advanced merely defers the decision to the next tick.
		h := newHarness(1, 10)
		h.failCopyStateRead = true
		assert.False(t, tick(h, stream("pos1", 0)))
		assert.False(t, wrote(h, "vitess_liveness_indicator"))
		assert.NotContains(t, h.e.vreplicationProgress, uuid, "a failed lookup must not establish a baseline")
	})
	t.Run("copy phase, first observation is baseline only", func(t *testing.T) {
		h := newHarness(1, 10)
		assert.False(t, tick(h, stream("pos1", 0)),
			"the first observation must not refresh liveness: an executor restart would otherwise grant a stuck copy a fresh budget")
		assert.Equal(t, vreplStreamProgress{copyStateID: 10}, h.e.vreplicationProgress[uuid])
	})
	t.Run("copy phase, no progress is not liveness", func(t *testing.T) {
		h := newHarness(1, 10)
		require.False(t, tick(h, stream("pos1", 0)))
		assert.False(t, tick(h, stream("pos1", 0)))
	})
	t.Run("copy phase, a newer copy_state checkpoint is liveness", func(t *testing.T) {
		h := newHarness(1, 10)
		require.False(t, tick(h, stream("pos1", 0)))
		h.copyStateMaxID = 11
		assert.True(t, tick(h, stream("pos1", 0)))
		assert.True(t, wrote(h, "vitess_liveness_indicator"))
		// Progress must be relative to the last observation, not the first.
		assert.False(t, tick(h, stream("pos1", 0)))
	})
	t.Run("copy phase, position-only advancement is not liveness", func(t *testing.T) {
		h := newHarness(1, 10)
		require.False(t, tick(h, stream("pos1", 0)))
		assert.False(t, tick(h, stream("pos2", 0)),
			"catchup advances the position before every copy attempt, so it cannot prove the copy is progressing")
	})
	t.Run("copy phase, rows_copied past the acknowledged record is liveness", func(t *testing.T) {
		// The migration record holds the rows_copied the executor last
		// acknowledged, so a stream past it has committed copy batches
		// since; at the acknowledged value it proves nothing.
		h := newHarness(1, 10)
		h.acknowledgedRowsCopied = 100
		s := stream("pos1", 0)
		s.rowsCopied = 100
		require.False(t, tick(h, s))
		s.rowsCopied = 101
		assert.True(t, tick(h, s))
		assert.True(t, wrote(h, "vitess_liveness_indicator"))
	})
	t.Run("copy phase, rows_copied progress counts without a baseline", func(t *testing.T) {
		// Unlike the checkpoint baseline, the acknowledged rows_copied
		// survives a restart or failover, so the first tick after adoption
		// can refresh liveness on it rather than defer to the next tick —
		// the stale review runs later in that same tick.
		h := newHarness(1, 10)
		h.acknowledgedRowsCopied = 100
		s := stream("pos1", 0)
		s.rowsCopied = 101
		assert.True(t, tick(h, s), "the first observation after adoption must not defer durable progress to the next tick")
		assert.Equal(t, vreplStreamProgress{copyStateID: 10}, h.e.vreplicationProgress[uuid], "the checkpoint baseline is still established")
	})
	t.Run("copy phase, active throttling is liveness", func(t *testing.T) {
		h := newHarness(1, 10)
		require.False(t, tick(h, stream("pos1", 0)))
		assert.True(t, tick(h, stream("pos1", time.Now().Unix())))
	})
	t.Run("copy phase, stale throttle stamp is not liveness", func(t *testing.T) {
		h := newHarness(1, 10)
		require.False(t, tick(h, stream("pos1", 0)))
		staleThrottle := time.Now().Add(-2 * vreplThrottleLivenessWindow).Unix()
		assert.False(t, tick(h, stream("pos1", staleThrottle)))
	})
	t.Run("failed timestamp write is retried with the same checkpoint", func(t *testing.T) {
		// The indicator and the baseline must not advance on a failed
		// timestamp write: either would consume the checkpoint's credit and
		// leave liveness_timestamp stale until the next checkpoint.
		h := newHarness(1, 10)
		require.False(t, tick(h, stream("pos1", 0)))
		h.copyStateMaxID = 11
		h.failTimestampWrite = true
		assert.False(t, tick(h, stream("pos1", 0)))
		assert.False(t, wrote(h, "vitess_liveness_indicator"),
			"the indicator must not acknowledge a time_updated whose liveness refresh did not land")
		assert.Equal(t, vreplStreamProgress{copyStateID: 10}, h.e.vreplicationProgress[uuid],
			"the baseline must not consume the checkpoint whose refresh did not land")
		h.failTimestampWrite = false
		assert.True(t, tick(h, stream("pos1", 0)), "the same checkpoint must earn the refresh once the write succeeds")
		assert.True(t, wrote(h, "vitess_liveness_indicator"))
		assert.Equal(t, vreplStreamProgress{copyStateID: 11}, h.e.vreplicationProgress[uuid])
	})
}

// TestReviewVReplStreamError tests the per-stream verdict: unrecoverable or
// legacy Error-state streams cancel; a retries-exhausted park is repaired
// unless it has outlived the retry window; a transient error is tolerated
// until it outlives the window; a clean stream is left alone.
func TestReviewVReplStreamError(t *testing.T) {
	newExecutor := func() *Executor {
		return &Executor{
			vreplicationLastError: make(map[string]*vterrors.LastError),
		}
	}
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"

	unrecoverableStream := &VReplStream{
		state:   binlogdatapb.VReplicationWorkflowState_Error,
		message: vreplication.UnrecoverableErrorIndicator + ": bad data",
	}
	retriesExhaustedStream := &VReplStream{
		state:   binlogdatapb.VReplicationWorkflowState_Error,
		message: vreplication.RetriesExhaustedIndicator + ": the same error was encountered continuously for longer than --vreplication-max-time-to-retry-on-error (15m0s): connection refused",
	}
	transientErrorStream := &VReplStream{
		state:   binlogdatapb.VReplicationWorkflowState_Running,
		message: "error connecting to source tablet",
	}

	t.Run("unrecoverable stream is cancelled", func(t *testing.T) {
		e := newExecutor()
		assert.Equal(t, vreplStreamCancel, e.reviewVReplStreamError(uuid, unrecoverableStream))
	})
	t.Run("retries-exhausted stream is repaired", func(t *testing.T) {
		// Only a stream created before the retry-forever override existed
		// can park this way; repairing it preserves the copy progress.
		e := newExecutor()
		assert.Equal(t, vreplStreamRepair, e.reviewVReplStreamError(uuid, retriesExhaustedStream))
	})
	t.Run("retries-exhausted stream past the retry window is cancelled", func(t *testing.T) {
		// E.g. the repair itself keeps failing.
		e := newExecutor()
		lastError := vterrors.NewLastError("test", -time.Nanosecond)
		lastError.Record(errors.New(retriesExhaustedStream.message))
		e.vreplicationLastError[uuid] = lastError
		assert.Equal(t, vreplStreamCancel, e.reviewVReplStreamError(uuid, retriesExhaustedStream))
	})
	t.Run("transient error within the retry window is tolerated", func(t *testing.T) {
		e := newExecutor()
		assert.Equal(t, vreplStreamNoAction, e.reviewVReplStreamError(uuid, transientErrorStream))
	})
	t.Run("transient error past the retry window is cancelled", func(t *testing.T) {
		e := newExecutor()
		// A negative window makes ShouldRetry false once the error is
		// recorded, without sleeping.
		lastError := vterrors.NewLastError("test", -time.Nanosecond)
		lastError.Record(errors.New(transientErrorStream.message))
		e.vreplicationLastError[uuid] = lastError
		assert.Equal(t, vreplStreamCancel, e.reviewVReplStreamError(uuid, transientErrorStream))
	})
	t.Run("clean stream is left alone", func(t *testing.T) {
		e := newExecutor()
		cleanStream := &VReplStream{
			state: binlogdatapb.VReplicationWorkflowState_Running,
			pos:   "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
		}
		assert.Equal(t, vreplStreamNoAction, e.reviewVReplStreamError(uuid, cleanStream))
	})
}

// TestRepairVReplicationQuery pins the repair statement: restart the stream,
// clear the parked message, merge in the retry-forever override without
// discarding other config overrides, and guard on the Error state.
func TestRepairVReplicationQuery(t *testing.T) {
	query := repairVReplicationQuery(42)
	assert.Equal(t,
		`update _vt.vreplication set state='Running', message='', options=json_set(json_insert(coalesce(nullif(options, ''), '{}'), '$.config', json_object()), '$.config."vreplication-max-time-to-retry-on-error"', '0s') where id=42 and state='Error'`,
		query)
	// The park's history row becomes the repair's record: a previous release's
	// history scan takes any Error row as authoritative, so leaving it would
	// fail the repaired migration after a downgrade.
	assert.Equal(t,
		`update _vt.vreplication_log set state='Running', message=concat('Online DDL repaired the stream with the retry-forever override after: ', message) where vrepl_id=42 and state='Error' and message like 'retries exhausted:%'`,
		repairVReplicationHistoryQuery(42))
}

// TestTerminallyFailMigrationWriteOrder pins that the message is written
// before the status, and that a failed message write aborts the transition:
// once terminal, a migration leaves every review path and a missing reason
// could never be repaired.
func TestTerminallyFailMigrationWriteOrder(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	onlineDDL := &schema.OnlineDDL{UUID: uuid}
	newExecutor := func(failOn string) (*Executor, *[]string) {
		var queries []string
		e := &Executor{
			ticks: timer.NewTimer(time.Hour),
			execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
				if failOn != "" && strings.Contains(query, failOn) {
					return nil, errors.New("transient")
				}
				queries = append(queries, query)
				return &sqltypes.Result{RowsAffected: 1}, nil
			},
		}
		return e, &queries
	}
	indexOf := func(queries []string, substr string) int {
		for i, q := range queries {
			if strings.Contains(q, substr) {
				return i
			}
		}
		return -1
	}

	t.Run("message is written before the status", func(t *testing.T) {
		e, queries := newExecutor("")
		require.NoError(t, e.terminallyFailMigration(t.Context(), onlineDDL, errors.New("the reason")))
		messageIdx := indexOf(*queries, "message=")
		statusIdx := indexOf(*queries, "migration_status")
		require.GreaterOrEqual(t, messageIdx, 0, "no message write issued")
		require.GreaterOrEqual(t, statusIdx, 0, "no status transition issued")
		assert.Less(t, messageIdx, statusIdx, "the message must land before the status becomes terminal")
	})
	t.Run("failed message write aborts the transition", func(t *testing.T) {
		e, queries := newExecutor("message=")
		require.Error(t, e.terminallyFailMigration(t.Context(), onlineDDL, errors.New("the reason")))
		assert.Equal(t, -1, indexOf(*queries, "migration_status"),
			"the status must not become terminal while the reason could not be recorded")
	})
	t.Run("failed status write is reported", func(t *testing.T) {
		e, queries := newExecutor("migration_status")
		require.Error(t, e.terminallyFailMigration(t.Context(), onlineDDL, errors.New("the reason")))
		assert.GreaterOrEqual(t, indexOf(*queries, "message="), 0, "the message write is idempotent and precedes the transition")
	})
	t.Run("no error skips the message write", func(t *testing.T) {
		e, queries := newExecutor("")
		require.NoError(t, e.terminallyFailMigration(t.Context(), onlineDDL, nil))
		assert.Equal(t, -1, indexOf(*queries, "message="))
		assert.GreaterOrEqual(t, indexOf(*queries, "migration_status"), 0)
	})
}

// TestFailStaleMigration pins the stale-migration terminal transition: it is
// the failed-or-cancelled one, prefers a pending cancellation's reason over
// the stale message, and drops the stream tracking only once it has landed.
func TestFailStaleMigration(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	onlineDDL := &schema.OnlineDDL{UUID: uuid}
	newExecutor := func() (*Executor, *[]string) {
		var queries []string
		e := &Executor{
			vreplicationLastError:     map[string]*vterrors.LastError{uuid: vterrors.NewLastError("test", time.Minute)},
			vreplicationPendingCancel: map[string]string{},
			vreplicationProgress:      map[string]vreplStreamProgress{uuid: {copyStateID: 1}},
			ticks:                     timer.NewTimer(time.Hour),
			execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
				queries = append(queries, query)
				return &sqltypes.Result{RowsAffected: 1}, nil
			},
		}
		return e, &queries
	}
	findQuery := func(queries []string, substr string) string {
		for _, q := range queries {
			if strings.Contains(q, substr) {
				return q
			}
		}
		return ""
	}
	const staleMessage = "stale migration: found running but indicates no liveness"

	t.Run("pending cancellation is recorded as such", func(t *testing.T) {
		e, queries := newExecutor()
		e.vreplicationPendingCancel[uuid] = "cancelled by user"

		require.NoError(t, e.failStaleMigration(t.Context(), onlineDDL, staleMessage))

		transition := findQuery(*queries, "migration_status")
		require.NotEmpty(t, transition, "no status transition issued")
		assert.Contains(t, transition, "IF(cancelled_timestamp IS NULL, 'failed', 'cancelled')",
			"the transition must follow cancelled_timestamp so a user-cancelled migration ends 'cancelled', not 'failed'")
		message := findQuery(*queries, "message=")
		require.NotEmpty(t, message, "no message update issued")
		assert.Contains(t, message, "cancelled by user")
		assert.NotContains(t, message, staleMessage)

		assert.NotContains(t, e.vreplicationLastError, uuid)
		assert.NotContains(t, e.vreplicationPendingCancel, uuid)
		assert.NotContains(t, e.vreplicationProgress, uuid)
	})
	t.Run("no pending cancellation records the stale message", func(t *testing.T) {
		e, queries := newExecutor()

		require.NoError(t, e.failStaleMigration(t.Context(), onlineDDL, staleMessage))

		transition := findQuery(*queries, "migration_status")
		require.NotEmpty(t, transition, "no status transition issued")
		assert.Contains(t, transition, "IF(cancelled_timestamp IS NULL, 'failed', 'cancelled')")
		message := findQuery(*queries, "message=")
		require.NotEmpty(t, message, "no message update issued")
		assert.Contains(t, message, staleMessage)

		assert.NotContains(t, e.vreplicationLastError, uuid)
		assert.NotContains(t, e.vreplicationProgress, uuid)
	})
	t.Run("failed transition keeps the tracking and ownership", func(t *testing.T) {
		e, _ := newExecutor()
		e.vreplicationPendingCancel[uuid] = "cancelled by user"
		e.execQuery = func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if strings.Contains(query, "migration_status") {
				return nil, errors.New("transient")
			}
			return &sqltypes.Result{RowsAffected: 1}, nil
		}

		require.Error(t, e.failStaleMigration(t.Context(), onlineDDL, staleMessage))

		// Still 'running': the next tick must be able to re-drive the
		// cancellation with its original reason, and the scheduler's
		// conflict checks must keep seeing the migration.
		assert.Contains(t, e.vreplicationPendingCancel, uuid)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned, "a migration still running after a failed terminal transition must stay owned")
	})
}

// TestReviewStaleMigrationsUnconfirmedTermination pins that a stale migration
// whose termination fails stays owned: it is still 'running', and the
// scheduler consults ownership alone before the next review can re-adopt it.
func TestReviewStaleMigrationsUnconfirmedTermination(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	var terminalTransitionAttempted bool
	e := &Executor{
		tabletAlias:               alias,
		vreplicationLastError:     map[string]*vterrors.LastError{},
		vreplicationPendingCancel: map[string]string{},
		vreplicationProgress:      map[string]vreplStreamProgress{},
		ticks:                     timer.NewTimer(time.Hour),
		// No tablet record exists, so terminateMigration's topology lookup
		// fails before the stream is stopped.
		ts: memorytopo.NewServer(t.Context(), "cell"),
		execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
			q := strings.ToLower(query)
			switch {
			case strings.HasPrefix(strings.TrimSpace(q), "update"):
				if strings.Contains(q, "migration_status") {
					terminalTransitionAttempted = true
				}
				return &sqltypes.Result{RowsAffected: 1}, nil
			case strings.Contains(q, "liveness_timestamp <"):
				return sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("migration_uuid|stale_minutes", "varchar|int64"), uuid+"|200"), nil
			case strings.Contains(q, "migration_uuid="):
				return sqltypes.MakeTestResult(
					sqltypes.MakeTestFields("migration_uuid|migration_status|strategy|tablet", "varchar|varchar|varchar|varchar"),
					uuid+"|running|vitess|"+topoproto.TabletAliasString(alias)), nil
			default:
				return &sqltypes.Result{}, nil
			}
		},
	}
	e.isOpen.Store(1)

	require.NoError(t, e.reviewStaleMigrations(t.Context()))

	assert.False(t, terminalTransitionAttempted, "the terminal transition must not run when termination was not confirmed")
	_, owned := e.ownedRunningMigrations.Load(uuid)
	assert.True(t, owned, "a stale migration whose termination failed is still running and must stay owned")
}

// TestGetNonConflictingMigrationCancellationIntent pins that the scheduler
// never picks a migration carrying an unfulfilled cancellation intent, and
// re-drives its terminal transition instead.
func TestGetNonConflictingMigrationCancellationIntent(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	type harness struct {
		e                   *Executor
		transitionAttempted *bool
	}
	newSchedulerExecutor := func(migrationFields, migrationTypes, migrationRow string) harness {
		var transitionAttempted bool
		e := &Executor{
			tabletAlias:               &topodatapb.TabletAlias{Cell: "cell", Uid: 1},
			vreplicationLastError:     map[string]*vterrors.LastError{},
			vreplicationPendingCancel: map[string]string{},
			ticks:                     timer.NewTimer(time.Hour),
			execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
				q := strings.ToLower(query)
				switch {
				case strings.HasPrefix(strings.TrimSpace(q), "update"):
					if strings.Contains(q, "migration_status") {
						transitionAttempted = true
					}
					return &sqltypes.Result{RowsAffected: 1}, nil
				case strings.Contains(q, "migration_status='ready'"):
					return sqltypes.MakeTestResult(
						sqltypes.MakeTestFields("migration_uuid", "varchar"), uuid), nil
				case strings.Contains(q, "in ('queued', 'ready', 'running')"):
					return &sqltypes.Result{}, nil
				case strings.HasPrefix(strings.TrimSpace(q), "select") && strings.Contains(q, "migration_uuid="):
					return sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(migrationFields, migrationTypes), migrationRow), nil
				default:
					return &sqltypes.Result{}, nil
				}
			},
		}
		e.isOpen.Store(1)
		return harness{e: e, transitionAttempted: &transitionAttempted}
	}

	t.Run("durable cancellation intent is re-driven, not scheduled", func(t *testing.T) {
		h := newSchedulerExecutor(
			"migration_uuid|migration_status|strategy|cancelled_timestamp",
			"varchar|varchar|varchar|varchar",
			uuid+"|ready|vitess|2026-09-02 18:00:00")
		onlineDDL, err := h.e.getNonConflictingMigration(t.Context())
		require.NoError(t, err)
		assert.Nil(t, onlineDDL, "a cancelled migration must not be picked for execution")
		assert.True(t, *h.transitionAttempted, "the terminal transition must be re-driven instead")
	})
	t.Run("pending in-memory intent is re-driven, not scheduled", func(t *testing.T) {
		h := newSchedulerExecutor(
			"migration_uuid|migration_status|strategy",
			"varchar|varchar|varchar",
			uuid+"|ready|vitess")
		h.e.vreplicationPendingCancel[uuid] = "internal cancel"
		onlineDDL, err := h.e.getNonConflictingMigration(t.Context())
		require.NoError(t, err)
		assert.Nil(t, onlineDDL)
		assert.True(t, *h.transitionAttempted)
		assert.NotContains(t, h.e.vreplicationPendingCancel, uuid,
			"a successfully re-driven cancellation must clear its pending intent")
	})
	t.Run("a candidate without intent is scheduled", func(t *testing.T) {
		h := newSchedulerExecutor(
			"migration_uuid|migration_status|strategy",
			"varchar|varchar|varchar",
			uuid+"|ready|vitess")
		onlineDDL, err := h.e.getNonConflictingMigration(t.Context())
		require.NoError(t, err)
		require.NotNil(t, onlineDDL)
		assert.Equal(t, uuid, onlineDDL.UUID)
		assert.False(t, *h.transitionAttempted)
	})
}
