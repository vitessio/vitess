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
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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

// resumingTabletManagerClient reports every VReplicationExec call as having
// affected one row, simulating a successfully issued stream resume. When
// cancel is set it is invoked before returning, simulating a resume that
// succeeds just as the shared bounded context expires.
type resumingTabletManagerClient struct {
	tmclient.TabletManagerClient
	cancel context.CancelFunc
}

func (c *resumingTabletManagerClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	if c.cancel != nil {
		c.cancel()
	}
	return &querypb.QueryResult{RowsAffected: 1}, nil
}

func (resumingTabletManagerClient) Close() {}

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

// TestResumeBackoffElapsed tests the pacing of automatic vreplication
// stream resume attempts: the first attempt is immediate, subsequent
// delays double from the initial backoff, and the delay caps at the
// maximum backoff regardless of attempt count.
func TestResumeBackoffElapsed(t *testing.T) {
	now := time.Now()
	state := &vreplResumeState{}
	assert.True(t, resumeBackoffElapsed(state, now), "first attempt must be immediate")

	state = &vreplResumeState{attempts: 1, lastAttempt: now.Add(-30 * time.Second)}
	assert.False(t, resumeBackoffElapsed(state, now), "1st retry before initial backoff")
	state.lastAttempt = now.Add(-vreplResumeInitialBackoff)
	assert.True(t, resumeBackoffElapsed(state, now), "1st retry at initial backoff")

	// A resume attempt that failed before ever succeeding (e.g. GetTablet kept
	// erroring) still records lastAttempt but does not increment attempts (see
	// maybeResumeVReplication). attempts==0 must not bypass pacing in that case,
	// or a persistently failing attempt would hot-loop on every tick.
	state = &vreplResumeState{attempts: 0, lastAttempt: now.Add(-30 * time.Second)}
	assert.False(t, resumeBackoffElapsed(state, now), "failed 1st attempt before initial backoff")
	state.lastAttempt = now.Add(-vreplResumeInitialBackoff)
	assert.True(t, resumeBackoffElapsed(state, now), "failed 1st attempt at initial backoff")

	state = &vreplResumeState{attempts: 3, lastAttempt: now.Add(-2 * vreplResumeInitialBackoff)}
	assert.False(t, resumeBackoffElapsed(state, now), "3rd retry doubles twice")
	state.lastAttempt = now.Add(-4 * vreplResumeInitialBackoff)
	assert.True(t, resumeBackoffElapsed(state, now))

	// Far beyond the doubling range the delay must cap, not overflow.
	state = &vreplResumeState{attempts: 500, lastAttempt: now.Add(-vreplResumeMaxBackoff)}
	assert.True(t, resumeBackoffElapsed(state, now), "delay caps at max backoff")
	state.lastAttempt = now.Add(-vreplResumeMaxBackoff + time.Second)
	assert.False(t, resumeBackoffElapsed(state, now))
}

// TestOverrideStateFromHistory tests readVReplStream's _vt.vreplication_log
// history-scan decision: a class-B (retries-exhausted) historical row must
// not force the live state back to Error once the stream is actually
// running again (Init/Copying/Running) — class B is resumable by
// definition, so a live running stream supersedes a stale class-B history
// row. Class A / legacy terminal-error rows keep their original stickiness
// and always override, regardless of the live state.
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
			// The live row is written before the best-effort history insert,
			// so a live Error is always at least as fresh as any history row
			// and must stay authoritative.
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
			// Classification is anchored to the message boundary: a class A
			// error whose cause text embeds the class B marker (e.g. user
			// data quoted in a MySQL error) must not classify as class B.
			name:              "class A embedding class B marker in cause, live Running -> override",
			liveState:         binlogdatapb.VReplicationWorkflowState_Running,
			historicalMessage: vreplication.UnrecoverableErrorIndicator + ": Duplicate entry '" + vreplication.RetriesExhaustedIndicator + "' for key 'val'",
			wantOverride:      true,
		},
		{
			// A resumable-class history row never forces the Error state,
			// regardless of the live state.
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

// TestResumeBudgetExhausted tests the explicit per-episode resume budget:
// a migration whose stream keeps parking on resumable errors is only
// resumed for staleMigrationFailMinutes after the episode's first park,
// after which it is cancelled. The budget is tracked from the first park
// timestamp, deliberately independent of LastError's message-equality
// window: the park/resume cycle alternates the stream's message between
// the retries-exhausted wrapper and raw retry errors, which resets that
// window every cycle and would otherwise leave a flapping stream
// unbounded.
func TestResumeBudgetExhausted(t *testing.T) {
	now := time.Now()

	state := &vreplResumeState{}
	assert.False(t, resumeBudgetExhausted(state, now), "no park recorded yet")

	state = &vreplResumeState{firstParked: now.Add(-time.Minute)}
	assert.False(t, resumeBudgetExhausted(state, now), "just parked")

	state = &vreplResumeState{firstParked: now.Add(-staleMigrationFailMinutes*time.Minute + time.Minute)}
	assert.False(t, resumeBudgetExhausted(state, now), "under the budget")

	state = &vreplResumeState{firstParked: now.Add(-staleMigrationFailMinutes * time.Minute)}
	assert.True(t, resumeBudgetExhausted(state, now), "budget exhausted")
}

// TestReviewVReplStreamError tests the executor's per-stream decision in
// reviewRunningMigrations: a retries-exhausted (class B) stream is resumed
// rather than cancelled, an unrecoverable (class A) stream is cancelled, and
// a resumable stream whose error episode has outlived the resume budget is
// cancelled. The lifecycle subtest walks a single migration through
// park -> resume -> budget exhaustion -> recovery -> fresh episode.
// TestForgetVReplStreamOrdering pins WHEN the cleanup runs, not just what it
// deletes: a no-op RETRY (against a migration that is not failed/cancelled)
// and a cancellation that fails before its durable transition must both
// leave an active migration's recovery tracking intact — clearing it would
// grant the still-running stream a fresh error episode and resume budget.
func TestForgetVReplStreamOrdering(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	newTrackedExecutor := func(execQuery func(ctx context.Context, query string) (*sqltypes.Result, error)) *Executor {
		e := &Executor{
			vreplicationLastError:     map[string]*vterrors.LastError{uuid: vterrors.NewLastError("test", time.Minute)},
			vreplicationResumeState:   map[string]*vreplResumeState{uuid: {firstParked: time.Now()}},
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
		assert.Contains(t, e.vreplicationResumeState, uuid,
			"a RETRY that requeued nothing must not clear an active migration's episode")
		assert.Contains(t, e.vreplicationLastError, uuid)
	})
	t.Run("actual retry clears tracking", func(t *testing.T) {
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			return &sqltypes.Result{RowsAffected: 1}, nil
		})
		_, err := e.RetryMigration(t.Context(), uuid)
		require.NoError(t, err)
		assert.NotContains(t, e.vreplicationResumeState, uuid)
		assert.NotContains(t, e.vreplicationLastError, uuid)
	})
	t.Run("cancellation whose terminal transition fails retains tracking", func(t *testing.T) {
		// An internal (non-user) cancellation has no durable
		// cancelled_timestamp: the deferred failMigration IS the terminal
		// transition. If it fails, the migration and its parked stream stay
		// active, and erased tracking would let the next review resume a
		// stream that was just cancelled.
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "UPDATE") {
				return nil, errors.New("backend unavailable")
			}
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status", "varchar|varchar"),
				uuid+"|running"), nil
		})
		_, _ = e.CancelMigration(t.Context(), uuid, "internal cancel", false)
		assert.Contains(t, e.vreplicationResumeState, uuid,
			"tracking must survive a cancellation whose terminal transition failed")
		assert.Contains(t, e.vreplicationLastError, uuid)
		assert.Equal(t, "internal cancel", e.vreplicationPendingCancel[uuid],
			"an internal cancellation whose transition failed must record a pending intent: a successful stop leaves no stream verdict to re-trigger it")
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"a still-running migration under pending cancellation must stay owned, or the scheduler's conflict checks miss it")
	})
	t.Run("successful internal cancellation clears tracking", func(t *testing.T) {
		// The terminal-transition verdict must come from the actual status
		// update, not from failMigration's propagate-the-cause return value
		// (which is always non-nil for a cancellation): a successful
		// internal cancellation must clear the tracking, not log a false
		// transition failure and leak it.
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
		assert.NotContains(t, e.vreplicationResumeState, uuid,
			"a successfully cancelled migration must not retain stream tracking")
		assert.NotContains(t, e.vreplicationLastError, uuid)
		assert.NotContains(t, e.vreplicationPendingCancel, uuid)
	})
	t.Run("user cancellation with failed transition retains tracking", func(t *testing.T) {
		// cancelled_timestamp alone is not a terminal transition: the
		// running-migrations review only filters on migration_status, so
		// until the status update lands the migration remains eligible for
		// review and its tracking must survive — the cancellation intent
		// (not the cleanup) is what blocks auto-resume in the meantime.
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
		assert.Contains(t, e.vreplicationResumeState, uuid,
			"tracking must survive until the terminal status transition actually lands")
		assert.Contains(t, e.vreplicationLastError, uuid)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"a still-running migration under pending cancellation must stay owned")
	})
	t.Run("cancellation with expired caller context still completes the transition", func(t *testing.T) {
		// The deferred terminal transition must not reuse a context that
		// terminateMigration may have exhausted: it runs on a bounded,
		// non-cancellable context, so an expired caller context cannot
		// leave a cancelled-intent migration stuck in 'running'.
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
		// let the initial read succeed, then expire the context mid-flight
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
		assert.NotContains(t, e.vreplicationResumeState, uuid,
			"the transition must succeed on its own bounded context and clear the tracking")
		assert.NotContains(t, e.vreplicationLastError, uuid)
	})
	t.Run("unconfirmed termination defers the terminal transition", func(t *testing.T) {
		// terminateMigration can fail before the stream is stopped (its
		// topology lookup, an expired caller context). Durably failing the
		// migration then would orphan a live stream: once the migration
		// leaves 'running', the review loop no longer sees it and nothing
		// stops the stream until artifact GC. The terminal transition must
		// wait for a termination attempt that did not error — the migration
		// stays in 'running' and the next review tick re-drives the
		// cancellation.
		var terminalTransitionAttempted bool
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "UPDATE") {
				if strings.Contains(query, "migration_status") {
					terminalTransitionAttempted = true
				}
				return &sqltypes.Result{RowsAffected: 1}, nil
			}
			// A 'vitess' strategy routes terminateMigration through the
			// vreplication stop path, whose topology lookup fails below.
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status|strategy", "varchar|varchar|varchar"),
				uuid+"|running|vitess"), nil
		})
		// No tablet record exists: terminateMigration's topology lookup fails.
		e.ts = memorytopo.NewServer(t.Context(), "cell")
		_, err := e.CancelMigration(t.Context(), uuid, "internal cancel", false)
		require.Error(t, err)
		assert.False(t, terminalTransitionAttempted,
			"the durable terminal transition must not run when termination was not confirmed")
		assert.Contains(t, e.vreplicationResumeState, uuid)
		assert.Contains(t, e.vreplicationLastError, uuid)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"terminateMigration disowned the migration, but with an unconfirmed stop it remains live: ownership must be restored")
	})
	t.Run("failed stream stop defers the terminal transition", func(t *testing.T) {
		// When no delete follows, the graceful stop IS the termination: a
		// swallowed stop failure would let the deferred transition durably
		// fail the migration while its stream keeps applying changes,
		// invisible to the running-migrations review until artifact GC.
		// The stop failure must propagate so the transition is deferred
		// and the next review tick re-drives the cancellation.
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
		assert.Contains(t, e.vreplicationResumeState, uuid)
		assert.Contains(t, e.vreplicationLastError, uuid)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"the stream may still be applying changes: ownership must be restored until the cancellation lands")
	})
	t.Run("failed cancellation retains tracking", func(t *testing.T) {
		e := newTrackedExecutor(func(ctx context.Context, query string) (*sqltypes.Result, error) {
			// The cancelled_timestamp UPDATE fails; the SELECT that
			// readMigration issues (which also names that column) succeeds
			// with a running (cancellable) migration.
			if strings.Contains(query, "SET cancelled_timestamp=NOW(6)") {
				return nil, errors.New("backend unavailable")
			}
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|migration_status", "varchar|varchar"),
				uuid+"|running"), nil
		})
		_, err := e.CancelMigration(t.Context(), uuid, "test cancel", true)
		require.Error(t, err)
		assert.Contains(t, e.vreplicationResumeState, uuid,
			"a cancellation that failed before any durable transition must not clear the episode")
		assert.Contains(t, e.vreplicationLastError, uuid)
		// The intent write failed before the deferred cleanup was even
		// registered: no cancellation was accepted, so no pending intent
		// may exist — a record here would make the scheduler fail a
		// migration whose cancellation the caller was told did not happen.
		assert.NotContains(t, e.vreplicationPendingCancel, uuid,
			"a rejected cancellation must not leave a pending intent behind")
	})
}

// TestTerminallyFailMigrationMetric pins that FailedMigrations counts only
// migrations that actually reached a terminal state: the review loop
// re-drives a failed terminal transition on every tick, and an
// unconditional increment would inflate the counter once per tick while
// the migration is still running.
func TestTerminallyFailMigrationMetric(t *testing.T) {
	onlineDDL := &schema.OnlineDDL{UUID: "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"}
	newMetricExecutor := func(execQuery func(ctx context.Context, query string) (*sqltypes.Result, error)) *Executor {
		e := &Executor{
			vreplicationLastError:     map[string]*vterrors.LastError{},
			vreplicationResumeState:   map[string]*vreplResumeState{},
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

// TestReviewRunningMigrationsNilStreamCancellation pins the re-drive path
// for a migration whose vreplication stream is absent: an unfulfilled
// cancellation intent (cancelled_timestamp written, terminal transition
// never landed — e.g. a CancelMigration interrupted between the intent
// write and the transition) has no stream-side review to convert it, so
// the running-migrations review must emit the cancellation itself; without
// this the migration would stay in 'running' until the stale-migration
// fallback. A nil-stream migration without recorded intent stays untouched
// — a starting migration's stream may simply not exist yet.
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
			vreplicationResumeState:   map[string]*vreplResumeState{},
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
		// An internal cancellation has no durable cancelled_timestamp, and
		// its successful stream stop erased the Error verdict that
		// triggered it: the live row is now Stopped. Only the recorded
		// pending intent can convert this into a cancellation — without it
		// the migration idles in 'running' until the stale-migration
		// fallback. The stop rewrites only the state, so the row still
		// carries its pre-stop message: the cancellation must report the
		// recorded reason, not that stale leftover.
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
		// A healthy Running stream can carry a pending cancellation intent
		// (a prior cancellation's terminal transition failed). The cancel
		// verdict must end this migration's review: proceeding into the
		// ownership/liveness/cutover flow could cut the migration over and
		// complete it this very tick, before cancelMigrations processes
		// the queued cancellation.
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

// TestResolveVReplStreamAction pins that a migration carrying an unfulfilled
// cancellation intent (cancelled_timestamp written, terminal transition not
// yet landed) is never auto-resumed: the resume verdict is converted into a
// cancellation, which re-drives the terminal transition. All other verdicts
// pass through untouched.
func TestResolveVReplStreamAction(t *testing.T) {
	assert.Equal(t, vreplStreamCancel, resolveVReplStreamAction(vreplStreamResume, true),
		"an unfulfilled cancellation intent must convert resume into cancel")
	assert.Equal(t, vreplStreamCancel, resolveVReplStreamAction(vreplStreamNoAction, true),
		"a surviving clean stream yields no action, but the pending cancellation must still be re-driven")
	assert.Equal(t, vreplStreamCancel, resolveVReplStreamAction(vreplStreamCancel, true))
	assert.Equal(t, vreplStreamResume, resolveVReplStreamAction(vreplStreamResume, false))
	assert.Equal(t, vreplStreamCancel, resolveVReplStreamAction(vreplStreamCancel, false))
	assert.Equal(t, vreplStreamNoAction, resolveVReplStreamAction(vreplStreamNoAction, false))
}

// TestForgetVReplStream pins the helper's deletion semantics; the lifecycle
// points at which it may run are pinned by TestForgetVReplStreamOrdering.
func TestForgetVReplStream(t *testing.T) {
	e := &Executor{
		vreplicationLastError:     make(map[string]*vterrors.LastError),
		vreplicationResumeState:   make(map[string]*vreplResumeState),
		vreplicationPendingCancel: make(map[string]string),
	}
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	e.vreplicationLastError[uuid] = vterrors.NewLastError("test", time.Minute)
	e.vreplicationResumeState[uuid] = &vreplResumeState{firstParked: time.Now()}
	e.vreplicationPendingCancel[uuid] = "internal cancel"

	e.forgetVReplStream(uuid)
	assert.NotContains(t, e.vreplicationLastError, uuid)
	assert.NotContains(t, e.vreplicationResumeState, uuid)
	assert.NotContains(t, e.vreplicationPendingCancel, uuid)
}

// TestResumeVReplicationQuery pins the resume statement's conditional shape:
// it must only transition the stream back to Running when the row is still in
// the exact Error state (and message) that the resume decision was based on.
// An unconditional update would
// silently override an operator's concurrent Stop and erase its message.
func TestResumeVReplicationQuery(t *testing.T) {
	message := vreplication.RetriesExhaustedIndicator + ": it's 'complicated'"
	query := resumeVReplicationQuery(42, message)
	assert.Equal(t,
		"update _vt.vreplication set state='Running', message='' where id=42 and state='Error' and message="+sqltypes.EncodeStringSQL(message),
		query)
}

func TestReviewVReplStreamError(t *testing.T) {
	newExecutor := func() *Executor {
		return &Executor{
			vreplicationLastError:   make(map[string]*vterrors.LastError),
			vreplicationResumeState: make(map[string]*vreplResumeState),
		}
	}
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	now := time.Now()

	unrecoverableStream := &VReplStream{
		state:   binlogdatapb.VReplicationWorkflowState_Error,
		message: vreplication.UnrecoverableErrorIndicator + ": bad data",
	}
	resumableStream := &VReplStream{
		state:   binlogdatapb.VReplicationWorkflowState_Error,
		message: vreplication.RetriesExhaustedIndicator + ": the same error was encountered continuously for longer than --vreplication-max-time-to-retry-on-error (15m0s): connection refused",
	}
	transientErrorStream := &VReplStream{
		state:   binlogdatapb.VReplicationWorkflowState_Running,
		message: "error connecting to source tablet",
	}

	t.Run("unrecoverable stream is cancelled", func(t *testing.T) {
		e := newExecutor()
		assert.Equal(t, vreplStreamCancel, e.reviewVReplStreamError(uuid, unrecoverableStream, now))
		assert.NotContains(t, e.vreplicationResumeState, uuid)
	})
	t.Run("transient error within the retry window is tolerated", func(t *testing.T) {
		e := newExecutor()
		assert.Equal(t, vreplStreamNoAction, e.reviewVReplStreamError(uuid, transientErrorStream, now))
	})
	t.Run("transient error past the retry window is cancelled", func(t *testing.T) {
		e := newExecutor()
		// Seed a LastError whose window is already expired (negative max
		// time in error) so that ShouldRetry is deterministically false
		// once the error has been recorded, without sleeping.
		lastError := vterrors.NewLastError("test", -time.Nanosecond)
		lastError.Record(errors.New(transientErrorStream.message))
		e.vreplicationLastError[uuid] = lastError
		assert.Equal(t, vreplStreamCancel, e.reviewVReplStreamError(uuid, transientErrorStream, now))
	})
	t.Run("retries-exhausted lifecycle", func(t *testing.T) {
		e := newExecutor()
		// First tick parked on a resumable terminal error: resume, not cancel.
		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, resumableStream, now))
		require.Contains(t, e.vreplicationResumeState, uuid)
		assert.Equal(t, now, e.vreplicationResumeState[uuid].firstParked)

		// Later ticks within the budget keep resuming, and the episode
		// start is not restamped.
		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, resumableStream, now.Add(30*time.Minute)))
		assert.Equal(t, now, e.vreplicationResumeState[uuid].firstParked)

		// Once the episode outlives the resume budget, the migration is
		// cancelled even though the stream error is still class B.
		assert.Equal(t, vreplStreamCancel, e.reviewVReplStreamError(uuid, resumableStream, now.Add(staleMigrationFailMinutes*time.Minute)))

		// The stream recovering — reporting no error AND having advanced past
		// the position it parked on — ends the episode and clears the state.
		recoveredStream := &VReplStream{
			state: binlogdatapb.VReplicationWorkflowState_Running,
			pos:   "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
		}
		assert.Equal(t, vreplStreamNoAction, e.reviewVReplStreamError(uuid, recoveredStream, now.Add(staleMigrationFailMinutes*time.Minute)))
		assert.NotContains(t, e.vreplicationResumeState, uuid)

		// A later park starts a fresh episode with a fresh budget.
		later := now.Add(24 * time.Hour)
		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, resumableStream, later))
		assert.Equal(t, later, e.vreplicationResumeState[uuid].firstParked)
	})
	t.Run("synthetic clean row after a resume does not renew the budget", func(t *testing.T) {
		// Issuing a resume rewrites the stream to a clean Running row
		// (the resume clears the state and message) before the
		// stream has done any work. That synthetic observation must not
		// end the error episode: otherwise every park/resume/park cycle
		// would restamp firstParked and the resume budget could be renewed
		// indefinitely. Only forward progress — an advanced position —
		// ends the episode.
		e := newExecutor()
		pos := "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42"
		parkedStream := &VReplStream{
			state:   binlogdatapb.VReplicationWorkflowState_Error,
			message: vreplication.RetriesExhaustedIndicator + ": connection refused",
			pos:     pos,
		}
		syntheticCleanStream := &VReplStream{
			state: binlogdatapb.VReplicationWorkflowState_Running,
			pos:   pos,
		}

		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, parkedStream, now))
		require.Contains(t, e.vreplicationResumeState, uuid)
		assert.Equal(t, now, e.vreplicationResumeState[uuid].firstParked)
		// reviewRunningMigrations issues the resume in the same tick,
		// stamping lastAttempt; model that here.
		e.vreplicationResumeState[uuid].lastAttempt = now

		// The post-resume clean row at the same position keeps the episode.
		assert.Equal(t, vreplStreamNoAction, e.reviewVReplStreamError(uuid, syntheticCleanStream, now.Add(time.Minute)))
		require.Contains(t, e.vreplicationResumeState, uuid)
		assert.Equal(t, now, e.vreplicationResumeState[uuid].firstParked,
			"the episode start must survive the synthetic clean observation")

		// Re-parking within the budget keeps resuming on the original clock.
		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, parkedStream, now.Add(30*time.Minute)))
		assert.Equal(t, now, e.vreplicationResumeState[uuid].firstParked)
		e.vreplicationResumeState[uuid].lastAttempt = now.Add(30 * time.Minute)
		assert.Equal(t, vreplStreamNoAction, e.reviewVReplStreamError(uuid, syntheticCleanStream, now.Add(31*time.Minute)))

		// Exhaustion is measured from the original park, not the last one.
		assert.Equal(t, vreplStreamCancel, e.reviewVReplStreamError(uuid, parkedStream, now.Add(staleMigrationFailMinutes*time.Minute)))
	})
	t.Run("copy-phase progress ends the episode without a position change", func(t *testing.T) {
		// During the copy phase rows_copied advances while pos can stay
		// fixed: advancing row-copy checkpoints are real forward progress
		// and must end the episode, so a much later error starts a fresh
		// budget instead of cancelling a productive migration.
		e := newExecutor()
		pos := "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42"
		parkedStream := &VReplStream{
			state:      binlogdatapb.VReplicationWorkflowState_Error,
			message:    vreplication.RetriesExhaustedIndicator + ": connection refused",
			pos:        pos,
			rowsCopied: 100,
		}
		copyingStream := &VReplStream{
			state:      binlogdatapb.VReplicationWorkflowState_Copying,
			pos:        pos,
			rowsCopied: 5000,
		}

		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, parkedStream, now))
		e.vreplicationResumeState[uuid].lastAttempt = now

		assert.Equal(t, vreplStreamNoAction, e.reviewVReplStreamError(uuid, copyingStream, now.Add(time.Minute)))
		assert.NotContains(t, e.vreplicationResumeState, uuid,
			"advancing rows_copied is forward progress and must end the episode")

		// A later park starts a fresh episode with a fresh budget.
		later := now.Add(staleMigrationFailMinutes * time.Minute)
		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, parkedStream, later))
		assert.Equal(t, later, e.vreplicationResumeState[uuid].firstParked)
	})
	t.Run("adopted parked stream seeds the episode from the stream's own park time", func(t *testing.T) {
		// The executor's in-memory episode state dies with it (tablet
		// restart, failover, reopen), but the parked row's time_updated is
		// durable and stops advancing when the stream parks. Seeding the
		// episode from it means a stream that parked hours before the
		// executor (re)opened does not get a fresh 180-minute budget on
		// every restart.
		e := newExecutor()
		parkedLongAgo := &VReplStream{
			state:       binlogdatapb.VReplicationWorkflowState_Error,
			message:     vreplication.RetriesExhaustedIndicator + ": connection refused",
			timeUpdated: now.Add(-staleMigrationFailMinutes * time.Minute).Unix(),
		}
		assert.Equal(t, vreplStreamCancel, e.reviewVReplStreamError(uuid, parkedLongAgo, now),
			"a stream that parked a full budget ago must not get a fresh budget from an executor restart")

		// A freshly parked stream (time_updated ~ now) is unaffected.
		e2 := newExecutor()
		freshlyParked := &VReplStream{
			state:       binlogdatapb.VReplicationWorkflowState_Error,
			message:     vreplication.RetriesExhaustedIndicator + ": connection refused",
			timeUpdated: now.Unix(),
		}
		assert.Equal(t, vreplStreamResume, e2.reviewVReplStreamError(uuid, freshlyParked, now))
	})
	t.Run("progress between errors starts a fresh episode", func(t *testing.T) {
		// A resumed stream can advance its checkpoints and hit a NEW error
		// before any error-free observation lands — every tick then carries
		// an error. Forward progress must end the previous episode anyway:
		// the new error deserves a fresh budget, not the old episode's
		// nearly-spent one.
		e := newExecutor()
		parkedAtX := &VReplStream{
			state:   binlogdatapb.VReplicationWorkflowState_Error,
			message: vreplication.RetriesExhaustedIndicator + ": connection refused",
			pos:     "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42",
		}
		// Same error text as the first park, deliberately: LastError resets
		// its window on a DIFFERENT error by itself, so only an identical
		// recurring error exercises the hazard — inheriting the old
		// window's firstSeen across the progress reset.
		parkedAtY := &VReplStream{
			state:   binlogdatapb.VReplicationWorkflowState_Error,
			message: vreplication.RetriesExhaustedIndicator + ": connection refused",
			pos:     "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
		}

		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, parkedAtX, now))
		e.vreplicationResumeState[uuid].lastAttempt = now

		// The re-park arrives with an advanced position, past the old
		// episode's budget: it must be treated as a fresh episode and
		// resumed, not cancelled against the previous clock.
		later := now.Add(staleMigrationFailMinutes * time.Minute)
		originalLastError := e.vreplicationLastError[uuid]
		require.NotNil(t, originalLastError)
		assert.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, parkedAtY, later),
			"a re-park after forward progress must start a fresh episode")
		assert.Equal(t, later, e.vreplicationResumeState[uuid].firstParked)
		// The LastError retry window tracks the same episode: the same
		// error text recurring after real progress must not inherit the
		// old window's firstSeen, or !ShouldRetry() would cancel the
		// migration before the fresh resume budget could matter. A fresh
		// window means a fresh LastError.
		assert.NotSame(t, originalLastError, e.vreplicationLastError[uuid],
			"the retry window must restart with the fresh episode")
	})
	t.Run("clean observation recovery conditions", func(t *testing.T) {
		// A resumed stream's episode may end on a TRULY CLEAN observation:
		// no message, Running state, past the recovery grace of the last
		// resume attempt, and fresh non-throttle liveness. Each condition
		// closes a distinct false-recovery path:
		//   - the grace outwaits every path that can delay a resumed
		//     stream's error report (the longest is the vplayer stall
		//     deadline it is derived from);
		//   - an unrecognized non-empty message is a stream mid-retry, not
		//     recovery (hasError reports nil for it, but it is not clean);
		//   - the Copying state can hold a clean row through a copy attempt
		//     blocked for up to the copy-phase duration, far past the
		//     grace — the copy phase demonstrates recovery only through
		//     advancing checkpoints (the progress signal);
		//   - only a functioning applier advances time_updated (non-Error
		//     setState does not stamp it), so a clean row still carrying
		//     the old park's stale stamp is an internal retry window;
		//   - throttle updates stamp time_updated and time_throttled
		//     together without replicating anything, so equal stamps are
		//     deliberate pausing, not health; a heartbeat after the
		//     throttle advances time_updated past time_throttled.
		// Ending the episode on any of the false paths would grant the
		// eventual re-park a fresh resume budget every time.
		require.Greater(t, vreplResumeRecoveryGrace(), vreplication.VPlayerProgressDeadline(),
			"the recovery grace must outwait the vplayer stall deadline")

		pos := "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42"
		grace := vreplResumeRecoveryGrace()
		testCases := []struct {
			name          string
			state         binlogdatapb.VReplicationWorkflowState
			message       string
			timeUpdated   int64
			timeThrottled int64
			observedAt    time.Time
			wantRecovered bool
		}{
			{
				name:          "idle caught-up stream with fresh liveness past the grace recovers",
				state:         binlogdatapb.VReplicationWorkflowState_Running,
				timeUpdated:   now.Add(grace).Unix(),
				observedAt:    now.Add(grace),
				wantRecovered: true,
			},
			{
				name:        "within the grace of the resume attempt: retained",
				state:       binlogdatapb.VReplicationWorkflowState_Running,
				timeUpdated: now.Add(grace - time.Second).Unix(),
				observedAt:  now.Add(grace - time.Second),
			},
			{
				name:        "unrecognized retry message is mid-retry: retained",
				state:       binlogdatapb.VReplicationWorkflowState_Running,
				message:     "connection refused",
				timeUpdated: now.Add(grace).Unix(),
				observedAt:  now.Add(grace),
			},
			{
				name:        "blocked copy, clean Copying row without checkpoint movement: retained",
				state:       binlogdatapb.VReplicationWorkflowState_Copying,
				timeUpdated: now.Add(grace).Unix(),
				observedAt:  now.Add(grace),
			},
			{
				name:        "internal retry window, clean row with the park's stale liveness: retained",
				state:       binlogdatapb.VReplicationWorkflowState_Running,
				timeUpdated: now.Unix(),
				observedAt:  now.Add(grace),
			},
			{
				name:          "throttle-driven liveness is pausing, not recovery: retained",
				state:         binlogdatapb.VReplicationWorkflowState_Running,
				timeUpdated:   now.Add(grace).Unix(),
				timeThrottled: now.Add(grace).Unix(),
				observedAt:    now.Add(grace),
			},
			{
				name:          "heartbeat after the last throttle stamp recovers",
				state:         binlogdatapb.VReplicationWorkflowState_Running,
				timeUpdated:   now.Add(grace).Unix(),
				timeThrottled: now.Unix(),
				observedAt:    now.Add(grace),
				wantRecovered: true,
			},
		}
		parkedStream := &VReplStream{
			state:   binlogdatapb.VReplicationWorkflowState_Error,
			message: vreplication.RetriesExhaustedIndicator + ": connection refused",
			pos:     pos,
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				e := newExecutor()
				require.Equal(t, vreplStreamResume, e.reviewVReplStreamError(uuid, parkedStream, now))
				// reviewRunningMigrations issues the resume in the same
				// tick, stamping lastAttempt; model that here.
				e.vreplicationResumeState[uuid].lastAttempt = now

				// The position never moves: checkpoint progress is a
				// separate recovery signal, exercised elsewhere.
				observed := &VReplStream{
					state:         tc.state,
					message:       tc.message,
					pos:           pos,
					timeUpdated:   tc.timeUpdated,
					timeThrottled: tc.timeThrottled,
				}
				assert.Equal(t, vreplStreamNoAction, e.reviewVReplStreamError(uuid, observed, tc.observedAt))
				if tc.wantRecovered {
					assert.NotContains(t, e.vreplicationResumeState, uuid)
				} else {
					assert.Contains(t, e.vreplicationResumeState, uuid)
				}
			})
		}
	})
}

// TestMaybeResumeVReplicationRefreshesLiveness pins that every paced resume
// attempt — issued or failed — refreshes the migration's
// liveness_timestamp, and that the refresh does not ride the shared bounded
// context. The stale reaper runs right after the running-migrations review
// in the same tick, and a class-B park arrives after
// --vreplication-max-time-to-retry-on-error of continuous failure with no
// heartbeats: with a retry window at or above the stale threshold, the
// liveness clock is already expired at the very first park, and without
// this stamp the reaper would fail the migration in the same tick that is
// managing it — bypassing both the attempt pacing and the resume budget.
// The stamp is budget-gated — attempts stop when the episode's budget
// expires — so it cannot hold off the reaper indefinitely.
func TestMaybeResumeVReplicationRefreshesLiveness(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	parkedStream := &VReplStream{
		id:      1,
		state:   binlogdatapb.VReplicationWorkflowState_Error,
		message: vreplication.RetriesExhaustedIndicator + ": connection refused",
	}
	newLivenessExecutor := func(ts *topo.Server, requireLiveCtx bool) (*Executor, *bool) {
		var livenessRefreshed bool
		e := &Executor{
			ts:          ts,
			tabletAlias: &topodatapb.TabletAlias{Cell: "cell", Uid: 1},
			ticks:       timer.NewTimer(time.Hour),
			execQuery: func(ctx context.Context, query string) (*sqltypes.Result, error) {
				if requireLiveCtx {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
				}
				if strings.Contains(query, "liveness_timestamp=NOW(6)") {
					livenessRefreshed = true
				}
				return &sqltypes.Result{RowsAffected: 1}, nil
			},
		}
		e.isOpen.Store(1)
		return e, &livenessRefreshed
	}

	t.Run("issued resume stamps on its own context", func(t *testing.T) {
		// The resume RPC succeeds just as the shared context dies (which a
		// slow resume can cause): the refresh must be issued and must land
		// on its own bounded context.
		parentCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		protocolName := t.Name()
		resetProtocol := tmclienttest.SetProtocol(t.Name(), protocolName)
		defer resetProtocol()
		tmclient.RegisterTabletManagerClientFactory(protocolName, func() tmclient.TabletManagerClient {
			// The resume RPC succeeds, but the shared context dies with it.
			return &resumingTabletManagerClient{cancel: cancel}
		})
		alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
		ts := memorytopo.NewServer(t.Context(), "cell")
		require.NoError(t, ts.CreateTablet(t.Context(), &topodatapb.Tablet{
			Alias:    alias,
			Keyspace: "ks",
			Shard:    "0",
			Type:     topodatapb.TabletType_PRIMARY,
		}))
		e, livenessRefreshed := newLivenessExecutor(ts, true)
		e.maybeResumeVReplication(parentCtx, uuid, parkedStream, &vreplResumeState{firstParked: time.Now()})
		assert.True(t, *livenessRefreshed,
			"the liveness refresh must land on its own bounded context even when the resume exhausted the shared one")
	})
	t.Run("failed attempt still stamps", func(t *testing.T) {
		// The attempt fails at the tablet lookup (no tablet record) and is
		// recorded for a paced retry: the migration is still under active
		// management within its budget, so the stamp must land — otherwise
		// the same tick's stale reaper would fail the migration instead of
		// allowing the paced retry.
		ts := memorytopo.NewServer(t.Context(), "cell")
		e, livenessRefreshed := newLivenessExecutor(ts, false)
		state := &vreplResumeState{firstParked: time.Now()}
		e.maybeResumeVReplication(t.Context(), uuid, parkedStream, state)
		assert.True(t, *livenessRefreshed,
			"a failed attempt is still active management: the stamp must hold off the reaper for the paced retry")
		assert.False(t, state.lastAttempt.IsZero(), "the failed attempt must still be paced")
	})
}

// TestReviewRunningMigrationsAdoptsParkedResumableStream pins that a
// migration whose stream is classified resumable is adopted into
// ownedRunningMigrations for the whole recovery episode. The review's
// snapshot shows the parked (Error) row, so the normal adoption path is
// skipped — and runNextMigration runs BEFORE the next review in each tick,
// so without adoption here the parked-but-resumable migration (which can
// stay in 'running' for the whole 180-minute budget) is invisible to the
// scheduler's conflict and concurrency checks, letting a conflicting (even
// non-concurrent) migration start alongside it. Adoption must not depend on
// the paced attempt being issued or succeeding: a failed or backed-off
// attempt leaves the migration just as alive.
func TestReviewRunningMigrationsAdoptsParkedResumableStream(t *testing.T) {
	uuid := "1cbcd662_8ed6_11ee_bc8f_0a43f95f28a3"
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), tabletenv.NewDefaultConfig(), "ExecutorTest")
	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	parkedStream := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|workflow|source|pos|state|message", "int32|varchar|varchar|varchar|varchar|varchar"),
		"1|"+uuid+"|||Error|"+vreplication.RetriesExhaustedIndicator+": connection refused")
	newAdoptionExecutor := func(ts *topo.Server) *Executor {
		e := &Executor{
			env:                       env,
			ts:                        ts,
			tabletAlias:               alias,
			vreplicationLastError:     map[string]*vterrors.LastError{},
			vreplicationResumeState:   map[string]*vreplResumeState{},
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
					return parkedStream, nil
				case strings.HasPrefix(strings.TrimSpace(q), "select") && strings.Contains(q, "migration_uuid="):
					return sqltypes.MakeTestResult(
						sqltypes.MakeTestFields("migration_uuid|migration_status|strategy", "varchar|varchar|varchar"),
						uuid+"|running|vitess"), nil
				default:
					return &sqltypes.Result{}, nil
				}
			},
		}
		e.isOpen.Store(1)
		return e
	}

	t.Run("successful resume adopts", func(t *testing.T) {
		protocolName := t.Name()
		resetProtocol := tmclienttest.SetProtocol(t.Name(), protocolName)
		defer resetProtocol()
		tmclient.RegisterTabletManagerClientFactory(protocolName, func() tmclient.TabletManagerClient {
			return &resumingTabletManagerClient{}
		})
		ts := memorytopo.NewServer(t.Context(), "cell")
		require.NoError(t, ts.CreateTablet(t.Context(), &topodatapb.Tablet{
			Alias:    alias,
			Keyspace: "ks",
			Shard:    "0",
			Type:     topodatapb.TabletType_PRIMARY,
		}))
		e := newAdoptionExecutor(ts)
		_, cancellable, err := e.reviewRunningMigrations(t.Context())
		require.NoError(t, err)
		assert.Empty(t, cancellable)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"the tick that resumes a parked stream must adopt the migration")
	})
	t.Run("failed resume attempt still adopts", func(t *testing.T) {
		// No tablet record: the paced attempt fails at the tablet lookup,
		// but the parked migration is still live, resumable work and must be
		// visible to the scheduler's conflict checks.
		ts := memorytopo.NewServer(t.Context(), "cell")
		e := newAdoptionExecutor(ts)
		_, cancellable, err := e.reviewRunningMigrations(t.Context())
		require.NoError(t, err)
		assert.Empty(t, cancellable)
		_, owned := e.ownedRunningMigrations.Load(uuid)
		assert.True(t, owned,
			"a parked-resumable migration must be adopted even when the paced attempt fails")
	})
}

// TestGetNonConflictingMigrationCancellationIntent pins the scheduling gate:
// queued/ready selection ignores cancellation, so a migration carrying an
// unfulfilled cancellation intent (durable cancelled_timestamp, or the
// in-memory pending record) must not be picked for execution — the
// scheduler re-drives its terminal transition instead. Without the gate a
// user-cancelled migration whose terminal transition failed would be
// scheduled and start executing.
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
			vreplicationResumeState:   map[string]*vreplResumeState{},
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
