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
	"fmt"
	"strings"
	"sync"
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
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
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
				assert.Error(t, err)
				require.Equal(t, tcase.expect, defaultCutOverThreshold)
				// And keep testing, because we then also expect the threshold to be the default
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tcase.expect, threshold)
		})
	}
}

func TestGetInOrderCompletionPendingCount(t *testing.T) {
	onlineDDL := &schema.OnlineDDL{UUID: t.Name()}
	{
		require.Zero(t, getInOrderCompletionPendingCount(onlineDDL, nil))
	}
	{
		require.Zero(t, getInOrderCompletionPendingCount(onlineDDL, []string{}))
	}
	{
		pendingMigrationsUUIDs := []string{t.Name()}
		require.Zero(t, getInOrderCompletionPendingCount(onlineDDL, pendingMigrationsUUIDs))
	}
	{
		pendingMigrationsUUIDs := []string{"a", "b", "c", t.Name(), "x"}
		require.Equal(t, uint64(3), getInOrderCompletionPendingCount(onlineDDL, pendingMigrationsUUIDs))
	}
}

func TestInitDBConnectionLockWaitTimeout(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()
	connector := dbconfigs.NewTestDBConfigs(*params, *params, params.DbName).DbaWithDB()
	conn, err := dbconnpool.NewDBConnection(context.Background(), connector)
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

func TestShouldPreBufferWaitForParallelApply(t *testing.T) {
	config := vttablet.InitVReplicationConfigDefaults()
	savedParallelWorkers := config.ParallelReplicationWorkers
	t.Cleanup(func() {
		config.ParallelReplicationWorkers = savedParallelWorkers
	})

	config.ParallelReplicationWorkers = 1
	shouldWait, err := shouldPreBufferWaitForParallelApply(nil)
	require.NoError(t, err)
	require.False(t, shouldWait)

	config.ParallelReplicationWorkers = 2
	shouldWait, err = shouldPreBufferWaitForParallelApply(nil)
	require.NoError(t, err)
	require.True(t, shouldWait)
}

func TestShouldPreBufferWaitForParallelApplyPrefersWorkflowOverride(t *testing.T) {
	config := vttablet.InitVReplicationConfigDefaults()
	savedParallelWorkers := config.ParallelReplicationWorkers
	t.Cleanup(func() {
		config.ParallelReplicationWorkers = savedParallelWorkers
	})

	config.ParallelReplicationWorkers = 1

	stream := &VReplStream{}
	stream.bls = &binlogdatapb.BinlogSource{}
	stream.options = `{"config":{"vreplication-parallel-replication-workers":"2"}}`

	shouldWait, err := shouldPreBufferWaitForParallelApply(stream)
	require.NoError(t, err)
	require.True(t, shouldWait)
}

func TestShouldPreBufferWaitForParallelApplyRejectsInvalidWorkflowOverride(t *testing.T) {
	config := vttablet.InitVReplicationConfigDefaults()
	savedParallelWorkers := config.ParallelReplicationWorkers
	t.Cleanup(func() {
		config.ParallelReplicationWorkers = savedParallelWorkers
	})

	config.ParallelReplicationWorkers = 2

	stream := &VReplStream{}
	stream.bls = &binlogdatapb.BinlogSource{}
	stream.options = `{"config":{"vreplication-parallel-replication-workers":"not-an-int"}}`

	_, err := shouldPreBufferWaitForParallelApply(stream)
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid value for vreplication-parallel-replication-workers")
}

func TestShouldPreBufferWaitForParallelApplyIgnoresUnknownWorkflowOverrideKeys(t *testing.T) {
	config := vttablet.InitVReplicationConfigDefaults()
	savedParallelWorkers := config.ParallelReplicationWorkers
	t.Cleanup(func() {
		config.ParallelReplicationWorkers = savedParallelWorkers
	})

	config.ParallelReplicationWorkers = 2

	stream := &VReplStream{}
	stream.bls = &binlogdatapb.BinlogSource{}
	stream.options = `{"config":{"user":"admin","password":"secret"}}`

	shouldWait, err := shouldPreBufferWaitForParallelApply(stream)
	require.NoError(t, err)
	require.True(t, shouldWait)
}

type recordingTabletManagerClient struct {
	tmclient.TabletManagerClient

	mu                 sync.Mutex
	waitCalls          []string
	waitErr            error
	waitErrs           []error
	refreshStateCalled int
}

func (c *recordingTabletManagerClient) Close() {}

func (c *recordingTabletManagerClient) ReloadSchema(ctx context.Context, tablet *topodatapb.Tablet, waitPosition string) error {
	return nil
}

func (c *recordingTabletManagerClient) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshStateCalled++
	return nil
}

func (c *recordingTabletManagerClient) VReplicationWaitForPos(ctx context.Context, tablet *topodatapb.Tablet, id int32, pos string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waitCalls = append(c.waitCalls, pos)
	if len(c.waitErrs) > 0 {
		err := c.waitErrs[0]
		c.waitErrs = c.waitErrs[1:]
		if err != nil {
			return err
		}
		return nil
	}
	if c.waitErr != nil {
		return c.waitErr
	}
	return nil
}

func (c *recordingTabletManagerClient) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	return &querypb.QueryResult{}, nil
}

func (c *recordingTabletManagerClient) WaitCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.waitCalls...)
}

func (c *recordingTabletManagerClient) RefreshStateCalled() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.refreshStateCalled
}

func newCutoverTestExecutor(t *testing.T, db *fakesqldb.DB, ts *topo.Server, alias *topodatapb.TabletAlias) *Executor {
	t.Helper()

	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbconfigs.NewTestDBConfigs(*db.ConnParams(), *db.ConnParams(), db.ConnParams().DbName)
	venv := vtenv.NewTestEnv()

	executor := &Executor{
		env:         tabletenv.NewEnv(venv, cfg, "ExecutorTest"),
		ts:          ts,
		tabletAlias: alias,
		ticks:       timer.NewTimer(migrationCheckInterval),
		isPreparedPoolEmpty: func(tableName string) bool {
			return false
		},
	}
	executor.execQuery = func(ctx context.Context, query string) (*sqltypes.Result, error) {
		loweredQuery := strings.ToLower(query)
		switch {
		case strings.Contains(loweredQuery, "from _vt.schema_migrations"):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|keyspace|mysql_table|mysql_schema|migration_statement|strategy|options|migration_status|retries|ready_to_complete|was_ready_to_complete|tablet|migration_context|cutover_threshold_seconds|shadow_analyzed_timestamp", "varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|int64|int64|int64|varchar|varchar|int64|varchar"),
				strings.Join([]string{
					t.Name(),
					"ks",
					"t1",
					db.ConnParams().DbName,
					"alter table t1 add column i int",
					"vitess",
					"-vreplication-test-suite",
					"running",
					"0",
					"1",
					"1",
					"cell-0000000001",
					"",
					"5",
					"null",
				}, "|"),
			), nil
		case strings.Contains(loweredQuery, "from _vt.vreplication_log"):
			return &sqltypes.Result{Fields: sqltypes.MakeTestFields("state|message", "varchar|varchar")}, nil
		case strings.Contains(loweredQuery, "from _vt.vreplication"):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("id|workflow|source|options|pos|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|reason_throttled|state|message|rows_copied", "int64|varchar|varchar|varchar|varchar|int64|int64|int64|int64|varchar|varchar|varchar|varchar|int64"),
				"1|"+t.Name()+"|keyspace:\"ks\" shard:\"0\" filter:{rules:{match:\"_vt_HOLD_"+t.Name()+"\"}}|{\"config\":{\"vreplication-parallel-replication-workers\":\"2\"}}|MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-4|1|1|1|0|||Running||10",
			), nil
		default:
			return &sqltypes.Result{}, nil
		}
	}
	executor.pool = connpool.NewPool(executor.env, "OnlineDDLExecutorPoolTest", tabletenv.ConnPoolConfig{
		Size:        databasePoolSize,
		IdleTimeout: executor.env.Config().OltpReadPool.IdleTimeout,
	})
	executor.pool.Open(executor.env.Config().DB.AppWithDB(), executor.env.Config().DB.DbaWithDB(), executor.env.Config().DB.AppDebugWithDB())
	t.Cleanup(executor.pool.Close)

	return executor
}

func TestCutOverVReplMigrationBuffersBeforeParallelApplyCatchUpWait(t *testing.T) {
	ctx := t.Context()
	db := fakesqldb.New(t)
	defer db.Close()
	protocolName := t.Name()
	resetProtocol := tmclienttest.SetProtocol(t.Name(), protocolName)
	defer resetProtocol()

	waitErr := vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "vreplication still catching up")
	tmClient := &recordingTabletManagerClient{waitErr: waitErr}
	tmclient.RegisterTabletManagerClientFactory(protocolName, func() tmclient.TabletManagerClient {
		return tmClient
	})

	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	ts := memorytopo.NewServer(ctx, "cell")
	err := ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias:    alias,
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	})
	require.NoError(t, err)

	addSessionTimeoutQueries := func(lockWaitSeconds int64) {
		db.AddQuery("set @lock_wait_timeout=@@session.lock_wait_timeout", &sqltypes.Result{})
		db.AddQuery(fmt.Sprintf("set @@session.lock_wait_timeout=%d", lockWaitSeconds), &sqltypes.Result{})
		db.AddQuery("set @wait_timeout=@@session.wait_timeout", &sqltypes.Result{})
		db.AddQuery(fmt.Sprintf("set @@session.wait_timeout=%d", int64(waitTimeoutDuringCutOver.Seconds())), &sqltypes.Result{})
		db.AddQuery("set @@session.wait_timeout=@wait_timeout", &sqltypes.Result{})
		db.AddQuery("set @@session.lock_wait_timeout=@lock_wait_timeout", &sqltypes.Result{})
	}
	addSessionTimeoutQueries(15)
	addSessionTimeoutQueries(10)

	db.AddQuery("show global variables like 'rename_table_preserve_foreign_key'", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("@@global.gtid_executed", "varchar"),
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	))
	db.AddQueryPattern(`(?is)^drop table if exists .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^unlock tables$`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^kill \d+$`, &sqltypes.Result{})

	executor := newCutoverTestExecutor(t, db, ts, alias)

	bufferEvents := []bool{}
	var bufferMu sync.Mutex
	executor.toggleBufferTableFunc = func(cancelCtx context.Context, tableName string, timeout time.Duration, bufferQueries bool) {
		bufferMu.Lock()
		defer bufferMu.Unlock()
		bufferEvents = append(bufferEvents, bufferQueries)
	}

	stream := &VReplStream{
		id:       1,
		workflow: t.Name(),
		options:  `{"config":{"vreplication-parallel-replication-workers":"2"}}`,
		bls: &binlogdatapb.BinlogSource{
			Filter: &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: "_vt_HOLD_" + t.Name()}}},
		},
	}

	err = executor.cutOverVReplMigration(ctx, stream, true)
	require.Error(t, err)
	require.ErrorContains(t, err, "checking prepared pool for table")

	bufferMu.Lock()
	bufferEventsCopy := append([]bool(nil), bufferEvents...)
	bufferMu.Unlock()
	assert.Equal(t, []bool{true, false}, bufferEventsCopy)
	assert.Equal(t, 1, tmClient.RefreshStateCalled())
	assert.Len(t, tmClient.WaitCalls(), 0)
}

func TestCutOverVReplMigrationWaitsForParallelApplyAfterLocking(t *testing.T) {
	ctx := t.Context()
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()

	protocolName := t.Name()
	resetProtocol := tmclienttest.SetProtocol(t.Name(), protocolName)
	defer resetProtocol()

	waitErr := vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "vreplication still catching up")
	tmClient := &recordingTabletManagerClient{waitErrs: []error{nil, waitErr}}
	tmclient.RegisterTabletManagerClientFactory(protocolName, func() tmclient.TabletManagerClient {
		return tmClient
	})

	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	ts := memorytopo.NewServer(ctx, "cell")
	err := ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias:    alias,
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	})
	require.NoError(t, err)

	addSessionTimeoutQueries := func(lockWaitSeconds int64) {
		db.AddQuery("set @lock_wait_timeout=@@session.lock_wait_timeout", &sqltypes.Result{})
		db.AddQuery(fmt.Sprintf("set @@session.lock_wait_timeout=%d", lockWaitSeconds), &sqltypes.Result{})
		db.AddQuery("set @wait_timeout=@@session.wait_timeout", &sqltypes.Result{})
		db.AddQuery(fmt.Sprintf("set @@session.wait_timeout=%d", int64(waitTimeoutDuringCutOver.Seconds())), &sqltypes.Result{})
		db.AddQuery("set @@session.wait_timeout=@wait_timeout", &sqltypes.Result{})
		db.AddQuery("set @@session.lock_wait_timeout=@lock_wait_timeout", &sqltypes.Result{})
	}
	addSessionTimeoutQueries(15)
	addSessionTimeoutQueries(10)

	db.AddQuery("show global variables like 'rename_table_preserve_foreign_key'", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("@@global.gtid_executed", "varchar"),
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	))
	db.AddQueryPattern(`(?is)^update _vt\.schema_migrations set artifacts=.*$`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^drop table if exists .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^create table if not exists .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^lock tables .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^unlock tables$`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^kill \d+$`, &sqltypes.Result{})

	executor := newCutoverTestExecutor(t, db, ts, alias)
	executor.execQuery = func(ctx context.Context, query string) (*sqltypes.Result, error) {
		loweredQuery := strings.ToLower(query)
		switch {
		case strings.Contains(loweredQuery, "from _vt.schema_migrations"):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|keyspace|mysql_table|mysql_schema|migration_statement|strategy|options|migration_status|retries|ready_to_complete|was_ready_to_complete|tablet|migration_context|cutover_threshold_seconds|shadow_analyzed_timestamp", "varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|int64|int64|int64|varchar|varchar|int64|varchar"),
				strings.Join([]string{
					t.Name(),
					"ks",
					"t1",
					params.DbName,
					"alter table t1 add column i int",
					"vitess",
					"",
					"running",
					"0",
					"1",
					"1",
					"cell-0000000001",
					"",
					"5",
					"done",
				}, "|"),
			), nil
		case strings.Contains(loweredQuery, "select id, info as info from information_schema.processlist"):
			return &sqltypes.Result{Fields: sqltypes.MakeTestFields("id|info", "int64|varchar")}, nil
		case strings.Contains(loweredQuery, "from _vt.vreplication_log"):
			return &sqltypes.Result{Fields: sqltypes.MakeTestFields("state|message", "varchar|varchar")}, nil
		case strings.Contains(loweredQuery, "from _vt.vreplication"):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("id|workflow|source|options|pos|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|reason_throttled|state|message|rows_copied", "int64|varchar|varchar|varchar|varchar|int64|int64|int64|int64|varchar|varchar|varchar|varchar|int64"),
				"1|"+t.Name()+"|keyspace:\"ks\" shard:\"0\" filter:{rules:{match:\"_vt_HOLD_"+t.Name()+"\"}}|{\"config\":{\"vreplication-parallel-replication-workers\":\"2\"}}|MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-4|1|1|1|0|||Running||10",
			), nil
		default:
			return &sqltypes.Result{}, nil
		}
	}

	bufferEvents := []bool{}
	var bufferMu sync.Mutex
	executor.toggleBufferTableFunc = func(cancelCtx context.Context, tableName string, timeout time.Duration, bufferQueries bool) {
		bufferMu.Lock()
		defer bufferMu.Unlock()
		bufferEvents = append(bufferEvents, bufferQueries)
	}

	stream := &VReplStream{
		id:       1,
		workflow: t.Name(),
		options:  `{"config":{"vreplication-parallel-replication-workers":"2"}}`,
		bls: &binlogdatapb.BinlogSource{
			Filter: &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: "_vt_HOLD_" + t.Name()}}},
		},
	}

	err = executor.cutOverVReplMigration(ctx, stream, false)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed waiting for vreplication to catch up before renaming")

	bufferMu.Lock()
	bufferEventsCopy := append([]bool(nil), bufferEvents...)
	bufferMu.Unlock()
	assert.Equal(t, []bool{true, false}, bufferEventsCopy)
	assert.Equal(t, 1, tmClient.RefreshStateCalled())
	assert.Len(t, tmClient.WaitCalls(), 2)
	assert.NotContains(t, db.QueryLog(), "rename table")
}

func TestCutOverVReplMigrationSkipsSecondPostLockWaitAfterParallelApplyCatchUp(t *testing.T) {
	ctx := t.Context()
	db := fakesqldb.New(t)
	defer db.Close()
	params := db.ConnParams()

	protocolName := t.Name()
	resetProtocol := tmclienttest.SetProtocol(t.Name(), protocolName)
	defer resetProtocol()

	waitErr := vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "unexpected extra wait after parallel apply catch up")
	tmClient := &recordingTabletManagerClient{waitErrs: []error{nil, nil, waitErr}}
	tmclient.RegisterTabletManagerClientFactory(protocolName, func() tmclient.TabletManagerClient {
		return tmClient
	})

	alias := &topodatapb.TabletAlias{Cell: "cell", Uid: 1}
	ts := memorytopo.NewServer(ctx, "cell")
	err := ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias:    alias,
		Keyspace: "ks",
		Shard:    "0",
		Type:     topodatapb.TabletType_PRIMARY,
	})
	require.NoError(t, err)

	addSessionTimeoutQueries := func(lockWaitSeconds int64) {
		db.AddQuery("set @lock_wait_timeout=@@session.lock_wait_timeout", &sqltypes.Result{})
		db.AddQuery(fmt.Sprintf("set @@session.lock_wait_timeout=%d", lockWaitSeconds), &sqltypes.Result{})
		db.AddQuery("set @wait_timeout=@@session.wait_timeout", &sqltypes.Result{})
		db.AddQuery(fmt.Sprintf("set @@session.wait_timeout=%d", int64(waitTimeoutDuringCutOver.Seconds())), &sqltypes.Result{})
		db.AddQuery("set @@session.wait_timeout=@wait_timeout", &sqltypes.Result{})
		db.AddQuery("set @@session.lock_wait_timeout=@lock_wait_timeout", &sqltypes.Result{})
	}
	addSessionTimeoutQueries(15)
	addSessionTimeoutQueries(10)

	db.AddQuery("show global variables like 'rename_table_preserve_foreign_key'", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("@@global.gtid_executed", "varchar"),
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
	))
	db.AddQueryPattern(`(?is)^update _vt\.schema_migrations set artifacts=.*$`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^drop table if exists .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^create table if not exists .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^lock tables .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^rename table .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^drop table .*`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^unlock tables$`, &sqltypes.Result{})
	db.AddQueryPattern(`(?is)^kill \d+$`, &sqltypes.Result{})

	executor := newCutoverTestExecutor(t, db, ts, alias)
	executor.execQuery = func(ctx context.Context, query string) (*sqltypes.Result, error) {
		loweredQuery := strings.ToLower(query)
		switch {
		case strings.Contains(loweredQuery, "from _vt.schema_migrations"):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("migration_uuid|keyspace|mysql_table|mysql_schema|migration_statement|strategy|options|migration_status|retries|ready_to_complete|was_ready_to_complete|tablet|migration_context|cutover_threshold_seconds|shadow_analyzed_timestamp", "varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar|int64|int64|int64|varchar|varchar|int64|varchar"),
				strings.Join([]string{
					t.Name(),
					"ks",
					"t1",
					params.DbName,
					"alter table t1 add column i int",
					"vitess",
					"",
					"running",
					"0",
					"1",
					"1",
					"cell-0000000001",
					"",
					"5",
					"done",
				}, "|"),
			), nil
		case strings.Contains(loweredQuery, "select id, info as info from information_schema.processlist"):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("id|info", "int64|varchar"),
				"3|rename table `t1` to `_vt_hld_dummy`",
			), nil
		case strings.Contains(loweredQuery, "from _vt.vreplication_log"):
			return &sqltypes.Result{Fields: sqltypes.MakeTestFields("state|message", "varchar|varchar")}, nil
		case strings.Contains(loweredQuery, "from _vt.vreplication"):
			return sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("id|workflow|source|options|pos|time_updated|transaction_timestamp|time_heartbeat|time_throttled|component_throttled|reason_throttled|state|message|rows_copied", "int64|varchar|varchar|varchar|varchar|int64|int64|int64|int64|varchar|varchar|varchar|varchar|int64"),
				"1|"+t.Name()+"|keyspace:\"ks\" shard:\"0\" filter:{rules:{match:\"_vt_HOLD_"+t.Name()+"\"}}|{\"config\":{\"vreplication-parallel-replication-workers\":\"2\"}}|MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-4|1|1|1|0|||Running||10",
			), nil
		default:
			return &sqltypes.Result{}, nil
		}
	}

	bufferEvents := []bool{}
	var bufferMu sync.Mutex
	executor.toggleBufferTableFunc = func(cancelCtx context.Context, tableName string, timeout time.Duration, bufferQueries bool) {
		bufferMu.Lock()
		defer bufferMu.Unlock()
		bufferEvents = append(bufferEvents, bufferQueries)
	}

	stream := &VReplStream{
		id:       1,
		workflow: t.Name(),
		options:  `{"config":{"vreplication-parallel-replication-workers":"2"}}`,
		bls: &binlogdatapb.BinlogSource{
			Filter: &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: "_vt_HOLD_" + t.Name()}}},
		},
	}

	err = executor.cutOverVReplMigration(ctx, stream, false)
	require.NoError(t, err)

	bufferMu.Lock()
	bufferEventsCopy := append([]bool(nil), bufferEvents...)
	bufferMu.Unlock()
	assert.Equal(t, []bool{true, false}, bufferEventsCopy)
	assert.Equal(t, 1, tmClient.RefreshStateCalled())
	assert.Len(t, tmClient.WaitCalls(), 2)
	assert.Contains(t, strings.ToLower(db.QueryLog()), "rename table")
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
