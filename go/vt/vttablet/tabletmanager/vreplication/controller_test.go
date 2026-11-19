/*
Copyright 2019 The Vitess Authors.

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

package vreplication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	qh "vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication/queryhistory"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	testSettingsResponse = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pos", Type: sqltypes.VarBinary},
			{Name: "stop_pos", Type: sqltypes.VarBinary},
			{Name: "max_tps", Type: sqltypes.Int64},
			{Name: "max_replication_lag", Type: sqltypes.Int64},
			{Name: "state", Type: sqltypes.VarBinary},
			{Name: "workflow_type", Type: sqltypes.Int64},
			{Name: "workflow", Type: sqltypes.VarChar},
			{Name: "workflow_sub_type", Type: sqltypes.Int64},
			{Name: "defer_secondary_keys", Type: sqltypes.Int64},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"), // pos
				sqltypes.NULL,                          // stop_pos
				sqltypes.NewInt64(9223372036854775807), // max_tps
				sqltypes.NewInt64(9223372036854775807), // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),      // workflow_type
				sqltypes.NewVarChar("wf"), // workflow
				sqltypes.NewInt64(0),      // workflow_sub_type
				sqltypes.NewInt64(0),      // defer_secondary_keys
			},
		},
	}
	testSelectorResponse1      = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1)}}}
	testSelectorResponse2      = &sqltypes.Result{Rows: [][]sqltypes.Value{{sqltypes.NewInt64(1)}, {sqltypes.NewInt64(2)}}}
	testDMLResponse            = &sqltypes.Result{RowsAffected: 1}
	testPos                    = "MariaDB/0-1-1083"
	defaultTabletPickerOptions = discovery.TabletPickerOptions{}
)

func setTabletTypesStr(tabletTypesStr string) func() {
	oldTabletTypesStr := vttablet.DefaultVReplicationConfig.TabletTypesStr
	vttablet.DefaultVReplicationConfig.TabletTypesStr = tabletTypesStr
	return func() {
		vttablet.DefaultVReplicationConfig.TabletTypesStr = oldTabletTypesStr
	}
}

func TestControllerKeyRange(t *testing.T) {
	resetBinlogClient()
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)
	params := map[string]string{
		"id":      "1",
		"state":   binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":  fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"options": "{}",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	defer setTabletTypesStr("replica")()
	ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop(true)
	}()

	dbClient.Wait()
	expectFBCRequest(t, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}

func TestControllerTables(t *testing.T) {
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)
	resetBinlogClient()

	params := map[string]string{
		"id":      "1",
		"state":   binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":  fmt.Sprintf(`keyspace:"%s" shard:"0" tables:"table1" tables:"/funtables_/" `, env.KeyspaceName),
		"options": "{}",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{
		Schema: &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:              "table1",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name:              "funtables_one",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
				{
					Name:              "excluded_table",
					Columns:           []string{"id", "msg", "keyspace_id"},
					PrimaryKeyColumns: []string{"id"},
					Type:              tmutils.TableBaseTable,
				},
			},
		},
	}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)
	defer setTabletTypesStr("replica")()
	ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop(true)
	}()

	dbClient.Wait()
	expectFBCRequest(t, wantTablet, testPos, []string{"table1", "funtables_one"}, nil)
}

func TestControllerBadID(t *testing.T) {
	params := map[string]string{
		"id":      "bad",
		"options": "{}",
	}
	_, err := newController(t.Context(), params, nil, nil, nil, "", nil, nil, defaultTabletPickerOptions)
	want := `strconv.ParseInt: parsing "bad": invalid syntax`
	require.EqualErrorf(t, err, want, "newController err: %v, want %v", err, want)
}

func TestControllerStopped(t *testing.T) {
	params := map[string]string{
		"id":      "1",
		"state":   binlogdatapb.VReplicationWorkflowState_Stopped.String(),
		"options": "{}",
	}

	ct, err := newController(t.Context(), params, nil, nil, nil, "", nil, nil, defaultTabletPickerOptions)
	require.NoError(t, err)
	defer ct.Stop(true)

	select {
	case <-ct.done:
	default:
		assert.Failf(t, "context not closed", "context should be closed, but is not: %v", ct)
	}
}

func TestControllerOverrides(t *testing.T) {
	resetBinlogClient()
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":           "1",
		"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"cell":         env.Cells[0],
		"tablet_types": "replica",
		"options":      "{}",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	defer setTabletTypesStr("rdonly")()
	ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop(true)
	}()

	dbClient.Wait()
	expectFBCRequest(t, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}

func TestControllerCanceledContext(t *testing.T) {
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":      "1",
		"state":   binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":  fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"options": "{}",
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, nil, nil, nil, "", nil)

	ct, err := newController(ctx, params, nil, nil, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)
	defer ct.Stop(true)

	select {
	case <-ct.done:
	case <-time.After(1 * time.Second):
		assert.Failf(t, "context not closed", "context should be closed, but is not: %v", ct)
	}
}

func TestControllerRetry(t *testing.T) {
	savedDelay := vttablet.DefaultVReplicationConfig.RetryDelay
	defer func() { vttablet.DefaultVReplicationConfig.RetryDelay = savedDelay }()
	vttablet.DefaultVReplicationConfig.RetryDelay = 10 * time.Millisecond

	resetBinlogClient()
	defer deleteTablet(addTablet(100))

	params := map[string]string{
		"id":           "1",
		"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"cell":         env.Cells[0],
		"tablet_types": "replica",
		"options":      "{}",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, nil, errors.New("(expected error)"))
	dbClient.ExpectRequest("update _vt.vreplication set state='Error', message='error (expected error) in selecting vreplication settings select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys, options from _vt.vreplication where id=1' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, testSettingsResponse, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	defer setTabletTypesStr("rdonly")()
	ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)
	defer ct.Stop(true)

	dbClient.Wait()
}

func TestControllerStopPosition(t *testing.T) {
	resetBinlogClient()
	wantTablet := addTablet(100)
	defer deleteTablet(wantTablet)

	params := map[string]string{
		"id":      "1",
		"state":   binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":  fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"options": "{}",
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.ExpectRequestRE("update _vt.vreplication set message='Picked source tablet.*", testDMLResponse, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Running', message='' where id=1", testDMLResponse, nil)
	withStop := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "pos", Type: sqltypes.VarBinary},
			{Name: "stop_pos", Type: sqltypes.VarBinary},
			{Name: "max_tps", Type: sqltypes.Int64},
			{Name: "max_replication_lag", Type: sqltypes.Int64},
			{Name: "state", Type: sqltypes.VarBinary},
			{Name: "workflow_type", Type: sqltypes.Int64},
			{Name: "workflow", Type: sqltypes.VarChar},
			{Name: "workflow_sub_type", Type: sqltypes.Int64},
			{Name: "defer_secondary_keys", Type: sqltypes.Int64},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1083"),                                      // pos
				sqltypes.NewVarBinary("MariaDB/0-1-1235"),                                      // stop_pos
				sqltypes.NewInt64(9223372036854775807),                                         // max_tps
				sqltypes.NewInt64(9223372036854775807),                                         // max_replication_lag
				sqltypes.NewVarBinary(binlogdatapb.VReplicationWorkflowState_Running.String()), // state
				sqltypes.NewInt64(1),                                                           // workflow_type
				sqltypes.NewVarChar("wf"),                                                      // workflow
				sqltypes.NewInt64(1),                                                           // workflow_sub_type
				sqltypes.NewInt64(1),                                                           // defer_secondary_keys
			},
		},
	}
	dbClient.ExpectRequest(binlogplayer.TestGetWorkflowQueryId1, withStop, nil)
	dbClient.ExpectRequest("begin", nil, nil)
	dbClient.ExpectRequest("insert into t values(1)", testDMLResponse, nil)
	dbClient.ExpectRequestRE("update _vt.vreplication set pos='MariaDB/0-1-1235', time_updated=.*", testDMLResponse, nil)
	dbClient.ExpectRequest("commit", nil, nil)
	dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='Reached stopping position, done playing logs' where id=1", testDMLResponse, nil)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop(true)
	}()

	// Also confirm that replication stopped.
	select {
	case <-ct.done:
	case <-time.After(1 * time.Second):
		assert.Failf(t, "context not closed", "context should be closed, but is not: %v", ct)
	}

	dbClient.Wait()
	expectFBCRequest(t, wantTablet, testPos, nil, &topodatapb.KeyRange{End: []byte{0x80}})
}

// Test how tablet picker errors are handled.
func TestControllerTabletPickerErrors(t *testing.T) {
	type testCase struct {
		name              string
		tabletTypesStr    string
		setupFunc         func()
		expectRetry       bool
		expectedErrSubstr string
	}

	tcases := []testCase{
		{
			name:              "canceled context from picker timeout",
			tabletTypesStr:    "replica",
			setupFunc:         func() { /* don't add any tablets, so tablet picker times out */ },
			expectRetry:       true, // tablet picker errors should always retry
			expectedErrSubstr: "context has expired",
		},
		{
			name:              "failed precondition from invalid tablet type",
			tabletTypesStr:    "invalid_tablet_type",
			setupFunc:         func() { /* tablet type parsing will fail, so no need to register tablets */ },
			expectRetry:       true, // tablet picker errors should always retry
			expectedErrSubstr: "failed to parse list of tablet types",
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			savedDelay := vttablet.DefaultVReplicationConfig.RetryDelay
			defer func() { vttablet.DefaultVReplicationConfig.RetryDelay = savedDelay }()
			vttablet.DefaultVReplicationConfig.RetryDelay = 10 * time.Millisecond

			savedPickerDelay := discovery.GetTabletPickerRetryDelay()
			defer discovery.SetTabletPickerRetryDelay(savedPickerDelay)
			discovery.SetTabletPickerRetryDelay(10 * time.Millisecond)

			resetBinlogClient()

			if tc.setupFunc != nil {
				tc.setupFunc()
			}

			params := map[string]string{
				"id":           "1",
				"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
				"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
				"cell":         env.Cells[0],
				"tablet_types": tc.tabletTypesStr,
				"options":      "{}",
			}

			dbClient := binlogplayer.NewMockDBClient(t)
			dbClient.AddInvariant("update _vt.vreplication set message=", testDMLResponse)
			dbClient.AddInvariant("update _vt.vreplication set state=", testDMLResponse)

			dbClientFactory := func() binlogplayer.DBClient { return dbClient }
			mysqld := &mysqlctl.FakeMysqlDaemon{}
			mysqld.MysqlPort.Store(3306)
			vre := NewTestEngine(nil, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

			// We use blpStats to ensure that we triggered the expected error.
			blpStats := binlogplayer.NewStats()
			defer blpStats.Stop()

			ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], blpStats, vre, defaultTabletPickerOptions)
			require.NoError(t, err)

			select {
			case <-ct.done:
				require.FailNow(t, "Controller stopped unexpectedly, expected it to keep retrying")
			case <-time.After(500 * time.Millisecond):
				ct.Stop(true)

				records := blpStats.History.Records()
				var foundExpectedErr bool
				var lastMsg string
				for _, rec := range records {
					if r, ok := rec.(*binlogplayer.StatsHistoryRecord); ok {
						lastMsg = r.Message
						if strings.Contains(r.Message, tc.expectedErrSubstr) {
							foundExpectedErr = true
							break
						}
					}
				}

				require.True(t, foundExpectedErr, "Expected error containing %q in history, but last message was: %s", tc.expectedErrSubstr, lastMsg)

				require.True(t, tc.expectRetry, "Expected controller to fail immediately, but it kept retrying. Last error: %s", lastMsg)
			}
		})
	}
}

// Test how replication errors are handled.
func TestControllerReplicationErrors(t *testing.T) {
	type testCase struct {
		name         string
		code         vtrpcpb.Code
		msg          string
		shouldRetry  bool
		ignoreTablet bool
	}

	tcases := []testCase{
		{
			name:         "failed precondition",
			code:         vtrpcpb.Code_FAILED_PRECONDITION,
			msg:          "",
			shouldRetry:  true,
			ignoreTablet: false,
		},
		{
			name:         "gtid mismatch",
			code:         vtrpcpb.Code_INVALID_ARGUMENT,
			msg:          vterrors.GTIDSetMismatch + " aa",
			shouldRetry:  true,
			ignoreTablet: true,
		},
		{
			name:         "unavailable",
			code:         vtrpcpb.Code_UNAVAILABLE,
			msg:          "",
			shouldRetry:  true,
			ignoreTablet: false,
		},
		{
			name:         "invalid argument",
			code:         vtrpcpb.Code_INVALID_ARGUMENT,
			msg:          "final error",
			shouldRetry:  true, // Although this is a non-ephemeral error, in vreplication controller we will still retry.
			ignoreTablet: false,
		},
		{
			name:         "query interrupted",
			code:         vtrpcpb.Code_UNKNOWN,
			msg:          "vttablet: rpc error: code = Unknown desc = Query execution was interrupted, maximum statement execution time exceeded (errno 3024) (sqlstate HY000)",
			shouldRetry:  true,
			ignoreTablet: false,
		},
		{
			name:         "binary log purged",
			code:         vtrpcpb.Code_UNKNOWN,
			msg:          "vttablet: rpc error: code = Unknown desc = stream (at source tablet) error @ 013c5ddc-dd89-11ed-b3a1-125a006436b9:1-305627274: Cannot replicate because the source purged required binary logs (errno 1236) (sqlstate HY000)",
			shouldRetry:  true,
			ignoreTablet: true,
		},
		{
			name:         "source purged required gtids",
			code:         vtrpcpb.Code_UNKNOWN,
			msg:          "vttablet: rpc error: code = Unknown desc = Cannot replicate because the source purged required binary logs. Missing transactions are: 013c5ddc-dd89-11ed-b3a1-125a006436b9:305627275-305627280 (errno 1789) (sqlstate HY000)",
			shouldRetry:  true,
			ignoreTablet: true,
		},
		{
			name:         "non-ephemeral sql error",
			code:         vtrpcpb.Code_UNKNOWN,
			msg:          "vttablet: rpc error: code = Unknown desc = Duplicate entry '1' for key 'PRIMARY' (errno 1062) (sqlstate 23000)",
			shouldRetry:  true, // Although this is a non-ephemeral error, in vreplication controller we will still retry.
			ignoreTablet: false,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			savedDelay := vttablet.DefaultVReplicationConfig.RetryDelay
			defer func() { vttablet.DefaultVReplicationConfig.RetryDelay = savedDelay }()
			vttablet.DefaultVReplicationConfig.RetryDelay = 10 * time.Millisecond

			resetBinlogClient()

			ctx := t.Context()
			err := env.AddCell(ctx, "cell2")
			require.NoError(t, err)

			err = env.TopoServ.CreateCellsAlias(ctx, "region1", &topodatapb.CellsAlias{
				Cells: []string{"cell1", "cell2"},
			})
			require.NoError(t, err)
			defer env.TopoServ.DeleteCellsAlias(ctx, "region1")

			tablet1 := addTabletWithCell(100, env.Cells[0])
			tablet2 := addTabletWithCell(200, env.Cells[1])
			defer deleteTablet(tablet1)
			defer deleteTablet(tablet2)

			params := map[string]string{
				"id":           "1",
				"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
				"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
				"cell":         "",
				"tablet_types": "replica",
				"options":      "{}",
			}

			pickedTablets := []uint32{}
			var mu sync.Mutex

			oldErrors := fakeBinlogClientErrorsByTablet
			defer func() { fakeBinlogClientErrorsByTablet = oldErrors }()
			fakeBinlogClientErrorsByTablet = map[uint32]error{
				100: vterrors.New(tc.code, tc.msg),
			}

			oldCallback := fakeBinlogClientCallback
			defer func() { fakeBinlogClientCallback = oldCallback }()
			fakeBinlogClientCallback = func(tablet *topodatapb.Tablet) {
				mu.Lock()
				defer mu.Unlock()
				pickedTablets = append(pickedTablets, tablet.Alias.Uid)
			}

			dbClient := binlogplayer.NewMockDBClient(t)
			dbClient.AddInvariant("update _vt.vreplication set message=", testDMLResponse)
			dbClient.AddInvariant("update _vt.vreplication set state=", testDMLResponse)
			dbClient.AddInvariant("select pos", testSettingsResponse)
			dbClient.AddInvariant("begin", testDMLResponse)
			dbClient.AddInvariant("insert into t", testDMLResponse)
			dbClient.AddInvariant("update _vt.vreplication set pos=", testDMLResponse)
			dbClient.AddInvariant("commit", testDMLResponse)

			dbClientFactory := func() binlogplayer.DBClient { return dbClient }
			mysqld := &mysqlctl.FakeMysqlDaemon{}
			mysqld.MysqlPort.Store(3306)
			vre := NewTestEngine(nil, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

			defer setTabletTypesStr("replica")()

			ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
			require.NoError(t, err)

			expectedPicks := 2
			if !tc.shouldRetry && !tc.ignoreTablet {
				// For TabletErrorActionFail, we will only pick one tablet and stop.
				expectedPicks = 1
			}
			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(pickedTablets) >= expectedPicks
			}, 30*time.Second, 10*time.Millisecond)

			ct.Stop(true)

			mu.Lock()
			defer mu.Unlock()

			if tc.ignoreTablet {
				require.Equal(t, []uint32{100, 200}, pickedTablets[:2])
			} else if tc.shouldRetry {
				require.Equal(t, []uint32{100, 100}, pickedTablets[:2])
			} else {
				require.Equal(t, []uint32{100}, pickedTablets)
			}
		})
	}
}

// Test what happens when all tablets are added to the ignore list.
func TestControllerIgnoreListExhaustion(t *testing.T) {
	savedDelay := vttablet.DefaultVReplicationConfig.RetryDelay
	defer func() { vttablet.DefaultVReplicationConfig.RetryDelay = savedDelay }()
	vttablet.DefaultVReplicationConfig.RetryDelay = 10 * time.Millisecond

	// Reduce tablet picker retry delay to speed up the test.
	oldPickerDelay := discovery.GetTabletPickerRetryDelay()
	defer discovery.SetTabletPickerRetryDelay(oldPickerDelay)
	discovery.SetTabletPickerRetryDelay(10 * time.Millisecond)

	resetBinlogClient()

	ctx := t.Context()
	err := env.AddCell(ctx, "cell2")
	require.NoError(t, err)

	err = env.TopoServ.CreateCellsAlias(ctx, "region1", &topodatapb.CellsAlias{
		Cells: []string{"cell1", "cell2"},
	})
	require.NoError(t, err)
	defer env.TopoServ.DeleteCellsAlias(ctx, "region1")

	tablet1 := addTabletWithCell(100, env.Cells[0])
	tablet2 := addTabletWithCell(200, env.Cells[1])
	defer deleteTablet(tablet1)
	defer deleteTablet(tablet2)

	params := map[string]string{
		"id":           "1",
		"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"cell":         "",
		"tablet_types": "replica",
		"options":      "{}",
	}

	pickedTablets := []uint32{}
	var mu sync.Mutex

	oldErrorQueues := fakeBinlogClientErrorQueuesByTablet
	defer func() { fakeBinlogClientErrorQueuesByTablet = oldErrorQueues }()
	// Create a queue of errors for each tablet, after the queue is exhausted, the tablet will not return any errors.
	fakeBinlogClientErrorQueuesByTablet = map[uint32][]error{
		100: {vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.GTIDSetMismatch+" tablet 100")},
		200: {vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.GTIDSetMismatch+" tablet 200")},
	}

	oldCallback := fakeBinlogClientCallback
	defer func() { fakeBinlogClientCallback = oldCallback }()
	fakeBinlogClientCallback = func(tablet *topodatapb.Tablet) {
		mu.Lock()
		defer mu.Unlock()
		pickedTablets = append(pickedTablets, tablet.Alias.Uid)
	}

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.AddInvariant("update _vt.vreplication set message=", testDMLResponse)
	dbClient.AddInvariant("update _vt.vreplication set state=", testDMLResponse)
	dbClient.AddInvariant("select pos", testSettingsResponse)
	dbClient.AddInvariant("begin", testDMLResponse)
	dbClient.AddInvariant("insert into t", testDMLResponse)
	dbClient.AddInvariant("update _vt.vreplication set pos=", testDMLResponse)
	dbClient.AddInvariant("commit", testDMLResponse)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	defer setTabletTypesStr("replica")()

	ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)
	defer ct.Stop(true)

	require.Eventuallyf(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		// We expect to attempt each tablet (receive an error for each), and then reset the ignore list
		// so that we can attempt the tablets again. However, if the ignore list is not reset,
		// then this test will timeout because no other tablets will be picked.
		return slices.Equal(pickedTablets, []uint32{100, 200, 100})
	}, 5*time.Second, 10*time.Millisecond, "expected ignore list to be reset and tablet 100 to be picked again (sequence should be [100 200 100...])")
}

// Test that logged messages match the expected format.
func TestControllerErrorLogMessages(t *testing.T) {
	savedDelay := vttablet.DefaultVReplicationConfig.RetryDelay
	defer func() { vttablet.DefaultVReplicationConfig.RetryDelay = savedDelay }()
	vttablet.DefaultVReplicationConfig.RetryDelay = 10 * time.Millisecond

	resetBinlogClient()

	ctx := t.Context()
	err := env.AddCell(ctx, "cell2")
	require.NoError(t, err)

	err = env.TopoServ.CreateCellsAlias(ctx, "region1", &topodatapb.CellsAlias{
		Cells: []string{"cell1", "cell2"},
	})
	require.NoError(t, err)
	defer env.TopoServ.DeleteCellsAlias(ctx, "region1")

	tablet1 := addTabletWithCell(100, env.Cells[0])
	tablet2 := addTabletWithCell(200, env.Cells[1])
	defer deleteTablet(tablet1)
	defer deleteTablet(tablet2)

	workflowName := "test_workflow"
	streamID := "1"
	params := map[string]string{
		"id":           streamID,
		"workflow":     workflowName,
		"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" key_range:{end:"\x80"}`, env.KeyspaceName),
		"cell":         "",
		"tablet_types": "replica",
		"options":      "{}",
	}

	oldErrors := fakeBinlogClientErrorsByTablet
	defer func() { fakeBinlogClientErrorsByTablet = oldErrors }()
	fakeBinlogClientErrorsByTablet = map[uint32]error{
		100: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.GTIDSetMismatch+" test "+strings.Repeat("x", 1000)),
	}

	logs := logtest.Capture(t, slog.LevelInfo)

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.AddInvariant("update _vt.vreplication set message=", testDMLResponse)
	dbClient.AddInvariant("update _vt.vreplication set state=", testDMLResponse)
	dbClient.AddInvariant("select pos", testSettingsResponse)
	dbClient.AddInvariant("begin", testDMLResponse)
	dbClient.AddInvariant("insert into t", testDMLResponse)
	dbClient.AddInvariant("update _vt.vreplication set pos=", testDMLResponse)
	dbClient.AddInvariant("commit", testDMLResponse)

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	defer setTabletTypesStr("replica")()

	ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)

	ignored := logs.Wait(t, "adding tablet to ignore list due to error")
	retried := logs.Wait(t, "error, will retry")
	ct.Stop(true)

	for _, record := range []map[string]any{ignored, retried} {
		assert.Equal(t, workflowName, record["workflow"])
		assert.EqualValues(t, 1, record["stream_id"])
		assert.Contains(t, record["error"], vterrors.GTIDSetMismatch+" test")
	}
	assert.Equal(t, "INFO", ignored["level"])
	assert.Equal(t, "cell1-0000000100", ignored["tablet"])
	assert.Equal(t, "INVALID_ARGUMENT", ignored["error_code"])
	assert.Equal(t, "ERROR", retried["level"])
	assert.EqualValues(t, 10*time.Millisecond, retried["retry_delay"])
	assert.LessOrEqual(t, len(retried["error"].(string)), 950, "the retry error is truncated like the vreplication message")
}

// TestControllerSetStateErrorLogs checks that when the controller can't save a
// terminal error, it logs the save failure and the terminal error as separate
// records, each under error.
func TestControllerSetStateErrorLogs(t *testing.T) {
	savedDelay := vttablet.DefaultVReplicationConfig.RetryDelay
	defer func() { vttablet.DefaultVReplicationConfig.RetryDelay = savedDelay }()
	vttablet.DefaultVReplicationConfig.RetryDelay = time.Hour

	defer deleteTablet(addTablet(100))

	const rowValue = "zq7rowvalue"
	params := map[string]string{
		"id":           "1",
		"workflow":     "test_workflow",
		"state":        binlogdatapb.VReplicationWorkflowState_Running.String(),
		"source":       fmt.Sprintf(`keyspace:"%s" shard:"0" filter:{rules:{match:"t1"}}`, env.KeyspaceName),
		"cell":         "",
		"tablet_types": "replica",
		"options":      "{}",
	}

	logs := logtest.Capture(t, slog.LevelInfo)

	dbClient := binlogplayer.NewMockDBClient(t)
	dbClient.AddInvariant("update _vt.vreplication set message=", testDMLResponse)
	dbClient.AddInvariant("update _vt.vreplication set state='Running'", testDMLResponse)
	dbClient.AddInvariant("select pos", testSettingsResponse)
	dbClient.AddInvariant("set @@session.", testDMLResponse)
	dbClient.AddInvariant("set names", testDMLResponse)
	dbClient.AddInvariant("select rows_copied", sqltypes.MakeTestResult(sqltypes.MakeTestFields("rows_copied", "int64"), "0"))
	// An unrecoverable error, like a duplicate key carrying a row value, makes
	// the controller save the error, and saving it fails.
	dbClient.ExpectRequestRE("from _vt.copy_state", nil, sqlerror.NewSQLError(sqlerror.ERDupEntry, sqlerror.SSConstraintViolation, "Duplicate entry '"+rowValue+"' for key 'val'"))
	dbClient.ExpectRequestRE("update _vt.vreplication set state='Error'", nil, errors.New("zq7setstatefailure"))

	dbClientFactory := func() binlogplayer.DBClient { return dbClient }
	mysqld := &mysqlctl.FakeMysqlDaemon{}
	mysqld.MysqlPort.Store(3306)
	vre := NewTestEngine(nil, env.Cells[0], mysqld, dbClientFactory, dbClientFactory, dbClient.DBName(), nil)

	defer setTabletTypesStr("replica")()

	ct, err := newController(t.Context(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	require.NoError(t, err)

	setStateRecord := logs.Wait(t, "INTERNAL: unable to setState() in controller")
	errorTextRecord := logs.Wait(t, "INTERNAL: could not set error text")
	ct.Stop(true)

	for _, record := range []map[string]any{setStateRecord, errorTextRecord} {
		assert.Equal(t, "ERROR", record["level"])
		assert.Equal(t, "test_workflow", record["workflow"])
		assert.EqualValues(t, 1, record["stream_id"])
	}
	// Each record carries only its own error.
	assert.Equal(t, []string{"error", "level", "msg", "stream_id", "time", "workflow"}, slices.Sorted(maps.Keys(setStateRecord)))
	assert.Equal(t, []string{"error", "error_code", "level", "msg", "stream_id", "time", "workflow"}, slices.Sorted(maps.Keys(errorTextRecord)))
	assert.Contains(t, setStateRecord["error"], "zq7setstatefailure")
	assert.Contains(t, errorTextRecord["error"], rowValue)
	assert.NotContains(t, errorTextRecord["error"], "zq7setstatefailure")
	assert.Equal(t, "UNKNOWN", errorTextRecord["error_code"])
}

// TestTerminalVReplicationError tests the terminal error messages: the
// unrecoverable class keeps the "terminal error:" prefix the Online DDL
// executor's history scan matches; the resumable retries-exhausted class
// starts with its own marker — a resumable condition must not be labeled a
// terminal error — and names the flag bounding the retry window and its
// effective value.
func TestTerminalVReplicationError(t *testing.T) {
	assert.False(t, strings.HasPrefix(RetriesExhaustedIndicator, TerminalErrorIndicator),
		"the resumable marker must not be labeled with the legacy terminal prefix")
	// The unrecoverable class must read as terminal to every version.
	assert.True(t, strings.HasPrefix(UnrecoverableErrorIndicator, TerminalErrorIndicator))

	baseErr := errors.New("connection refused")

	err := terminalVReplicationError(baseErr, true, 15*time.Minute)
	require.ErrorContains(t, err, UnrecoverableErrorIndicator)
	require.ErrorContains(t, err, "connection refused")
	require.True(t, strings.HasPrefix(err.Error(), TerminalErrorIndicator+":"))

	err = terminalVReplicationError(baseErr, false, 15*time.Minute)
	require.ErrorContains(t, err, "--vreplication-max-time-to-retry-on-error (15m0s)")
	require.ErrorContains(t, err, "connection refused")
	require.True(t, strings.HasPrefix(err.Error(), RetriesExhaustedIndicator+":"))
	require.False(t, strings.HasPrefix(err.Error(), TerminalErrorIndicator))
}

// TestControllerRetryKeepsPickedNoticeFromHidingError checks the query text of
// the "Picked source tablet" notice: unconditional on a controller's first
// run, as before, and guarded on a retry, with a NULL message treated as
// empty.
func TestControllerRetryKeepsPickedNoticeFromHidingError(t *testing.T) {
	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	for _, retrying := range []bool{false, true} {
		t.Run(fmt.Sprintf("retrying=%t", retrying), func(t *testing.T) {
			dbClient := binlogplayer.NewMockDBClient(t)
			blpStats := binlogplayer.NewStats()
			defer blpStats.Stop()
			ct := &controller{id: 1, blpStats: blpStats, retrying: retrying}
			want := fmt.Sprintf("update _vt.vreplication set message=%s where id=1", encodeString("Picked source tablet: "+alias.String()))
			if retrying {
				want += " and (ifnull(message, '') = '' or message like 'Picked source tablet: %' or message like 'Error picking tablet%' or message like 'Error creating tablet picker%')"
			}
			dbClient.ExpectRequest(want, testDMLResponse, nil)
			require.NoError(t, ct.setPickedTabletMessage(dbClient, alias))
			dbClient.Wait()
		})
	}
}

// vreplicationRow reads a stream's state and message, with a NULL message
// returned as "NULL".
func vreplicationRow(t *testing.T, id int) (string, string) {
	t.Helper()
	qr, err := env.Mysqld.FetchSuperQuery(t.Context(), fmt.Sprintf("select state, ifnull(message, 'NULL') from _vt.vreplication where id=%d", id))
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	return qr.Rows[0][0].ToString(), qr.Rows[0][1].ToString()
}

// setGlobalThrottledCount gives globalStats a ThrottledCount for the test if
// it has none: the engine tests replace globalStats with an empty one, and
// recording a throttle increments its ThrottledCount.
func setGlobalThrottledCount(t *testing.T) {
	if globalStats.ThrottledCount != nil {
		return
	}
	savedGlobalStats := globalStats
	globalStats = &vrStats{ThrottledCount: stats.NewCounter("", "")}
	t.Cleanup(func() { globalStats = savedGlobalStats })
}

// TestRetryMessageHandling runs the message writes of a stream's run against
// MySQL, on a _vt.vreplication row that no controller is running, so that the
// predicates are checked against real rows, including NULL messages.
func TestRetryMessageHandling(t *testing.T) {
	ctx := t.Context()
	tablet := addTablet(100)
	t.Cleanup(func() { deleteTablet(tablet) })

	savedPickerDelay := discovery.GetTabletPickerRetryDelay()
	t.Cleanup(func() { discovery.SetTabletPickerRetryDelay(savedPickerDelay) })
	discovery.SetTabletPickerRetryDelay(10 * time.Millisecond)

	// Inserted directly, so the engine does not start a controller for it.
	bls := &binlogdatapb.BinlogSource{Keyspace: env.KeyspaceName, Shard: env.ShardName, Filter: &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{Match: "t1"}},
	}}
	insert := binlogplayer.CreateVReplicationState("retry_messages", bls, "", binlogdatapb.VReplicationWorkflowState_Stopped, vrepldb, 0, 0)
	qr, err := env.Mysqld.FetchSuperQuery(ctx, insert)
	require.NoError(t, err)
	id := int(qr.InsertID)
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanup functions run.
		require.NoError(t, env.Mysqld.ExecuteSuperQuery(context.WithoutCancel(ctx), fmt.Sprintf("delete from _vt.vreplication where id=%d", id)))
	})

	const (
		applyErr    = "error applying event: retryable failure"
		pickerErr   = "Error picking tablet: no healthy tablet"
		pickedOther = "Picked source tablet: zone1-0000000200"
		null        = "NULL"
	)
	running := binlogdatapb.VReplicationWorkflowState_Running.String()
	copying := binlogdatapb.VReplicationWorkflowState_Copying.String()

	dbClient := &realDBClient{nolog: true}
	require.NoError(t, dbClient.Connect())
	t.Cleanup(dbClient.Close)

	newCt := func(retrying bool, keyspace string) *controller {
		blpStats := binlogplayer.NewStats()
		t.Cleanup(blpStats.Stop)
		return &controller{
			vre:              playerEngine,
			id:               int32(id),
			blpStats:         blpStats,
			source:           &binlogdatapb.BinlogSource{Keyspace: keyspace, Shard: env.ShardName},
			retrying:         retrying,
			tpTs:             env.TopoServ,
			tpCells:          []string{env.Cells[0]},
			tpTabletTypesStr: "replica",
		}
	}
	pickTablet := func(retrying bool) func(t *testing.T) {
		return func(t *testing.T) {
			_, err := newCt(retrying, env.KeyspaceName).pickSourceTablet(ctx, dbClient)
			require.NoError(t, err)
		}
	}
	// No tablet serves this keyspace, so the picker fails and writes
	// "Error picking tablet: ...".
	pickNoTablet := func(retrying bool) func(t *testing.T) {
		return func(t *testing.T) {
			_, err := newCt(retrying, "no_tablets_ks").pickSourceTablet(ctx, dbClient)
			require.Error(t, err)
		}
	}
	setStateRunning := func(retrying bool) func(t *testing.T) {
		return func(t *testing.T) {
			stats := binlogplayer.NewStats()
			defer stats.Stop()
			vr := &vreplicator{id: int32(id), stats: stats, dbClient: newVDBClient(dbClient, stats, 0)}
			vr.preserveMessage.Store(retrying)
			require.NoError(t, vr.setState(binlogdatapb.VReplicationWorkflowState_Running, ""))
		}
	}
	copyProgress := func(rowsCopied int64) func(t *testing.T) {
		return func(t *testing.T) {
			stats := binlogplayer.NewStats()
			defer stats.Stop()
			vr := &vreplicator{id: int32(id), stats: stats, dbClient: newVDBClient(dbClient, stats, 0)}
			vr.preserveMessage.Store(true)
			stats.CopyRowCount.Set(rowsCopied)
			vr.clearPreservedMessageOnCopyProgress()
			require.Equal(t, rowsCopied == 0, vr.preserveMessage.Load())
		}
	}
	// newVR returns a vreplicator for the stream, for a retry if retrying
	// is set, that can record a throttle.
	newVR := func(t *testing.T, retrying bool) *vreplicator {
		playerStats := binlogplayer.NewStats()
		t.Cleanup(playerStats.Stop)
		vr := &vreplicator{
			id:                         int32(id),
			stats:                      playerStats,
			dbClient:                   newVDBClient(dbClient, playerStats, 0),
			workflowConfig:             vttablet.DefaultVReplicationConfig,
			throttleUpdatesRateLimiter: timer.NewRateLimiter(time.Second),
		}
		t.Cleanup(vr.throttleUpdatesRateLimiter.Stop)
		vr.preserveMessage.Store(retrying)
		setGlobalThrottledCount(t)
		return vr
	}
	// heartbeat applies a vstreamer heartbeat as the vplayer of a retry
	// does in the given phase, with or without a transaction open.
	heartbeat := func(phase string, throttled, inTransaction bool) func(t *testing.T) {
		return func(t *testing.T) {
			vr := newVR(t, true)
			if inTransaction {
				require.NoError(t, vr.dbClient.Begin())
				defer func() { require.NoError(t, vr.dbClient.Rollback()) }()
			}
			vp := &vplayer{vr: vr, phase: phase}
			require.NoError(t, vp.applyEvent(ctx, &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_HEARTBEAT, Throttled: throttled}, false))
			require.Equal(t, !throttled && (inTransaction || phase != replicatePhase), vr.preserveMessage.Load())
		}
	}
	// throttled records a throttle of the given component, as the copy
	// phase does for a denied vcopier check or a throttled rowstreamer
	// (TestPlayerCopyRetryThrottledClearsKeptError runs the denied vcopier
	// check end to end).
	throttled := func(retrying bool, app throttlerapp.Name) func(t *testing.T) {
		return func(t *testing.T) {
			vr := newVR(t, retrying)
			require.NoError(t, vr.updateTimeThrottled(app, "throttled for the test"))
			require.False(t, vr.preserveMessage.Load())
		}
	}
	// deniedVPlayerThrottleCheck runs the vplayer of a retry in the
	// replicate phase while the throttler denies its checks.
	deniedVPlayerThrottleCheck := func(t *testing.T) {
		vr := newVR(t, true)
		throttler := &fakeThrottleChecker{}
		throttler.deny.Store(true)
		vr.vre = &Engine{throttlerClient: throttler}
		vp := &vplayer{vr: vr, phase: replicatePhase, throttlerAppName: throttlerapp.VPlayerName.ConcatenateString(vr.throttlerAppName())}
		runCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		// The relay log is not read while the checks are denied.
		require.ErrorIs(t, vp.applyEvents(runCtx, nil), context.DeadlineExceeded)
		require.False(t, vr.preserveMessage.Load())
		qr, err := env.Mysqld.FetchSuperQuery(ctx, fmt.Sprintf("select component_throttled from _vt.vreplication where id=%d", id))
		require.NoError(t, err)
		require.Equal(t, throttlerapp.VPlayerName.String(), qr.Rows[0][0].ToString())
	}

	type testCase struct {
		name        string
		state       string
		message     string // null for a NULL message
		run         func(t *testing.T)
		wantState   string
		wantMessage string // "Picked" is the notice for the tablet picked here
	}
	tcases := []testCase{
		// A first run behaves as before: every write goes through.
		{"first run: picked notice replaces an old error", running, applyErr, pickTablet(false), running, "Picked"},
		{"first run: picked notice on a NULL message", running, null, pickTablet(false), running, "Picked"},
		{"first run: setState clears an old error", running, applyErr, setStateRunning(false), running, ""},
		// A retry keeps the error that made the previous run fail.
		{"retry: picked notice keeps an apply error", running, applyErr, pickTablet(true), running, applyErr},
		{"retry: picked notice on a NULL message", running, null, pickTablet(true), running, "Picked"},
		{"retry: picked notice on an empty message", running, "", pickTablet(true), running, "Picked"},
		{"retry: picked notice replaces a picker error", running, pickerErr, pickTablet(true), running, "Picked"},
		{"retry: picked notice replaces an older notice", running, pickedOther, pickTablet(true), running, "Picked"},
		{"retry: picker error replaces an apply error", running, applyErr, pickNoTablet(true), running, "Error picking tablet: "},
		{"retry: picker error on a NULL message", running, null, pickNoTablet(true), running, "Error picking tablet: "},
		{"first run: picker error on a NULL message", running, null, pickNoTablet(false), running, "Error picking tablet: "},
		{"retry: setState keeps an apply error", running, applyErr, setStateRunning(true), running, applyErr},
		{"retry: setState on a NULL message", running, null, setStateRunning(true), running, ""},
		{"retry: setState to a new state clears the message", copying, applyErr, setStateRunning(true), running, ""},
		{"retry: copy progress clears a kept error", copying, applyErr, copyProgress(10), copying, ""},
		{"retry: no copy progress keeps the error", copying, applyErr, copyProgress(0), copying, applyErr},
		{"retry: idle heartbeat clears a kept error", running, applyErr, heartbeat(replicatePhase, false, false), running, ""},
		{"retry: throttled heartbeat clears a kept error", running, applyErr, heartbeat(replicatePhase, true, false), running, ""},
		{"retry: throttled heartbeat in the copy phase clears a kept error", copying, applyErr, heartbeat("catchup", true, false), copying, ""},
		{"retry: heartbeat in a transaction keeps the error", running, applyErr, heartbeat(replicatePhase, false, true), running, applyErr},
		{"retry: idle heartbeat in the copy phase keeps the error", copying, applyErr, heartbeat("catchup", false, false), copying, applyErr},
		// A retry that is throttled may wait for as long as the throttling
		// lasts, and time_throttled then explains why it does not progress.
		{"retry: denied vplayer throttle check clears a kept error", running, applyErr, deniedVPlayerThrottleCheck, running, ""},
		{"retry: vcopier throttle clears a kept error", copying, applyErr, throttled(true, throttlerapp.VCopierName), copying, ""},
		{"retry: throttled rowstreamer clears a kept error", copying, applyErr, throttled(true, throttlerapp.RowStreamerName), copying, ""},
		{"first run: throttle keeps the message", copying, pickedOther, throttled(false, throttlerapp.VCopierName), copying, pickedOther},
	}
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			message := encodeString(tc.message)
			if tc.message == null {
				message = "NULL"
			}
			require.NoError(t, env.Mysqld.ExecuteSuperQuery(ctx, fmt.Sprintf("update _vt.vreplication set state=%s, message=%s where id=%d",
				encodeString(tc.state), message, id)))
			tc.run(t)
			gotState, gotMessage := vreplicationRow(t, id)
			assert.Equal(t, tc.wantState, gotState)
			switch tc.wantMessage {
			case "Picked":
				assert.Equal(t, pickedTabletMessagePrefix+tablet.Alias.String(), gotMessage)
			case "Error picking tablet: ":
				assert.True(t, strings.HasPrefix(gotMessage, tc.wantMessage), "message: %q", gotMessage)
			default:
				assert.Equal(t, tc.wantMessage, gotMessage)
			}
		})
	}
}

// TestEngineExecRestartClearsOldError restarts a Running stream that still
// shows an old apply error through Engine.Exec, as Workflow start and update
// do, with no writes on the source. The new controller's first run must clear
// the message right away, without waiting for a position save.
func TestEngineExecRestartClearsOldError(t *testing.T) {
	ctx := t.Context()
	defer deleteTablet(addTablet(100))

	// Source and target are the same MySQL server here, so the test's own
	// writes reach the vplayer as empty transactions, and it saves their
	// position (clearing the message) after idleTimeout. Raise it so that
	// only the restart itself can clear the message within the deadline.
	savedIdleTimeout := idleTimeout
	defer func() { idleTimeout = savedIdleTimeout }()
	idleTimeout = 30 * time.Second

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t1",
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})

	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{Match: "t1"}},
		},
	}
	cancel, id := startVReplication(t, bls, "")
	defer cancel()

	const oldErr = "error applying event: old failure"
	require.NoError(t, env.Mysqld.ExecuteSuperQuery(ctx, fmt.Sprintf("update _vt.vreplication set message=%s where id=%d", encodeString(oldErr), id)))
	_, message := vreplicationRow(t, id)
	require.Equal(t, oldErr, message)

	_, err := playerEngine.Exec(fmt.Sprintf("update _vt.vreplication set state='Running' where id=%d", id))
	require.NoError(t, err)
	expectDBClientQueries(t, qh.Expect(
		"/update _vt.vreplication set state = 'Running' where id in",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running', message=left\\('', 1000\\) where id=",
	))

	// Nothing but the restart can clear the message within this deadline.
	require.Eventually(t, func() bool {
		state, message := vreplicationRow(t, id)
		return state == binlogdatapb.VReplicationWorkflowState_Running.String() && message == ""
	}, 5*time.Second, 10*time.Millisecond, "old error not cleared by a fresh start")
}

// TestControllerRetryAfterThrottleGiveUpClearsMessage checks the message a
// retry starts with, read on a separate connection right before the retry
// streams from the source. After the source vstreamer gave up because it was
// fully throttled, the retry clears the message as on a first run, whether
// the error came from a local or a remote source. After another retryable
// error, the retry keeps it.
func TestControllerRetryAfterThrottleGiveUpClearsMessage(t *testing.T) {
	throttleGiveUp := vterrors.Wrapf(
		vterrors.Errorf(vtrpcpb.Code_INTERNAL, "%s %v, giving up so that we can retry", vstreamer.FullyThrottledErrorPrefix, 10*time.Minute),
		"stream (at source tablet) error @ (including the GTID we failed to process) %v", "MySQL56/00000000-0000-0000-0000-000000000001:1-5")
	tcases := []struct {
		name      string
		err       error
		keepError bool
	}{
		{"local throttle give-up", throttleGiveUp, false},
		{"remote throttle give-up", tabletconn.ErrorFromGRPC(vterrors.ToGRPC(throttleGiveUp)), false},
		{"other retryable error", vterrors.New(vtrpcpb.Code_UNKNOWN, "vttablet: rpc error: code = Unknown desc = retryable stream failure"), true},
	}
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			defer deleteTablet(addTablet(100))

			savedRetryDelay := vttablet.DefaultVReplicationConfig.RetryDelay
			defer func() { vttablet.DefaultVReplicationConfig.RetryDelay = savedRetryDelay }()
			vttablet.DefaultVReplicationConfig.RetryDelay = 10 * time.Millisecond

			// Every run fails on the source with tc.err. Right before the
			// retry streams, read the message the retry has left.
			var (
				mu       sync.Mutex
				streamID int64
				calls    int
			)
			retryMessage := make(chan string, 1)
			vstreamErrorsByTablet = map[uint32]error{100: tc.err}
			vstreamHook = func(context.Context) {
				mu.Lock()
				calls++
				call, id := calls, streamID
				mu.Unlock()
				if call != 2 {
					return
				}
				qr, err := env.Mysqld.FetchSuperQuery(context.Background(), fmt.Sprintf("select ifnull(message, '') from _vt.vreplication where id=%d", id))
				if err == nil && len(qr.Rows) == 1 {
					retryMessage <- qr.Rows[0][0].ToString()
				}
			}
			defer func() {
				vstreamHook = nil
				vstreamErrorsByTablet = nil
			}()

			bls := &binlogdatapb.BinlogSource{Keyspace: env.KeyspaceName, Shard: env.ShardName, Filter: &binlogdatapb.Filter{
				Rules: []*binlogdatapb.Rule{{Match: "t1"}},
			}}
			mu.Lock()
			qr, err := playerEngine.Exec(binlogplayer.CreateVReplication("test", bls, primaryPosition(t), 9223372036854775807, 9223372036854775807, 0, vrepldb,
				binlogdatapb.VReplicationWorkflowType_MoveTables, 0, false))
			if err == nil {
				streamID = int64(qr.InsertID)
			}
			mu.Unlock()
			require.NoError(t, err)
			defer func() {
				_, err := playerEngine.Exec(fmt.Sprintf("delete from _vt.vreplication where id = %d", qr.InsertID))
				require.NoError(t, err)
				timeout := time.After(recvTimeout)
				for {
					select {
					case got := <-globalDBQueries:
						if strings.HasPrefix(got, "delete from _vt.post_copy_action") {
							drainQuietDBQueries(t)
							return
						}
					case <-timeout:
						require.FailNow(t, "the stream was not deleted")
					}
				}
			}()

			// Drain the query log until the retry has read its message.
			var written, message string
			timeout := time.After(10 * time.Second)
			for got := false; !got; {
				select {
				case q := <-globalDBQueries:
					if written == "" && strings.HasPrefix(q, "update _vt.vreplication set message=") && strings.Contains(q, "stream") {
						written = q
					}
				case message = <-retryMessage:
					got = true
				case <-timeout:
					require.FailNow(t, "the stream did not retry")
				}
			}
			t.Logf("message written by the failed run: %s", written)
			if tc.keepError {
				require.Equal(t, binlogplayer.MessageTruncate(tc.err.Error()), message)
				return
			}
			require.Empty(t, message, "the retry kept the message of a throttle give-up")
		})
	}
}

// drainQuietDBQueries drains the queries a test's stream left in
// globalDBQueries, such as the commit of its delete, so that they do not
// reach the expectations of the next test.
func drainQuietDBQueries(t *testing.T) {
	t.Helper()
	for {
		select {
		case <-globalDBQueries:
		case <-time.After(200 * time.Millisecond):
			return
		}
	}
}
