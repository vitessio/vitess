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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	"vitess.io/vitess/go/sqltypes"
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
	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	if err != nil {
		t.Fatal(err)
	}
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
	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	if err != nil {
		t.Fatal(err)
	}
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
	_, err := newController(context.Background(), params, nil, nil, nil, "", nil, nil, defaultTabletPickerOptions)
	want := `strconv.ParseInt: parsing "bad": invalid syntax`
	if err == nil || err.Error() != want {
		t.Errorf("newController err: %v, want %v", err, want)
	}
}

func TestControllerStopped(t *testing.T) {
	params := map[string]string{
		"id":      "1",
		"state":   binlogdatapb.VReplicationWorkflowState_Stopped.String(),
		"options": "{}",
	}

	ct, err := newController(context.Background(), params, nil, nil, nil, "", nil, nil, defaultTabletPickerOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop(true)

	select {
	case <-ct.done:
	default:
		t.Errorf("context should be closed, but is not: %v", ct)
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
	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	if err != nil {
		t.Fatal(err)
	}
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

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	vre := NewTestEngine(nil, wantTablet.GetAlias().Cell, nil, nil, nil, "", nil)

	ct, err := newController(ctx, params, nil, nil, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer ct.Stop(true)

	select {
	case <-ct.done:
	case <-time.After(1 * time.Second):
		t.Errorf("context should be closed, but is not: %v", ct)
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
	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	if err != nil {
		t.Fatal(err)
	}
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

	ct, err := newController(context.Background(), params, dbClientFactory, mysqld, env.TopoServ, env.Cells[0], nil, vre, defaultTabletPickerOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		dbClient.ExpectRequest("update _vt.vreplication set state='Stopped', message='context canceled' where id=1", testDMLResponse, nil)
		ct.Stop(true)
	}()

	// Also confirm that replication stopped.
	select {
	case <-ct.done:
	case <-time.After(1 * time.Second):
		t.Errorf("context should be closed, but is not: %v", ct)
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
			if err != nil {
				t.Fatal(err)
			}

			select {
			case <-ct.done:
				if tc.expectRetry {
					t.Fatalf("Controller stopped unexpectedly, expected it to keep retrying")
				}
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

				if !foundExpectedErr {
					t.Fatalf("Expected error containing %q in history, but last message was: %s", tc.expectedErrSubstr, lastMsg)
				}

				if !tc.expectRetry {
					t.Fatalf("Expected controller to fail immediately, but it kept retrying. Last error: %s", lastMsg)
				}
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
			msg:          "GTIDSet Mismatch aa",
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
			if err != nil {
				t.Fatal(err)
			}

			err = env.TopoServ.CreateCellsAlias(ctx, "region1", &topodatapb.CellsAlias{
				Cells: []string{"cell1", "cell2"},
			})
			if err != nil {
				t.Fatal(err)
			}
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
			}, 500*time.Millisecond, 10*time.Millisecond)

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
