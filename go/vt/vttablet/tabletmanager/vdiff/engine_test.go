/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestEngineOpen(t *testing.T) {
	vdenv := newTestVDiffEnv(t)
	defer vdenv.close()
	UUID := uuid.New().String()
	tests := []struct {
		name  string
		state VDiffState
	}{
		// This needs to be started, for the first time, on open
		{
			name:  "pending vdiff",
			state: PendingState,
		},
		// This needs to be restarted on open as it was previously started
		// but was unable to terminate normally (e.g. crash) in the previous
		// engine.
		{
			name:  "started vdiff",
			state: StartedState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vdenv.dbClient = binlogplayer.NewMockDBClient(t)
			vdenv.vde.Close() // ensure we close any open one
			vdenv.vde = nil
			vdenv.vde = NewTestEngine(tstenv.TopoServ, vdenv.tablets[100].tablet, vdiffDBName, vdenv.dbClientFactory, vdenv.tmClientFactory)
			require.False(t, vdenv.vde.IsOpen())

			initialQR := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				vdiffTestCols,
				vdiffTestColTypes,
			),
				fmt.Sprintf("1|%s|%s|%s|%s|%s|%s|%s|", UUID, vdenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, tt.state, optionsJS),
			)

			vdenv.dbClient.ExpectRequest("select * from _vt.vdiff where state in ('started','pending')", initialQR, nil)
			vdenv.dbClient.ExpectRequest("select * from _vt.vdiff where id = 1", sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				vdiffTestCols,
				vdiffTestColTypes,
			),
				fmt.Sprintf("1|%s|%s|%s|%s|%s|%s|%s|", UUID, vdiffenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, tt.state, optionsJS),
			), nil)
			vdenv.dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where workflow = '%s' and db_name = '%s'", vdiffenv.workflow, vdiffDBName), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				"id|workflow|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type",
				"int64|varbinary|blob|varbinary|varbinary|int64|int64|varbinary|varbinary|int64|int64|varbinary|varbinary|varbinary|int64|varbinary|int64|int64|int64|varchar|int64",
			),
				fmt.Sprintf("1|%s|%s|%s||9223372036854775807|9223372036854775807||PRIMARY,REPLICA|1669511347|0|Running||%s|200||1669511347|1|0||1", vdiffenv.workflow, vreplSource, vdiffSourceGtid, vdiffDBName),
			), nil)

			// Now let's short circuit the vdiff as we know that the open has worked as expected.
			shortCircuitTestAfterQuery("update _vt.vdiff set state = 'started', last_error = '' , started_at = utc_timestamp() where id = 1", vdiffenv.dbClient)

			vdenv.vde.Open(context.Background(), vdiffenv.vre)
			defer vdenv.vde.Close()
			assert.True(t, vdenv.vde.IsOpen())
			assert.Equal(t, 1, len(vdenv.vde.controllers))
			vdenv.dbClient.Wait()
		})
	}
}

// Test the full set of VDiff queries on a tablet.
func TestVDiff(t *testing.T) {
	vdenv := newTestVDiffEnv(t)
	defer vdenv.close()
	UUID := uuid.New().String()
	options := &tabletmanagerdatapb.VDiffOptions{
		CoreOptions: &tabletmanagerdatapb.VDiffCoreOptions{
			Tables:         "t1",
			TimeoutSeconds: 60,
			MaxRows:        100,
		},
		PickerOptions: &tabletmanagerdatapb.VDiffPickerOptions{
			SourceCell:  tstenv.Cells[0],
			TargetCell:  tstenv.Cells[0],
			TabletTypes: "primary",
		},
		ReportOptions: &tabletmanagerdatapb.VDiffReportOptions{
			DebugQuery: false,
			Format:     "json",
		},
	}

	controllerQR := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		vdiffTestCols,
		vdiffTestColTypes,
	),
		fmt.Sprintf("1|%s|%s|%s|%s|%s|pending|%s|", UUID, vdenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, optionsJS),
	)

	vdenv.dbClient.ExpectRequest("select * from _vt.vdiff where id = 1", controllerQR, nil)
	vdenv.dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where workflow = '%s' and db_name = '%s'", vdiffenv.workflow, vdiffDBName), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|workflow|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type",
		"int64|varbinary|blob|varbinary|varbinary|int64|int64|varbinary|varbinary|int64|int64|varbinary|varbinary|varbinary|int64|varbinary|int64|int64|int64|varchar|int64",
	),
		fmt.Sprintf("1|%s|%s|%s||9223372036854775807|9223372036854775807||PRIMARY,REPLICA|1669511347|0|Running||%s|200||1669511347|1|0||1", vdiffenv.workflow, vreplSource, vdiffSourceGtid, vdiffDBName),
	), nil)
	vdenv.dbClient.ExpectRequest("update _vt.vdiff set state = 'started', last_error = '' , started_at = utc_timestamp() where id = 1", singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest("insert into _vt.vdiff_log(vdiff_id, message) values (1, 'State changed to: started')", singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest(`select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = 1 and vdt.table_name = 't1'`, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"lastpk|mismatch|report",
		"varbinary|int64|json",
	),
		`fields:{name:"c1" type:INT64 table:"t1" org_table:"t1" database:"vt_customer" org_name:"c1" column_length:20 charset:63 flags:53251} rows:{lengths:1 values:"1"}|0|{}`,
	), nil)
	vdenv.dbClient.ExpectRequest(fmt.Sprintf("select column_name as column_name, collation_name as collation_name from information_schema.columns where table_schema='%s' and table_name='t1' and column_name in ('c1')", vdiffDBName), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"collation_name",
		"varchar",
	),
		"NULL",
	), nil)
	vdenv.dbClient.ExpectRequest(fmt.Sprintf("select table_name as table_name, table_rows as table_rows from INFORMATION_SCHEMA.TABLES where table_schema = '%s' and table_name in ('t1')", vdiffDBName), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"table_name|table_rows",
		"varchar|int64",
	),
		"t1|1",
	), nil)
	vdenv.dbClient.ExpectRequest(`select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = 1 and vdt.table_name = 't1'`, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"lastpk|mismatch|report",
		"varbinary|int64|json",
	),
		`fields:{name:"c1" type:INT64 table:"t1" org_table:"t1" database:"vt_customer" org_name:"c1" column_length:20 charset:63 flags:53251} rows:{lengths:1 values:"1"}|0|{"TableName": "t1", "MatchingRows": 1, "ProcessedRows": 1, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
	), nil)

	vdenv.dbClient.ExpectRequest("update _vt.vdiff_table set table_rows = 1 where vdiff_id = 1 and table_name = 't1'", singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest(`select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = 1 and vdt.table_name = 't1'`, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"lastpk|mismatch|report",
		"varbinary|int64|json",
	),
		`fields:{name:"c1" type:INT64 table:"t1" org_table:"t1" database:"vt_customer" org_name:"c1" column_length:20 charset:63 flags:53251} rows:{lengths:1 values:"1"}|0|{"TableName": "t1", "MatchingRows": 1, "ProcessedRows": 1, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`,
	), nil)
	vdenv.dbClient.ExpectRequest("update _vt.vdiff_table set state = 'started' where vdiff_id = 1 and table_name = 't1'", singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest(`insert into _vt.vdiff_log(vdiff_id, message) values (1, 'started: table \'t1\'')`, singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest(fmt.Sprintf("select id, source, pos from _vt.vreplication where workflow = '%s' and db_name = '%s'", vdiffenv.workflow, vdiffDBName), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|source|pos",
		"int64|varbinary|varbinary",
	),
		fmt.Sprintf("1|%s|%s", vreplSource, vdiffSourceGtid),
	), nil)
	vdenv.dbClient.ExpectRequest(`select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from _vt.vdiff as vd inner join _vt.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = 1 and vdt.table_name = 't1'`, sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"lastpk|mismatch|report",
		"varbinary|int64|json",
	),
		`fields:{name:"c1" type:INT64 table:"t1" org_table:"t1" database:"vt_customer" org_name:"c1" column_length:20 charset:63 flags:53251} rows:{lengths:1 values:"1"}|0|{}`,
	), nil)
	vdenv.dbClient.ExpectRequest(`update _vt.vdiff_table set rows_compared = 0, report = '{\"TableName\":\"t1\",\"ProcessedRows\":0,\"MatchingRows\":0,\"MismatchedRows\":0,\"ExtraRowsSource\":0,\"ExtraRowsTarget\":0}' where vdiff_id = 1 and table_name = 't1'`, singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest(`update _vt.vdiff_table set state = 'completed', rows_compared = 0, report = '{\"TableName\":\"t1\",\"ProcessedRows\":0,\"MatchingRows\":0,\"MismatchedRows\":0,\"ExtraRowsSource\":0,\"ExtraRowsTarget\":0}' where vdiff_id = 1 and table_name = 't1'`, singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest(`insert into _vt.vdiff_log(vdiff_id, message) values (1, 'completed: table \'t1\'')`, singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest("update _vt.vdiff_table set state = 'completed' where vdiff_id = 1 and table_name = 't1'", singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest(`insert into _vt.vdiff_log(vdiff_id, message) values (1, 'completed: table \'t1\'')`, singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest("select table_name as table_name from _vt.vdiff_table where vdiff_id = 1 and state != 'completed'", singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest("update _vt.vdiff set state = 'completed', last_error = '' , completed_at = utc_timestamp() where id = 1", singleRowAffected, nil)
	vdenv.dbClient.ExpectRequest("insert into _vt.vdiff_log(vdiff_id, message) values (1, 'State changed to: completed')", singleRowAffected, nil)

	vdenv.vde.mu.Lock()
	err := vdenv.vde.addController(controllerQR.Named().Row(), options)
	vdenv.vde.mu.Unlock()
	require.NoError(t, err)

	vdenv.dbClient.Wait()
}

func TestEngineRetryErroredVDiffs(t *testing.T) {
	vdenv := newTestVDiffEnv(t)
	defer vdenv.close()
	UUID := uuid.New().String()
	expectedControllerCnt := 0
	tests := []struct {
		name              string
		retryQueryResults *sqltypes.Result
		expectRetry       bool
	}{
		{
			name:              "nothing to retry",
			retryQueryResults: noResults,
		},
		{
			name: "non-ephemeral error",
			retryQueryResults: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				vdiffTestCols,
				vdiffTestColTypes,
			),
				fmt.Sprintf("1|%s|%s|%s|%s|%s|error|%s|%v", UUID, vdiffenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, optionsJS,
					mysql.NewSQLError(mysql.ERNoSuchTable, "42S02", "Table 'foo' doesn't exist")),
			),
		},
		{
			name: "ephemeral error",
			retryQueryResults: sqltypes.MakeTestResult(sqltypes.MakeTestFields(
				vdiffTestCols,
				vdiffTestColTypes,
			),
				fmt.Sprintf("1|%s|%s|%s|%s|%s|error|%s|%v", UUID, vdiffenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, optionsJS,
					mysql.NewSQLError(mysql.ERLockWaitTimeout, "HY000", "Lock wait timeout exceeded; try restarting transaction")),
			),
			expectRetry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vdiffenv.dbClient.ExpectRequest("select * from _vt.vdiff where state = 'error' and json_unquote(json_extract(options, '$.core_options.auto_retry')) = 'true'", tt.retryQueryResults, nil)

			// Right now this only supports a single row as with multiple rows we have
			// multiple controllers in separate goroutines and the order is not
			// guaranteed. If we want to support multiple rows here then we'll need to
			// switch to using the queryhistory package. That will also require building
			// out that package to support MockDBClient and its Expect* functions
			// (query+results+err) as right now it only supports a real DBClient and
			// checks for query execution.
			for _, row := range tt.retryQueryResults.Rows {
				id := row[0].ToString()
				if tt.expectRetry {
					vdiffenv.dbClient.ExpectRequestRE("update _vt.vdiff as vd left join _vt.vdiff_table as vdt on \\(vd.id = vdt.vdiff_id\\) set vd.state = 'pending'.*", singleRowAffected, nil)
					vdiffenv.dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vdiff where id = %s", id), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						vdiffTestCols,
						vdiffTestColTypes,
					),
						fmt.Sprintf("%s|%s|%s|%s|%s|%s|pending|%s|", id, UUID, vdiffenv.workflow, tstenv.KeyspaceName, tstenv.ShardName, vdiffDBName, optionsJS),
					), nil)
					vdiffenv.dbClient.ExpectRequest(fmt.Sprintf("select * from _vt.vreplication where workflow = '%s' and db_name = '%s'", vdiffenv.workflow, vdiffDBName), sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"id|workflow|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type",
						"int64|varbinary|blob|varbinary|varbinary|int64|int64|varbinary|varbinary|int64|int64|varbinary|varbinary|varbinary|int64|varbinary|int64|int64|int64|varchar|int64",
					),
						fmt.Sprintf("%s|%s|%s|%s||9223372036854775807|9223372036854775807||PRIMARY,REPLICA|1669511347|0|Running||%s|200||1669511347|1|0||1", id, vdiffenv.workflow, vreplSource, vdiffSourceGtid, vdiffDBName),
					), nil)

					// At this point we know that we kicked off the expected retry so we can short circit the vdiff.
					shortCircuitTestAfterQuery(fmt.Sprintf("update _vt.vdiff set state = 'started', last_error = '' , started_at = utc_timestamp() where id = %s", id), vdiffenv.dbClient)

					expectedControllerCnt++
				}
			}

			err := vdiffenv.vde.retryVDiffs(vdiffenv.vde.ctx)
			assert.NoError(t, err)
			assert.Equal(t, expectedControllerCnt, len(vdiffenv.vde.controllers))
			vdiffenv.dbClient.Wait()
		})
	}

}
