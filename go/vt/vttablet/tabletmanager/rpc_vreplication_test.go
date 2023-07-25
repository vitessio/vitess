/*
Copyright 2023 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtctl/workflow"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

const (
	insertVReplicationPrefix = "insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys)"
	getWorkflow              = "select id from _vt.vreplication where db_name='vt_%s' and workflow='%s'"
	checkForWorkflow         = "select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'"
	checkForFrozenWorkflow   = "select 1 from _vt.vreplication where db_name='vt_%s' and message='FROZEN' and workflow_sub_type != 1"
	checkForJournal          = "/select val from _vt.resharding_journal where id="
	getWorkflowStatus        = "select id, workflow, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, message, tags, workflow_type, workflow_sub_type from _vt.vreplication where workflow = '%s' and db_name = 'vt_%s'"
	getWorkflowState         = "select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1"
	getCopyState             = "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
	getNumCopyStateTable     = "select count(distinct table_name) from _vt.copy_state where vrepl_id=1"
	getLatestCopyState       = "select table_name, lastpk from _vt.copy_state where vrepl_id = 1 and id in (select max(id) from _vt.copy_state where vrepl_id = 1 group by vrepl_id, table_name)"
	getAutoIncrementStep     = "select @@session.auto_increment_increment"
	setSessionTZ             = "set @@session.time_zone = '+00:00'"
	setNames                 = "set names 'binary'"
	setSQLMode               = "set @@session.sql_mode = CONCAT(@@session.sql_mode, ',NO_AUTO_VALUE_ON_ZERO')"
	setPermissiveSQLMode     = "SET @@session.sql_mode='NO_AUTO_VALUE_ON_ZERO'"
	setStrictSQLMode         = "SET @@session.sql_mode='ONLY_FULL_GROUP_BY,NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
	getSQLMode               = "SELECT @@session.sql_mode AS sql_mode"
	getFKChecks              = "select @@foreign_key_checks;"
	disableFKChecks          = "set foreign_key_checks=1;"
	sqlMode                  = "ONLY_FULL_GROUP_BY,NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
	getBinlogRowImage        = "select @@binlog_row_image"
	insertStreamsCreatedLog  = "insert into _vt.vreplication_log(vrepl_id, type, state, message) values(1, 'Stream Created', '', '%s'"
	getVReplicationRecord    = "select * from _vt.vreplication where id = 1"
	startWorkflow            = "update _vt.vreplication set state='Running' where db_name='vt_%s' and workflow='%s'"

	position = "MySQL56/9d10e6ec-07a0-11ee-ae73-8e53f4cf3083:1-97"
)

var (
	errShortCircuit = fmt.Errorf("short circuiting test")
	defaultSchema   = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:              "t1",
				Columns:           []string{"c1", "c2"},
				PrimaryKeyColumns: []string{"c1"},
				Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			},
		},
	}
)

// TestCreateVReplicationWorkflow tests the query generated
// from a VtctldServer MoveTablesCreate request to ensure
// that the VReplication stream(s) are created correctly.
func TestCreateVReplicationWorkflow(t *testing.T) {
	ctx := context.Background()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetTabletUID := 300
	shard := "0"
	wf := "testwf"
	defaultSchema := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:              "t1",
				Columns:           []string{"c1", "c2"},
				PrimaryKeyColumns: []string{"c1"},
				Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			},
		},
	}
	tenv := newTestEnv(t, targetKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(tenv.ts, tenv.tmc)

	tests := []struct {
		name   string
		req    *vtctldatapb.MoveTablesCreateRequest
		schema *tabletmanagerdatapb.SchemaDefinition
		query  string
	}{
		{
			name: "defaults",
			req: &vtctldatapb.MoveTablesCreateRequest{
				SourceKeyspace: sourceKs,
				TargetKeyspace: targetKs,
				Workflow:       wf,
				Cells:          tenv.cells,
				AllTables:      true,
			},
			query: fmt.Sprintf(`%s values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"}}', '', 0, 0, '%s', '', now(), 0, 'Stopped', '%s', 1, 0, 0)`,
				insertVReplicationPrefix, wf, sourceKs, shard, tenv.cells[0], tenv.dbName),
		},
		{
			name: "all values",
			schema: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name:              "t1",
						Columns:           []string{"c1", "c2"},
						PrimaryKeyColumns: []string{"c1"},
						Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
					},
					{
						Name:              "wut",
						Columns:           []string{"c1"},
						PrimaryKeyColumns: []string{"c1"},
						Fields:            sqltypes.MakeTestFields("c1", "int64"),
					},
				},
			},
			req: &vtctldatapb.MoveTablesCreateRequest{
				SourceKeyspace:     sourceKs,
				TargetKeyspace:     targetKs,
				Workflow:           wf,
				Cells:              tenv.cells,
				IncludeTables:      []string{defaultSchema.TableDefinitions[0].Name},
				ExcludeTables:      []string{"wut"},
				SourceTimeZone:     "EDT",
				OnDdl:              binlogdatapb.OnDDLAction_EXEC.String(),
				StopAfterCopy:      true,
				DropForeignKeys:    true,
				DeferSecondaryKeys: true,
				AutoStart:          true,
			},
			query: fmt.Sprintf(`%s values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"}} on_ddl:EXEC stop_after_copy:true source_time_zone:\"EDT\" target_time_zone:\"UTC\"', '', 0, 0, '%s', '', now(), 0, 'Stopped', '%s', 1, 0, 1)`,
				insertVReplicationPrefix, wf, sourceKs, shard, tenv.cells[0], tenv.dbName),
		},
	}

	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'",
		targetKs, wf), &sqltypes.Result{})
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and message='FROZEN' and workflow_sub_type != 1",
		targetKs), &sqltypes.Result{})
	tenv.tmc.setVReplicationExecResults(sourceTablet.tablet, "select val from _vt.resharding_journal where id=7224776740563431192", &sqltypes.Result{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This is needed because MockDBClient uses t.Fatal()
			// which doesn't play well with subtests.
			defer func() {
				if err := recover(); err != nil {
					t.Errorf("Recovered from panic: %v; Stack: %s", err, string(debug.Stack()))
				}
			}()

			require.NotNil(t, tt.req, "No MoveTablesCreate request provided")
			require.NotEmpty(t, tt.query, "No expected query provided")

			if tt.schema == nil {
				tt.schema = defaultSchema
			}
			tenv.tmc.SetSchema(tt.schema)

			tenv.vrdbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
			// This is our expected query, which will also short circuit
			// the test with an error as at this point we've tested what
			// we wanted to test.
			tenv.vrdbClient.ExpectRequest(tt.query, nil, errShortCircuit)
			_, err := ws.MoveTablesCreate(ctx, tt.req)
			tenv.vrdbClient.Wait()
			require.ErrorIs(t, err, errShortCircuit)
		})
	}
}

// TestMoveTables tests the query generated from a VtctldServer
// MoveTablesCreate request to ensure that the VReplication
// stream(s) are created correctly. Followed by ensuring that
// SwitchTraffic and ReverseTraffic work as expected.
func TestMoveTables(t *testing.T) {
	ctx := context.Background()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetTabletUID := 300
	shard := "0"
	wf := "testwf"

	tenv := newTestEnv(t, targetKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(tenv.ts, tenv.tmc)

	tenv.mysqld.Schema = defaultSchema
	tenv.mysqld.Schema.DatabaseSchema = tenv.dbName
	tenv.mysqld.FetchSuperQueryMap = make(map[string]*sqltypes.Result)
	tenv.mysqld.FetchSuperQueryMap[`select character_set_name, collation_name, column_name, data_type, column_type, extra from information_schema.columns where .*`] = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"character_set_name|collation_name|column_name|data_type|column_type|extra",
			"varchar|varchar|varchar|varchar|varchar|varchar",
		),
		"NULL|NULL|c1|bigint|bigint|",
		"NULL|NULL|c2|bigint|bigint|",
	)

	req := &vtctldatapb.MoveTablesCreateRequest{
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		Workflow:       wf,
		Cells:          tenv.cells,
		AllTables:      true,
		AutoStart:      true,
	}
	insert := fmt.Sprintf(`%s values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"}}', '', 0, 0, '%s', '', now(), 0, 'Stopped', '%s', 1, 0, 0)`,
		insertVReplicationPrefix, wf, sourceKs, shard, tenv.cells[0], tenv.dbName)
	bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"}}", sourceKs, shard)

	tenv.tmc.SetSchema(defaultSchema)

	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, fmt.Sprintf(checkForWorkflow, targetKs, wf), &sqltypes.Result{})
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, fmt.Sprintf(checkForFrozenWorkflow, targetKs), &sqltypes.Result{})
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, fmt.Sprintf(getWorkflow, targetKs, wf),
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id",
				"int64",
			),
			"1",
		),
	)
	tenv.tmc.setVReplicationExecResults(sourceTablet.tablet, checkForJournal, &sqltypes.Result{})
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, getCopyState, &sqltypes.Result{})
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, fmt.Sprintf(getWorkflowStatus, wf, targetKs),
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|workflow|source|pos|stop_pos|max_replication_log|state|db_name|time_updated|transaction_timestamp|message|tags|workflow_type|workflow_sub_type",
				"int64|varchar|blob|varchar|varchar|int64|varchar|varchar|int64|int64|varchar|varchar|int64|int64",
			),
			fmt.Sprintf("1|%s|%s|%s|NULL|0|running|vt_%s|1686577659|0|||1|0", wf, bls, position, targetKs),
		),
	)
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, getLatestCopyState, &sqltypes.Result{})

	tenv.vrdbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	tenv.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: 1}, nil)
	tenv.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
	tenv.vrdbClient.ExpectRequest(getVReplicationRecord,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source",
				"int64|varchar",
			),
			fmt.Sprintf("1|%s", bls),
		), nil)
	tenv.vrdbClient.ExpectRequest(`update _vt.vreplication set message='Picked source tablet: cell:\"zone1\" uid:200' where id=1`, &sqltypes.Result{}, nil)
	tenv.vrdbClient.ExpectRequest(setSessionTZ, &sqltypes.Result{}, nil)
	tenv.vrdbClient.ExpectRequest(setNames, &sqltypes.Result{}, nil)
	tenv.vrdbClient.ExpectRequest(setSQLMode, &sqltypes.Result{}, nil)
	tenv.vrdbClient.ExpectRequest(getSQLMode, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("sql_mode", "varchar"),
		sqlMode,
	), nil)
	tenv.vrdbClient.ExpectRequest(getWorkflowState, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"pos|stop_pos|max_tps|max_replication_lag|state|workflow_type|workflow|workflow_sub_type|defer_secondary_keys",
			"varchar|varchar|int64|int64|varchar|int64|varchar|int64|int64",
		),
		fmt.Sprintf("||0|0|Stopped|1|%s|0|0", wf),
	), nil)
	tenv.vrdbClient.ExpectRequest(getNumCopyStateTable, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"count(distinct table_name)",
			"int64",
		),
		"1",
	), nil)
	tenv.vrdbClient.ExpectRequest(setPermissiveSQLMode, &sqltypes.Result{}, nil)
	tenv.vrdbClient.ExpectRequest(getFKChecks, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"@@foreign_key_checks",
			"int64",
		),
		"1",
	), nil)
	tenv.vrdbClient.ExpectRequest(getWorkflowState, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"pos|stop_pos|max_tps|max_replication_lag|state|workflow_type|workflow|workflow_sub_type|defer_secondary_keys",
			"varchar|varchar|int64|int64|varchar|int64|varchar|int64|int64",
		),
		fmt.Sprintf("||0|0|Stopped|1|%s|0|0", wf),
	), nil)
	tenv.vrdbClient.ExpectRequest(getNumCopyStateTable, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"count(distinct table_name)",
			"int64",
		),
		"1",
	), nil)
	tenv.vrdbClient.ExpectRequest(getBinlogRowImage, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"@@binlog_row_image",
			"varchar",
		),
		"FULL",
	), nil)
	tenv.vrdbClient.ExpectRequest(disableFKChecks, &sqltypes.Result{}, nil)
	tenv.vrdbClient.ExpectRequest(setStrictSQLMode, &sqltypes.Result{}, nil)

	tenv.vrdbClient.ExpectRequest(fmt.Sprintf(insertStreamsCreatedLog, bls), &sqltypes.Result{}, nil)
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, fmt.Sprintf(getWorkflow, targetKs, wf),
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id",
				"int64",
			),
			"1",
		),
	)
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, fmt.Sprintf(startWorkflow, targetKs, wf), &sqltypes.Result{})
	_, err := ws.MoveTablesCreate(ctx, req)
	require.NoError(t, err)

	tenv.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecardb.DefaultName), &sqltypes.Result{}, nil)
	_, err = ws.WorkflowSwitchTraffic(ctx, &vtctldatapb.WorkflowSwitchTrafficRequest{
		Keyspace:                 req.TargetKeyspace,
		Workflow:                 req.Workflow,
		Cells:                    req.Cells,
		TabletTypes:              req.TabletTypes,
		MaxReplicationLagAllowed: &vttime.Duration{Seconds: 922337203},
		Direction:                int32(workflow.DirectionForward),
	})
	require.NoError(t, err)
	_, err = ws.WorkflowSwitchTraffic(ctx, &vtctldatapb.WorkflowSwitchTrafficRequest{
		Keyspace:                 req.TargetKeyspace,
		Workflow:                 req.Workflow,
		Cells:                    req.Cells,
		TabletTypes:              req.TabletTypes,
		MaxReplicationLagAllowed: &vttime.Duration{Seconds: 922337203},
		Direction:                int32(workflow.DirectionBackward),
	})
	require.NoError(t, err)
}

func TestUpdateVReplicationWorkflow(t *testing.T) {
	ctx := context.Background()
	cells := []string{"zone1"}
	tabletTypes := []string{"replica"}
	workflow := "testwf"
	keyspace := "testks"
	vreplID := 1
	tabletUID := 100

	tenv := newTestEnv(t, keyspace, []string{shard})
	defer tenv.close()

	tablet := tenv.addTablet(tabletUID, keyspace, shard)
	defer tenv.deleteTablet(tablet.tablet)

	parsed := sqlparser.BuildParsedQuery(sqlSelectVReplicationWorkflowConfig, sidecardb.DefaultName, ":wf")
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(workflow),
	}
	selectQuery, err := parsed.GenerateQuery(bindVars, nil)
	require.NoError(t, err)
	blsStr := fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"customer" filter:"select * from customer"} rules:{match:"corder" filter:"select * from corder"}}`,
		keyspace, shard)
	selectRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|cell|tablet_types",
			"int64|varchar|varchar|varchar",
		),
		fmt.Sprintf("%d|%s|%s|%s", vreplID, blsStr, cells[0], tabletTypes[0]),
	)
	idQuery, err := sqlparser.ParseAndBind("select id from _vt.vreplication where id = %a",
		sqltypes.Int64BindVariable(int64(vreplID)))
	require.NoError(t, err)
	idRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		fmt.Sprintf("%d", vreplID),
	)

	tests := []struct {
		name    string
		request *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest
		query   string
	}{
		{
			name: "update cells",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow: workflow,
				Cells:    []string{"zone2"},
				// TabletTypes is an empty value, so the current value should be cleared
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Stopped', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '' where id in (%d)`,
				keyspace, shard, "zone2", vreplID),
		},
		{
			name: "update cells, NULL tablet_types",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:    workflow,
				Cells:       []string{"zone3"},
				TabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)}, // So keep the current value of replica
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Stopped', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, "zone3", tabletTypes[0], vreplID),
		},
		{
			name: "update tablet_types",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:                  workflow,
				TabletSelectionPreference: tabletmanagerdatapb.TabletSelectionPreference_INORDER,
				TabletTypes:               []topodatapb.TabletType{topodatapb.TabletType_RDONLY, topodatapb.TabletType_REPLICA},
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Stopped', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, "in_order:rdonly,replica", vreplID),
		},
		{
			name: "update tablet_types, NULL cells",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:    workflow,
				Cells:       textutil.SimulatedNullStringSlice, // So keep the current value of zone1
				TabletTypes: []topodatapb.TabletType{topodatapb.TabletType_RDONLY},
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Stopped', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, cells[0], "rdonly", vreplID),
		},
		{
			name: "update on_ddl",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow: workflow,
				OnDdl:    binlogdatapb.OnDDLAction_EXEC,
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Stopped', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}} on_ddl:%s', cell = '', tablet_types = '' where id in (%d)`,
				keyspace, shard, binlogdatapb.OnDDLAction_EXEC.String(), vreplID),
		},
		{
			name: "update cell,tablet_types,on_ddl",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:    workflow,
				Cells:       []string{"zone1", "zone2", "zone3"},
				TabletTypes: []topodatapb.TabletType{topodatapb.TabletType_RDONLY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_PRIMARY},
				OnDdl:       binlogdatapb.OnDDLAction_EXEC_IGNORE,
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Stopped', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}} on_ddl:%s', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, binlogdatapb.OnDDLAction_EXEC_IGNORE.String(), "zone1,zone2,zone3", "rdonly,replica,primary", vreplID),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This is needed because MockDBClient uses t.Fatal()
			// which doesn't play well with subtests.
			defer func() {
				if err := recover(); err != nil {
					t.Errorf("Recovered from panic: %v", err)
				}
			}()

			require.NotNil(t, tt.request, "No request provided")
			require.NotEqual(t, "", tt.query, "No expected query provided")

			tt.request.State = binlogdatapb.VReplicationWorkflowState_Stopped

			// These are the same for each RPC call.
			tenv.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecardb.DefaultName), &sqltypes.Result{}, nil)
			tenv.vrdbClient.ExpectRequest(selectQuery, selectRes, nil)
			tenv.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecardb.DefaultName), &sqltypes.Result{}, nil)
			tenv.vrdbClient.ExpectRequest(idQuery, idRes, nil)

			// This is our expected query, which will also short circuit
			// the test with an error as at this point we've tested what
			// we wanted to test.
			tenv.vrdbClient.ExpectRequest(tt.query, &sqltypes.Result{RowsAffected: 1}, errShortCircuit)
			_, err = tenv.tmc.tm.UpdateVReplicationWorkflow(ctx, tt.request)
			tenv.vrdbClient.Wait()
			require.ErrorIs(t, err, errShortCircuit)
		})
	}
}
