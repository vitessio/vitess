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
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

const (
	insertVReplicationPrefix = "insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys)"
	getWorkflow              = "select id from _vt.vreplication where db_name='vt_%s' and workflow='%s'"
	checkForWorkflow         = "select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'"
	checkForFrozenWorkflow   = "select 1 from _vt.vreplication where db_name='vt_%s' and message='FROZEN' and workflow_sub_type != 1"
	freezeWorkflow           = "update _vt.vreplication set message = 'FROZEN' where db_name='vt_%s' and workflow='%s'"
	checkForJournal          = "/select val from _vt.resharding_journal where id="
	getWorkflowStatus        = "select id, workflow, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, message, tags, workflow_type, workflow_sub_type, time_heartbeat, defer_secondary_keys, component_throttled, time_throttled, rows_copied, tablet_types, cell from _vt.vreplication where workflow = '%s' and db_name = 'vt_%s'"
	getWorkflowState         = "select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=1"
	getCopyState             = "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
	getNumCopyStateTable     = "select count(distinct table_name) from _vt.copy_state where vrepl_id=1"
	getLatestCopyState       = "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)"
	getAutoIncrementStep     = "select @@session.auto_increment_increment"
	setSessionTZ             = "set @@session.time_zone = '+00:00'"
	setNames                 = "set names 'binary'"
	getBinlogRowImage        = "select @@binlog_row_image"
	insertStreamsCreatedLog  = "insert into _vt.vreplication_log(vrepl_id, type, state, message) values(1, 'Stream Created', '', '%s'"
	getVReplicationRecord    = "select * from _vt.vreplication where id = 1"
	startWorkflow            = "update _vt.vreplication set state='Running' where db_name='vt_%s' and workflow='%s'"
	stopForCutover           = "update _vt.vreplication set state='Stopped', message='stopped for cutover' where id=1"
	getMaxValForSequence     = "select max(`id`) as maxval from `vt_%s`.`%s`"
	initSequenceTable        = "insert into %a.%a (id, next_id, cache) values (0, %d, 1000) on duplicate key update next_id = if(next_id < %d, %d, next_id)"
	deleteWorkflow           = "delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'"
	updatePickedSourceTablet = `update _vt.vreplication set message='Picked source tablet: cell:\"%s\" uid:%d' where id=1`
	getRowsCopied            = "SELECT rows_copied FROM _vt.vreplication WHERE id=1"
)

var (
	errShortCircuit = fmt.Errorf("short circuiting test")
	defaultSchema   = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:              "t1",
				Columns:           []string{"id", "c2"},
				PrimaryKeyColumns: []string{"id"},
				Fields:            sqltypes.MakeTestFields("id|c2", "int64|int64"),
			},
		},
	}
	position           = fmt.Sprintf("%s/%s", gtidFlavor, gtidPosition)
	setNetReadTimeout  = fmt.Sprintf("set @@session.net_read_timeout = %v", vttablet.VReplicationNetReadTimeout)
	setNetWriteTimeout = fmt.Sprintf("set @@session.net_write_timeout = %v", vttablet.VReplicationNetWriteTimeout)
)

// TestCreateVReplicationWorkflow tests the query generated
// from a VtctldServer MoveTablesCreate request to ensure
// that the VReplication stream(s) are created correctly.
func TestCreateVReplicationWorkflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetTabletUID := 300
	shard := "0"
	wf := "testwf"
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv.NewTestEnv(), tenv.ts, tenv.tmc)

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
						Columns:           []string{"id", "c2"},
						PrimaryKeyColumns: []string{"id"},
						Fields:            sqltypes.MakeTestFields("id|c2", "int64|int64"),
					},
					{
						Name:              "wut",
						Columns:           []string{"id"},
						PrimaryKeyColumns: []string{"id"},
						Fields:            sqltypes.MakeTestFields("id", "int64"),
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

			tenv.tmc.tablets[targetTabletUID].vrdbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
			// This is our expected query, which will also short circuit
			// the test with an error as at this point we've tested what
			// we wanted to test.
			tenv.tmc.tablets[targetTabletUID].vrdbClient.ExpectRequest(tt.query, nil, errShortCircuit)
			_, err := ws.MoveTablesCreate(ctx, tt.req)
			tenv.tmc.tablets[targetTabletUID].vrdbClient.Wait()
			require.ErrorIs(t, err, errShortCircuit)
		})
	}
}

// TestMoveTables tests the query generated from a VtctldServer
// MoveTablesCreate request to ensure that the VReplication
// stream(s) are created correctly. Followed by ensuring that
// SwitchTraffic and ReverseTraffic work as expected.
func TestMoveTables(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	sourceShard := "0"
	globalKs := "global"
	globalShard := "0"
	wf := "testwf"
	tabletTypes := []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}

	tenv := newTestEnv(t, ctx, sourceKs, []string{sourceShard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, 300, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, 310, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	globalTablet := tenv.addTablet(t, 500, globalKs, globalShard)
	defer tenv.deleteTablet(globalTablet.tablet)

	tenv.ts.SaveVSchema(ctx, globalKs, &vschemapb.Keyspace{
		Sharded: false,
		Tables: map[string]*vschemapb.Table{
			"t1_seq": {
				Type: vindexes.TypeSequence,
			},
		},
	})
	tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
				AutoIncrement: &vschemapb.AutoIncrement{
					Column:   "id",
					Sequence: "t1_seq",
				},
			},
		},
	})

	ws := workflow.NewServer(vtenv.NewTestEnv(), tenv.ts, tenv.tmc)

	tenv.mysqld.Schema = defaultSchema
	tenv.mysqld.Schema.DatabaseSchema = tenv.dbName
	tenv.mysqld.FetchSuperQueryMap = make(map[string]*sqltypes.Result)
	tenv.mysqld.FetchSuperQueryMap[`select character_set_name, collation_name, column_name, data_type, column_type, extra from information_schema.columns where .*`] = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"character_set_name|collation_name|column_name|data_type|column_type|extra",
			"varchar|varchar|varchar|varchar|varchar|varchar",
		),
		"NULL|NULL|id|bigint|bigint|",
		"NULL|NULL|c2|bigint|bigint|",
	)

	bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"}}", sourceKs, sourceShard)

	tenv.tmc.SetSchema(defaultSchema)

	tenv.tmc.setVReplicationExecResults(sourceTablet.tablet, checkForJournal, &sqltypes.Result{})

	for _, ftc := range targetShards {
		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(checkForWorkflow, targetKs, wf), &sqltypes.Result{})
		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(checkForFrozenWorkflow, targetKs), &sqltypes.Result{})
		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(getWorkflow, targetKs, wf),
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"int64",
				),
				"1",
			),
		)
		tenv.tmc.setVReplicationExecResults(ftc.tablet, getCopyState, &sqltypes.Result{})
		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(getWorkflowStatus, wf, targetKs),
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|workflow|source|pos|stop_pos|max_replication_log|state|db_name|time_updated|transaction_timestamp|message|tags|workflow_type|workflow_sub_type|time_heartbeat|defer_secondary_keys|component_throttled|time_throttled|rows_copied",
					"int64|varchar|blob|varchar|varchar|int64|varchar|varchar|int64|int64|varchar|varchar|int64|int64|int64|int64|varchar|int64|int64",
				),
				fmt.Sprintf("1|%s|%s|%s|NULL|0|running|vt_%s|1686577659|0|||1|0|0|0||0|10", wf, bls, position, targetKs),
			),
		)
		tenv.tmc.setVReplicationExecResults(ftc.tablet, getLatestCopyState, &sqltypes.Result{})

		ftc.vrdbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
		insert := fmt.Sprintf(`%s values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(id, \'%s.hash\', \'%s\')\"}}', '', 0, 0, '%s', 'primary,replica,rdonly', now(), 0, 'Stopped', '%s', 1, 0, 0)`,
			insertVReplicationPrefix, wf, sourceKs, sourceShard, targetKs, ftc.tablet.Shard, tenv.cells[0], tenv.dbName)
		ftc.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: 1}, nil)
		ftc.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
		ftc.vrdbClient.ExpectRequest(getVReplicationRecord,
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|source",
					"int64|varchar",
				),
				fmt.Sprintf("1|%s", bls),
			), nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(updatePickedSourceTablet, tenv.cells[0], sourceTabletUID), &sqltypes.Result{}, nil)
		ftc.vrdbClient.ExpectRequest(setSessionTZ, &sqltypes.Result{}, nil)
		ftc.vrdbClient.ExpectRequest(setNames, &sqltypes.Result{}, nil)
		ftc.vrdbClient.ExpectRequest(setNetReadTimeout, &sqltypes.Result{}, nil)
		ftc.vrdbClient.ExpectRequest(setNetWriteTimeout, &sqltypes.Result{}, nil)
		ftc.vrdbClient.ExpectRequest(getRowsCopied,
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"rows_copied",
					"int64",
				),
				"0",
			),
			nil,
		)
		ftc.vrdbClient.ExpectRequest(getWorkflowState, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"pos|stop_pos|max_tps|max_replication_lag|state|workflow_type|workflow|workflow_sub_type|defer_secondary_keys",
				"varchar|varchar|int64|int64|varchar|int64|varchar|int64|int64",
			),
			fmt.Sprintf("||0|0|Stopped|1|%s|0|0", wf),
		), nil)
		ftc.vrdbClient.ExpectRequest(getNumCopyStateTable, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"count(distinct table_name)",
				"int64",
			),
			"1",
		), nil)
		ftc.vrdbClient.ExpectRequest(getWorkflowState, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"pos|stop_pos|max_tps|max_replication_lag|state|workflow_type|workflow|workflow_sub_type|defer_secondary_keys",
				"varchar|varchar|int64|int64|varchar|int64|varchar|int64|int64",
			),
			fmt.Sprintf("||0|0|Stopped|1|%s|0|0", wf),
		), nil)
		ftc.vrdbClient.ExpectRequest(getNumCopyStateTable, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"count(distinct table_name)",
				"int64",
			),
			"1",
		), nil)
		ftc.vrdbClient.ExpectRequest(getBinlogRowImage, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"@@binlog_row_image",
				"varchar",
			),
			"FULL",
		), nil)

		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(insertStreamsCreatedLog, bls), &sqltypes.Result{}, nil)
		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(getWorkflow, targetKs, wf),
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"int64",
				),
				"1",
			),
		)
		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(startWorkflow, targetKs, wf), &sqltypes.Result{})
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.DefaultName), &sqltypes.Result{}, nil)

		tenv.tmc.setVReplicationExecResults(ftc.tablet, stopForCutover, &sqltypes.Result{})
		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(freezeWorkflow, targetKs, wf), &sqltypes.Result{})

		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(getMaxValForSequence, targetKs, "t1"),
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"maxval",
					"int64",
				),
				fmt.Sprintf("%d", ftc.tablet.Alias.Uid), // Use the tablet's UID as the max value
			),
		)
	}

	// We use the tablet's UID in the mocked results for the max value used on each target shard.
	nextSeqVal := int(math.Max(float64(targetShards["-80"].tablet.Alias.Uid), float64(targetShards["80-"].tablet.Alias.Uid))) + 1
	tenv.tmc.setVReplicationExecResults(globalTablet.tablet,
		sqlparser.BuildParsedQuery(initSequenceTable, sqlescape.EscapeID(fmt.Sprintf("vt_%s", globalKs)), sqlescape.EscapeID("t1_seq"), nextSeqVal, nextSeqVal, nextSeqVal).Query,
		&sqltypes.Result{RowsAffected: 0},
	)

	_, err := ws.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		Workflow:       wf,
		TabletTypes:    tabletTypes,
		Cells:          tenv.cells,
		AllTables:      true,
		AutoStart:      true,
	})
	require.NoError(t, err)

	_, err = ws.WorkflowSwitchTraffic(ctx, &vtctldatapb.WorkflowSwitchTrafficRequest{
		Keyspace:                  targetKs,
		Workflow:                  wf,
		Cells:                     tenv.cells,
		MaxReplicationLagAllowed:  &vttime.Duration{Seconds: 922337203},
		EnableReverseReplication:  true,
		InitializeTargetSequences: true,
		Direction:                 int32(workflow.DirectionForward),
	})
	require.NoError(t, err)

	tenv.tmc.setVReplicationExecResults(sourceTablet.tablet, fmt.Sprintf(getWorkflowStatus, workflow.ReverseWorkflowName(wf), sourceKs),
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|workflow|source|pos|stop_pos|max_replication_log|state|db_name|time_updated|transaction_timestamp|message|tags|workflow_type|workflow_sub_type|time_heartbeat|defer_secondary_keys|component_throttled|time_throttled|rows_copied",
				"int64|varchar|blob|varchar|varchar|int64|varchar|varchar|int64|int64|varchar|varchar|int64|int64|int64|int64|varchar|int64|int64",
			),
			fmt.Sprintf("1|%s|%s|%s|NULL|0|running|vt_%s|1686577659|0|||1|0|0|0||0|10", workflow.ReverseWorkflowName(wf), bls, position, sourceKs),
		),
	)

	_, err = ws.WorkflowSwitchTraffic(ctx, &vtctldatapb.WorkflowSwitchTrafficRequest{
		Keyspace:                 targetKs,
		Workflow:                 wf,
		Cells:                    tenv.cells,
		MaxReplicationLagAllowed: &vttime.Duration{Seconds: 922337203},
		EnableReverseReplication: true,
		Direction:                int32(workflow.DirectionBackward),
	})
	require.NoError(t, err)
}

func TestUpdateVReplicationWorkflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cells := []string{"zone1"}
	tabletTypes := []string{"replica"}
	workflow := "testwf"
	keyspace := "testks"
	vreplID := 1
	tabletUID := 100

	tenv := newTestEnv(t, ctx, keyspace, []string{shard})
	defer tenv.close()

	tablet := tenv.addTablet(t, tabletUID, keyspace, shard)
	defer tenv.deleteTablet(tablet.tablet)

	parsed := sqlparser.BuildParsedQuery(sqlSelectVReplicationWorkflowConfig, sidecar.DefaultName, ":wf")
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(workflow),
	}
	selectQuery, err := parsed.GenerateQuery(bindVars, nil)
	require.NoError(t, err)
	blsStr := fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"customer" filter:"select * from customer"} rules:{match:"corder" filter:"select * from corder"}}`,
		keyspace, shard)
	selectRes := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|cell|tablet_types|state|message",
			"int64|varchar|varchar|varchar|varchar|varbinary",
		),
		fmt.Sprintf("%d|%s|%s|%s|Running|", vreplID, blsStr, cells[0], tabletTypes[0]),
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
	getCopyStateQuery := fmt.Sprintf(sqlGetVReplicationCopyStatus, sidecar.GetIdentifier(), int64(vreplID))
	copyStatusFields := sqltypes.MakeTestFields(
		"id",
		"int64",
	)
	notCopying := sqltypes.MakeTestResult(copyStatusFields)
	copying := sqltypes.MakeTestResult(copyStatusFields, "1")
	tests := []struct {
		name      string
		request   *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest
		query     string
		isCopying bool
	}{
		{
			name: "update cells",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow: workflow,
				State:    binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				Cells:    []string{"zone2"},
				// TabletTypes is an empty value, so the current value should be cleared
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '' where id in (%d)`,
				keyspace, shard, "zone2", vreplID),
		},
		{
			name: "update cells, NULL tablet_types",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:    workflow,
				State:       binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				Cells:       []string{"zone3"},
				TabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)}, // So keep the current value of replica
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, "zone3", tabletTypes[0], vreplID),
		},
		{
			name: "update tablet_types",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:                  workflow,
				State:                     binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				TabletSelectionPreference: tabletmanagerdatapb.TabletSelectionPreference_INORDER,
				TabletTypes:               []topodatapb.TabletType{topodatapb.TabletType_RDONLY, topodatapb.TabletType_REPLICA},
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, "in_order:rdonly,replica", vreplID),
		},
		{
			name: "update tablet_types, NULL cells",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:    workflow,
				State:       binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				Cells:       textutil.SimulatedNullStringSlice, // So keep the current value of zone1
				TabletTypes: []topodatapb.TabletType{topodatapb.TabletType_RDONLY},
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, cells[0], "rdonly", vreplID),
		},
		{
			name: "update on_ddl",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow: workflow,
				State:    binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				OnDdl:    binlogdatapb.OnDDLAction_EXEC,
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}} on_ddl:%s', cell = '', tablet_types = '' where id in (%d)`,
				keyspace, shard, binlogdatapb.OnDDLAction_EXEC.String(), vreplID),
		},
		{
			name: "update cell,tablet_types,on_ddl",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:    workflow,
				State:       binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				Cells:       []string{"zone1", "zone2", "zone3"},
				TabletTypes: []topodatapb.TabletType{topodatapb.TabletType_RDONLY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_PRIMARY},
				OnDdl:       binlogdatapb.OnDDLAction_EXEC_IGNORE,
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}} on_ddl:%s', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, binlogdatapb.OnDDLAction_EXEC_IGNORE.String(), "zone1,zone2,zone3", "rdonly,replica,primary", vreplID),
		},
		{
			name: "update state",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:    workflow,
				State:       binlogdatapb.VReplicationWorkflowState_Stopped,
				Cells:       textutil.SimulatedNullStringSlice,
				TabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)},
				OnDdl:       binlogdatapb.OnDDLAction(textutil.SimulatedNullInt),
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = '%s', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				binlogdatapb.VReplicationWorkflowState_Stopped.String(), keyspace, shard, cells[0], tabletTypes[0], vreplID),
		},
		{
			name: "update to running while copying",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow:    workflow,
				State:       binlogdatapb.VReplicationWorkflowState_Running,
				Cells:       textutil.SimulatedNullStringSlice,
				TabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)},
				OnDdl:       binlogdatapb.OnDDLAction(textutil.SimulatedNullInt),
			},
			isCopying: true,
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Copying', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"customer\" filter:\"select * from customer\"} rules:{match:\"corder\" filter:\"select * from corder\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, cells[0], tabletTypes[0], vreplID),
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

			// These are the same for each RPC call.
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.DefaultName), &sqltypes.Result{}, nil)
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(selectQuery, selectRes, nil)
			if tt.request.State == binlogdatapb.VReplicationWorkflowState_Running ||
				tt.request.State == binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt) {
				tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
				if tt.isCopying {
					tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(getCopyStateQuery, copying, nil)
				} else {
					tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(getCopyStateQuery, notCopying, nil)

				}
			}
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.DefaultName), &sqltypes.Result{}, nil)
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(idQuery, idRes, nil)

			// This is our expected query, which will also short circuit
			// the test with an error as at this point we've tested what
			// we wanted to test.
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(tt.query, &sqltypes.Result{RowsAffected: 1}, errShortCircuit)
			_, err = tenv.tmc.tablets[tabletUID].tm.UpdateVReplicationWorkflow(ctx, tt.request)
			tenv.tmc.tablets[tabletUID].vrdbClient.Wait()
			require.ErrorIs(t, err, errShortCircuit)
		})
	}
}

// TestSourceShardSelection tests the RPC calls made by VtctldServer to tablet
// managers include the correct set of BLS settings.
//
// errShortCircuit is intentionally injected into the MoveTables workflow to
// short-circuit the workflow after we've validated everything we wanted to in
// the test.
func TestSourceShardSelection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceKs := "sourceks"
	sourceShard0 := "-55"
	sourceShard1 := "55-aa"
	sourceShard2 := "aa-"
	sourceTabletUID0 := 200
	sourceTabletUID1 := 201
	sourceTabletUID2 := 202

	targetKs := "targetks"
	targetShard0 := "-80"
	targetShard1 := "80-"
	targetTabletUID0 := 300
	targetTabletUID1 := 301

	wf := "testwf"

	tenv := newTestEnv(t, ctx, sourceKs, []string{sourceShard0, sourceShard1, sourceShard2})
	defer tenv.close()

	sourceTablets := map[int]*fakeTabletConn{
		sourceTabletUID0: tenv.addTablet(t, sourceTabletUID0, sourceKs, sourceShard0),
		sourceTabletUID1: tenv.addTablet(t, sourceTabletUID1, sourceKs, sourceShard1),
		sourceTabletUID2: tenv.addTablet(t, sourceTabletUID2, sourceKs, sourceShard2),
	}
	for _, st := range sourceTablets {
		defer tenv.deleteTablet(st.tablet)
	}

	targetTablets := map[int]*fakeTabletConn{
		targetTabletUID0: tenv.addTablet(t, targetTabletUID0, targetKs, targetShard0),
		targetTabletUID1: tenv.addTablet(t, targetTabletUID1, targetKs, targetShard1),
	}
	for _, tt := range targetTablets {
		defer tenv.deleteTablet(tt.tablet)
	}

	ws := workflow.NewServer(vtenv.NewTestEnv(), tenv.ts, tenv.tmc)

	tenv.ts.SaveVSchema(ctx, sourceKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
		},
	})
	tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
		},
	})

	tests := []struct {
		name    string
		req     *vtctldatapb.MoveTablesCreateRequest
		schema  *tabletmanagerdatapb.SchemaDefinition
		vschema *vschemapb.Keyspace
		streams map[int][]string
	}{
		{
			name: "same primary vindexes, use intersecting source shards",
			req: &vtctldatapb.MoveTablesCreateRequest{
				SourceKeyspace: sourceKs,
				TargetKeyspace: targetKs,
				Workflow:       wf,
				Cells:          tenv.cells,
				AllTables:      true,
				AutoStart:      false,
			},
			streams: map[int][]string{
				targetTabletUID0: {
					sourceShard0,
					sourceShard1,
				},
				targetTabletUID1: {
					sourceShard1,
					sourceShard2,
				},
			},
		},
		{
			name: "different primary vindexes, use all source shards",
			req: &vtctldatapb.MoveTablesCreateRequest{
				SourceKeyspace: sourceKs,
				TargetKeyspace: targetKs,
				Workflow:       wf,
				Cells:          tenv.cells,
				AllTables:      true,
				AutoStart:      false,
			},
			vschema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "xxhash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Column: "id",
							Name:   "hash",
						}},
					},
				},
			},
			streams: map[int][]string{
				targetTabletUID0: {
					sourceShard0,
					sourceShard1,
					sourceShard2,
				},
				targetTabletUID1: {
					sourceShard0,
					sourceShard1,
					sourceShard2,
				},
			},
		},
	}

	for _, tt := range targetTablets {
		tenv.tmc.setVReplicationExecResults(tt.tablet, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'",
			targetKs, wf), &sqltypes.Result{})
		tenv.tmc.setVReplicationExecResults(tt.tablet, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and message='FROZEN' and workflow_sub_type != 1",
			targetKs), &sqltypes.Result{})
	}

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
			require.NotEmpty(t, tt.streams, "No expected streams provided")

			if tt.schema == nil {
				tt.schema = defaultSchema
			}
			tenv.tmc.SetSchema(tt.schema)

			if tt.vschema != nil {
				tenv.ts.SaveVSchema(ctx, targetKs, tt.vschema)
			}

			for uid, streams := range tt.streams {
				tt := targetTablets[uid]
				for i, sourceShard := range streams {
					tt.vrdbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
					var err error
					if i == len(streams)-1 {
						// errShortCircuit is intentionally injected into the MoveTables
						// workflow to short-circuit the workflow after we've validated
						// everything we wanted to in the test.
						err = errShortCircuit
					}
					tt.vrdbClient.ExpectRequest(
						fmt.Sprintf(`%s values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(id, \'%s.hash\', \'%s\')\"}}', '', 0, 0, '%s', '', now(), 0, 'Stopped', '%s', 1, 0, 0)`,
							insertVReplicationPrefix, wf, sourceKs, sourceShard, targetKs, tt.tablet.Shard, tenv.cells[0], tenv.dbName),
						&sqltypes.Result{InsertID: uint64(i + 1)},
						err,
					)
					if errors.Is(err, errShortCircuit) {
						break
					}
					tt.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
					tt.vrdbClient.ExpectRequest(
						fmt.Sprintf("select * from _vt.vreplication where id = %d", uint64(i+1)),
						sqltypes.MakeTestResult(
							sqltypes.MakeTestFields(
								"id|source|state",
								"int64|varchar|varchar",
							),
							fmt.Sprintf("%d|%s|Stopped", uint64(i+1), fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"t1" filter:"select * from t1 where in_keyrange(id, '%s.hash', '%s')"}}`, sourceKs, sourceShard, targetKs, tt.tablet.Shard)),
						),
						nil,
					)
				}
			}

			_, err := ws.MoveTablesCreate(ctx, tt.req)
			for _, tt := range targetTablets {
				tt.vrdbClient.Wait()
			}
			// errShortCircuit is intentionally injected into the MoveTables
			// workflow to short-circuit the workflow after we've validated
			// everything we wanted to in the test.
			require.ErrorContains(t, err, fmt.Sprintf("%s\n%s", errShortCircuit.Error(), errShortCircuit.Error()))
		})
	}
}

// TestFailedMoveTablesCreateCleanup tests that the workflow
// and its artifacts are cleaned up when the workflow creation
// fails -- specifically after the point where we have created
// the workflow streams.
func TestFailedMoveTablesCreateCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	shard := "0"
	targetTabletUID := 300
	targetKs := "targetks"
	wf := "testwf"
	table := defaultSchema.TableDefinitions[0].Name
	invalidTimeZone := "NOPE"
	bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"%s\" filter:\"select * from %s\"}}",
		sourceKs, shard, table, table)
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()
	ws := workflow.NewServer(vtenv.NewTestEnv(), tenv.ts, tenv.tmc)

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	tenv.mysqld.Schema = defaultSchema
	tenv.mysqld.Schema.DatabaseSchema = tenv.dbName
	tenv.mysqld.FetchSuperQueryMap = make(map[string]*sqltypes.Result)
	tenv.mysqld.FetchSuperQueryMap[`select character_set_name, collation_name, column_name, data_type, column_type, extra from information_schema.columns where .*`] = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"character_set_name|collation_name|column_name|data_type|column_type|extra",
			"varchar|varchar|varchar|varchar|varchar|varchar",
		),
		"NULL|NULL|id|bigint|bigint|",
		"NULL|NULL|c2|bigint|bigint|",
	)

	// Let's be sure that the routing rules are empty to start.
	err := topotools.SaveRoutingRules(ctx, tenv.ts, nil)
	require.NoError(t, err, "failed to save routing rules")

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
	targetTablet.vrdbClient.ExpectRequest("use _vt", &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(
		fmt.Sprintf("%s %s",
			insertVReplicationPrefix,
			fmt.Sprintf(`values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"%s\" filter:\"select * from %s\"}} source_time_zone:\"%s\" target_time_zone:\"UTC\"', '', 0, 0, '%s', 'primary', now(), 0, 'Stopped', '%s', 1, 0, 0)`,
				wf, sourceKs, shard, table, table, invalidTimeZone, strings.Join(tenv.cells, ","), tenv.dbName),
		),
		&sqltypes.Result{
			RowsAffected: 1,
			InsertID:     1,
		},
		nil,
	)
	targetTablet.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(getVReplicationRecord,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source",
				"int64|varchar",
			),
			fmt.Sprintf("1|%s", bls),
		),
		nil,
	)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(updatePickedSourceTablet, tenv.cells[0], sourceTabletUID),
		&sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(setSessionTZ, &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(setNames, &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(setNetReadTimeout, &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(setNetWriteTimeout, &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(getRowsCopied,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"rows_copied",
				"int64",
			),
			"0",
		),
		nil,
	)
	targetTablet.vrdbClient.ExpectRequest(getWorkflowState,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"pos|stop_pos|max_tps|max_replication_lag|state|workflow_type|workflow|workflow_sub_type|defer_secondary_keys",
				"varchar|varchar|int64|int64|varchar|int64|varchar|int64|int64",
			),
			fmt.Sprintf("||0|0|Stopped|1|%s|0|0", wf),
		),
		nil,
	)
	targetTablet.vrdbClient.ExpectRequest(getNumCopyStateTable,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"count(distinct table_name)",
				"int64",
			),
			"1",
		),
		nil,
	)
	targetTablet.vrdbClient.ExpectRequest(getWorkflowState,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"pos|stop_pos|max_tps|max_replication_lag|state|workflow_type|workflow|workflow_sub_type|defer_secondary_keys",
				"varchar|varchar|int64|int64|varchar|int64|varchar|int64|int64",
			),
			fmt.Sprintf("||0|0|Stopped|1|%s|0|0", wf),
		),
		nil,
	)
	targetTablet.vrdbClient.ExpectRequest(getNumCopyStateTable,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"count(distinct table_name)",
				"int64",
			),
			"1",
		),
		nil,
	)
	targetTablet.vrdbClient.ExpectRequest(getBinlogRowImage,
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"@@binlog_row_image",
				"varchar",
			),
			"FULL",
		),
		nil,
	)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(insertStreamsCreatedLog, bls), &sqltypes.Result{}, nil)

	tenv.tmc.setVReplicationExecResults(targetTablet.tablet,
		fmt.Sprintf("select convert_tz('2006-01-02 15:04:05', '%s', 'UTC')", invalidTimeZone),
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				fmt.Sprintf("convert_tz('2006-01-02 15:04:05', '%s', 'UTC')", invalidTimeZone),
				"datetime",
			),
			"NULL",
		),
	)

	// We expect the workflow creation to fail due to the invalid time
	// zone and thus the workflow itself to be cleaned up.
	tenv.tmc.setVReplicationExecResults(sourceTablet.tablet,
		fmt.Sprintf(deleteWorkflow, sourceKs, workflow.ReverseWorkflowName(wf)),
		&sqltypes.Result{RowsAffected: 1},
	)
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet,
		fmt.Sprintf(deleteWorkflow, targetKs, wf),
		&sqltypes.Result{RowsAffected: 1},
	)

	// Save the current target vschema.
	vs, err := tenv.ts.GetVSchema(ctx, targetKs)
	require.NoError(t, err, "failed to get target vschema")

	_, err = ws.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		Cells:          tenv.cells,
		TabletTypes:    []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
		IncludeTables:  []string{table},
		SourceTimeZone: invalidTimeZone,
	})
	require.ErrorContains(t, err, fmt.Sprintf("unable to perform time_zone conversions from %s to UTC", invalidTimeZone))

	// Check that there are no orphaned routing rules.
	rules, err := topotools.GetRoutingRules(ctx, tenv.ts)
	require.NoError(t, err, "failed to get routing rules")
	require.Equal(t, 0, len(rules), "expected no routing rules to be present")

	// Check that our vschema changes were also rolled back.
	vs2, err := tenv.ts.GetVSchema(ctx, targetKs)
	require.NoError(t, err, "failed to get target vschema")
	require.Equal(t, vs, vs2, "expected vschema to be unchanged")
}
