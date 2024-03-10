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
	"reflect"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

const (
	insertVReplicationPrefix = "insert into _vt.vreplication (workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys)"
	checkForJournal          = "/select val from _vt.resharding_journal where id="
	getWorkflowState         = "select pos, stop_pos, max_tps, max_replication_lag, state, workflow_type, workflow, workflow_sub_type, defer_secondary_keys from _vt.vreplication where id=%d"
	getCopyState             = "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
	getNumCopyStateTable     = "select count(distinct table_name) from _vt.copy_state where vrepl_id=%d"
	getLatestCopyState       = "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (%d) and id in (select max(id) from _vt.copy_state where vrepl_id in (%d) group by vrepl_id, table_name)"
	getAutoIncrementStep     = "select @@session.auto_increment_increment"
	setSessionTZ             = "set @@session.time_zone = '+00:00'"
	setNames                 = "set names 'binary'"
	getBinlogRowImage        = "select @@binlog_row_image"
	insertStreamsCreatedLog  = "insert into _vt.vreplication_log(vrepl_id, type, state, message) values(1, 'Stream Created', '', '%s'"
	getVReplicationRecord    = "select * from _vt.vreplication where id = %d"
	startWorkflow            = "update _vt.vreplication set state='Running' where db_name='vt_%s' and workflow='%s'"
	stopForCutover           = "update _vt.vreplication set state='Stopped', message='stopped for cutover' where id=1"
	getMaxValForSequence     = "select max(`id`) as maxval from `vt_%s`.`%s`"
	initSequenceTable        = "insert into %a.%a (id, next_id, cache) values (0, %d, 1000) on duplicate key update next_id = if(next_id < %d, %d, next_id)"
	deleteWorkflow           = "delete from _vt.vreplication where db_name = 'vt_%s' and workflow = '%s'"
	updatePickedSourceTablet = `update _vt.vreplication set message='Picked source tablet: cell:\"%s\" uid:%d' where id=%d`
	getRowsCopied            = "SELECT rows_copied FROM _vt.vreplication WHERE id=%d"
	hasWorkflows             = "select if(count(*) > 0, 1, 0) as has_workflows from _vt.vreplication where db_name = '%s'"
	readAllWorkflows         = "select workflow, id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name = '%s'%s group by workflow, id order by workflow, id"
	readWorkflowsLimited     = "select workflow, id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name = '%s' and workflow in ('%s') group by workflow, id order by workflow, id"
	readWorkflow             = "select id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys from _vt.vreplication where workflow = '%s' and db_name = '%s'"
	readWorkflowConfig       = "select id, source, cell, tablet_types, state, message from _vt.vreplication where workflow = '%s'"
	updateWorkflow           = "update _vt.vreplication set state = '%s', source = '%s', cell = '%s', tablet_types = '%s' where id in (%d)"
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
		{
			name: "binlog source order with include",
			schema: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name:              "zt",
						Columns:           []string{"id"},
						PrimaryKeyColumns: []string{"id"},
						Fields:            sqltypes.MakeTestFields("id", "int64"),
					},
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
				IncludeTables:      []string{"zt", "wut", "t1"},
				SourceTimeZone:     "EDT",
				OnDdl:              binlogdatapb.OnDDLAction_EXEC.String(),
				StopAfterCopy:      true,
				DropForeignKeys:    true,
				DeferSecondaryKeys: true,
				AutoStart:          true,
			},
			query: fmt.Sprintf(`%s values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"} rules:{match:\"wut\" filter:\"select * from wut\"} rules:{match:\"zt\" filter:\"select * from zt\"}} on_ddl:EXEC stop_after_copy:true source_time_zone:\"EDT\" target_time_zone:\"UTC\"', '', 0, 0, '%s', '', now(), 0, 'Stopped', '%s', 1, 0, 1)`,
				insertVReplicationPrefix, wf, sourceKs, shard, tenv.cells[0], tenv.dbName),
		},
		{
			name: "binlog source order with all-tables",
			schema: &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
					{
						Name:              "zt",
						Columns:           []string{"id"},
						PrimaryKeyColumns: []string{"id"},
						Fields:            sqltypes.MakeTestFields("id", "int64"),
					},
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
				AllTables:          true,
				SourceTimeZone:     "EDT",
				OnDdl:              binlogdatapb.OnDDLAction_EXEC.String(),
				StopAfterCopy:      true,
				DropForeignKeys:    true,
				DeferSecondaryKeys: true,
				AutoStart:          true,
			},
			query: fmt.Sprintf(`%s values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"} rules:{match:\"wut\" filter:\"select * from wut\"} rules:{match:\"zt\" filter:\"select * from zt\"}} on_ddl:EXEC stop_after_copy:true source_time_zone:\"EDT\" target_time_zone:\"UTC\"', '', 0, 0, '%s', '', now(), 0, 'Stopped', '%s', 1, 0, 1)`,
				insertVReplicationPrefix, wf, sourceKs, shard, tenv.cells[0], tenv.dbName),
		},
	}

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

			// This is our expected query, which will also short circuit
			// the test with an error as at this point we've tested what
			// we wanted to test.
			targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
			targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
			targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
			targetTablet.vrdbClient.ExpectRequest(tt.query, &sqltypes.Result{}, errShortCircuit)
			_, err := ws.MoveTablesCreate(ctx, tt.req)
			tenv.tmc.tablets[targetTabletUID].vrdbClient.Wait()
			require.ErrorIs(t, err, errShortCircuit)
		})
	}
}

// TestMoveTables tests the query sequence originating from a
// VtctldServer MoveTablesCreate request to ensure that the
// VReplication stream(s) are created correctly and expected
// results returned. Followed by ensuring that SwitchTraffic
// and ReverseTraffic also work as expected.
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
	vreplID := 1
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

	err := tenv.ts.SaveVSchema(ctx, globalKs, &vschemapb.Keyspace{
		Sharded: false,
		Tables: map[string]*vschemapb.Table{
			"t1_seq": {
				Type: vindexes.TypeSequence,
			},
		},
	})
	require.NoError(t, err)
	err = tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
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
	require.NoError(t, err)

	ws := workflow.NewServer(vtenv.NewTestEnv(), tenv.ts, tenv.tmc)

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
		addInvariants(ftc.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])

		tenv.tmc.setVReplicationExecResults(ftc.tablet, getCopyState, &sqltypes.Result{})
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
		insert := fmt.Sprintf(`%s values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(id, \'%s.hash\', \'%s\')\"}}', '', 0, 0, '%s', 'primary,replica,rdonly', now(), 0, 'Stopped', '%s', %d, 0, 0)`,
			insertVReplicationPrefix, wf, sourceKs, sourceShard, targetKs, ftc.tablet.Shard, tenv.cells[0], tenv.dbName, vreplID)
		ftc.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: 1}, nil)
		ftc.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|source",
					"int64|varchar",
				),
				fmt.Sprintf("%d|%s", vreplID, bls),
			), nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, wf, tenv.dbName), sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
				"int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
			),
			fmt.Sprintf("%d|%s|%s|NULL|0|0|||1686577659|0|Stopped||%s|1||0|0|0||0|1", vreplID, bls, position, targetKs),
		), nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflowConfig, wf), sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source|cell|tablet_types|state|message",
				"int64|blob|varchar|varchar|varchar|varchar",
			),
			fmt.Sprintf("%d|%s|||Stopped|", vreplID, bls),
		), nil)
		ftc.vrdbClient.ExpectRequest(idQuery, idRes, nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(updateWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String(), strings.Replace(bls, `"`, `\"`, -1), "", "", vreplID), &sqltypes.Result{}, nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|source",
					"int64|varchar",
				),
				fmt.Sprintf("%d|%s", vreplID, bls),
			), nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, wf, tenv.dbName), sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
				"int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
			),
			fmt.Sprintf("%d|%s|%s|NULL|0|0|||1686577659|0|Running||%s|1||0|0|0||0|1", vreplID, bls, position, targetKs),
		), nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflowsLimited, tenv.dbName, wf), sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"workflow|id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
				"workflow|int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
			),
			fmt.Sprintf("%s|%d|%s|%s|NULL|0|0|||1686577659|0|Running||%s|1||0|0|0||0|1", wf, vreplID, bls, position, targetKs),
		), nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, wf, tenv.dbName), sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
				"int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
			),
			fmt.Sprintf("%d|%s|%s|NULL|0|0|||1686577659|0|Running||%s|1||0|0|0||0|1", vreplID, bls, position, targetKs),
		), nil)
		tenv.tmc.setVReplicationExecResults(ftc.tablet, fmt.Sprintf(getLatestCopyState, vreplID, vreplID), &sqltypes.Result{})
	}

	// We use the tablet's UID in the mocked results for the max value used on each target shard.
	nextSeqVal := int(math.Max(float64(targetShards["-80"].tablet.Alias.Uid), float64(targetShards["80-"].tablet.Alias.Uid))) + 1
	tenv.tmc.setVReplicationExecResults(globalTablet.tablet,
		sqlparser.BuildParsedQuery(initSequenceTable, sqlescape.EscapeID(fmt.Sprintf("vt_%s", globalKs)), sqlescape.EscapeID("t1_seq"), nextSeqVal, nextSeqVal, nextSeqVal).Query,
		&sqltypes.Result{RowsAffected: 0},
	)

	_, err = ws.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		Workflow:       wf,
		TabletTypes:    tabletTypes,
		Cells:          tenv.cells,
		AllTables:      true,
		AutoStart:      true,
	})
	require.NoError(t, err)

	for _, ftc := range targetShards {
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflowsLimited, tenv.dbName, wf), sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"workflow|id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
				"workflow|int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
			),
			fmt.Sprintf("%s|%d|%s|%s|NULL|0|0|||1686577659|0|Running||%s|1||0|0|0||0|1", wf, vreplID, bls, position, targetKs),
		), nil)
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, wf, tenv.dbName), sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
				"int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
			),
			fmt.Sprintf("%d|%s|%s|NULL|0|0|||1686577659|0|Running||%s|1||0|0|0||0|1", vreplID, bls, position, targetKs),
		), nil)
	}

	_, err = ws.WorkflowSwitchTraffic(ctx, &vtctldatapb.WorkflowSwitchTrafficRequest{
		Keyspace:                  targetKs,
		Workflow:                  wf,
		Cells:                     tenv.cells,
		MaxReplicationLagAllowed:  &vttimepb.Duration{Seconds: 922337203},
		EnableReverseReplication:  true,
		InitializeTargetSequences: true,
		Direction:                 int32(workflow.DirectionForward),
	})
	require.NoError(t, err)

	for _, ftc := range targetShards {
		ftc.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, wf, tenv.dbName), sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
				"int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
			),
			fmt.Sprintf("%d|%s|%s|NULL|0|0|||1686577659|0|Running||%s|1||0|0|0||0|1", vreplID, bls, position, targetKs),
		), nil)
	}
	addInvariants(sourceTablet.vrdbClient, vreplID, sourceTabletUID, position, workflow.ReverseWorkflowName(wf), tenv.cells[0])
	sourceTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, workflow.ReverseWorkflowName(wf), tenv.dbName), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
		),
		fmt.Sprintf("%d|%s|%s|NULL|0|0|||1686577659|0|Running||%s|1||0|0|0||0|1", vreplID, bls, position, sourceKs),
	), nil)
	sourceTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflowsLimited, tenv.dbName, workflow.ReverseWorkflowName(wf)), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"workflow|id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
			"workflow|int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
		),
		fmt.Sprintf("%s|%d|%s|%s|NULL|0|0|||1686577659|0|Running||%s|1||0|0|0||0|1", workflow.ReverseWorkflowName(wf), vreplID, bls, position, sourceKs),
	), nil)
	sourceTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, wf, tenv.dbName), &sqltypes.Result{}, nil)

	_, err = ws.WorkflowSwitchTraffic(ctx, &vtctldatapb.WorkflowSwitchTrafficRequest{
		Keyspace:                 targetKs,
		Workflow:                 wf,
		Cells:                    tenv.cells,
		MaxReplicationLagAllowed: &vttimepb.Duration{Seconds: 922337203},
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

	parsed := sqlparser.BuildParsedQuery(sqlSelectVReplicationWorkflowConfig, sidecar.GetIdentifier(), ":wf")
	bindVars := map[string]*querypb.BindVariable{
		"wf": sqltypes.StringBindVariable(workflow),
	}
	selectQuery, err := parsed.GenerateQuery(bindVars, nil)
	require.NoError(t, err)
	blsStr := fmt.Sprintf(`keyspace:"%s" shard:"%s" filter:{rules:{match:"corder" filter:"select * from corder"} rules:{match:"customer" filter:"select * from customer"}}`,
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

	tests := []struct {
		name    string
		request *tabletmanagerdatapb.UpdateVReplicationWorkflowRequest
		query   string
	}{
		{
			name: "update cells",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow: workflow,
				State:    binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				Cells:    []string{"zone2"},
				// TabletTypes is an empty value, so the current value should be cleared
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"corder\" filter:\"select * from corder\"} rules:{match:\"customer\" filter:\"select * from customer\"}}', cell = '%s', tablet_types = '' where id in (%d)`,
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
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"corder\" filter:\"select * from corder\"} rules:{match:\"customer\" filter:\"select * from customer\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
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
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"corder\" filter:\"select * from corder\"} rules:{match:\"customer\" filter:\"select * from customer\"}}', cell = '', tablet_types = '%s' where id in (%d)`,
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
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"corder\" filter:\"select * from corder\"} rules:{match:\"customer\" filter:\"select * from customer\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				keyspace, shard, cells[0], "rdonly", vreplID),
		},
		{
			name: "update on_ddl",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Workflow: workflow,
				State:    binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				OnDdl:    binlogdatapb.OnDDLAction_EXEC,
			},
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"corder\" filter:\"select * from corder\"} rules:{match:\"customer\" filter:\"select * from customer\"}} on_ddl:%s', cell = '', tablet_types = '' where id in (%d)`,
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
			query: fmt.Sprintf(`update _vt.vreplication set state = 'Running', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"corder\" filter:\"select * from corder\"} rules:{match:\"customer\" filter:\"select * from customer\"}} on_ddl:%s', cell = '%s', tablet_types = '%s' where id in (%d)`,
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
			query: fmt.Sprintf(`update _vt.vreplication set state = '%s', source = 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"corder\" filter:\"select * from corder\"} rules:{match:\"customer\" filter:\"select * from customer\"}}', cell = '%s', tablet_types = '%s' where id in (%d)`,
				binlogdatapb.VReplicationWorkflowState_Stopped.String(), keyspace, shard, cells[0], tabletTypes[0], vreplID),
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
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(selectQuery, selectRes, nil)
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
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

func TestUpdateVReplicationWorkflows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	keyspace := "testks"
	tabletUID := 100
	// VREngine.Exec queries the records in the table and explicitly adds a where id in (...) clause.
	vreplIDs := []string{"1", "2", "3"}

	tenv := newTestEnv(t, ctx, keyspace, []string{shard})
	defer tenv.close()

	tablet := tenv.addTablet(t, tabletUID, keyspace, shard)
	defer tenv.deleteTablet(tablet.tablet)

	tests := []struct {
		name    string
		request *tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest
		query   string
	}{
		{
			name: "update only state=running for all workflows",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				AllWorkflows: true,
				State:        binlogdatapb.VReplicationWorkflowState_Running,
				Message:      textutil.SimulatedNullString,
				StopPosition: textutil.SimulatedNullString,
			},
			query: fmt.Sprintf(`update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Running' where id in (%s)`, strings.Join(vreplIDs, ", ")),
		},
		{
			name: "update only state=running for all but reverse workflows",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				ExcludeWorkflows: []string{workflow.ReverseWorkflowName("testwf")},
				State:            binlogdatapb.VReplicationWorkflowState_Running,
				Message:          textutil.SimulatedNullString,
				StopPosition:     textutil.SimulatedNullString,
			},
			query: fmt.Sprintf(`update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Running' where id in (%s)`, strings.Join(vreplIDs, ", ")),
		},
		{
			name: "update all vals for all workflows",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				AllWorkflows: true,
				State:        binlogdatapb.VReplicationWorkflowState_Running,
				Message:      "hi",
				StopPosition: position,
			},
			query: fmt.Sprintf(`update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Running', message = 'hi', stop_pos = '%s' where id in (%s)`, position, strings.Join(vreplIDs, ", ")),
		},
		{
			name: "update state=stopped, messege=for vdiff for two workflows",
			request: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				IncludeWorkflows: []string{"testwf", "testwf2"},
				State:            binlogdatapb.VReplicationWorkflowState_Running,
				Message:          textutil.SimulatedNullString,
				StopPosition:     textutil.SimulatedNullString,
			},
			query: fmt.Sprintf(`update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Running' where id in (%s)`, strings.Join(vreplIDs, ", ")),
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
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
			addlPredicates := ""
			if len(tt.request.GetIncludeWorkflows()) > 0 {
				addlPredicates = fmt.Sprintf(" and workflow in ('%s')", strings.Join(tt.request.GetIncludeWorkflows(), "', '"))
			}
			if len(tt.request.GetExcludeWorkflows()) > 0 {
				addlPredicates = fmt.Sprintf(" and workflow not in ('%s')", strings.Join(tt.request.GetExcludeWorkflows(), "', '"))
			}
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(fmt.Sprintf("select id from _vt.vreplication where db_name = '%s'%s", tenv.dbName, addlPredicates),
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"id",
						"int64",
					),
					vreplIDs...),
				nil)

			// This is our expected query, which will also short circuit
			// the test with an error as at this point we've tested what
			// we wanted to test.
			tenv.tmc.tablets[tabletUID].vrdbClient.ExpectRequest(tt.query, &sqltypes.Result{}, errShortCircuit)
			_, err := tenv.tmc.tablets[tabletUID].tm.UpdateVReplicationWorkflows(ctx, tt.request)
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

	err := tenv.ts.SaveVSchema(ctx, sourceKs, &vschemapb.Keyspace{
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
	require.NoError(t, err)
	err = tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
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
	require.NoError(t, err)

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
		tenv.tmc.setVReplicationExecResults(tt.tablet, getCopyState, &sqltypes.Result{})
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
				err = tenv.ts.SaveVSchema(ctx, targetKs, tt.vschema)
				require.NoError(t, err)
			}

			for uid, streams := range tt.streams {
				tt := targetTablets[uid]
				tt.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
				tt.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
				for i, sourceShard := range streams {
					var err error
					if i == len(streams)-1 {
						// errShortCircuit is intentionally injected into the MoveTables
						// workflow to short-circuit the workflow after we've validated
						// everything we wanted to in the test.
						err = errShortCircuit
					}
					tt.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
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
	vreplID := 1
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

	addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])

	tenv.tmc.tablets[targetTabletUID].vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
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
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"id|source",
				"int64|varchar",
			),
			fmt.Sprintf("%d|%s", vreplID, bls),
		),
		nil,
	)

	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, wf, tenv.dbName), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|pos|stop_pos|max_tps|max_replication_lag|cell|tablet_types|time_updated|transaction_timestamp|state|message|db_name|rows_copied|tags|time_heartbeat|workflow_type|time_throttled|component_throttled|workflow_sub_type|defer_secondary_keys",
			"int64|varchar|blob|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|varchar|int64|varchar|int64|int64|int64|varchar|int64|int64",
		),
		fmt.Sprintf("%d|%s|%s|NULL|0|0|||1686577659|0|Stopped||%s|1||0|0|0||0|1", vreplID, bls, position, targetKs),
	), nil)
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

// TestHasVReplicationWorkflows tests the simple RPC to be sure
// that it generates the expected query and results for each
// request.
func TestHasVReplicationWorkflows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetTabletUID := 300
	shard := "0"
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	tests := []struct {
		name     string
		tablet   *fakeTabletConn
		queryRes *sqltypes.Result
		want     *tabletmanagerdatapb.HasVReplicationWorkflowsResponse
		wantErr  bool
	}{
		{
			name:   "source tablet",
			tablet: sourceTablet,
			queryRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"has_workflows",
					"int64",
				),
				"0",
			),
			want: &tabletmanagerdatapb.HasVReplicationWorkflowsResponse{
				Has: false,
			},
		},
		{
			name:   "target tablet",
			tablet: targetTablet,
			queryRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"has_workflows",
					"int64",
				),
				"1",
			),
			want: &tabletmanagerdatapb.HasVReplicationWorkflowsResponse{
				Has: true,
			},
		},
		{
			name:   "target tablet with error",
			tablet: targetTablet,
			queryRes: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"wut|yu",
					"varchar|varchar",
				),
				"byeee|felicia",
				"no|more",
			),
			want:    nil,
			wantErr: true,
		},
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

			require.NotNil(t, tt.tablet, "No tablet provided")

			req := &tabletmanagerdatapb.HasVReplicationWorkflowsRequest{}

			tt.tablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
			tt.tablet.vrdbClient.ExpectRequest(fmt.Sprintf(hasWorkflows, tenv.dbName), tt.queryRes, nil)

			got, err := tenv.tmc.HasVReplicationWorkflows(ctx, tt.tablet.tablet, req)
			if (err != nil) != tt.wantErr {
				t.Errorf("TabletManager.HasVReplicationWorkflows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TabletManager.HasVReplicationWorkflows() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestReadVReplicationWorkflows tests the RPC requests are turned
// into the expected proper SQL query.
func TestReadVReplicationWorkflows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tabletUID := 300
	ks := "targetks"
	shard := "0"
	tenv := newTestEnv(t, ctx, ks, []string{shard})
	defer tenv.close()

	tablet := tenv.addTablet(t, tabletUID, ks, shard)
	defer tenv.deleteTablet(tablet.tablet)

	tests := []struct {
		name      string
		req       *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest
		wantPreds string // Additional query predicates
		wantErr   bool
	}{
		{
			name: "nothing",
			req:  &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{},
			// No additional query predicates.
		},
		{
			name: "all except frozen",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				ExcludeFrozen: true,
			},
			wantPreds: " and message != 'FROZEN'",
		},
		{
			name: "1-3 unless frozen",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				IncludeIds:    []int32{1, 2, 3},
				ExcludeFrozen: true,
			},
			wantPreds: " and message != 'FROZEN' and id in (1,2,3)",
		},
		{
			name: "all but wf1 and wf2",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				ExcludeWorkflows: []string{"wf1", "wf2"},
			},
			wantPreds: " and workflow not in ('wf1','wf2')",
		},
		{
			name: "all but wf1 and wf2",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				ExcludeWorkflows: []string{"wf1", "wf2"},
			},
			wantPreds: " and workflow not in ('wf1','wf2')",
		},
		{
			name: "only wf1 and wf2",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				IncludeWorkflows: []string{"wf1", "wf2"},
				ExcludeStates: []binlogdatapb.VReplicationWorkflowState{
					binlogdatapb.VReplicationWorkflowState_Stopped,
				},
			},
			wantPreds: " and workflow in ('wf1','wf2') and state not in ('Stopped')",
		},
		{
			name: "only copying or running",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				IncludeStates: []binlogdatapb.VReplicationWorkflowState{
					binlogdatapb.VReplicationWorkflowState_Copying,
					binlogdatapb.VReplicationWorkflowState_Running,
				},
			},
			wantPreds: " and state in ('Copying','Running')",
		},
		{
			name: "mess of predicates",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				IncludeIds:       []int32{1, 3},
				IncludeWorkflows: []string{"wf1"},
				ExcludeWorkflows: []string{"wf2"},
				ExcludeStates: []binlogdatapb.VReplicationWorkflowState{
					binlogdatapb.VReplicationWorkflowState_Copying,
				},
				ExcludeFrozen: true,
			},
			wantPreds: " and message != 'FROZEN' and id in (1,3) and workflow in ('wf1') and workflow not in ('wf2') and state not in ('Copying')",
		},
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

			require.NotNil(t, tt.req, "No request provided")

			if !tt.wantErr { // Errors we're testing for occur before executing any queries.
				tablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
				tablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, tt.wantPreds), &sqltypes.Result{}, nil)
			}

			_, err := tenv.tmc.ReadVReplicationWorkflows(ctx, tablet.tablet, tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("TabletManager.ReadVReplicationWorkflows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

// addInvariants adds handling for queries that can be injected into the
// sequence of queries, N times, in a non-deterministic order.
func addInvariants(dbClient *binlogplayer.MockDBClient, vreplID, sourceTabletUID int, position, workflow, cell string) {
	// This reduces a lot of noise, but is also needed as it's executed when any of the
	// other queries here are executed via engine.exec().
	dbClient.AddInvariant(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{})

	// The binlogplayer queries result from the controller starting up and the sequence
	// within everything else is non-deterministic.
	dbClient.AddInvariant(fmt.Sprintf(getWorkflowState, vreplID), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"pos|stop_pos|max_tps|max_replication_lag|state|workflow_type|workflow|workflow_sub_type|defer_secondary_keys",
			"varchar|varchar|int64|int64|varchar|int64|varchar|int64|int64",
		),
		fmt.Sprintf("%s||0|0|Stopped|1|%s|0|0", position, workflow),
	))
	dbClient.AddInvariant(setSessionTZ, &sqltypes.Result{})
	dbClient.AddInvariant(setNames, &sqltypes.Result{})
	dbClient.AddInvariant(setNetReadTimeout, &sqltypes.Result{})
	dbClient.AddInvariant(setNetWriteTimeout, &sqltypes.Result{})

	// Same for the vreplicator queries.
	dbClient.AddInvariant(fmt.Sprintf(getNumCopyStateTable, vreplID), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"count(distinct table_name)",
			"int64",
		),
		"0",
	))
	dbClient.AddInvariant(getBinlogRowImage, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"@@binlog_row_image",
			"varchar",
		),
		"FULL",
	))
	dbClient.AddInvariant(fmt.Sprintf(getRowsCopied, vreplID), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"rows_copied",
			"int64",
		),
		"0",
	))
	dbClient.AddInvariant(fmt.Sprintf(updatePickedSourceTablet, cell, sourceTabletUID, vreplID), &sqltypes.Result{})
}

func addMaterializeSettingsTablesToSchema(ms *vtctldatapb.MaterializeSettings, tenv *testEnv, venv *vtenv.Environment) {
	schema := defaultSchema.CloneVT()
	for _, ts := range ms.TableSettings {
		tableName := ts.TargetTable
		table, err := venv.Parser().TableFromStatement(ts.SourceExpression)
		if err == nil {
			tableName = table.Name.String()
		}
		schema.TableDefinitions = append(schema.TableDefinitions, &tabletmanagerdatapb.TableDefinition{
			Name:   tableName,
			Schema: fmt.Sprintf("%s_schema", tableName),
		})
		schema.TableDefinitions = append(schema.TableDefinitions, &tabletmanagerdatapb.TableDefinition{
			Name:   ts.TargetTable,
			Schema: fmt.Sprintf("%s_schema", ts.TargetTable),
		})
	}
	tenv.tmc.SetSchema(schema)
}

func TestExternalizeLookupVindex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShard := "0"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	addInvariants(targetShards["-80"].vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)
	addInvariants(targetShards["80-"].vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		// Keyspace where the vindex is created.
		SourceKeyspace: sourceKs,
		// Keyspace where the lookup table and VReplication workflow is created.
		TargetKeyspace: targetKs,
		Cell:           tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	sourceVschema := &vschemapb.Keyspace{
		Sharded: false,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
			"owned_lookup": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      "targetks.owned_lookup",
					"from":       "c1",
					"to":         "c2",
					"write_only": "true",
				},
				Owner: "t1",
			},
			"unowned_lookup": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      "targetks.unowned_lookup",
					"from":       "c1",
					"to":         "c2",
					"write_only": "true",
				},
			},
			"unqualified_lookup": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table": "unqualified",
					"from":  "c1",
					"to":    "c2",
				},
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}, {
					Name:   "owned_lookup",
					Column: "col2",
				}},
			},
		},
	}

	trxTS := fmt.Sprintf("%d", time.Now().Unix())
	fields := sqltypes.MakeTestFields(
		"id|state|message|source|workflow_type|workflow_sub_type|max_tps|max_replication_lag|time_updated|time_heartbeat|time_throttled|transaction_timestamp|rows_copied",
		"int64|varbinary|varbinary|blob|int64|int64|int64|int64|int64|int64|int64|int64|int64",
	)
	wftype := fmt.Sprintf("%d", binlogdatapb.VReplicationWorkflowType_CreateLookupIndex)
	ownedSourceStopAfterCopy := fmt.Sprintf(`keyspace:"%s",shard:"0",filter:{rules:{match:"owned_lookup" filter:"select * from t1 where in_keyrange(col1, '%s.xxhash', '-80')"}} stop_after_copy:true`,
		ms.SourceKeyspace, ms.SourceKeyspace)
	ownedSourceKeepRunningAfterCopy := fmt.Sprintf(`keyspace:"%s",shard:"0",filter:{rules:{match:"owned_lookup" filter:"select * from t1 where in_keyrange(col1, '%s.xxhash', '-80')"}}`,
		ms.SourceKeyspace, ms.SourceKeyspace)
	ownedRunning := sqltypes.MakeTestResult(fields, "1|Running|msg|"+ownedSourceKeepRunningAfterCopy+"|"+wftype+"|0|0|0|0|0|0|"+trxTS+"|5")
	ownedStopped := sqltypes.MakeTestResult(fields, "1|Stopped|Stopped after copy|"+ownedSourceStopAfterCopy+"|"+wftype+"|0|0|0|0|0|0|"+trxTS+"|5")
	unownedSourceStopAfterCopy := fmt.Sprintf(`keyspace:"%s",shard:"0",filter:{rules:{match:"unowned_lookup" filter:"select * from t1 where in_keyrange(col1, '%s.xxhash', '-80')"}} stop_after_copy:true`,
		ms.SourceKeyspace, ms.SourceKeyspace)
	unownedSourceKeepRunningAfterCopy := fmt.Sprintf(`keyspace:"%s",shard:"0",filter:{rules:{match:"unowned_lookup" filter:"select * from t1 where in_keyrange(col1, '%s.xxhash', '-80')"}}`,
		ms.SourceKeyspace, ms.SourceKeyspace)
	unownedRunning := sqltypes.MakeTestResult(fields, "2|Running|msg|"+unownedSourceKeepRunningAfterCopy+"|"+wftype+"|0|0|0|0|0|0|"+trxTS+"|5")
	unownedStopped := sqltypes.MakeTestResult(fields, "2|Stopped|Stopped after copy|"+unownedSourceStopAfterCopy+"|"+wftype+"|0|0|0|0|0|0|"+trxTS+"|5")

	testcases := []struct {
		request         *vtctldatapb.LookupVindexExternalizeRequest
		vrResponse      *sqltypes.Result
		err             string
		expectedVschema *vschemapb.Keyspace
		expectDelete    bool
	}{
		{
			request: &vtctldatapb.LookupVindexExternalizeRequest{
				Name:          "owned_lookup",
				Keyspace:      ms.SourceKeyspace,
				TableKeyspace: ms.TargetKeyspace,
			},
			vrResponse: ownedStopped,
			expectedVschema: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"owned_lookup": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.owned_lookup",
							"from":  "c1",
							"to":    "c2",
						},
						Owner: "t1",
					},
				},
			},
			expectDelete: true,
		},
		{
			request: &vtctldatapb.LookupVindexExternalizeRequest{
				Name:          "unowned_lookup",
				Keyspace:      ms.SourceKeyspace,
				TableKeyspace: ms.TargetKeyspace,
			},
			vrResponse: unownedStopped,
			expectedVschema: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"unowned_lookup": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.unowned_lookup",
							"from":  "c1",
							"to":    "c2",
						},
					},
				},
			},
			err: "is not in Running state",
		},
		{
			request: &vtctldatapb.LookupVindexExternalizeRequest{
				Name:          "owned_lookup",
				Keyspace:      ms.SourceKeyspace,
				TableKeyspace: ms.TargetKeyspace,
			},
			vrResponse: ownedRunning,
			expectedVschema: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"owned_lookup": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.owned_lookup",
							"from":  "c1",
							"to":    "c2",
						},
						Owner: "t1",
					},
				},
			},
			expectDelete: true,
		},
		{
			request: &vtctldatapb.LookupVindexExternalizeRequest{
				Name:          "unowned_lookup",
				Keyspace:      ms.SourceKeyspace,
				TableKeyspace: ms.TargetKeyspace,
			},
			vrResponse: unownedRunning,
			expectedVschema: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"unowned_lookup": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.unowned_lookup",
							"from":  "c1",
							"to":    "c2",
						},
					},
				},
			},
		},
		{
			request: &vtctldatapb.LookupVindexExternalizeRequest{
				Name:          "absent_lookup",
				Keyspace:      ms.SourceKeyspace,
				TableKeyspace: ms.TargetKeyspace,
			},
			expectedVschema: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"absent_lookup": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.absent_lookup",
							"from":  "c1",
							"to":    "c2",
						},
					},
				},
			},
			err: "vindex absent_lookup not found in the sourceks keyspace",
		},
	}
	for _, tcase := range testcases {
		t.Run(tcase.request.Name, func(t *testing.T) {
			// Resave the source schema for every iteration.
			err := tenv.ts.SaveVSchema(ctx, tcase.request.Keyspace, sourceVschema)
			require.NoError(t, err)
			err = tenv.ts.RebuildSrvVSchema(ctx, []string{tenv.cells[0]})
			require.NoError(t, err)

			require.NotNil(t, tcase.request, "No request provided")

			for _, targetTablet := range targetShards {
				targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, tcase.request.Name, tenv.dbName), tcase.vrResponse, nil)
				if tcase.err == "" {
					// We query the workflow again to build the status output when
					// it's successfully created.
					targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readWorkflow, tcase.request.Name, tenv.dbName), tcase.vrResponse, nil)
				}
			}

			preWorkflowDeleteCalls := tenv.tmc.workflowDeleteCalls
			_, err = ws.LookupVindexExternalize(ctx, tcase.request)
			if tcase.err != "" {
				if err == nil || !strings.Contains(err.Error(), tcase.err) {
					require.FailNow(t, "LookupVindexExternalize error", "ExternalizeVindex(%v) err: %v, must contain %v", tcase.request, err, tcase.err)
				}
				return
			}
			require.NoError(t, err)
			expectedWorkflowDeleteCalls := preWorkflowDeleteCalls
			if tcase.expectDelete {
				// We expect the RPC to be called on each target shard.
				expectedWorkflowDeleteCalls = preWorkflowDeleteCalls + (len(targetShards))
			}
			require.Equal(t, expectedWorkflowDeleteCalls, tenv.tmc.workflowDeleteCalls)

			aftervschema, err := tenv.ts.GetVSchema(ctx, ms.SourceKeyspace)
			require.NoError(t, err)
			vindex := aftervschema.Vindexes[tcase.request.Name]
			expectedVindex := tcase.expectedVschema.Vindexes[tcase.request.Name]
			require.NotNil(t, vindex, "vindex %s not found in vschema", tcase.request.Name)
			require.NotContains(t, vindex.Params, "write_only", tcase.request)
			require.Equal(t, expectedVindex, vindex, "vindex mismatch. expected: %+v, got: %+v", expectedVindex, vindex)
		})
	}
}

func TestMaterializerOneToOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetTabletUID := 300
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{
			{
				TargetTable:      "t1",
				SourceExpression: "select * from t1",
				CreateDdl:        "t1ddl",
			},
			{
				TargetTable:      "t2",
				SourceExpression: "select * from t3",
				CreateDdl:        "t2ddl",
			},
			{
				TargetTable:      "t4",
				SourceExpression: "", // empty
				CreateDdl:        "t4ddl",
			},
		},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	insert := insertVReplicationPrefix +
		fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"} rules:{match:\"t2\" filter:\"select * from t3\"} rules:{match:\"t4\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
			wf, sourceKs, shard, tenv.cells[0], tenv.dbName)
	targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{}, errShortCircuit)

	err := ws.Materialize(ctx, ms)
	targetTablet.vrdbClient.Wait()
	require.ErrorIs(t, err, errShortCircuit)
}

func TestMaterializerManyToOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	sourceShards := make(map[string]*fakeTabletConn)
	targetKs := "targetks"
	targetTabletUID := 300
	targetShard := "0"
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceShards["-80"] = tenv.addTablet(t, sourceTabletUID, sourceKs, "-80")
	defer tenv.deleteTablet(sourceShards["-80"].tablet)
	sourceShards["80-"] = tenv.addTablet(t, sourceTabletUID+10, sourceKs, "80-")
	defer tenv.deleteTablet(sourceShards["80-"].tablet)

	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, targetShard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t3",
			CreateDdl:        "t2ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)
	targetTablet.vrdbClient.AddInvariant("update _vt.vreplication set message='no schema defined' where id=1", &sqltypes.Result{}) // If the first workflow controller progresses ...
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	for _, sourceShard := range []string{"-80", "80-"} { // One insert per [binlog]source/stream
		addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])

		bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"} rules:{match:\"t2\" filter:\"select * from t3\"}}", sourceKs, sourceShard)
		insert := insertVReplicationPrefix +
			fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"} rules:{match:\"t2\" filter:\"select * from t3\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
				wf, sourceKs, sourceShard, tenv.cells[0], tenv.dbName)
		if vreplID == 1 {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, nil)
			targetTablet.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
			targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"id|source",
						"int64|varchar",
					),
					fmt.Sprintf("%d|%s", vreplID, bls),
				), nil)
			vreplID++
		} else {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, errShortCircuit)
		}
	}

	err := ws.Materialize(ctx, ms)
	targetTablet.vrdbClient.Wait()
	require.ErrorIs(t, err, errShortCircuit)
}

func TestMaterializerOneToMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShard := "0"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "xxhash",
				}},
			},
		},
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	for _, targetShard := range []string{"-80", "80-"} {
		targetTablet := targetShards[targetShard]
		addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])
		targetTablet.vrdbClient.AddInvariant("update _vt.vreplication set message='no schema defined' where id=1", &sqltypes.Result{}) // If the first workflow controller progresses ...
		targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

		bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(c1, '%s.xxhash', '%s')\"}}",
			sourceKs, sourceShard, targetKs, targetShard)
		insert := insertVReplicationPrefix +
			fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(c1, \'%s.xxhash\', \'%s\')\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
				wf, sourceKs, sourceShard, targetKs, targetShard, tenv.cells[0], tenv.dbName)
		if targetShard == "-80" {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, nil)
			targetTablet.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
			targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"id|source",
						"int64|varchar",
					),
					fmt.Sprintf("%d|%s", vreplID, bls),
				), nil)
		} else {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, errShortCircuit)
		}
	}

	err = ws.Materialize(ctx, ms)
	for _, targetTablet := range targetShards {
		targetTablet.vrdbClient.Wait()
	}
	require.ErrorIs(t, err, errShortCircuit)
}

func TestMaterializerManyToMany(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShards := make(map[string]*fakeTabletConn)
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceShards["-40"] = tenv.addTablet(t, sourceTabletUID, sourceKs, "-40")
	defer tenv.deleteTablet(sourceShards["-40"].tablet)
	sourceShards["40-"] = tenv.addTablet(t, sourceTabletUID+10, sourceKs, "40-")
	defer tenv.deleteTablet(sourceShards["40-"].tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "xxhash",
				}},
			},
		},
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	for _, targetShard := range []string{"-80", "80-"} {
		targetTablet := targetShards[targetShard]
		targetTablet.vrdbClient.AddInvariant("update _vt.vreplication set message='no schema defined' where id=1", &sqltypes.Result{}) // If the first workflow controller progresses ...
		targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

		for i, sourceShard := range []string{"-40", "40-"} { // One insert per [binlog]source/stream
			addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID+(i*10), position, wf, tenv.cells[0])
			bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(c1, '%s.xxhash', '%s')\"}}",
				sourceKs, sourceShard, targetKs, targetShard)
			insert := insertVReplicationPrefix +
				fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(c1, \'%s.xxhash\', \'%s\')\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
					wf, sourceKs, sourceShard, targetKs, targetShard, tenv.cells[0], tenv.dbName)
			if targetShard == "80-" && sourceShard == "40-" { // Last insert
				targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, errShortCircuit)
			} else { // Can't short circuit as we will do more inserts
				targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, nil)
				targetTablet.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
				targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
					sqltypes.MakeTestResult(
						sqltypes.MakeTestFields(
							"id|source",
							"int64|varchar",
						),
						fmt.Sprintf("%d|%s", vreplID, bls),
					), nil)
			}
		}
	}

	err = ws.Materialize(ctx, ms)
	for _, targetTablet := range targetShards {
		targetTablet.vrdbClient.Wait()
	}
	require.ErrorIs(t, err, errShortCircuit)
}

func TestMaterializerMulticolumnVindex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShard := "0"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"region": {
				Type: "region_experimental",
				Params: map[string]string{
					"region_bytes": "1",
				},
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Columns: []string{"c1", "c2"},
					Name:    "region",
				}},
			},
		},
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	for _, targetShard := range []string{"-80", "80-"} {
		targetTablet := targetShards[targetShard]
		addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])
		targetTablet.vrdbClient.AddInvariant("update _vt.vreplication set message='no schema defined' where id=1", &sqltypes.Result{}) // If the first workflow controller progresses ...
		targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

		bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(c1, c2, '%s.region', '%s')\"}}",
			sourceKs, sourceShard, targetKs, targetShard)
		insert := insertVReplicationPrefix +
			fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1 where in_keyrange(c1, c2, \'%s.region\', \'%s\')\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
				wf, sourceKs, sourceShard, targetKs, targetShard, tenv.cells[0], tenv.dbName)
		if targetShard == "-80" {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, nil)
			targetTablet.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
			targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"id|source",
						"int64|varchar",
					),
					fmt.Sprintf("%d|%s", vreplID, bls),
				), nil)
		} else {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, errShortCircuit)
		}
	}

	err = ws.Materialize(ctx, ms)
	for _, targetTablet := range targetShards {
		targetTablet.vrdbClient.Wait()
	}
	require.ErrorIs(t, err, errShortCircuit)
}

func TestMaterializerDeploySchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t3",
			CreateDdl:        "t2ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// Remove t2 from the target tablet's schema so that it must
	// be deployed.
	schema := tenv.tmc.schema.CloneVT()
	for i, sd := range schema.TableDefinitions {
		if sd.Name == "t2" {
			schema.TableDefinitions = append(schema.TableDefinitions[:i], schema.TableDefinitions[i+1:]...)
		}
	}
	tenv.tmc.tabletSchemas[targetTabletUID] = schema
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, `t2ddl`, &sqltypes.Result{}) // Execute the fake CreateDdl

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	insert := insertVReplicationPrefix +
		fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"} rules:{match:\"t2\" filter:\"select * from t3\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
			wf, sourceKs, shard, tenv.cells[0], tenv.dbName)
	targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{}, errShortCircuit)

	err := ws.Materialize(ctx, ms)
	targetTablet.vrdbClient.Wait()
	require.ErrorIs(t, err, errShortCircuit)
	require.Equal(t, 1, tenv.tmc.getSchemaRequestCount(sourceTabletUID))
	require.Equal(t, 1, tenv.tmc.getSchemaRequestCount(targetTabletUID))
}

func TestMaterializerCopySchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "copy",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t3",
			CreateDdl:        "t2ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// Remove t1 from the target tablet's schema so that it must
	// be copied. The workflow should still succeed w/o it existing
	// when we start.
	schema := tenv.tmc.schema.CloneVT()
	for i, sd := range schema.TableDefinitions {
		if sd.Name == "t1" {
			schema.TableDefinitions = append(schema.TableDefinitions[:i], schema.TableDefinitions[i+1:]...)
		}
	}
	tenv.tmc.tabletSchemas[targetTabletUID] = schema
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	insert := insertVReplicationPrefix +
		fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"} rules:{match:\"t2\" filter:\"select * from t3\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
			wf, sourceKs, shard, tenv.cells[0], tenv.dbName)
	targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{}, errShortCircuit)

	err := ws.Materialize(ctx, ms)
	targetTablet.vrdbClient.Wait()
	require.ErrorIs(t, err, errShortCircuit)
	require.Equal(t, 0, tenv.tmc.getSchemaRequestCount(sourceTabletUID))
	require.Equal(t, 1, tenv.tmc.getSchemaRequestCount(targetTabletUID))
}

func TestMaterializerExplicitColumns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShard := "0"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select c1, c1+c2, c2 from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"region": {
				Type: "region_experimental",
				Params: map[string]string{
					"region_bytes": "1",
				},
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Columns: []string{"c1", "c2"},
					Name:    "region",
				}},
			},
		},
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	for _, targetShard := range []string{"-80", "80-"} {
		targetTablet := targetShards[targetShard]
		addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])
		targetTablet.vrdbClient.AddInvariant("update _vt.vreplication set message='no schema defined' where id=1", &sqltypes.Result{}) // If the first workflow controller progresses ...
		targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

		bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select c1, c1 + c2, c2 from t1 where in_keyrange(c1, c2, '%s.region', '%s')\"}}",
			sourceKs, sourceShard, targetKs, targetShard)
		insert := insertVReplicationPrefix +
			fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select c1, c1 + c2, c2 from t1 where in_keyrange(c1, c2, \'%s.region\', \'%s\')\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
				wf, sourceKs, sourceShard, targetKs, targetShard, tenv.cells[0], tenv.dbName)
		if targetShard == "-80" {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, nil)
			targetTablet.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
			targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"id|source",
						"int64|varchar",
					),
					fmt.Sprintf("%d|%s", vreplID, bls),
				), nil)
		} else {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, errShortCircuit)
		}
	}

	err = ws.Materialize(ctx, ms)
	for _, targetTablet := range targetShards {
		targetTablet.vrdbClient.Wait()
	}
	require.ErrorIs(t, err, errShortCircuit)
}

func TestMaterializerRenamedColumns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShard := "0"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select c3 as c1, c1+c2, c4 as c2 from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"region": {
				Type: "region_experimental",
				Params: map[string]string{
					"region_bytes": "1",
				},
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Columns: []string{"c1", "c2"},
					Name:    "region",
				}},
			},
		},
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	for _, targetShard := range []string{"-80", "80-"} {
		targetTablet := targetShards[targetShard]
		addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])
		targetTablet.vrdbClient.AddInvariant("update _vt.vreplication set message='no schema defined' where id=1", &sqltypes.Result{}) // If the first workflow controller progresses ...
		targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

		bls := fmt.Sprintf("keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select c3 as c1, c1 + c2, c4 as c2 from t1 where in_keyrange(c3, c4, '%s.region', '%s')\"}}",
			sourceKs, sourceShard, targetKs, targetShard)
		insert := insertVReplicationPrefix +
			fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select c3 as c1, c1 + c2, c4 as c2 from t1 where in_keyrange(c3, c4, \'%s.region\', \'%s\')\"}}', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
				wf, sourceKs, sourceShard, targetKs, targetShard, tenv.cells[0], tenv.dbName)
		if targetShard == "-80" {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, nil)
			targetTablet.vrdbClient.ExpectRequest(getAutoIncrementStep, &sqltypes.Result{}, nil)
			targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(getVReplicationRecord, vreplID),
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"id|source",
						"int64|varchar",
					),
					fmt.Sprintf("%d|%s", vreplID, bls),
				), nil)
		} else {
			targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{InsertID: uint64(vreplID)}, errShortCircuit)
		}
	}

	err = ws.Materialize(ctx, ms)
	for _, targetTablet := range targetShards {
		targetTablet.vrdbClient.Wait()
	}
	require.ErrorIs(t, err, errShortCircuit)
}

func TestMaterializerStopAfterCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetTabletUID := 300
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		StopAfterCopy:  true,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t3",
			CreateDdl:        "t2ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)

	// This is our expected query, which will also short circuit
	// the test with an error as at this point we've tested what
	// we wanted to test.
	insert := insertVReplicationPrefix +
		fmt.Sprintf(` values ('%s', 'keyspace:\"%s\" shard:\"%s\" filter:{rules:{match:\"t1\" filter:\"select * from t1\"} rules:{match:\"t2\" filter:\"select * from t3\"}} stop_after_copy:true', '', 0, 0, '%s', 'primary,rdonly', now(), 0, 'Stopped', '%s', 0, 0, 0)`,
			wf, sourceKs, shard, tenv.cells[0], tenv.dbName)
	targetTablet.vrdbClient.ExpectRequest(insert, &sqltypes.Result{}, errShortCircuit)

	err := ws.Materialize(ctx, ms)
	targetTablet.vrdbClient.Wait()
	require.ErrorIs(t, err, errShortCircuit)
}

func TestMaterializerNoTargetVSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

	err = ws.Materialize(ctx, ms)
	targetTablet.vrdbClient.Wait()
	require.EqualError(t, err, fmt.Sprintf("table t1 not found in vschema for keyspace %s", targetKs))
}

func TestMaterializerNoDDL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	// Clear out the schema on the target tablet.
	tenv.tmc.tabletSchemas[targetTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

	err := ws.Materialize(ctx, ms)
	require.EqualError(t, err, "target table t1 does not exist and there is no create ddl defined")
	require.Equal(t, tenv.tmc.getSchemaRequestCount(100), 0)
	require.Equal(t, tenv.tmc.getSchemaRequestCount(200), 1)

}

func TestMaterializerNoSourcePrimary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "copy",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	tenv.tmc.tabletSchemas[targetTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	targetTablet.tablet.Type = topodatapb.TabletType_REPLICA
	_, _ = tenv.ts.UpdateShardFields(tenv.ctx, targetKs, shard, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = nil
		return nil
	})

	err := ws.Materialize(ctx, ms)
	require.EqualError(t, err, "shard has no primary: 0")
}

func TestMaterializerTableMismatchNonCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t2",
			CreateDdl:        "",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// Clear out the schema on the target tablet.
	tenv.tmc.tabletSchemas[targetTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

	err := ws.Materialize(ctx, ms)
	require.EqualError(t, err, "target table t1 does not exist and there is no create ddl defined")
}

func TestMaterializerTableMismatchCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t2",
			CreateDdl:        "copy",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// Clear out the schema on the target tablet.
	tenv.tmc.tabletSchemas[targetTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

	err := ws.Materialize(ctx, ms)
	require.EqualError(t, err, "source and target table names must match for copying schema: t2 vs t1")
}

func TestMaterializerNoSourceTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "copy",
		}},
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// Clear out the schema on the source and target tablet.
	tenv.tmc.tabletSchemas[sourceTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	tenv.tmc.tabletSchemas[targetTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

	err := ws.Materialize(ctx, ms)
	require.EqualError(t, err, "source table t1 does not exist")
}

func TestMaterializerSyntaxError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "bad query",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// Clear out the schema on the source and target tablet.
	tenv.tmc.tabletSchemas[sourceTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	tenv.tmc.tabletSchemas[targetTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, ms.TableSettings[0].CreateDdl, &sqltypes.Result{})
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

	err := ws.Materialize(ctx, ms)
	require.EqualError(t, err, "syntax error at position 4 near 'bad'")
}

func TestMaterializerNotASelect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceTabletUID := 100
	targetKs := "targetks"
	targetTabletUID := 200
	shard := "0"
	wf := "testwf"
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, shard)
	defer tenv.deleteTablet(sourceTablet.tablet)
	targetTablet := tenv.addTablet(t, targetTabletUID, targetKs, shard)
	defer tenv.deleteTablet(targetTablet.tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "update t1 set val=1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// Clear out the schema on the source and target tablet.
	tenv.tmc.tabletSchemas[sourceTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	tenv.tmc.tabletSchemas[targetTabletUID] = &tabletmanagerdatapb.SchemaDefinition{}
	tenv.tmc.setVReplicationExecResults(targetTablet.tablet, ms.TableSettings[0].CreateDdl, &sqltypes.Result{})
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf("use %s", sidecar.GetIdentifier()), &sqltypes.Result{}, nil)
	targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)

	err := ws.Materialize(ctx, ms)
	require.EqualError(t, err, "unrecognized statement: update t1 set val=1")
}

func TestMaterializerNoGoodVindex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShard := "0"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"lookup_unique": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table": "t1",
					"from":  "c1",
					"to":    "c2",
				},
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "lookup_unique",
				}},
			},
		},
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// This is aggregated from the two target shards.
	errNoVindex := "could not find a vindex to compute keyspace id for table t1"
	errs := make([]string, 0, len(targetShards))

	for _, targetShard := range []string{"-80", "80-"} {
		targetTablet := targetShards[targetShard]
		addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])
		targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
		errs = append(errs, errNoVindex)
	}

	err = ws.Materialize(ctx, ms)
	require.EqualError(t, err, strings.Join(errs, "\n"))
}

func TestMaterializerComplexVindexExpression(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShard := "0"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select a+b as c1 from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "xxhash",
				}},
			},
		},
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// This is aggregated from the two target shards.
	errNoVindex := "vindex column cannot be a complex expression: a + b as c1"
	errs := make([]string, 0, len(targetShards))

	for _, targetShard := range []string{"-80", "80-"} {
		targetTablet := targetShards[targetShard]
		addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])
		targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
		errs = append(errs, errNoVindex)
	}

	err = ws.Materialize(ctx, ms)
	require.EqualError(t, err, strings.Join(errs, "\n"))
}

func TestMaterializerNoVindexInExpression(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceKs := "sourceks"
	sourceShard := "0"
	sourceTabletUID := 200
	targetKs := "targetks"
	targetShards := make(map[string]*fakeTabletConn)
	targetTabletUID := 300
	wf := "testwf"
	vreplID := 1
	vtenv := vtenv.NewTestEnv()
	tenv := newTestEnv(t, ctx, sourceKs, []string{shard})
	defer tenv.close()

	sourceTablet := tenv.addTablet(t, sourceTabletUID, sourceKs, sourceShard)
	defer tenv.deleteTablet(sourceTablet.tablet)

	targetShards["-80"] = tenv.addTablet(t, targetTabletUID, targetKs, "-80")
	defer tenv.deleteTablet(targetShards["-80"].tablet)
	targetShards["80-"] = tenv.addTablet(t, targetTabletUID+10, targetKs, "80-")
	defer tenv.deleteTablet(targetShards["80-"].tablet)

	ws := workflow.NewServer(vtenv, tenv.ts, tenv.tmc)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       wf,
		SourceKeyspace: sourceKs,
		TargetKeyspace: targetKs,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select c2 from t1",
			CreateDdl:        "t1ddl",
		}},
		Cell: tenv.cells[0],
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	err := tenv.ts.SaveVSchema(ctx, targetKs, &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "xxhash",
				}},
			},
		},
	})
	require.NoError(t, err)

	addMaterializeSettingsTablesToSchema(ms, tenv, vtenv)

	// This is aggregated from the two target shards.
	errNoVindex := "could not find vindex column c1"
	errs := make([]string, 0, len(targetShards))

	for _, targetShard := range []string{"-80", "80-"} {
		targetTablet := targetShards[targetShard]
		addInvariants(targetTablet.vrdbClient, vreplID, sourceTabletUID, position, wf, tenv.cells[0])
		targetTablet.vrdbClient.ExpectRequest(fmt.Sprintf(readAllWorkflows, tenv.dbName, ""), &sqltypes.Result{}, nil)
		errs = append(errs, errNoVindex)
	}

	err = ws.Materialize(ctx, ms)
	require.EqualError(t, err, strings.Join(errs, "\n"))
}

func TestBuildReadVReplicationWorkflowsQuery(t *testing.T) {
	tm := &TabletManager{
		DBConfigs: &dbconfigs.DBConfigs{
			DBName: "vt_testks",
		},
	}
	tests := []struct {
		name    string
		req     *tabletmanagerdatapb.ReadVReplicationWorkflowsRequest
		want    string
		wantErr string
	}{
		{
			name: "all options",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				IncludeIds:       []int32{1, 2, 3},
				IncludeWorkflows: []string{"wf1", "wf2"},
				ExcludeWorkflows: []string{"1wf"},
				IncludeStates:    []binlogdatapb.VReplicationWorkflowState{binlogdatapb.VReplicationWorkflowState_Stopped, binlogdatapb.VReplicationWorkflowState_Error},
				ExcludeFrozen:    true,
			},
			want: "select workflow, id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name = 'vt_testks' and message != 'FROZEN' and id in (1,2,3) and workflow in ('wf1','wf2') and workflow not in ('1wf') and state in ('Stopped','Error') group by workflow, id order by workflow, id",
		},
		{
			name: "2 workflows if running",
			req: &tabletmanagerdatapb.ReadVReplicationWorkflowsRequest{
				IncludeWorkflows: []string{"wf1", "wf2"},
				IncludeStates:    []binlogdatapb.VReplicationWorkflowState{binlogdatapb.VReplicationWorkflowState_Running},
			},
			want: "select workflow, id, source, pos, stop_pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, message, db_name, rows_copied, tags, time_heartbeat, workflow_type, time_throttled, component_throttled, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name = 'vt_testks' and workflow in ('wf1','wf2') and state in ('Running') group by workflow, id order by workflow, id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tm.buildReadVReplicationWorkflowsQuery(tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got, "buildReadVReplicationWorkflowsQuery() = %v, want %v", got, tt.want)
		})
	}
}

func TestBuildUpdateVReplicationWorkflowsQuery(t *testing.T) {
	tm := &TabletManager{
		DBConfigs: &dbconfigs.DBConfigs{
			DBName: "vt_testks",
		},
	}
	tests := []struct {
		name    string
		req     *tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest
		want    string
		wantErr string
	}{
		{
			name: "nothing to update",
			req: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				State:        binlogdatapb.VReplicationWorkflowState(textutil.SimulatedNullInt),
				Message:      textutil.SimulatedNullString,
				StopPosition: textutil.SimulatedNullString,
			},
			wantErr: errNoFieldsToUpdate.Error(),
		},
		{
			name: "mutually exclusive options",
			req: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				State:            binlogdatapb.VReplicationWorkflowState_Running,
				AllWorkflows:     true,
				ExcludeWorkflows: []string{"wf1"},
			},
			wantErr: errAllWithIncludeExcludeWorkflows.Error(),
		},
		{
			name: "all values and options",
			req: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				State:            binlogdatapb.VReplicationWorkflowState_Running,
				Message:          "test message",
				StopPosition:     "MySQL56/17b1039f-21b6-13ed-b365-1a43f95f28a3:1-20",
				IncludeWorkflows: []string{"wf2", "wf3"},
				ExcludeWorkflows: []string{"1wf"},
			},
			want: "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Running', message = 'test message', stop_pos = 'MySQL56/17b1039f-21b6-13ed-b365-1a43f95f28a3:1-20' where db_name = 'vt_testks' and workflow in ('wf2','wf3') and workflow not in ('1wf')",
		},
		{
			name: "state for all",
			req: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				State:        binlogdatapb.VReplicationWorkflowState_Running,
				Message:      textutil.SimulatedNullString,
				StopPosition: textutil.SimulatedNullString,
				AllWorkflows: true,
			},
			want: "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Running' where db_name = 'vt_testks'",
		},
		{
			name: "stop all for vdiff",
			req: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				State:        binlogdatapb.VReplicationWorkflowState_Stopped,
				Message:      "for vdiff",
				StopPosition: textutil.SimulatedNullString,
				AllWorkflows: true,
			},
			want: "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Stopped', message = 'for vdiff' where db_name = 'vt_testks'",
		},
		{
			name: "start one until position",
			req: &tabletmanagerdatapb.UpdateVReplicationWorkflowsRequest{
				State:            binlogdatapb.VReplicationWorkflowState_Running,
				Message:          "for until position",
				StopPosition:     "MySQL56/17b1039f-21b6-13ed-b365-1a43f95f28a3:1-9999",
				IncludeWorkflows: []string{"wf1"},
			},
			want: "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Running', message = 'for until position', stop_pos = 'MySQL56/17b1039f-21b6-13ed-b365-1a43f95f28a3:1-9999' where db_name = 'vt_testks' and workflow in ('wf1')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tm.buildUpdateVReplicationWorkflowsQuery(tt.req)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got, "buildUpdateVReplicationWorkflowsQuery() = %v, want %v", got, tt.want)
		})
	}
}
