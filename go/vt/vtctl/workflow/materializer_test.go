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

package workflow

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const getWorkflowQuery = "select id from _vt.vreplication where db_name='vt_targetks' and workflow='workflow'"
const mzUpdateQuery = "update _vt.vreplication set state='Running' where db_name='vt_targetks' and workflow='workflow'"
const mzSelectFrozenQuery = "select 1 from _vt.vreplication where db_name='vt_targetks' and message='FROZEN' and workflow_sub_type != 1"
const mzCheckJournal = "/select val from _vt.resharding_journal where id="
const mzGetWorkflowStatusQuery = "select id, workflow, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, message, tags, workflow_type, workflow_sub_type, time_heartbeat, defer_secondary_keys, component_throttled, time_throttled, rows_copied from _vt.vreplication where workflow = 'workflow' and db_name = 'vt_targetks'"
const mzGetCopyState = "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
const mzGetLatestCopyState = "select table_name, lastpk from _vt.copy_state where vrepl_id = 1 and id in (select max(id) from _vt.copy_state where vrepl_id = 1 group by vrepl_id, table_name)"
const insertPrefix = `/insert into _vt.vreplication\(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys\) values `
const eol = "$"

var (
	defaultOnDDL = binlogdatapb.OnDDLAction_IGNORE.String()
	binlogSource = &binlogdatapb.BinlogSource{
		Keyspace: "sourceks",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t1",
			}},
		},
	}
	getWorkflowRes = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
			"int64|blob|varchar|varchar|varchar|int64|int64|int64",
		),
		fmt.Sprintf("1|%s||zone1|replica|1|0|1", binlogSource),
	)
	getWorkflowStatusRes = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|workflow|source|pos|stop_pos|max_replication_log|state|db_name|time_updated|transaction_timestamp|message|tags|workflow_type|workflow_sub_type|time_heartbeat|defer_secondary_keys|component_throttled|time_throttled|rows_copied",
			"int64|varchar|blob|varchar|varchar|int64|varchar|varchar|int64|int64|varchar|varchar|int64|int64|int64|int64|varchar|int64|int64",
		),
		fmt.Sprintf("1|wf1|%s|MySQL56/9d10e6ec-07a0-11ee-ae73-8e53f4cf3083:1-97|NULL|0|running|vt_ks|1686577659|0|||1|0|0|0||0|10", binlogSource),
	)
)

func TestStripForeignKeys(t *testing.T) {
	tcs := []struct {
		desc string
		ddl  string

		hasErr bool
		newDDL string
	}{
		{
			desc: "has FK constraints",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"`foreign_id` int(11) CHECK (foreign_id>10),\n" +
				"PRIMARY KEY (`id`),\n" +
				"KEY `fk_table1_ref_foreign_id` (`foreign_id`),\n" +
				"CONSTRAINT `fk_table1_ref_foreign_id` FOREIGN KEY (`foreign_id`) REFERENCES `foreign` (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",

			newDDL: "create table table1 (\n" +
				"\tid int(11) not null auto_increment,\n" +
				"\tforeign_id int(11),\n" +
				"\tPRIMARY KEY (id),\n" +
				"\tKEY fk_table1_ref_foreign_id (foreign_id),\n" +
				"\tcheck (foreign_id > 10)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",

			hasErr: false,
		},
		{
			desc: "no FK constraints",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"`foreign_id` int(11) NOT NULL  CHECK (foreign_id>10),\n" +
				"`user_id` int(11) NOT NULL,\n" +
				"PRIMARY KEY (`id`),\n" +
				"KEY `fk_table1_ref_foreign_id` (`foreign_id`),\n" +
				"KEY `fk_table1_ref_user_id` (`user_id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",

			newDDL: "create table table1 (\n" +
				"\tid int(11) not null auto_increment,\n" +
				"\tforeign_id int(11) not null,\n" +
				"\tuser_id int(11) not null,\n" +
				"\tPRIMARY KEY (id),\n" +
				"\tKEY fk_table1_ref_foreign_id (foreign_id),\n" +
				"\tKEY fk_table1_ref_user_id (user_id),\n" +
				"\tcheck (foreign_id > 10)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",
		},
	}

	for _, tc := range tcs {
		newDDL, err := stripTableForeignKeys(tc.ddl)
		if tc.hasErr != (err != nil) {
			t.Fatalf("hasErr does not match: err: %v, tc: %+v", err, tc)
		}

		if newDDL != tc.newDDL {
			utils.MustMatch(t, tc.newDDL, newDDL, fmt.Sprintf("newDDL does not match. tc: %+v", tc))
		}
	}
}

func TestStripConstraints(t *testing.T) {
	tcs := []struct {
		desc string
		ddl  string

		hasErr bool
		newDDL string
	}{
		{
			desc: "constraints",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"`foreign_id` int(11) NOT NULL,\n" +
				"`user_id` int(11) NOT NULL,\n" +
				"PRIMARY KEY (`id`),\n" +
				"KEY `fk_table1_ref_foreign_id` (`foreign_id`),\n" +
				"KEY `fk_table1_ref_user_id` (`user_id`),\n" +
				"CONSTRAINT `fk_table1_ref_foreign_id` FOREIGN KEY (`foreign_id`) REFERENCES `foreign` (`id`),\n" +
				"CONSTRAINT `fk_table1_ref_user_id` FOREIGN KEY (`user_id`) REFERENCES `core_user` (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",

			newDDL: "create table table1 (\n" +
				"\tid int(11) not null auto_increment,\n" +
				"\tforeign_id int(11) not null,\n" +
				"\tuser_id int(11) not null,\n" +
				"\tPRIMARY KEY (id),\n" +
				"\tKEY fk_table1_ref_foreign_id (foreign_id),\n" +
				"\tKEY fk_table1_ref_user_id (user_id)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",

			hasErr: false,
		},
		{
			desc: "no constraints",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` int(11) NOT NULL AUTO_INCREMENT,\n" +
				"`foreign_id` int(11) NOT NULL,\n" +
				"`user_id` int(11) NOT NULL,\n" +
				"PRIMARY KEY (`id`),\n" +
				"KEY `fk_table1_ref_foreign_id` (`foreign_id`),\n" +
				"KEY `fk_table1_ref_user_id` (`user_id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",

			newDDL: "create table table1 (\n" +
				"\tid int(11) not null auto_increment,\n" +
				"\tforeign_id int(11) not null,\n" +
				"\tuser_id int(11) not null,\n" +
				"\tPRIMARY KEY (id),\n" +
				"\tKEY fk_table1_ref_foreign_id (foreign_id),\n" +
				"\tKEY fk_table1_ref_user_id (user_id)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",
		},
		{
			desc: "bad ddl has error",
			ddl:  "bad ddl",

			hasErr: true,
		},
	}

	for _, tc := range tcs {
		newDDL, err := stripTableConstraints(tc.ddl)
		if tc.hasErr != (err != nil) {
			t.Fatalf("hasErr does not match: err: %v, tc: %+v", err, tc)
		}

		if newDDL != tc.newDDL {
			utils.MustMatch(t, tc.newDDL, newDDL, fmt.Sprintf("newDDL does not match. tc: %+v", tc))
		}
	}
}

func TestAddTablesToVSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()
	srcks := "source"
	ws := &Server{
		ts: ts,
	}
	tests := []struct {
		name              string
		sourceVSchema     *vschemapb.Keyspace
		inTargetVSchema   *vschemapb.Keyspace
		tables            []string
		copyVSchema       bool
		wantTargetVSchema *vschemapb.Keyspace
	}{
		{
			name: "no target vschema; copy source vschema",
			sourceVSchema: &vschemapb.Keyspace{
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: vindexes.TypeReference,
					},
					"t2": {
						Type: vindexes.TypeSequence,
					},
					"t3": {
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "t2",
						},
					},
				},
			},
			inTargetVSchema: &vschemapb.Keyspace{},
			tables:          []string{"t1", "t2", "t3", "t4"},
			copyVSchema:     true,
			wantTargetVSchema: &vschemapb.Keyspace{
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: vindexes.TypeReference,
					},
					"t2": {
						Type: vindexes.TypeSequence,
					},
					"t3": {
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "t2",
						},
					},
					"t4": {},
				},
			},
		},
		{
			name: "no target vschema; copy source vschema; sharded source",
			sourceVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: vindexes.TypeReference,
					},
					"t2": {
						Type:   vindexes.TypeSequence,
						Pinned: "123456",
					},
					"t3": {
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "t2",
						},
						ColumnVindexes: []*vschemapb.ColumnVindex{ // Should be stripped on target
							{
								Column: "c1",
								Name:   "hash",
							},
						},
					},
					"t4": {
						ColumnVindexes: []*vschemapb.ColumnVindex{ // Should be stripped on target
							{
								Column: "c1",
								Name:   "hash",
							},
						},
					},
				},
			},
			inTargetVSchema: &vschemapb.Keyspace{},
			tables:          []string{"t1", "t2", "t3", "t4"},
			copyVSchema:     true,
			wantTargetVSchema: &vschemapb.Keyspace{
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: vindexes.TypeReference,
					},
					"t2": {
						Type:   vindexes.TypeSequence,
						Pinned: "123456",
					},
					"t3": {
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "t2",
						},
					},
					"t4": {},
				},
			},
		},
		{
			name: "target vschema; copy source vschema",
			sourceVSchema: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: vindexes.TypeReference,
					},
					"t2": {
						Type: vindexes.TypeSequence,
					},
					"t3": {
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "t2",
						},
					},
					"t4": {
						ColumnVindexes: []*vschemapb.ColumnVindex{ // Should be stripped on target
							{
								Column: "c1",
								Name:   "hash",
							},
						},
					},
				},
			},
			inTargetVSchema: &vschemapb.Keyspace{
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: vindexes.TypeReference,
					},
					"t2": {},
					"t3": {},
					"t4": {},
				},
			},
			tables:      []string{"t1", "t2", "t3", "t4"},
			copyVSchema: true,
			wantTargetVSchema: &vschemapb.Keyspace{
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: vindexes.TypeReference,
					},
					"t2": {},
					"t3": {},
					"t4": {},
				},
			},
		},
		{
			name: "no target vschema; do not copy source vschema",
			sourceVSchema: &vschemapb.Keyspace{
				Tables: map[string]*vschemapb.Table{
					"t1": {
						Type: vindexes.TypeReference,
					},
					"t2": {
						Type: vindexes.TypeSequence,
					},
					"t3": {
						AutoIncrement: &vschemapb.AutoIncrement{
							Column:   "c1",
							Sequence: "t2",
						},
					},
				},
			},
			inTargetVSchema: &vschemapb.Keyspace{},
			tables:          []string{"t1", "t2"},
			copyVSchema:     false,
			wantTargetVSchema: &vschemapb.Keyspace{
				Tables: map[string]*vschemapb.Table{
					"t1": {},
					"t2": {},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ts.SaveVSchema(ctx, srcks, tt.sourceVSchema)
			require.NoError(t, err)
			err = ws.addTablesToVSchema(ctx, srcks, tt.inTargetVSchema, tt.tables, tt.copyVSchema)
			require.NoError(t, err)
			require.Equal(t, tt.wantTargetVSchema, tt.inTargetVSchema)
		})
	}
}

func TestMigrateVSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		Cell:           "cell",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
		}},
	}
	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, getWorkflowQuery, getWorkflowRes)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzGetWorkflowStatusQuery, getWorkflowStatusRes)
	env.tmc.expectVRQuery(200, mzGetLatestCopyState, &sqltypes.Result{})

	_, err := env.ws.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
		Workflow:       ms.Workflow,
		Cells:          []string{ms.Cell},
		TabletTypes:    []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
		SourceKeyspace: ms.SourceKeyspace,
		TargetKeyspace: ms.TargetKeyspace,
		IncludeTables:  []string{"t1"},
		AutoStart:      true,
		OnDdl:          defaultOnDDL,
	})
	require.NoError(t, err)
	vschema, err := env.ws.ts.GetSrvVSchema(ctx, env.cell)
	require.NoError(t, err)
	got := fmt.Sprintf("%v", vschema)
	want := []string{`keyspaces:{key:"sourceks" value:{}}`,
		`keyspaces:{key:"sourceks" value:{}} keyspaces:{key:"targetks" value:{tables:{key:"t1" value:{}}}}`,
		`rules:{from_table:"t1" to_tables:"sourceks.t1"}`,
		`rules:{from_table:"targetks.t1" to_tables:"sourceks.t1"}`,
	}
	for _, wantstr := range want {
		require.Contains(t, got, wantstr)
	}
}

// TestMoveTablesDDLFlag tests that we save the on-ddl flag value in the workflow.
// Note:
//   - TestPlayerDDL tests that the vplayer correctly implements the ddl behavior
//   - We have a manual e2e test for the full behavior: TestVReplicationDDLHandling
func TestMoveTablesDDLFlag(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
		}},
	}

	for onDDLAction := range binlogdatapb.OnDDLAction_value {
		t.Run(fmt.Sprintf("OnDDL Flag:%v", onDDLAction), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
			defer env.close()
			// This is the default and go does not marshal defaults
			// for prototext fields so we use the default insert stmt.
			//insert = fmt.Sprintf(`/insert into .vreplication\(.*on_ddl:%s.*`, onDDLAction)
			//env.tmc.expectVRQuery(100, "/.*", &sqltypes.Result{})

			// TODO: we cannot test the actual query generated w/o having a
			// TabletManager. Importing the tabletmanager package, however, causes
			// a circular dependency.
			// The TabletManager portion is tested in rpc_vreplication_test.go.
			env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, getWorkflowQuery, getWorkflowRes)
			env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzGetWorkflowStatusQuery, getWorkflowStatusRes)
			env.tmc.expectVRQuery(200, mzGetLatestCopyState, &sqltypes.Result{})

			targetShard, err := env.topoServ.GetShardNames(ctx, ms.TargetKeyspace)
			require.NoError(t, err)
			sourceShard, err := env.topoServ.GetShardNames(ctx, ms.SourceKeyspace)
			require.NoError(t, err)
			want := fmt.Sprintf("shard_streams:{key:\"%s/%s\" value:{streams:{id:1 tablet:{cell:\"%s\" uid:200} source_shard:\"%s/%s\" position:\"9d10e6ec-07a0-11ee-ae73-8e53f4cf3083:1-97\" status:\"running\" info:\"VStream Lag: 0s\"}}} traffic_state:\"Reads Not Switched. Writes Not Switched\"",
				ms.TargetKeyspace, targetShard[0], env.cell, ms.SourceKeyspace, sourceShard[0])

			res, err := env.ws.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
				Workflow:       ms.Workflow,
				SourceKeyspace: ms.SourceKeyspace,
				TargetKeyspace: ms.TargetKeyspace,
				IncludeTables:  []string{"t1"},
				OnDdl:          onDDLAction,
			})
			require.NoError(t, err)
			require.Equal(t, want, fmt.Sprintf("%+v", res))
		})
	}
}

// TestMoveTablesNoRoutingRules confirms that MoveTables does not create routing rules if --no-routing-rules is specified.
func TestMoveTablesNoRoutingRules(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()
	// This is the default and go does not marshal defaults
	// for prototext fields so we use the default insert stmt.
	//insert = fmt.Sprintf(`/insert into .vreplication\(.*on_ddl:%s.*`, onDDLAction)
	//env.tmc.expectVRQuery(100, "/.*", &sqltypes.Result{})

	// TODO: we cannot test the actual query generated w/o having a
	// TabletManager. Importing the tabletmanager package, however, causes
	// a circular dependency.
	// The TabletManager portion is tested in rpc_vreplication_test.go.
	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, getWorkflowQuery, getWorkflowRes)
	env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzGetWorkflowStatusQuery, getWorkflowStatusRes)
	env.tmc.expectVRQuery(200, mzGetLatestCopyState, &sqltypes.Result{})

	targetShard, err := env.topoServ.GetShardNames(ctx, ms.TargetKeyspace)
	require.NoError(t, err)
	sourceShard, err := env.topoServ.GetShardNames(ctx, ms.SourceKeyspace)
	require.NoError(t, err)
	want := fmt.Sprintf("shard_streams:{key:\"%s/%s\" value:{streams:{id:1 tablet:{cell:\"%s\" uid:200} source_shard:\"%s/%s\" position:\"9d10e6ec-07a0-11ee-ae73-8e53f4cf3083:1-97\" status:\"running\" info:\"VStream Lag: 0s\"}}} traffic_state:\"Reads Not Switched. Writes Not Switched\"",
		ms.TargetKeyspace, targetShard[0], env.cell, ms.SourceKeyspace, sourceShard[0])

	res, err := env.ws.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
		Workflow:       ms.Workflow,
		SourceKeyspace: ms.SourceKeyspace,
		TargetKeyspace: ms.TargetKeyspace,
		IncludeTables:  []string{"t1"},
		NoRoutingRules: true,
	})
	require.NoError(t, err)
	require.Equal(t, want, fmt.Sprintf("%+v", res))
	rr, err := env.ws.ts.GetRoutingRules(ctx)
	require.NoError(t, err)
	require.Zerof(t, len(rr.Rules), "routing rules should be empty, found %+v", rr.Rules)
}

func TestCreateLookupVindexFull(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "lookup",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	specs := &vschemapb.Keyspace{
		Vindexes: map[string]*vschemapb.Vindex{
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table": "targetks.lookup",
					"from":  "c1",
					"to":    "c2",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v",
					Column: "col2",
				}},
			},
		},
	}
	// Dummy sourceSchema
	sourceSchema := "CREATE TABLE `t1` (\n" +
		"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `col2` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1"

	sourceVSchema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}},
			},
		},
	}
	env.tmc.schema[ms.SourceKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Fields: []*querypb.Field{{
				Name: "col1",
				Type: querypb.Type_INT64,
			}, {
				Name: "col2",
				Type: querypb.Type_INT64,
			}},
			Schema: sourceSchema,
		}},
	}
	if err := env.topoServ.SaveVSchema(ctx, ms.TargetKeyspace, &vschemapb.Keyspace{}); err != nil {
		t.Fatal(err)
	}
	if err := env.topoServ.SaveVSchema(ctx, ms.SourceKeyspace, sourceVSchema); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, "/CREATE TABLE `lookup`", &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_targetks' and workflow='lookup'", &sqltypes.Result{})

	req := &vtctldatapb.LookupVindexCreateRequest{
		Workflow:    ms.Workflow,
		Keyspace:    ms.SourceKeyspace,
		Cells:       []string{"cell"},
		TabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY},
		Vindex:      specs,
	}

	_, err := env.ws.LookupVindexCreate(ctx, req)
	require.NoError(t, err)

	wantvschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      "targetks.lookup",
					"from":       "c1",
					"to":         "c2",
					"write_only": "true",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}, {
					Name:   "v",
					Column: "col2",
				}},
			},
		},
	}
	vschema, err := env.topoServ.GetVSchema(ctx, ms.SourceKeyspace)
	require.NoError(t, err)
	utils.MustMatch(t, wantvschema, vschema)

	wantvschema = &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"lookup": {},
		},
	}
	vschema, err = env.topoServ.GetVSchema(ctx, ms.TargetKeyspace)
	require.NoError(t, err)
	utils.MustMatch(t, wantvschema, vschema)
}

func TestCreateLookupVindexCreateDDL(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()
	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "col1",
					Name:   "xxhash",
				}},
			},
		},
	}
	if err := env.topoServ.SaveVSchema(ctx, ms.SourceKeyspace, vs); err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		description  string
		specs        *vschemapb.Keyspace
		sourceSchema string
		out          string
		err          string
	}{{
		description: "unique lookup",
		specs: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table": fmt.Sprintf("%s.lkp", ms.TargetKeyspace),
						"from":  "c1",
						"to":    "c2",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "v",
						Column: "col2",
					}},
				},
			},
		},
		sourceSchema: "CREATE TABLE `t1` (\n" +
			"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `col2` int(11) DEFAULT NULL,\n" +
			"  `col3` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1",
		out: "CREATE TABLE `lkp` (\n" +
			"  `c1` int(11),\n" +
			"  `c2` varbinary(128),\n" +
			"  PRIMARY KEY (`c1`)\n" +
			")",
	}, {
		description: "unique lookup, also pk",
		specs: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table": fmt.Sprintf("%s.lkp", ms.TargetKeyspace),
						"from":  "c1",
						"to":    "c2",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "v",
						Column: "col2",
					}},
				},
			},
		},
		sourceSchema: "CREATE TABLE `t1` (\n" +
			"  `col2` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `col1` int(11) DEFAULT NULL,\n" +
			"  `col4` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1",
		out: "CREATE TABLE `lkp` (\n" +
			"  `c1` int(11) NOT NULL,\n" +
			"  `c2` varbinary(128),\n" +
			"  PRIMARY KEY (`c1`)\n" +
			")",
	}, {
		description: "non-unique lookup, also pk",
		specs: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup",
					Params: map[string]string{
						"table": fmt.Sprintf("%s.lkp", ms.TargetKeyspace),
						"from":  "c1,c2",
						"to":    "c3",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:    "v",
						Columns: []string{"col2", "col1"},
					}},
				},
			},
		},
		sourceSchema: "CREATE TABLE `t1` (\n" +
			"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `col2` int(11) NOT NULL,\n" +
			"  `col3` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1",
		out: "CREATE TABLE `lkp` (\n" +
			"  `c1` int(11) NOT NULL,\n" +
			"  `c2` int(11) NOT NULL,\n" +
			"  `c3` varbinary(128),\n" +
			"  PRIMARY KEY (`c1`, `c2`)\n" +
			")",
	}, {
		description: "column missing",
		specs: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table": fmt.Sprintf("%s.lkp", ms.TargetKeyspace),
						"from":  "c1",
						"to":    "c2",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "v",
						Column: "nocol",
					}},
				},
			},
		},
		sourceSchema: "CREATE TABLE `t1` (\n" +
			"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `col2` int(11) NOT NULL,\n" +
			"  `col3` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1",
		err: "column nocol not found in schema",
	}, {
		description: "no table in schema",
		specs: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table": fmt.Sprintf("%s.lkp", ms.TargetKeyspace),
						"from":  "c1",
						"to":    "c2",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "v",
						Column: "nocol",
					}},
				},
			},
		},
		sourceSchema: "",
		err:          "unexpected number of tables (0) returned from sourceks schema",
	}}
	for _, tcase := range testcases {
		if tcase.sourceSchema != "" {
			env.tmc.schema[ms.SourceKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
				TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
					Schema: tcase.sourceSchema,
				}},
			}
		} else {
			delete(env.tmc.schema, ms.SourceKeyspace+".t1")
		}

		outms, _, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.SourceKeyspace, tcase.specs, false)
		if tcase.err != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("prepareCreateLookup(%s) err: %v, must contain %v", tcase.description, err, tcase.err)
			}
			continue
		}
		require.NoError(t, err)
		want := strings.Split(tcase.out, "\n")
		got := strings.Split(outms.TableSettings[0].CreateDdl, "\n")
		require.Equal(t, want, got, tcase.description)
	}
}

func TestCreateLookupVindexSourceVSchema(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	specs := &vschemapb.Keyspace{
		Vindexes: map[string]*vschemapb.Vindex{
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table": "targetks.lkp",
					"from":  "c1",
					"to":    "c2",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v",
					Column: "col2",
				}},
			},
		},
	}
	// Dummy sourceSchema
	sourceSchema := "CREATE TABLE `t1` (\n" +
		"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `col2` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1"

	testcases := []struct {
		description   string
		sourceVSchema *vschemapb.Keyspace
		out           *vschemapb.Keyspace
	}{{
		description: "source vschema has no prior info",
		sourceVSchema: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "xxhash",
						Column: "col1",
					}},
				},
			},
		},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table":      "targetks.lkp",
						"from":       "c1",
						"to":         "c2",
						"write_only": "true",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "xxhash",
						Column: "col1",
					}, {
						Name:   "v",
						Column: "col2",
					}},
				},
			},
		},
	}, {
		description: "source vschema has the lookup vindex",
		sourceVSchema: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table":      "targetks.lkp",
						"from":       "c1",
						"to":         "c2",
						"write_only": "true",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "xxhash",
						Column: "col1",
					}},
				},
			},
		},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table":      "targetks.lkp",
						"from":       "c1",
						"to":         "c2",
						"write_only": "true",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "xxhash",
						Column: "col1",
					}, {
						Name:   "v",
						Column: "col2",
					}},
				},
			},
		},
	}, {
		description: "source vschema table has a different vindex on same column",
		sourceVSchema: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table":      "targetks.lkp",
						"from":       "c1",
						"to":         "c2",
						"write_only": "true",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "xxhash",
						Column: "col1",
					}, {
						Name:   "xxhash",
						Column: "col2",
					}},
				},
			},
		},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table":      "targetks.lkp",
						"from":       "c1",
						"to":         "c2",
						"write_only": "true",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "xxhash",
						Column: "col1",
					}, {
						Name:   "xxhash",
						Column: "col2",
					}, {
						Name:   "v",
						Column: "col2",
					}},
				},
			},
		},
	}}
	for _, tcase := range testcases {
		env.tmc.schema[ms.SourceKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
				Fields: []*querypb.Field{{
					Name: "col1",
					Type: querypb.Type_INT64,
				}, {
					Name: "col2",
					Type: querypb.Type_INT64,
				}},
				Schema: sourceSchema,
			}},
		}
		if err := env.topoServ.SaveVSchema(ctx, ms.TargetKeyspace, &vschemapb.Keyspace{}); err != nil {
			t.Fatal(err)
		}
		if err := env.topoServ.SaveVSchema(ctx, ms.SourceKeyspace, tcase.sourceVSchema); err != nil {
			t.Fatal(err)
		}

		_, got, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.SourceKeyspace, specs, false)
		require.NoError(t, err)
		if !proto.Equal(got, tcase.out) {
			t.Errorf("%s: got:\n%v, want\n%v", tcase.description, got, tcase.out)
		}
	}
}

func TestCreateLookupVindexTargetVSchema(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()
	sourcevs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "col1",
					Name:   "xxhash",
				}},
			},
		},
	}
	if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, sourcevs); err != nil {
		t.Fatal(err)
	}

	// withTable is a target vschema with a pre-existing table.
	withTable := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "xxhash",
				}},
			},
		},
	}

	specs := &vschemapb.Keyspace{
		Vindexes: map[string]*vschemapb.Vindex{
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table": "will be set by the test case",
					"from":  "c1",
					"to":    "c2",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v",
					Column: "col2",
				}},
			},
		},
	}
	// Dummy sourceSchema
	sourceSchema := "CREATE TABLE `t1` (\n" +
		"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `col2` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1"

	testcases := []struct {
		description     string
		targetTable     string
		sourceFieldType querypb.Type
		targetVSchema   *vschemapb.Keyspace
		out             *vschemapb.Keyspace
		err             string
	}{{
		description:     "sharded, int64, empty target",
		targetTable:     "lkp",
		sourceFieldType: querypb.Type_INT64,
		targetVSchema:   &vschemapb.Keyspace{Sharded: true},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"lkp": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Column: "c1",
						Name:   "xxhash",
					}},
				},
			},
		},
	}, {
		description:     "sharded, varchar, empty target",
		targetTable:     "lkp",
		sourceFieldType: querypb.Type_VARCHAR,
		targetVSchema:   &vschemapb.Keyspace{Sharded: true},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"unicode_loose_md5": {
					Type: "unicode_loose_md5",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"lkp": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Column: "c1",
						Name:   "unicode_loose_md5",
					}},
				},
			},
		},
	}, {
		description:     "sharded, int64, good vindex",
		targetTable:     "lkp",
		sourceFieldType: querypb.Type_INT64,
		targetVSchema: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
			},
		},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"lkp": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Column: "c1",
						Name:   "xxhash",
					}},
				},
			},
		},
	}, {
		description:     "sharded, int64, bad vindex",
		targetTable:     "lkp",
		sourceFieldType: querypb.Type_INT64,
		targetVSchema: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				// Create a misleading vindex name.
				"xxhash": {
					Type: "unicode_loose_md5",
				},
			},
		},
		err: "a conflicting vindex named xxhash already exists in the targetks keyspace",
	}, {
		description:     "sharded, int64, good table",
		targetTable:     "t2",
		sourceFieldType: querypb.Type_INT64,
		targetVSchema:   withTable,
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"xxhash": {
					Type: "xxhash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t2": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Column: "c1",
						Name:   "xxhash",
					}},
				},
			},
		},
	}, {
		description:     "sharded, int64, table mismatch",
		targetTable:     "t2",
		sourceFieldType: querypb.Type_VARCHAR,
		targetVSchema:   withTable,
		err:             "a conflicting table named t2 already exists in the targetks vschema",
	}, {
		description:     "unsharded",
		targetTable:     "lkp",
		sourceFieldType: querypb.Type_INT64,
		targetVSchema:   &vschemapb.Keyspace{},
		out: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{},
			Tables: map[string]*vschemapb.Table{
				"lkp": {},
			},
		},
	}, {
		description:     "invalid column type",
		targetTable:     "lkp",
		sourceFieldType: querypb.Type_SET,
		targetVSchema:   &vschemapb.Keyspace{Sharded: true},
		err:             "type SET is not recommended for a vindex",
	}}
	for _, tcase := range testcases {
		env.tmc.schema[ms.SourceKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
				Fields: []*querypb.Field{{
					Name: "col2",
					Type: tcase.sourceFieldType,
				}},
				Schema: sourceSchema,
			}},
		}
		specs.Vindexes["v"].Params["table"] = fmt.Sprintf("%s.%s", ms.TargetKeyspace, tcase.targetTable)
		if err := env.topoServ.SaveVSchema(ctx, ms.TargetKeyspace, tcase.targetVSchema); err != nil {
			t.Fatal(err)
		}

		_, _, got, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.SourceKeyspace, specs, false)
		if tcase.err != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("prepareCreateLookup(%s) err: %v, must contain %v", tcase.description, err, tcase.err)
			}
			continue
		}
		require.NoError(t, err)
		utils.MustMatch(t, tcase.out, got, tcase.description)
	}
}

func TestCreateLookupVindexSameKeyspace(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "ks",
		TargetKeyspace: "ks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	specs := &vschemapb.Keyspace{
		Vindexes: map[string]*vschemapb.Vindex{
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table": "ks.lkp",
					"from":  "c1",
					"to":    "c2",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v",
					Column: "col2",
				}},
			},
		},
	}
	// Dummy sourceSchema
	sourceSchema := "CREATE TABLE `t1` (\n" +
		"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `col2` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1"

	vschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}},
			},
		},
	}
	want := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      "ks.lkp",
					"from":       "c1",
					"to":         "c2",
					"write_only": "true",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}, {
					Name:   "v",
					Column: "col2",
				}},
			},
			"lkp": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "xxhash",
				}},
			},
		},
	}
	env.tmc.schema[ms.SourceKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Fields: []*querypb.Field{{
				Name: "col1",
				Type: querypb.Type_INT64,
			}, {
				Name: "col2",
				Type: querypb.Type_INT64,
			}},
			Schema: sourceSchema,
		}},
	}
	if err := env.topoServ.SaveVSchema(ctx, ms.TargetKeyspace, vschema); err != nil {
		t.Fatal(err)
	}

	_, got, _, err := env.ws.prepareCreateLookup(ctx, "keyspace", ms.TargetKeyspace, specs, false)
	require.NoError(t, err)
	if !proto.Equal(got, want) {
		t.Errorf("same keyspace: got:\n%v, want\n%v", got, want)
	}
}

func TestCreateCustomizedVindex(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "ks",
		TargetKeyspace: "ks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	specs := &vschemapb.Keyspace{
		Vindexes: map[string]*vschemapb.Vindex{
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table": "ks.lookup",
					"from":  "c1",
					"to":    "col2",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v",
					Column: "col2",
				}},
			},
			"lookup": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "unicode_loose_md5",
					Column: "c1",
				}},
			},
		},
	}

	// Dummy sourceSchema
	sourceSchema := "CREATE TABLE `t1` (\n" +
		"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `col2` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1"

	vschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
			"unicode_loose_md5": { // Non default vindex type for the column.
				Type: "unicode_loose_md5",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}},
			},
		},
	}
	want := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
			"unicode_loose_md5": {
				Type: "unicode_loose_md5",
			},
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      "ks.lookup",
					"from":       "c1",
					"to":         "col2",
					"write_only": "true",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}, {
					Name:   "v",
					Column: "col2",
				}},
			},
			"lookup": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "unicode_loose_md5",
				}},
			},
		},
	}
	env.tmc.schema[ms.TargetKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Fields: []*querypb.Field{{
				Name: "col1",
				Type: querypb.Type_INT64,
			}, {
				Name: "col2",
				Type: querypb.Type_INT64,
			}},
			Schema: sourceSchema,
		}},
	}
	if err := env.topoServ.SaveVSchema(ctx, ms.TargetKeyspace, vschema); err != nil {
		t.Fatal(err)
	}

	_, got, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.TargetKeyspace, specs, false)
	require.NoError(t, err)
	if !proto.Equal(got, want) {
		t.Errorf("customize create lookup error same: got:\n%v, want\n%v", got, want)
	}
}

func TestCreateLookupVindexIgnoreNulls(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "ks",
		TargetKeyspace: "ks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	specs := &vschemapb.Keyspace{
		Vindexes: map[string]*vschemapb.Vindex{
			"v": {
				Type: "consistent_lookup",
				Params: map[string]string{
					"table":        "ks.lkp",
					"from":         "col2,col1",
					"to":           "keyspace_id",
					"ignore_nulls": "true",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:    "v",
					Columns: []string{"col2", "col1"},
				}},
			},
		},
	}
	// Dummy sourceSchema
	sourceSchema := "CREATE TABLE `t1` (\n" +
		"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `col2` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1"

	vschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}},
			},
		},
	}

	wantKs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
			"v": {
				Type: "consistent_lookup",
				Params: map[string]string{
					"table":        "ks.lkp",
					"from":         "col2,col1",
					"to":           "keyspace_id",
					"write_only":   "true",
					"ignore_nulls": "true",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}, {
					Name:    "v",
					Columns: []string{"col2", "col1"},
				}},
			},
			"lkp": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "col2",
					Name:   "xxhash",
				}},
			},
		},
	}
	wantQuery := "select col2 as col2, col1 as col1, keyspace_id() as keyspace_id from t1 where col2 is not null and col1 is not null group by col2, col1, keyspace_id"

	env.tmc.schema[ms.SourceKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Fields: []*querypb.Field{{
				Name: "col1",
				Type: querypb.Type_INT64,
			}, {
				Name: "col2",
				Type: querypb.Type_INT64,
			}},
			Schema: sourceSchema,
		}},
	}
	if err := env.topoServ.SaveVSchema(ctx, ms.TargetKeyspace, vschema); err != nil {
		t.Fatal(err)
	}

	ms, ks, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.TargetKeyspace, specs, false)
	require.NoError(t, err)
	if !proto.Equal(wantKs, ks) {
		t.Errorf("unexpected keyspace value: got:\n%v, want\n%v", ks, wantKs)
	}
	require.NotNil(t, ms)
	require.GreaterOrEqual(t, len(ms.TableSettings), 1)
	require.Equal(t, wantQuery, ms.TableSettings[0].SourceExpression, "unexpected query")
}

func TestStopAfterCopyFlag(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "ks",
		TargetKeyspace: "ks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()
	specs := &vschemapb.Keyspace{
		Vindexes: map[string]*vschemapb.Vindex{
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table": "ks.lkp",
					"from":  "c1",
					"to":    "col2",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v",
					Column: "col2",
				}},
			},
		},
	}
	// Dummy sourceSchema.
	sourceSchema := "CREATE TABLE `t1` (\n" +
		"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"  `col2` int(11) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1"

	vschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "xxhash",
					Column: "col1",
				}},
			},
		},
	}
	env.tmc.schema[ms.SourceKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Fields: []*querypb.Field{{
				Name: "col1",
				Type: querypb.Type_INT64,
			}, {
				Name: "col2",
				Type: querypb.Type_INT64,
			}},
			Schema: sourceSchema,
		}},
	}
	if err := env.topoServ.SaveVSchema(ctx, ms.SourceKeyspace, vschema); err != nil {
		t.Fatal(err)
	}

	ms1, _, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.TargetKeyspace, specs, false)
	require.NoError(t, err)
	require.Equal(t, ms1.StopAfterCopy, true)

	ms2, _, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.TargetKeyspace, specs, true)
	require.NoError(t, err)
	require.Equal(t, ms2.StopAfterCopy, false)
}

func TestCreateLookupVindexFailures(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		// Keyspace where the vindex is created.
		SourceKeyspace: "sourceks",
		// Keyspace where the lookup table and VReplication workflow is created.
		TargetKeyspace: "targetks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	unique := map[string]*vschemapb.Vindex{
		"v": {
			Type: "lookup_unique",
			Params: map[string]string{
				"table": "targetks.t",
				"from":  "c1",
				"to":    "c2",
			},
		},
	}

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
			"v": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      "targetks.t",
					"from":       "c1",
					"to":         "c2",
					"write_only": "true",
				},
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v",
					Column: "c1",
				}},
			},
		},
	}
	err := env.topoServ.SaveVSchema(ctx, ms.TargetKeyspace, vs)
	require.NoError(t, err)

	testcases := []struct {
		description string
		input       *vschemapb.Keyspace
		err         string
	}{
		{
			description: "dup vindex",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v1": {
						Type: "xxhash",
					},
					"v2": {
						Type: "xxhash",
					},
				},
			},
			err: "only one vindex must be specified",
		},
		{
			description: "not a lookup",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "xxhash",
					},
				},
			},
			err: "vindex xxhash is not a lookup type",
		},
		{
			description: "unqualified table",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "lookup",
						Params: map[string]string{
							"table": "t",
						},
					},
				},
			},
			err: "vindex table name (t) must be in the form <keyspace>.<table>",
		},
		{
			description: "unique lookup should have only one from column",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.t",
							"from":  "c1,c2",
							"to":    "c3",
						},
					},
				},
			},
			err: "unique vindex 'from' should have only one column",
		},
		{
			description: "non-unique lookup should have more than one column",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "lookup",
						Params: map[string]string{
							"table": "targetks.t",
							"from":  "c1",
							"to":    "c2",
						},
					},
				},
			},
			err: "non-unique vindex 'from' should have more than one column",
		},
		{
			description: "vindex not found",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "lookup_noexist",
						Params: map[string]string{
							"table": "targetks.t",
							"from":  "c1,c2",
							"to":    "c2",
						},
					},
				},
			},
			err: `vindexType "lookup_noexist" not found`,
		},
		{
			description: "no tables",
			input: &vschemapb.Keyspace{
				Vindexes: unique,
			},
			err: "one or two tables must be specified",
		},
		{
			description: "too many tables",
			input: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.t",
							"from":  "c1",
							"to":    "c2",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:   "v",
							Column: "c1",
						}},
					},
					"v": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:   "xxhash",
							Column: "c2",
						}},
					},
					"v2": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:   "xxhash",
							Column: "c1",
						}},
					},
				},
			},
			err: "one or two tables must be specified",
		},
		{
			description: "only one colvindex",
			input: &vschemapb.Keyspace{
				Vindexes: unique,
				Tables: map[string]*vschemapb.Table{
					"t1": {},
				},
			},
			err: "exactly one ColumnVindex must be specified for the t1 table",
		},
		{
			description: "vindex name must match",
			input: &vschemapb.Keyspace{
				Vindexes: unique,
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name: "other",
						}},
					},
				},
			},
			err: "ColumnVindex name (other) must match vindex name (v)",
		},
		{
			description: "owner must match",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.t",
							"from":  "c1",
							"to":    "c2",
						},
						Owner: "otherTable",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name: "v",
						}},
					},
				},
			},
			err: "vindex owner (otherTable) must match table name (t1)",
		},
		{
			description: "owner must match",
			input: &vschemapb.Keyspace{
				Vindexes: unique,
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name: "v",
						}},
					},
				},
			},
			err: "at least one column must be specified in ColumnVindexes",
		},
		{
			description: "columnvindex length mismatch",
			input: &vschemapb.Keyspace{
				Vindexes: unique,
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "v",
							Columns: []string{"col1", "col2"},
						}},
					},
				},
			},
			err: "length of table columns (2) differs from length of vindex columns (1)",
		},
		{
			description: "vindex mismatches with what's in vschema",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "targetks.t",
							"from":  "c1",
							"to":    "c2",
						},
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:   "xxhash",
							Column: "col",
						}},
					},
				},
			},
			err: "a conflicting vindex named xxhash already exists in the targetks keyspace",
		},
		{
			description: "source table not in vschema",
			input: &vschemapb.Keyspace{
				Vindexes: unique,
				Tables: map[string]*vschemapb.Table{
					"other": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:   "v",
							Column: "col",
						}},
					},
				},
			},
			err: "table other not found in the targetks keyspace",
		},
	}
	for _, tcase := range testcases {
		t.Run(tcase.description, func(t *testing.T) {
			req := &vtctldatapb.LookupVindexCreateRequest{
				Workflow: "lookup",
				Keyspace: ms.TargetKeyspace,
				Vindex:   tcase.input,
			}
			_, err := env.ws.LookupVindexCreate(ctx, req)
			if !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("CreateLookupVindex(%s) err: %v, must contain %v", tcase.description, err, tcase.err)
			}
		})
	}
}

func TestExternalizeLookupVindex(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		// Keyspace where the vindex is created.
		SourceKeyspace: "sourceks",
		// Keyspace where the lookup table and VReplication workflow is created.
		TargetKeyspace: "targetks",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

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
	fields := sqltypes.MakeTestFields(
		"id|state|message|source",
		"int64|varbinary|varbinary|blob",
	)
	ownedSourceStopAfterCopy := fmt.Sprintf(`keyspace:"%s",shard:"0",filter:{rules:{match:"owned_lookup" filter:"select * from t1 where in_keyrange(col1, '%s.xxhash', '-80')"}} stop_after_copy:true`,
		ms.SourceKeyspace, ms.SourceKeyspace)
	ownedSourceKeepRunningAfterCopy := fmt.Sprintf(`keyspace:"%s",shard:"0",filter:{rules:{match:"owned_lookup" filter:"select * from t1 where in_keyrange(col1, '%s.xxhash', '-80')"}}`,
		ms.SourceKeyspace, ms.SourceKeyspace)
	ownedRunning := sqltypes.MakeTestResult(fields, "1|Running|msg|"+ownedSourceKeepRunningAfterCopy)
	ownedStopped := sqltypes.MakeTestResult(fields, "1|Stopped|Stopped after copy|"+ownedSourceStopAfterCopy)
	unownedSourceStopAfterCopy := fmt.Sprintf(`keyspace:"%s",shard:"0",filter:{rules:{match:"unowned_lookup" filter:"select * from t1 where in_keyrange(col1, '%s.xxhash', '-80')"}} stop_after_copy:true`,
		ms.SourceKeyspace, ms.SourceKeyspace)
	unownedSourceKeepRunningAfterCopy := fmt.Sprintf(`keyspace:"%s",shard:"0",filter:{rules:{match:"unowned_lookup" filter:"select * from t1 where in_keyrange(col1, '%s.xxhash', '-80')"}}`,
		ms.SourceKeyspace, ms.SourceKeyspace)
	unownedRunning := sqltypes.MakeTestResult(fields, "2|Running|msg|"+unownedSourceKeepRunningAfterCopy)
	unownedStopped := sqltypes.MakeTestResult(fields, "2|Stopped|Stopped after copy|"+unownedSourceStopAfterCopy)

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
			err := env.topoServ.SaveVSchema(ctx, tcase.request.Keyspace, sourceVschema)
			require.NoError(t, err)
			err = env.topoServ.RebuildSrvVSchema(ctx, []string{env.cell})
			require.NoError(t, err)

			validationQuery := fmt.Sprintf("select id, state, message, source from _vt.vreplication where workflow='%s' and db_name='vt_%s'",
				tcase.request.Name, ms.TargetKeyspace)
			env.tmc.expectVRQuery(200, validationQuery, tcase.vrResponse)
			env.tmc.expectVRQuery(210, validationQuery, tcase.vrResponse)

			preWorkflowDeleteCalls := env.tmc.workflowDeleteCalls
			_, err = env.ws.LookupVindexExternalize(ctx, tcase.request)
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
				expectedWorkflowDeleteCalls = preWorkflowDeleteCalls + (len(env.targets))
			}
			require.Equal(t, expectedWorkflowDeleteCalls, env.tmc.workflowDeleteCalls)

			aftervschema, err := env.topoServ.GetVSchema(ctx, ms.SourceKeyspace)
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
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
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
		Cell: "zone1",
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\(`+
			`'workflow', `+
			(`'keyspace:\\"sourceks\\" shard:\\"0\\" `+
				`filter:{`+
				`rules:{match:\\"t1\\" filter:\\"select.*t1\\"} `+
				`rules:{match:\\"t2\\" filter:\\"select.*t3\\"} `+
				`rules:{match:\\"t4\\"}`+
				`}', `)+
			`'', [0-9]*, [0-9]*, 'zone1', 'primary,rdonly', [0-9]*, 0, 'Stopped', 'vt_targetks', 0, 0, false`+
			`\)`+eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestMaterializerManyToOne(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t3",
			CreateDdl:        "t2ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"-80", "80-"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"-80\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1\\"} rules:{match:\\"t2\\" filter:\\"select.*t3\\"}}', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks', 0, 0, false\)`+
			`, `+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"80-\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1\\"} rules:{match:\\"t2\\" filter:\\"select.*t3\\"}}', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks', 0, 0, false\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestMaterializerOneToMany(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
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
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.xxhash.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.xxhash.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestMaterializerManyToMany(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
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
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"-40\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.xxhash.*-80.*`+
			`.*shard:\\"40-\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.xxhash.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"-40\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.xxhash.*80-.*`+
			`.*shard:\\"40-\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.xxhash.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestMaterializerMulticolumnVindex(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
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
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1, c2.*targetks\.region.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1, c2.*targetks\.region.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestMaterializerDeploySchema(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t3",
			CreateDdl:        "t2ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t2")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, `t2ddl`, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1\\"} rules:{match:\\"t2\\" filter:\\"select.*t3\\"}}', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks', 0, 0, false\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
	require.Equal(t, env.tmc.getSchemaRequestCount(100), 1)
	require.Equal(t, env.tmc.getSchemaRequestCount(200), 1)
}

func TestMaterializerCopySchema(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "copy",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t3",
			CreateDdl:        "t2ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, `t1_schema`, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1\\"} rules:{match:\\"t2\\" filter:\\"select.*t3\\"}}', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks', 0, 0, false\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
	require.Equal(t, env.tmc.getSchemaRequestCount(100), 1)
	require.Equal(t, env.tmc.getSchemaRequestCount(200), 1)

}

func TestMaterializerExplicitColumns(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select c1, c1+c2, c2 from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
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
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1, c2.*targetks\.region.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1, c2.*targetks\.region.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestMaterializerRenamedColumns(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select c3 as c1, c1+c2, c4 as c2 from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
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
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c3, c4.*targetks\.region.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"0\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c3, c4.*targetks\.region.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestMaterializerStopAfterCopy(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
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
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix+`.*stop_after_copy:true`, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.ws.Materialize(ctx, ms)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestMaterializerNoTargetVSchema(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
		Sharded: true,
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "table t1 not found in vschema for keyspace targetks")
}

func TestMaterializerNoDDL(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "target table t1 does not exist and there is no create ddl defined")
	require.Equal(t, env.tmc.getSchemaRequestCount(100), 0)
	require.Equal(t, env.tmc.getSchemaRequestCount(200), 1)

}

func TestMaterializerNoSourcePrimary(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "copy",
		}},
	}
	sources := []string{"0"}
	targets := []string{"0"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Copied from newTestMaterializerEnv
	env := &testMaterializerEnv{
		ms:       ms,
		sources:  sources,
		targets:  targets,
		tablets:  make(map[int]*topodatapb.Tablet),
		topoServ: memorytopo.NewServer(ctx, "cell"),
		cell:     "cell",
		tmc:      newTestMaterializerTMClient(),
	}
	env.ws = NewServer(env.topoServ, env.tmc)
	defer env.close()

	tabletID := 100
	for _, shard := range sources {
		_ = env.addTablet(tabletID, env.ms.SourceKeyspace, shard, topodatapb.TabletType_REPLICA)
		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targets {
		_ = env.addTablet(tabletID, env.ms.TargetKeyspace, shard, topodatapb.TabletType_PRIMARY)
		tabletID += 10
	}

	// Skip the schema creation part.

	env.expectValidation()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "shard must have a primary for copying schema: 0")
}

func TestMaterializerTableMismatchNonCopy(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t2",
			CreateDdl:        "",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "target table t1 does not exist and there is no create ddl defined")
}

func TestMaterializerTableMismatchCopy(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t2",
			CreateDdl:        "copy",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "source and target table names must match for copying schema: t2 vs t1")
}

func TestMaterializerNoSourceTable(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "copy",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")
	delete(env.tmc.schema, "sourceks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "source table t1 does not exist")
}

func TestMaterializerSyntaxError(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "bad query",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "syntax error at position 4 near 'bad'")
}

func TestMaterializerNotASelect(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "update t1 set val=1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "unrecognized statement: update t1 set val=1")
}

func TestMaterializerNoGoodVindex(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
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
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "could not find a vindex to compute keyspace id for table t1")
}

func TestMaterializerComplexVindexExpression(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select a+b as c1 from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
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
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "vindex column cannot be a complex expression: a + b as c1")
}

func TestMaterializerNoVindexInExpression(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select c2 from t1",
			CreateDdl:        "t1ddl",
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
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
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.ws.Materialize(ctx, ms)
	require.EqualError(t, err, "could not find vindex column c1")
}
