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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const getWorkflowQuery = "select id from _vt.vreplication where db_name='vt_targetks' and workflow='workflow'"
const mzUpdateQuery = "update _vt.vreplication set state='Running' where db_name='vt_targetks' and workflow='workflow'"
const mzSelectFrozenQuery = "select 1 from _vt.vreplication where db_name='vt_targetks' and message='FROZEN' and workflow_sub_type != 1"
const mzCheckJournal = "/select val from _vt.resharding_journal where id="
const mzGetWorkflowStatusQuery = "select id, workflow, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, transaction_timestamp, message, tags, workflow_type, workflow_sub_type from _vt.vreplication where workflow = 'workflow' and db_name = 'vt_targetks'"
const mzGetCopyState = "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
const mzGetLatestCopyState = "select table_name, lastpk from _vt.copy_state where vrepl_id = 1 and id in (select max(id) from _vt.copy_state where vrepl_id = 1 group by vrepl_id, table_name)"

var defaultOnDDL = binlogdatapb.OnDDLAction_IGNORE.String()
var binlogSource = &binlogdatapb.BinlogSource{
	Keyspace: "sourceks",
	Shard:    "0",
	Filter: &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "t1",
			Filter: "select * from t1",
		}},
	},
}
var getWorkflowRes = sqltypes.MakeTestResult(
	sqltypes.MakeTestFields(
		"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
		"int64|blob|varchar|varchar|varchar|int64|int64|int64",
	),
	fmt.Sprintf("1|%s||zone1|replica|1|0|1", binlogSource),
)
var getWorkflowStatusRes = sqltypes.MakeTestResult(
	sqltypes.MakeTestFields(
		"id|workflow|source|pos|stop_pos|max_replication_log|state|db_name|time_updated|transaction_timestamp|message|tags|workflow_type|workflow_sub_type",
		"int64|varchar|blob|varchar|varchar|int64|varchar|varchar|int64|int64|varchar|varchar|int64|int64",
	),
	fmt.Sprintf("1|wf1|%s|MySQL56/9d10e6ec-07a0-11ee-ae73-8e53f4cf3083:1-97|NULL|0|running|vt_ks|1686577659|0|||1|0", binlogSource),
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
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, getWorkflowQuery, getWorkflowRes)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzGetWorkflowStatusQuery, getWorkflowStatusRes)
	env.tmc.expectVRQuery(200, mzGetLatestCopyState, &sqltypes.Result{})

	ctx := context.Background()
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
	ctx := context.Background()
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
			env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
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
			want := fmt.Sprintf("shard_streams:{key:\"%s/%s\" value:{streams:{id:1 tablet:{cell:\"%s\" uid:200} source_shard:\"%s/%s\" position:\"MySQL56/9d10e6ec-07a0-11ee-ae73-8e53f4cf3083:1-97\" status:\"running\" info:\"VStream Lag: 0s\"}}}",
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
