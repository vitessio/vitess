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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	position             = "9d10e6ec-07a0-11ee-ae73-8e53f4cf3083:1-97"
	mzUpdateQuery        = "update _vt.vreplication set state='Running' where db_name='vt_targetks' and workflow='workflow'"
	mzSelectFrozenQuery  = "select 1 from _vt.vreplication where db_name='vt_targetks' and message='FROZEN' and workflow_sub_type != 1"
	mzCheckJournal       = "/select val from _vt.resharding_journal where id="
	mzGetCopyState       = "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
	mzGetLatestCopyState = "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)"
	insertPrefix         = `/insert into _vt.vreplication\(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys\) values `
	eol                  = "$"
)

var (
	defaultOnDDL = binlogdatapb.OnDDLAction_IGNORE.String()
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
				"\tprimary key (id),\n" +
				"\tkey fk_table1_ref_foreign_id (foreign_id),\n" +
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
				"\tprimary key (id),\n" +
				"\tkey fk_table1_ref_foreign_id (foreign_id),\n" +
				"\tkey fk_table1_ref_user_id (user_id),\n" +
				"\tcheck (foreign_id > 10)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",
		},
	}

	for _, tc := range tcs {
		newDDL, err := stripTableForeignKeys(tc.ddl, sqlparser.NewTestParser())
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
				"\tprimary key (id),\n" +
				"\tkey fk_table1_ref_foreign_id (foreign_id),\n" +
				"\tkey fk_table1_ref_user_id (user_id)\n" +
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
				"\tprimary key (id),\n" +
				"\tkey fk_table1_ref_foreign_id (foreign_id),\n" +
				"\tkey fk_table1_ref_user_id (user_id)\n" +
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
		newDDL, err := stripTableConstraints(tc.ddl, sqlparser.NewTestParser())
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
	env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
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
			env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzGetLatestCopyState, &sqltypes.Result{})

			targetShard, err := env.topoServ.GetShardNames(ctx, ms.TargetKeyspace)
			require.NoError(t, err)
			sourceShard, err := env.topoServ.GetShardNames(ctx, ms.SourceKeyspace)
			require.NoError(t, err)
			want := fmt.Sprintf("shard_streams:{key:\"%s/%s\" value:{streams:{id:1 tablet:{cell:\"%s\" uid:200} source_shard:\"%s/%s\" position:\"%s\" status:\"Running\" info:\"VStream Lag: 0s\"}}} traffic_state:\"Reads Not Switched. Writes Not Switched\"",
				ms.TargetKeyspace, targetShard[0], env.cell, ms.SourceKeyspace, sourceShard[0], position)

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
	env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzGetLatestCopyState, &sqltypes.Result{})

	targetShard, err := env.topoServ.GetShardNames(ctx, ms.TargetKeyspace)
	require.NoError(t, err)
	sourceShard, err := env.topoServ.GetShardNames(ctx, ms.SourceKeyspace)
	require.NoError(t, err)
	want := &vtctldatapb.WorkflowStatusResponse{
		ShardStreams: map[string]*vtctldatapb.WorkflowStatusResponse_ShardStreams{
			fmt.Sprintf("%s/%s", ms.TargetKeyspace, targetShard[0]): {
				Streams: []*vtctldatapb.WorkflowStatusResponse_ShardStreamState{
					{
						Id: 1,
						Tablet: &topodatapb.TabletAlias{
							Cell: env.cell,
							Uid:  200,
						},
						SourceShard: fmt.Sprintf("%s/%s", ms.SourceKeyspace, sourceShard[0]),
						Position:    position,
						Status:      binlogdatapb.VReplicationWorkflowState_Running.String(),
						Info:        "VStream Lag: 0s",
					},
				},
			},
		},
		TrafficState: "Reads Not Switched. Writes Not Switched",
	}

	res, err := env.ws.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
		Workflow:       ms.Workflow,
		SourceKeyspace: ms.SourceKeyspace,
		TargetKeyspace: ms.TargetKeyspace,
		IncludeTables:  []string{"t1"},
		NoRoutingRules: true,
	})
	require.NoError(t, err)
	require.EqualValues(t, want, res, "got: %+v, want: %+v", res, want)
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
	env.tmc.expectVRQuery(200, "/CREATE TABLE `lookup`", &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzGetLatestCopyState, &sqltypes.Result{})
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

// TestKeyRangesEqualOptimization tests that we optimize the source
// filtering when there's only one source shard for the stream and
// its keyrange is equal to the target shard for the stream. This
// means that even if the target keyspace is sharded, the source
// does not need to perform the in_keyrange filtering.
func TestKeyRangesEqualOptimization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflow := "testwf"
	cells := []string{"cell"}
	sourceKs := "sourceks"
	targetKs := "targetks"
	table := "t1"
	tableSettings := []*vtctldatapb.TableMaterializeSettings{{
		TargetTable:      table,
		SourceExpression: fmt.Sprintf("select * from %s", table),
	}}
	targetVSchema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			table: {
				ColumnVindexes: []*vschemapb.ColumnVindex{
					{
						Column: "id",
						Name:   "xxhash",
					},
				},
			},
		},
	}

	testCases := []struct {
		name          string
		sourceShards  []string
		targetShards  []string
		moveTablesReq *vtctldatapb.MoveTablesCreateRequest
		// Target Shards are in the order specifed in the targetShards slice
		// with the UIDs starting at 200 and increasing by 10 for each tablet
		// and shard since there's only a primary tablet per shard.
		wantReqs map[uint32]*tabletmanagerdatapb.CreateVReplicationWorkflowRequest
	}{
		{
			name: "no in_keyrange filter -- partial, one equal shard",
			moveTablesReq: &vtctldatapb.MoveTablesCreateRequest{
				Workflow:       workflow,
				TargetKeyspace: targetKs,
				SourceKeyspace: sourceKs,
				Cells:          []string{"cell"},
				SourceShards:   []string{"-80"}, // Partial MoveTables just for this shard
				IncludeTables:  []string{table},
			},
			sourceShards: []string{"-80", "80-"},
			targetShards: []string{"-80", "80-"},
			wantReqs: map[uint32]*tabletmanagerdatapb.CreateVReplicationWorkflowRequest{
				200: {
					Workflow:        workflow,
					WorkflowType:    binlogdatapb.VReplicationWorkflowType_MoveTables,
					WorkflowSubType: binlogdatapb.VReplicationWorkflowSubType_Partial,
					Cells:           cells,
					BinlogSource: []*binlogdatapb.BinlogSource{
						{
							Keyspace: sourceKs,
							Shard:    "-80", // Keyranges are equal between the source and target
							Filter: &binlogdatapb.Filter{
								Rules: []*binlogdatapb.Rule{
									{
										Match:  table,
										Filter: fmt.Sprintf("select * from %s", table),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "in_keyrange filter -- unequal shards",
			moveTablesReq: &vtctldatapb.MoveTablesCreateRequest{
				Workflow:       workflow,
				TargetKeyspace: targetKs,
				SourceKeyspace: sourceKs,
				Cells:          []string{"cell"},
				IncludeTables:  []string{table},
			},
			sourceShards: []string{"-"},
			targetShards: []string{"-80", "80-"},
			wantReqs: map[uint32]*tabletmanagerdatapb.CreateVReplicationWorkflowRequest{
				200: {
					Workflow:     workflow,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
					Cells:        cells,
					BinlogSource: []*binlogdatapb.BinlogSource{
						{
							Keyspace: sourceKs,
							Shard:    "-",
							Filter: &binlogdatapb.Filter{
								Rules: []*binlogdatapb.Rule{
									{
										Match:  table,
										Filter: fmt.Sprintf("select * from %s where in_keyrange(id, '%s.xxhash', '-80')", table, targetKs),
									},
								},
							},
						},
					},
				},
				210: {
					Workflow:     workflow,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
					Cells:        cells,
					BinlogSource: []*binlogdatapb.BinlogSource{
						{
							Keyspace: sourceKs,
							Shard:    "-",
							Filter: &binlogdatapb.Filter{
								Rules: []*binlogdatapb.Rule{
									{
										Match:  table,
										Filter: fmt.Sprintf("select * from %s where in_keyrange(id, '%s.xxhash', '80-')", table, targetKs),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "in_keyrange filter -- unequal shards on merge",
			moveTablesReq: &vtctldatapb.MoveTablesCreateRequest{
				Workflow:       workflow,
				TargetKeyspace: targetKs,
				SourceKeyspace: sourceKs,
				Cells:          []string{"cell"},
				IncludeTables:  []string{table},
			},
			sourceShards: []string{"-80", "80-"},
			targetShards: []string{"-"},
			wantReqs: map[uint32]*tabletmanagerdatapb.CreateVReplicationWorkflowRequest{
				200: {
					Workflow:     workflow,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
					Cells:        cells,
					BinlogSource: []*binlogdatapb.BinlogSource{
						{
							Keyspace: sourceKs,
							Shard:    "-80",
							Filter: &binlogdatapb.Filter{
								Rules: []*binlogdatapb.Rule{
									{
										Match:  table,
										Filter: fmt.Sprintf("select * from %s where in_keyrange(id, '%s.xxhash', '-')", table, targetKs),
									},
								},
							},
						},
						{
							Keyspace: sourceKs,
							Shard:    "80-",
							Filter: &binlogdatapb.Filter{
								Rules: []*binlogdatapb.Rule{
									{
										Match:  table,
										Filter: fmt.Sprintf("select * from %s where in_keyrange(id, '%s.xxhash', '-')", table, targetKs),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "no in_keyrange filter -- all equal shards",
			moveTablesReq: &vtctldatapb.MoveTablesCreateRequest{
				Workflow:       workflow,
				TargetKeyspace: targetKs,
				SourceKeyspace: sourceKs,
				Cells:          []string{"cell"},
				IncludeTables:  []string{table},
			},
			sourceShards: []string{"-80", "80-"},
			targetShards: []string{"-80", "80-"},
			wantReqs: map[uint32]*tabletmanagerdatapb.CreateVReplicationWorkflowRequest{
				200: {
					Workflow:     workflow,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
					Cells:        cells,
					BinlogSource: []*binlogdatapb.BinlogSource{
						{
							Keyspace: sourceKs,
							Shard:    "-80",
							Filter: &binlogdatapb.Filter{
								Rules: []*binlogdatapb.Rule{
									{
										Match:  table,
										Filter: fmt.Sprintf("select * from %s", table),
									},
								},
							},
						},
					},
				},
				210: {
					Workflow:     workflow,
					WorkflowType: binlogdatapb.VReplicationWorkflowType_MoveTables,
					Cells:        cells,
					BinlogSource: []*binlogdatapb.BinlogSource{
						{
							Keyspace: sourceKs,
							Shard:    "80-",
							Filter: &binlogdatapb.Filter{
								Rules: []*binlogdatapb.Rule{
									{
										Match:  table,
										Filter: fmt.Sprintf("select * from %s", table),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if len(tc.wantReqs) == 0 {
				require.FailNow(t, "invalid test case", "no wanted requests specified")
			}
			workflowType := maps.Values(tc.wantReqs)[0].WorkflowType
			ms := &vtctldatapb.MaterializeSettings{
				Workflow:              tc.moveTablesReq.Workflow,
				MaterializationIntent: vtctldatapb.MaterializationIntent_MOVETABLES,
				SourceKeyspace:        sourceKs,
				TargetKeyspace:        targetKs,
				Cell:                  strings.Join(tc.moveTablesReq.Cells, ","),
				SourceShards:          tc.moveTablesReq.SourceShards,
				TableSettings:         tableSettings,
			}
			env := newTestMaterializerEnv(t, ctx, ms, tc.sourceShards, tc.targetShards)
			defer env.close()

			// Target is always sharded.
			err := env.ws.ts.SaveVSchema(ctx, targetKs, targetVSchema)
			require.NoError(t, err, "SaveVSchema failed: %v", err)

			for _, tablet := range env.tablets {
				// Queries will only be executed on primary tablets in the target keyspace.
				if tablet.Keyspace != targetKs || tablet.Type != topodatapb.TabletType_PRIMARY {
					continue
				}
				env.tmc.expectVRQuery(int(tablet.Alias.Uid), mzSelectFrozenQuery, &sqltypes.Result{})
				// If we are doing a partial MoveTables, we will only perform the workflow
				// stream creation / INSERT statment on the shard(s) we're migrating.
				if len(tc.moveTablesReq.SourceShards) > 0 && !slices.Contains(tc.moveTablesReq.SourceShards, tablet.Shard) {
					continue
				}
				env.tmc.expectCreateVReplicationWorkflowRequest(tablet.Alias.Uid, tc.wantReqs[tablet.Alias.Uid])
			}

			mz := &materializer{
				ctx:          ctx,
				ts:           env.ws.ts,
				sourceTs:     env.ws.ts,
				tmc:          env.tmc,
				ms:           ms,
				workflowType: workflowType,
				env:          vtenv.NewTestEnv(),
			}
			err = mz.createWorkflowStreams(&tabletmanagerdatapb.CreateVReplicationWorkflowRequest{
				Workflow:                  tc.moveTablesReq.Workflow,
				Cells:                     tc.moveTablesReq.Cells,
				TabletTypes:               tc.moveTablesReq.TabletTypes,
				TabletSelectionPreference: tc.moveTablesReq.TabletSelectionPreference,
				WorkflowType:              workflowType,
				DeferSecondaryKeys:        tc.moveTablesReq.DeferSecondaryKeys,
				AutoStart:                 tc.moveTablesReq.AutoStart,
				StopAfterCopy:             tc.moveTablesReq.StopAfterCopy,
			})
			require.NoError(t, err, "createWorkflowStreams failed: %v", err)
		})
	}
}
