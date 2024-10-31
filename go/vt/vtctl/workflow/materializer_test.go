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
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
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
	position             = "MySQL56/9d10e6ec-07a0-11ee-ae73-8e53f4cf3083:1-97"
	mzSelectFrozenQuery  = "select 1 from _vt.vreplication where db_name='vt_targetks' and message='FROZEN' and workflow_sub_type != 1"
	mzCheckJournal       = "/select val from _vt.resharding_journal where id="
	mzGetCopyState       = "select distinct table_name from _vt.copy_state cs, _vt.vreplication vr where vr.id = cs.vrepl_id and vr.id = 1"
	mzGetLatestCopyState = "select vrepl_id, table_name, lastpk from _vt.copy_state where vrepl_id in (1) and id in (select max(id) from _vt.copy_state where vrepl_id in (1) group by vrepl_id, table_name)"
	insertPrefix         = `/insert into _vt.vreplication\(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name, workflow_type, workflow_sub_type, defer_secondary_keys, options\) values `
	getNonEmptyTable     = "select 1 from `t1` limit 1"
)

var (
	defaultOnDDL = binlogdatapb.OnDDLAction_IGNORE.String()
)

func gtid(position string) string {
	arr := strings.Split(position, "/")
	if len(arr) != 2 {
		return ""
	}
	return arr[1]
}

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

func TestStripAutoIncrement(t *testing.T) {
	parser := sqlparser.NewTestParser()

	tcs := []struct {
		desc      string
		ddl       string
		want      string
		expectErr bool
	}{
		{
			desc: "invalid DDL",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` massiveint NOT NULL,\n" +
				"PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
			expectErr: true,
		},
		{
			desc: "has auto increment",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` int NOT NULL AUTO_INCREMENT,\n" +
				"`c1` varchar(128),\n" +
				"PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
			want: "create table table1 (\n" +
				"\tid int not null,\n" +
				"\tc1 varchar(128),\n" +
				"\tprimary key (id)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",
		},
		{
			desc: "has no auto increment",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` int NOT NULL,\n" +
				"`c1` varchar(128),\n" +
				"PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
			want: "create table table1 (\n" +
				"\tid int not null,\n" +
				"\tc1 varchar(128),\n" +
				"\tprimary key (id)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",
		},
		{
			desc: "has auto increment with secondary key",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` int NOT NULL auto_increment,\n" +
				"`c1` varchar(128),\n" +
				"`c2` varchar(128),\n" +
				"UNIQUE KEY `c1` (`c1`),\n" +
				"PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
			want: "create table table1 (\n" +
				"\tid int not null,\n" +
				"\tc1 varchar(128),\n" +
				"\tc2 varchar(128),\n" +
				"\tunique key c1 (c1),\n" +
				"\tprimary key (id)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",
		},
		{
			desc: "has auto increment with multi-col PK",
			ddl: "CREATE TABLE `table1` (\n" +
				"`id` int NOT NULL auto_increment,\n" +
				"`c1` varchar(128) NOT NULL,\n" +
				"`c2` varchar(128),\n" +
				"PRIMARY KEY (`id`, `c2`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;",
			want: "create table table1 (\n" +
				"\tid int not null,\n" +
				"\tc1 varchar(128) not null,\n" +
				"\tc2 varchar(128),\n" +
				"\tprimary key (id, c2)\n" +
				") ENGINE InnoDB,\n" +
				"  CHARSET latin1",
		},
	}

	for _, tc := range tcs {
		strippedDDL, err := stripAutoIncrement(tc.ddl, parser, nil)
		require.Equal(t, tc.expectErr, (err != nil), "unexpected error result", "expected error %t, got: %v", tc.expectErr, err)
		require.Equal(t, tc.want, strippedDDL, fmt.Sprintf("stripped DDL %q does not match our expected result: %q", strippedDDL, tc.want))
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
	env.tmc.expectFetchAsAllPrivsQuery(200, getNonEmptyTable, &sqltypes.Result{})
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
			// insert = fmt.Sprintf(`/insert into .vreplication\(.*on_ddl:%s.*`, onDDLAction)
			// env.tmc.expectVRQuery(100, "/.*", &sqltypes.Result{})

			// TODO: we cannot test the actual query generated w/o having a
			// TabletManager. Importing the tabletmanager package, however, causes
			// a circular dependency.
			// The TabletManager portion is tested in rpc_vreplication_test.go.
			env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
			env.tmc.expectFetchAsAllPrivsQuery(200, getNonEmptyTable, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzGetCopyState, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzGetLatestCopyState, &sqltypes.Result{})

			targetShard, err := env.topoServ.GetShardNames(ctx, ms.TargetKeyspace)
			require.NoError(t, err)
			sourceShard, err := env.topoServ.GetShardNames(ctx, ms.SourceKeyspace)
			require.NoError(t, err)
			want := fmt.Sprintf("shard_streams:{key:\"%s/%s\" value:{streams:{id:1 tablet:{cell:\"%s\" uid:200} source_shard:\"%s/%s\" position:\"%s\" status:\"Running\" info:\"VStream Lag: 0s\"}}} traffic_state:\"Reads Not Switched. Writes Not Switched\"",
				ms.TargetKeyspace, targetShard[0], env.cell, ms.SourceKeyspace, sourceShard[0], gtid(position))

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

// TestShardedAutoIncHandling tests the optional behaviors available when moving
// tables to a sharded keyspace and the tables being copied contain MySQL
// auto_increment clauses. The optional behaviors are:
// 1. LEAVE the tables' MySQL auto_increment clauses alone
// 2. REMOVE the tables' MySQL auto_increment clauses
// 3. REPLACE the table's MySQL auto_increment clauses with Vitess sequences
func TestShardedAutoIncHandling(t *testing.T) {
	tableName := "t1"
	tableDDL := fmt.Sprintf("create table %s (id int not null auto_increment primary key, c1 varchar(10))", tableName)
	validateEmptyTableQuery := fmt.Sprintf("select 1 from `%s` limit 1", tableName)
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      tableName,
			CreateDdl:        tableDDL,
			SourceExpression: fmt.Sprintf("select * from %s", tableName),
		}},
		WorkflowOptions: &vtctldatapb.WorkflowOptions{},
	}

	type testcase struct {
		name                  string
		value                 vtctldatapb.ShardedAutoIncrementHandling
		globalKeyspace        string
		targetShards          []string
		targetVSchema         *vschemapb.Keyspace
		wantTargetVSchema     *vschemapb.Keyspace
		expectQueries         []string
		expectAllPrivsQueries []string
		expectErr             string
	}
	testcases := []testcase{
		{
			name:           "global-keyspace does not exist",
			globalKeyspace: "foo",
			expectErr:      "global-keyspace foo does not exist",
		},
		{
			name:           "global keyspace is sharded",
			globalKeyspace: ms.TargetKeyspace,
			targetShards:   []string{"-80", "80-"},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			expectErr: fmt.Sprintf("global-keyspace %s is sharded and thus cannot be used for global resources",
				ms.TargetKeyspace),
		},
		{
			name:           "leave",
			globalKeyspace: ms.SourceKeyspace,
			targetShards:   []string{"-80", "80-"},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			value: vtctldatapb.ShardedAutoIncrementHandling_LEAVE,
			wantTargetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			expectQueries: []string{
				tableDDL, // Unchanged
			},
			expectAllPrivsQueries: []string{
				validateEmptyTableQuery,
			},
		},
		{
			name:           "remove",
			globalKeyspace: ms.SourceKeyspace,
			targetShards:   []string{"-80", "80-"},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			value: vtctldatapb.ShardedAutoIncrementHandling_REMOVE,
			wantTargetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			expectQueries: []string{ // auto_increment clause removed
				fmt.Sprintf(`create table %s (
	id int not null primary key,
	c1 varchar(10)
)`, tableName),
			},
			expectAllPrivsQueries: []string{
				validateEmptyTableQuery,
			},
		},
		{
			name:           "replace, but vschema AutoIncrement already in place",
			globalKeyspace: ms.SourceKeyspace,
			targetShards:   []string{"-80", "80-"},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
						AutoIncrement: &vschemapb.AutoIncrement{ // AutoIncrement definition exists
							Column:   "id",
							Sequence: fmt.Sprintf("%s_non_default_seq_name", tableName),
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			value: vtctldatapb.ShardedAutoIncrementHandling_REPLACE,
			wantTargetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
						AutoIncrement: &vschemapb.AutoIncrement{ // AutoIncrement definition left alone
							Column:   "id",
							Sequence: fmt.Sprintf("%s_non_default_seq_name", tableName),
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			expectQueries: []string{ // auto_increment clause removed
				fmt.Sprintf(`create table %s (
	id int not null primary key,
	c1 varchar(10)
)`, tableName),
			},
			expectAllPrivsQueries: []string{
				validateEmptyTableQuery,
			},
		},
		{
			name:           "replace",
			globalKeyspace: ms.SourceKeyspace,
			targetShards:   []string{"-80", "80-"},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			value: vtctldatapb.ShardedAutoIncrementHandling_REPLACE,
			wantTargetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Tables: map[string]*vschemapb.Table{
					tableName: {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Name:   "xxhash",
								Column: "id",
							},
						},
						AutoIncrement: &vschemapb.AutoIncrement{ // AutoIncrement definition added
							Column:   "id",
							Sequence: fmt.Sprintf(autoSequenceTableFormat, tableName),
						},
					},
				},
				Vindexes: map[string]*vschemapb.Vindex{
					"xxhash": {
						Type: "xxhash",
					},
				},
			},
			expectQueries: []string{ // auto_increment clause removed
				fmt.Sprintf(`create table %s (
	id int not null primary key,
	c1 varchar(10)
)`, tableName),
			},
			expectAllPrivsQueries: []string{
				validateEmptyTableQuery,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.targetShards == nil {
				tc.targetShards = []string{"0"}
			}
			env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, tc.targetShards)
			defer env.close()

			env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
			for i := range tc.targetShards {
				uid := startingTargetTabletUID + (i * tabletUIDStep)
				for _, query := range tc.expectQueries {
					env.tmc.expectVRQuery(uid, query, &sqltypes.Result{})
				}
				for _, query := range tc.expectAllPrivsQueries {
					env.tmc.expectFetchAsAllPrivsQuery(uid, query, &sqltypes.Result{})
				}
				env.tmc.expectVRQuery(uid, mzGetCopyState, &sqltypes.Result{})
				env.tmc.expectVRQuery(uid, mzGetLatestCopyState, &sqltypes.Result{})
				env.tmc.SetGetSchemaResponse(uid, &tabletmanagerdatapb.SchemaDefinition{}) // So that the schema is copied from the source
			}

			if tc.targetVSchema != nil {
				err := env.ws.ts.SaveVSchema(ctx, ms.TargetKeyspace, tc.targetVSchema)
				require.NoError(t, err)
			}

			_, err := env.ws.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
				Workflow:       ms.Workflow,
				SourceKeyspace: ms.SourceKeyspace,
				TargetKeyspace: ms.TargetKeyspace,
				IncludeTables:  []string{tableName},
				WorkflowOptions: &vtctldatapb.WorkflowOptions{
					ShardedAutoIncrementHandling: tc.value,
					GlobalKeyspace:               tc.globalKeyspace,
				},
			})
			if tc.expectErr != "" {
				require.EqualError(t, err, tc.expectErr)
			} else {
				require.NoError(t, err)
				if tc.wantTargetVSchema != nil {
					targetVSchema, err := env.ws.ts.GetVSchema(ctx, ms.TargetKeyspace)
					require.NoError(t, err)
					require.True(t, proto.Equal(targetVSchema, tc.wantTargetVSchema))
				}
			}
		})
	}
}

// TestMoveTablesNoRoutingRules confirms that MoveTables does not create routing rules if
// --no-routing-rules is specified.
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
	// insert = fmt.Sprintf(`/insert into .vreplication\(.*on_ddl:%s.*`, onDDLAction)
	// env.tmc.expectVRQuery(100, "/.*", &sqltypes.Result{})

	// TODO: we cannot test the actual query generated w/o having a
	// TabletManager. Importing the tabletmanager package, however, causes
	// a circular dependency.
	// The TabletManager portion is tested in rpc_vreplication_test.go.
	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectFetchAsAllPrivsQuery(200, getNonEmptyTable, &sqltypes.Result{})
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
						Position:    gtid(position),
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
	env.tmc.expectFetchAsAllPrivsQuery(200, "select 1 from `lookup` limit 1", &sqltypes.Result{})
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
	setStartingVschema := func() {
		err := env.topoServ.SaveVSchema(ctx, ms.SourceKeyspace, vs)
		require.NoError(t, err)
	}
	setStartingVschema()

	testcases := []struct {
		description  string
		specs        *vschemapb.Keyspace
		sourceSchema string
		preFunc      func()
		out          string
		err          string
	}{{
		description: "unique lookup re-use vschema",
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
		preFunc: func() {
			// The vschema entries will already exist and we will re-use them.
			err := env.ws.ts.SaveVSchema(ctx, ms.SourceKeyspace, &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table":      fmt.Sprintf("%s.lkp", ms.TargetKeyspace),
							"from":       "c1",
							"to":         "c2",
							"write_only": "true", // It has not been externalized yet
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
			})
			require.NoError(t, err)
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
		description: "unique lookup with conflicting vindex",
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
		preFunc: func() {
			// The existing vindex vschema entry differs from what we want to
			// create so we cannot re-use it.
			err := env.ws.ts.SaveVSchema(ctx, ms.SourceKeyspace, &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table":      fmt.Sprintf("%s.lkp", ms.TargetKeyspace),
							"from":       "c1",
							"to":         "c2",
							"write_only": "false", // This vindex has been externalized
						},
						Owner: "t1",
					},
				},
			})
			require.NoError(t, err)
		},
		err: "a conflicting vindex named v already exists in the sourceks keyspace",
		sourceSchema: "CREATE TABLE `t1` (\n" +
			"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `col2` int(11) DEFAULT NULL,\n" +
			"  `col3` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1",
	}, {
		description: "unique lookup with conflicting column vindexes",
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
		preFunc: func() {
			// The existing ColumnVindexes vschema entry differs from what we
			// want to create so we cannot re-use it.
			err := env.ws.ts.SaveVSchema(ctx, ms.SourceKeyspace, &vschemapb.Keyspace{
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "v",
							Columns: []string{"col1", "col2"},
						}},
					},
				},
			})
			require.NoError(t, err)
		},
		err: "a conflicting ColumnVindex on column(s) col1,col2 in table t1 already exists in the sourceks keyspace",
		sourceSchema: "CREATE TABLE `t1` (\n" +
			"  `col1` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `col2` int(11) DEFAULT NULL,\n" +
			"  `col3` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1",
	}, {
		description: "unique lookup using last column w/o primary key",
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
			"  `col2` int(11) DEFAULT NULL\n" + // Because it's the last entity in the definition it has no trailing comma
			") ENGINE=InnoDB",
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
		t.Run(tcase.description, func(t *testing.T) {
			if tcase.sourceSchema != "" {
				env.tmc.schema[ms.SourceKeyspace+".t1"] = &tabletmanagerdatapb.SchemaDefinition{
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
						Schema: tcase.sourceSchema,
					}},
				}
			} else {
				delete(env.tmc.schema, ms.SourceKeyspace+".t1")
			}

			if tcase.preFunc != nil {
				tcase.preFunc()
				defer func() {
					// Reset the vschema as it may have been changed in the pre
					// function.
					setStartingVschema()
				}()
			}
			outms, _, _, cancelFunc, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.SourceKeyspace, tcase.specs, false)
			if tcase.err != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tcase.err, "prepareCreateLookup(%s) err: %v, does not contain %v", tcase.description, err, tcase.err)
				return
			}
			require.NoError(t, err)
			// All of these test cases create a table and thus change the target
			// vschema.
			require.NotNil(t, cancelFunc)
			want := strings.Split(tcase.out, "\n")
			got := strings.Split(outms.TableSettings[0].CreateDdl, "\n")
			require.Equal(t, want, got, tcase.description)
		})
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

		_, got, _, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.SourceKeyspace, specs, false)
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
				"unicode_loose_xxhash": {
					Type: "unicode_loose_xxhash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"lkp": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Column: "c1",
						Name:   "unicode_loose_xxhash",
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
					Type: "unicode_loose_xxhash",
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

		_, _, got, cancelFunc, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.SourceKeyspace, specs, false)
		if tcase.err != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("prepareCreateLookup(%s) err: %v, must contain %v", tcase.description, err, tcase.err)
			}
			continue
		}
		require.NoError(t, err)
		// withTable is a vschema that already contains the table and thus
		// we don't make any vschema changes and there's nothing to cancel.
		require.True(t, (cancelFunc != nil) == (tcase.targetVSchema != withTable))
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

	_, got, _, _, err := env.ws.prepareCreateLookup(ctx, "keyspace", ms.TargetKeyspace, specs, false)
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
					Name:   "unicode_loose_xxhash",
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
			"unicode_loose_xxhash": { // Non default vindex type for the column.
				Type: "unicode_loose_xxhash",
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
			"unicode_loose_xxhash": {
				Type: "unicode_loose_xxhash",
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
					Name:   "unicode_loose_xxhash",
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

	_, got, _, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.TargetKeyspace, specs, false)
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

	ms, ks, _, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.TargetKeyspace, specs, false)
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

	ms1, _, _, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.TargetKeyspace, specs, false)
	require.NoError(t, err)
	require.Equal(t, ms1.StopAfterCopy, true)

	ms2, _, _, _, err := env.ws.prepareCreateLookup(ctx, "workflow", ms.TargetKeyspace, specs, true)
	require.NoError(t, err)
	require.Equal(t, ms2.StopAfterCopy, false)
}

func TestCreateLookupVindexFailures(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "sourceks", // Not used
		// Keyspace where the lookup table, vindex, and VReplication workflow is created.
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{
			{
				TargetTable: "t1",
				CreateDdl:   "CREATE TABLE `t1` (\n`c1` INT,\n PRIMARY KEY(`c1`)\n)",
			},
			{
				TargetTable: "t2",
				CreateDdl:   "CREATE TABLE `t2` (\n`c2` INT,\n PRIMARY KEY(`c2`)\n)",
			},
			{
				TargetTable: "t3",
				CreateDdl:   "CREATE TABLE `t3` (\n`c3` INT,\n PRIMARY KEY(`c3`)\n)",
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := newTestMaterializerEnv(t, ctx, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	unique := map[string]*vschemapb.Vindex{
		"v": {
			Type: "lookup_unique",
			Params: map[string]string{
				"table": fmt.Sprintf("%s.t", ms.TargetKeyspace),
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
			"v1": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      fmt.Sprintf("%s.t", ms.TargetKeyspace),
					"from":       "c1",
					"to":         "c2",
					"write_only": "true",
				},
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v1",
					Column: "c1",
				}},
			},
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "v2",
					Column: "c2",
				}},
			},
		},
	}
	err := env.topoServ.SaveVSchema(ctx, ms.TargetKeyspace, vs)
	require.NoError(t, err)

	testcases := []struct {
		description            string
		input                  *vschemapb.Keyspace
		createRequest          *createVReplicationWorkflowRequestResponse
		vrepExecQueries        []string
		fetchAsAllPrivsQueries []string
		schemaAdditions        []*tabletmanagerdatapb.TableDefinition
		err                    string
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
							"table": fmt.Sprintf("%s.t", ms.TargetKeyspace),
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
							"table": fmt.Sprintf("%s.t", ms.TargetKeyspace),
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
							"table": fmt.Sprintf("%s.t", ms.TargetKeyspace),
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
							"table": fmt.Sprintf("%s.t", ms.TargetKeyspace),
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
							"table": fmt.Sprintf("%s.t", ms.TargetKeyspace),
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
							"table": fmt.Sprintf("%s.t", ms.TargetKeyspace),
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
			err: fmt.Sprintf("a conflicting vindex named xxhash already exists in the %s keyspace", ms.TargetKeyspace),
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
			err: fmt.Sprintf("table other not found in the %s keyspace", ms.TargetKeyspace),
		},
		{
			description: "workflow creation error",
			input: &vschemapb.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"v2": {
						Type: "consistent_lookup_unique",
						Params: map[string]string{
							"table": fmt.Sprintf("%s.t1_lkp", ms.TargetKeyspace),
							"from":  "c1",
							"to":    "keyspace_id",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t2": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:   "v2",
							Column: "c2",
						}},
					},
				},
			},
			vrepExecQueries: []string{
				"CREATE TABLE `t1_lkp` (\n`c1` INT,\n  `keyspace_id` varbinary(128),\n  PRIMARY KEY (`c1`)\n)",
			},
			fetchAsAllPrivsQueries: []string{
				"select 1 from `t1_lkp` limit 1",
			},
			createRequest: &createVReplicationWorkflowRequestResponse{
				req: nil, // We don't care about defining it in this case
				res: &tabletmanagerdatapb.CreateVReplicationWorkflowResponse{},
				err: errors.New("we gots us an error"),
			},
			err: "we gots us an error",
		},
	}
	for _, tcase := range testcases {
		t.Run(tcase.description, func(t *testing.T) {
			req := &vtctldatapb.LookupVindexCreateRequest{
				Workflow: "lookup",
				Keyspace: ms.TargetKeyspace,
				Vindex:   tcase.input,
			}
			if len(tcase.schemaAdditions) > 0 {
				ogs := env.tmc.schema
				defer func() {
					env.tmc.schema = ogs
				}()
				// The tables are created in the target keyspace.
				for _, tbl := range tcase.schemaAdditions {
					env.tmc.schema[ms.TargetKeyspace+"."+tbl.Name] = &tabletmanagerdatapb.SchemaDefinition{
						TableDefinitions: []*tabletmanagerdatapb.TableDefinition{tbl},
					}
				}
			}
			for _, tablet := range env.tablets {
				if tablet.Keyspace == ms.TargetKeyspace {
					for _, vrq := range tcase.vrepExecQueries {
						env.tmc.expectVRQuery(int(tablet.Alias.Uid), vrq, &sqltypes.Result{})
					}
					for _, q := range tcase.fetchAsAllPrivsQueries {
						env.tmc.expectFetchAsAllPrivsQuery(int(tablet.Alias.Uid), q, &sqltypes.Result{})
					}
					if tcase.createRequest != nil {
						env.tmc.expectCreateVReplicationWorkflowRequest(tablet.Alias.Uid, tcase.createRequest)
					}
				}
			}
			_, err := env.ws.LookupVindexCreate(ctx, req)
			if !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("CreateLookupVindex(%s) err: %v, must contain %v", tcase.description, err, tcase.err)
			}
			// Confirm that the original vschema where the vindex would
			// be created is still in place -- since the workflow
			// creation failed in each test case. That vindex is created
			// in the target keyspace based on the MaterializeSettings
			// definition.
			cvs, err := env.ws.ts.GetVSchema(ctx, ms.TargetKeyspace)
			require.NoError(t, err)
			require.True(t, proto.Equal(vs, cvs), "expected: %+v, got: %+v", vs, cvs)
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
					Options: "{}",
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
					Options: "{}",
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
					Options: "{}",
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
					Options: "{}",
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
					Options: "{}",
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
					Options: "{}",
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
				env.tmc.expectFetchAsAllPrivsQuery(int(tablet.Alias.Uid), getNonEmptyTable, &sqltypes.Result{})
				// If we are doing a partial MoveTables, we will only perform the workflow
				// stream creation / INSERT statment on the shard(s) we're migrating.
				if len(tc.moveTablesReq.SourceShards) > 0 && !slices.Contains(tc.moveTablesReq.SourceShards, tablet.Shard) {
					continue
				}
				reqRes := &createVReplicationWorkflowRequestResponse{
					req: tc.wantReqs[tablet.Alias.Uid],
				}
				env.tmc.expectCreateVReplicationWorkflowRequest(tablet.Alias.Uid, reqRes)
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
				Options:                   "{}",
			})
			require.NoError(t, err, "createWorkflowStreams failed: %v", err)
		})
	}
}

func TestValidateEmptyTables(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	defer ts.Close()

	ks := "test_keyspace"
	shard1 := "-40"
	shard2 := "40-80"
	shard3 := "80-"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.CreateShard(ctx, ks, shard1)
	require.NoError(t, err)
	err = ts.CreateShard(ctx, ks, shard2)
	require.NoError(t, err)
	err = ts.CreateShard(ctx, ks, shard3)
	require.NoError(t, err)

	tablet1 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
		Keyspace: ks,
		Shard:    shard1,
	}
	tablet2 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  200,
		},
		Keyspace: ks,
		Shard:    shard2,
	}
	tablet3 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  300,
		},
		Keyspace: ks,
		Shard:    shard3,
	}
	err = ts.CreateTablet(ctx, tablet1)
	require.NoError(t, err)
	err = ts.CreateTablet(ctx, tablet2)
	require.NoError(t, err)
	err = ts.CreateTablet(ctx, tablet3)
	require.NoError(t, err)

	s1, err := ts.UpdateShardFields(ctx, ks, shard1, func(si *topo.ShardInfo) error {
		si.Shard.PrimaryAlias = tablet1.Alias
		return nil
	})
	require.NoError(t, err)
	s2, err := ts.UpdateShardFields(ctx, ks, shard2, func(si *topo.ShardInfo) error {
		si.Shard.PrimaryAlias = tablet2.Alias
		return nil
	})
	require.NoError(t, err)
	s3, err := ts.UpdateShardFields(ctx, ks, shard3, func(si *topo.ShardInfo) error {
		si.Shard.PrimaryAlias = tablet3.Alias
		return nil
	})
	require.NoError(t, err)

	tableSettings := []*vtctldatapb.TableMaterializeSettings{
		{
			TargetTable: "table1",
		},
		{
			TargetTable: "table2",
		},
		{
			TargetTable: "table3",
		},
	}

	tmc := newTestMaterializerTMClient(ks, []string{shard1, shard2, shard3}, tableSettings)

	table1Query := "select 1 from `table1` limit 1"
	table2Query := "select 1 from `table2` limit 1"
	table3Query := "select 1 from `table3` limit 1"
	nonEmptyTableResult := &sqltypes.Result{Rows: []sqltypes.Row{{sqltypes.NewInt64(1)}}}

	tmc.expectFetchAsAllPrivsQuery(int(tablet1.Alias.Uid), table1Query, &sqltypes.Result{})
	tmc.expectFetchAsAllPrivsQuery(int(tablet2.Alias.Uid), table1Query, &sqltypes.Result{})
	tmc.expectFetchAsAllPrivsQuery(int(tablet3.Alias.Uid), table1Query, nonEmptyTableResult)
	tmc.expectFetchAsAllPrivsQuery(int(tablet1.Alias.Uid), table2Query, &sqltypes.Result{})
	tmc.expectFetchAsAllPrivsQuery(int(tablet2.Alias.Uid), table2Query, &sqltypes.Result{})
	tmc.expectFetchAsAllPrivsQuery(int(tablet3.Alias.Uid), table2Query, &sqltypes.Result{})
	tmc.expectFetchAsAllPrivsQuery(int(tablet1.Alias.Uid), table3Query, nonEmptyTableResult)
	tmc.expectFetchAsAllPrivsQuery(int(tablet2.Alias.Uid), table3Query, nonEmptyTableResult)
	tmc.expectFetchAsAllPrivsQuery(int(tablet3.Alias.Uid), table3Query, nonEmptyTableResult)

	ms := &vtctldatapb.MaterializeSettings{
		TargetKeyspace: ks,
		TableSettings:  tableSettings,
	}

	mz := &materializer{
		ctx:          ctx,
		ts:           ts,
		sourceTs:     ts,
		tmc:          tmc,
		ms:           ms,
		targetShards: []*topo.ShardInfo{s1, s2, s3},
	}

	err = mz.validateEmptyTables()

	assert.ErrorContains(t, err, "table1")
	assert.NotContains(t, err.Error(), "table2")
	// Check if the error message doesn't include duplicate tables
	assert.Equal(t, strings.Count(err.Error(), "table3"), 1)
}
