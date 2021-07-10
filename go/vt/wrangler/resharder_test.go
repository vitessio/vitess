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

package wrangler

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"context"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

const rsSelectFrozenQuery = "select 1 from _vt.vreplication where db_name='vt_ks' and message='FROZEN'"
const insertPrefix = `/insert into _vt.vreplication\(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name\) values `
const eol = "$"

func TestResharderOneToMany(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.expectValidation()
	env.expectNoRefStream()

	type testCase struct {
		cells       string
		tabletTypes string
	}
	var newTestCase = func(cells, tabletTypes string) *testCase {
		return &testCase{
			cells:       cells,
			tabletTypes: tabletTypes,
		}
	}
	var testCases []*testCase

	testCases = append(testCases, newTestCase("", ""))
	testCases = append(testCases, newTestCase("cell", "master"))
	testCases = append(testCases, newTestCase("cell", "master,replica"))
	testCases = append(testCases, newTestCase("", "replica,rdonly"))

	for _, tc := range testCases {
		env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})

		schm := &tabletmanagerdatapb.SchemaDefinition{
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
				Name:              "t1",
				Columns:           []string{"c1", "c2"},
				PrimaryKeyColumns: []string{"c1"},
				Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			}},
		}
		env.tmc.schema = schm

		env.expectValidation()
		env.expectNoRefStream()
		name := tc.cells + "/" + tc.tabletTypes
		t.Run(name, func(t *testing.T) {
			env.tmc.expectVRQuery(
				200,
				insertPrefix+
					`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"/.*\\" filter:\\"-80\\" > > ', '', [0-9]*, [0-9]*, '`+
					tc.cells+`', '`+tc.tabletTypes+`', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+eol,
				&sqltypes.Result{},
			)
			env.tmc.expectVRQuery(
				210,
				insertPrefix+
					`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"/.*\\" filter:\\"80-\\" > > ', '', [0-9]*, [0-9]*, '`+
					tc.cells+`', '`+tc.tabletTypes+`', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+eol,
				&sqltypes.Result{},
			)
			env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})
			env.tmc.expectVRQuery(210, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

			err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, tc.cells, tc.tabletTypes, true, false)
			require.NoError(t, err)
			env.tmc.verifyQueries(t)
		})
		env.close()
	}
}

func TestResharderManyToOne(t *testing.T) {
	env := newTestResharderEnv(t, []string{"-80", "80-"}, []string{"0"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.expectValidation()
	env.expectNoRefStream()

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"-80\\" filter:<rules:<match:\\"/.*\\" filter:\\"-\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\).*`+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"80-\\" filter:<rules:<match:\\"/.*\\" filter:\\"-\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)

	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestResharderManyToMany(t *testing.T) {
	env := newTestResharderEnv(t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.expectValidation()
	env.expectNoRefStream()

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"-40\\" filter:<rules:<match:\\"/.*\\" filter:\\"-80\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\).*`+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"40-\\" filter:<rules:<match:\\"/.*\\" filter:\\"-80\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"40-\\" filter:<rules:<match:\\"/.*\\" filter:\\"80-\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)

	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})
	env.tmc.expectVRQuery(210, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
}

// TestResharderOneRefTable tests the case where there's one ref table, but no stream for it.
// This means that the table is being updated manually.
func TestResharderOneRefTable(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	vs := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"t1": {
				Type: vindexes.TypeReference,
			},
		},
	}
	if err := env.wr.ts.SaveVSchema(context.Background(), env.keyspace, vs); err != nil {
		t.Fatal(err)
	}

	env.expectValidation()
	env.expectNoRefStream()

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"exclude\\" > rules:<match:\\"/.*\\" filter:\\"-80\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"exclude\\" > rules:<match:\\"/.*\\" filter:\\"80-\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)

	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})
	env.tmc.expectVRQuery(210, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
}

// TestReshardStopFlags tests the flags -stop_started and -stop_after_copy
func TestReshardStopFlags(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	vs := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"t1": {
				Type: vindexes.TypeReference,
			},
		},
	}
	if err := env.wr.ts.SaveVSchema(context.Background(), env.keyspace, vs); err != nil {
		t.Fatal(err)
	}

	env.expectValidation()
	env.expectNoRefStream()
	// inserts into the two shards expects flag stop_after_copy to be true

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"exclude\\" > rules:<match:\\"/.*\\" filter:\\"-80\\" > > stop_after_copy:true ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"exclude\\" > rules:<match:\\"/.*\\" filter:\\"80-\\" > > stop_after_copy:true ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)
	// -auto_start=false is tested by NOT expecting the update query which sets state to RUNNING

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", false, true)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
}

// TestResharderOneRefStream tests the case where there's one ref table and an associated stream.
func TestResharderOneRefStream(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	vs := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"t1": {
				Type: vindexes.TypeReference,
			},
		},
	}
	if err := env.wr.ts.SaveVSchema(context.Background(), env.keyspace, vs); err != nil {
		t.Fatal(err)
	}

	env.expectValidation()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "t1",
			}},
		},
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow|source|cell|tablet_types",
		"varchar|varchar|varchar|varchar"),
		fmt.Sprintf("t1|%v|cell1|master,replica", bls),
	)
	env.tmc.expectVRQuery(100, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), result)

	refRow := `\('t1', 'keyspace:\\"ks1\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" > > ', '', [0-9]*, [0-9]*, 'cell1', 'master,replica', [0-9]*, 0, 'Stopped', 'vt_ks'\)`
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"exclude\\" > rules:<match:\\"/.*\\" filter:\\"-80\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\).*`+
			refRow+eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"exclude\\" > rules:<match:\\"/.*\\" filter:\\"80-\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\).*`+
			refRow+eol,
		&sqltypes.Result{},
	)

	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})
	env.tmc.expectVRQuery(210, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
}

// TestResharderNoRefStream tests the case where there's a stream, but it's not a reference.
func TestResharderNoRefStream(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
		},
	}
	if err := env.wr.ts.SaveVSchema(context.Background(), env.keyspace, vs); err != nil {
		t.Fatal(err)
	}

	env.expectValidation()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t2",
			}},
		},
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow|source|cell|tablet_types",
		"varchar|varchar|varchar|varchar"),
		fmt.Sprintf("t1|%v|cell1|master,replica", bls),
	)
	env.tmc.expectVRQuery(100, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), result)

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"/.*\\" filter:\\"-80\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"/.*\\" filter:\\"80-\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)

	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})
	env.tmc.expectVRQuery(210, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestResharderCopySchema(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.expectValidation()
	env.expectNoRefStream()

	// These queries confirm that the copy schema function is getting called.
	env.tmc.expectVRQuery(100, "SELECT 1 FROM information_schema.tables WHERE table_schema = '_vt' AND table_name = 'shard_metadata'", &sqltypes.Result{})
	env.tmc.expectVRQuery(100, "SELECT 1 FROM information_schema.tables WHERE table_schema = '_vt' AND table_name = 'shard_metadata'", &sqltypes.Result{})

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"/.*\\" filter:\\"-80\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`\('resharderTest', 'keyspace:\\"ks\\" shard:\\"0\\" filter:<rules:<match:\\"/.*\\" filter:\\"80-\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks'\)`+
			eol,
		&sqltypes.Result{},
	)

	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})
	env.tmc.expectVRQuery(210, "update _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, false, "", "", true, false)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
}

func TestResharderDupWorkflow(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.tmc.expectVRQuery(100, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(200, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"1",
		"int64"),
		"1",
	)
	env.tmc.expectVRQuery(210, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), result)

	env.tmc.expectVRQuery(200, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(100, rsSelectFrozenQuery, &sqltypes.Result{})

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.EqualError(t, err, "validateWorkflowName.VReplicationExec: workflow resharderTest already exists in keyspace ks on tablet 210")
	env.tmc.verifyQueries(t)
}

func TestResharderServingState(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.tmc.expectVRQuery(100, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(200, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(210, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(100, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, rsSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, []string{"-80"}, nil, true, "", "", true, false)
	assert.EqualError(t, err, "buildResharder: source shard -80 is not in serving state")

	env.tmc.expectVRQuery(100, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(200, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(210, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(100, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, rsSelectFrozenQuery, &sqltypes.Result{})
	err = env.wr.Reshard(context.Background(), env.keyspace, env.workflow, []string{"0"}, []string{"0"}, true, "", "", true, false)
	assert.EqualError(t, err, "buildResharder: target shard 0 is in serving state")

	env.tmc.expectVRQuery(100, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(200, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(210, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(100, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, rsSelectFrozenQuery, &sqltypes.Result{})
	err = env.wr.Reshard(context.Background(), env.keyspace, env.workflow, []string{"0"}, []string{"-80"}, true, "", "", true, false)
	assert.EqualError(t, err, "buildResharder: ValidateForReshard: source and target keyranges don't match: - vs -80")
}

func TestResharderTargetAlreadyResharding(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.tmc.expectVRQuery(100, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(200, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})
	env.tmc.expectVRQuery(210, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s' and workflow='%s'", env.keyspace, env.workflow), &sqltypes.Result{})

	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"1",
		"int64"),
		"1",
	)
	env.tmc.expectVRQuery(200, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, rsSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s'", env.keyspace), result)
	env.tmc.expectVRQuery(210, fmt.Sprintf("select 1 from _vt.vreplication where db_name='vt_%s'", env.keyspace), &sqltypes.Result{})
	env.tmc.expectVRQuery(100, rsSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.EqualError(t, err, "buildResharder: validateTargets: some streams already exist in the target shards, please clean them up and retry the command")
	env.tmc.verifyQueries(t)
}

func TestResharderUnnamedStream(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	vs := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"t1": {
				Type: vindexes.TypeReference,
			},
		},
	}
	if err := env.wr.ts.SaveVSchema(context.Background(), env.keyspace, vs); err != nil {
		t.Fatal(err)
	}

	env.expectValidation()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "t1",
			}},
		},
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow|source|cell|tablet_types",
		"varchar|varchar|varchar|varchar"),
		fmt.Sprintf("|%v|cell1|master,replica", bls),
	)
	env.tmc.expectVRQuery(100, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), result)

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.EqualError(t, err, "buildResharder: readRefStreams: VReplication streams must have named workflows for migration: shard: ks:0")
	env.tmc.verifyQueries(t)
}

func TestResharderMismatchedRefStreams(t *testing.T) {
	env := newTestResharderEnv(t, []string{"-80", "80-"}, []string{"0"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	vs := &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"t1": {
				Type: vindexes.TypeReference,
			},
		},
	}
	if err := env.wr.ts.SaveVSchema(context.Background(), env.keyspace, vs); err != nil {
		t.Fatal(err)
	}

	env.expectValidation()

	bls1 := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "t1",
			}},
		},
	}
	result1 := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow|source|cell|tablet_types",
		"varchar|varchar|varchar|varchar"),
		fmt.Sprintf("t1|%v|cell1|master,replica", bls1),
	)
	env.tmc.expectVRQuery(100, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), result1)
	bls2 := &binlogdatapb.BinlogSource{
		Keyspace: "ks2",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "t1",
			}},
		},
	}
	result2 := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow|source|cell|tablet_types",
		"varchar|varchar|varchar|varchar"),
		fmt.Sprintf("t1|%v|cell1|master,replica", bls1),
		fmt.Sprintf("t1|%v|cell1|master,replica", bls2),
	)
	env.tmc.expectVRQuery(110, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), result2)

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	want := "buildResharder: readRefStreams: streams are mismatched across source shards"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Reshard err: %v, want %v", err, want)
	}
	env.tmc.verifyQueries(t)
}

func TestResharderTableNotInVSchema(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	env.expectValidation()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "t1",
			}},
		},
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow|source|cell|tablet_types",
		"varchar|varchar|varchar|varchar"),
		fmt.Sprintf("t1|%v|cell1|master,replica", bls),
	)
	env.tmc.expectVRQuery(100, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), result)

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	assert.EqualError(t, err, "buildResharder: readRefStreams: blsIsReference: table t1 not found in vschema")
	env.tmc.verifyQueries(t)
}

func TestResharderMixedTablesOrder1(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
			"t2": {
				Type: vindexes.TypeReference,
			},
		},
	}
	if err := env.wr.ts.SaveVSchema(context.Background(), env.keyspace, vs); err != nil {
		t.Fatal(err)
	}

	env.expectValidation()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t1",
				Filter: "select * from t2",
			}, {
				Match:  "t2",
				Filter: "select * from t2",
			}},
		},
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow|source|cell|tablet_types",
		"varchar|varchar|varchar|varchar"),
		fmt.Sprintf("t1t2|%v|cell1|master,replica", bls),
	)
	env.tmc.expectVRQuery(100, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), result)

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	want := "buildResharder: readRefStreams: blsIsReference: cannot reshard streams with a mix of reference and sharded tables"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Reshard err: %v, want %v", err.Error(), want)
	}
	env.tmc.verifyQueries(t)
}

func TestResharderMixedTablesOrder2(t *testing.T) {
	env := newTestResharderEnv(t, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}
	env.tmc.schema = schm

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
				}},
			},
			"t2": {
				Type: vindexes.TypeReference,
			},
		},
	}
	if err := env.wr.ts.SaveVSchema(context.Background(), env.keyspace, vs); err != nil {
		t.Fatal(err)
	}

	env.expectValidation()

	bls := &binlogdatapb.BinlogSource{
		Keyspace: "ks1",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match:  "t2",
				Filter: "select * from t2",
			}, {
				Match:  "t1",
				Filter: "select * from t2",
			}},
		},
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"workflow|source|cell|tablet_types",
		"varchar|varchar|varchar|varchar"),
		fmt.Sprintf("t1t2|%v|cell1|master,replica", bls),
	)
	env.tmc.expectVRQuery(100, fmt.Sprintf("select workflow, source, cell, tablet_types from _vt.vreplication where db_name='vt_%s' and message != 'FROZEN'", env.keyspace), result)

	err := env.wr.Reshard(context.Background(), env.keyspace, env.workflow, env.sources, env.targets, true, "", "", true, false)
	want := "buildResharder: readRefStreams: blsIsReference: cannot reshard streams with a mix of reference and sharded tables"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Errorf("Reshard err: %v, want %v", err.Error(), want)
	}
	env.tmc.verifyQueries(t)
}
