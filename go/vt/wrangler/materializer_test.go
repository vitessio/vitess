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
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

const mzUpdateQuery = "update _vt.vreplication set state='Running' where db_name='vt_targetks' and workflow='workflow'"

func TestMigrateTables(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
		}},
	}
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	ctx := context.Background()
	err := env.wr.Migrate(ctx, "workflow", "sourceks", "targetks", "t1", "", "")
	assert.NoError(t, err)
	vschema, err := env.wr.ts.GetSrvVSchema(ctx, env.cell)
	assert.NoError(t, err)
	got := fmt.Sprintf("%v", vschema)
	want := `keyspaces:<key:"sourceks" value:<> > keyspaces:<key:"targetks" value:<> > ` +
		`routing_rules:<rules:<from_table:"t1" to_tables:"sourceks.t1" > rules:<from_table:"targetks.t1" to_tables:"sourceks.t1" > > `
	assert.Equal(t, got, want)
}

func TestMigrateVSchema(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
		}},
	}
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	ctx := context.Background()
	err := env.wr.Migrate(ctx, "workflow", "sourceks", "targetks", `{"t1":{}}`, "", "")
	assert.NoError(t, err)
	vschema, err := env.wr.ts.GetSrvVSchema(ctx, env.cell)
	assert.NoError(t, err)
	got := fmt.Sprintf("%v", vschema)
	want := `keyspaces:<key:"sourceks" value:<> > ` +
		`keyspaces:<key:"targetks" value:<tables:<key:"t1" value:<> > > > ` +
		`routing_rules:<rules:<from_table:"t1" to_tables:"sourceks.t1" > rules:<from_table:"targetks.t1" to_tables:"sourceks.t1" > > `
	assert.Equal(t, got, want)
}

func TestMaterializerOneToOne(t *testing.T) {
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1\\" > rules:<match:\\"t2\\" filter:\\"select.*t3\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
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
	env := newTestMaterializerEnv(t, ms, []string{"-80", "80-"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"-80\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1\\" > rules:<match:\\"t2\\" filter:\\"select.*t3\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks'\)`+
			`, `+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"80-\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1\\" > rules:<match:\\"t2\\" filter:\\"select.*t3\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

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

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.hash.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.hash.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
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
	env := newTestMaterializerEnv(t, ms, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer env.close()

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

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"-40\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.hash.*-80.*`+
			`.*shard:\\"40-\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.hash.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"-40\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.hash.*80-.*`+
			`.*shard:\\"40-\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1.*targetks\.hash.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
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

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1, c2.*targetks\.region.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1, c2.*targetks\.region.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t2")

	env.tmc.expectVRQuery(200, `t2ddl`, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1\\" > rules:<match:\\"t2\\" filter:\\"select.*t3\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, `t1_schema`, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('workflow', 'keyspace:\\"sourceks\\" shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1\\" > rules:<match:\\"t2\\" filter:\\"select.*t3\\" > > ', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_targetks'\)`+
			eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
	env.tmc.verifyQueries(t)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
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

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1, c2.*targetks\.region.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c1, c2.*targetks\.region.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
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

	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`.*shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c3, c4.*targetks\.region.*-80.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`.*shard:\\"0\\" filter:<rules:<match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(c3, c4.*targetks\.region.*80-.*`,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
	assert.NoError(t, err)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
		Sharded: true,
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}
	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "table t1 not found in vschema for keyspace targetks")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "target table t1 does not exist and there is no create ddl defined")
}

func TestMaterializerNoSourceMaster(t *testing.T) {
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

	// Copied from newTestMaterializerEnv
	env := &testMaterializerEnv{
		ms:       ms,
		sources:  sources,
		targets:  targets,
		tablets:  make(map[int]*topodatapb.Tablet),
		topoServ: memorytopo.NewServer("cell"),
		cell:     "cell",
		tmc:      newTestMaterializerTMClient(),
	}
	env.wr = New(logutil.NewConsoleLogger(), env.topoServ, env.tmc)
	defer env.close()

	tabletID := 100
	for _, shard := range sources {
		_ = env.addTablet(tabletID, env.ms.SourceKeyspace, shard, topodatapb.TabletType_REPLICA)
		tabletID += 10
	}
	tabletID = 200
	for _, shard := range targets {
		_ = env.addTablet(tabletID, env.ms.TargetKeyspace, shard, topodatapb.TabletType_MASTER)
		tabletID += 10
	}

	// Skip the schema creation part.

	env.expectValidation()

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "source shard must have a master for copying schema: 0")
}

func TestMaterializerTableMismatch(t *testing.T) {
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "source and target table names must match for copying schema: t2 vs t1")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")
	delete(env.tmc.schema, "sourceks.t1")

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "source table t1 does not exist")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "syntax error at position 4 near 'bad'")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "unrecognized statement: update t1 set val=1")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"lookup_unique": {
				Type: "lookup_unique",
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

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "could not find a vindex to compute keyspace id for table t1")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

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

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "vindex column cannot be a complex expression: a + b as c1")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

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

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}

	err := env.wr.Materialize(context.Background(), ms)
	assert.EqualError(t, err, "could not find vindex column c1")
}
