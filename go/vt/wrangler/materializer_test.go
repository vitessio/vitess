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
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

const mzUpdateQuery = "update _vt.vreplication set state='Running' where db_name='vt_targetks' and workflow='workflow'"
const mzSelectIDQuery = "select id from _vt.vreplication where db_name='vt_targetks' and workflow='workflow'"
const mzSelectFrozenQuery = "select 1 from _vt.vreplication where db_name='vt_targetks' and message='FROZEN'"
const mzCheckJournal = "/select val from _vt.resharding_journal where id="

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

	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	ctx := context.Background()
	err := env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1", "", "", false, "", true, false, "")
	require.NoError(t, err)
	vschema, err := env.wr.ts.GetSrvVSchema(ctx, env.cell)
	require.NoError(t, err)
	got := fmt.Sprintf("%v", vschema)
	want := []string{
		`keyspaces:<key:"sourceks" value:<> > keyspaces:<key:"targetks" value:<tables:<key:"t1" value:<> > > >`,
		`rules:<from_table:"t1" to_tables:"sourceks.t1" >`,
		`rules:<from_table:"targetks.t1" to_tables:"sourceks.t1" >`,
	}
	for _, wantstr := range want {
		require.Contains(t, got, wantstr)
	}
}

func TestMissingTables(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t2",
		}, {
			TargetTable:      "t3",
			SourceExpression: "select * from t3",
		}},
	}
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	ctx := context.Background()
	err := env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1,tyt", "", "", false, "", true, false, "")
	require.EqualError(t, err, "table(s) not found in source keyspace sourceks: tyt")
	err = env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1,tyt,t2,txt", "", "", false, "", true, false, "")
	require.EqualError(t, err, "table(s) not found in source keyspace sourceks: tyt,txt")
	err = env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1", "", "", false, "", true, false, "")
	require.NoError(t, err)
}

func TestMoveTablesAllAndExclude(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
		}, {
			TargetTable:      "t2",
			SourceExpression: "select * from t2",
		}, {
			TargetTable:      "t3",
			SourceExpression: "select * from t3",
		}},
	}

	ctx := context.Background()
	var err error

	var targetTables = func(env *testMaterializerEnv) []string {
		vschema, err := env.wr.ts.GetSrvVSchema(ctx, env.cell)
		require.NoError(t, err)
		var targetTables []string
		for table := range vschema.Keyspaces["targetks"].Tables {
			targetTables = append(targetTables, table)
		}
		sort.Strings(targetTables)
		return targetTables
	}
	allTables := []string{"t1", "t2", "t3"}
	sort.Strings(allTables)

	type testCase struct {
		allTables     bool
		excludeTables string
		want          []string
	}
	testCases := []*testCase{
		{true, "", allTables},
		{true, "t2,t3", []string{"t1"}},
		{true, "t1", []string{"t2", "t3"}},
	}
	for _, tcase := range testCases {
		t.Run("", func(t *testing.T) {
			env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
			defer env.close()
			env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
			err = env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "", "", "", tcase.allTables, tcase.excludeTables, true, false, "")
			require.NoError(t, err)
			require.EqualValues(t, tcase.want, targetTables(env))
		})

	}

}

func TestMoveTablesStopFlags(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
		}},
	}

	ctx := context.Background()
	var err error
	t.Run("StopStartedAndStopAfterCopyFlags", func(t *testing.T) {
		env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
		defer env.close()
		env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
		env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
		// insert expects flag stop_after_copy to be true
		insert := `/insert into _vt.vreplication\(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types, time_updated, transaction_timestamp, state, db_name\) values .*stop_after_copy:true.*`

		env.tmc.expectVRQuery(200, insert, &sqltypes.Result{})
		env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
		// -auto_start=false is tested by NOT expecting the update query which sets state to RUNNING
		err = env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1", "",
			"", false, "", false, true, "")
		require.NoError(t, err)
		env.tmc.verifyQueries(t)
	})
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

	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	ctx := context.Background()
	err := env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", `{"t1":{}}`, "", "", false, "", true, false, "")
	require.NoError(t, err)
	vschema, err := env.wr.ts.GetSrvVSchema(ctx, env.cell)
	require.NoError(t, err)
	got := fmt.Sprintf("%v", vschema)
	want := []string{`keyspaces:<key:"sourceks" value:<> >`,
		`keyspaces:<key:"sourceks" value:<> > keyspaces:<key:"targetks" value:<tables:<key:"t1" value:<> > > >`,
		`rules:<from_table:"t1" to_tables:"sourceks.t1" >`,
		`rules:<from_table:"targetks.t1" to_tables:"sourceks.t1" >`,
	}
	for _, wantstr := range want {
		require.Contains(t, got, wantstr)
	}
}

func TestCreateLookupVindexFull(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "lkp_vdx",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
	}
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
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

	sourceVSchema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "hash",
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
	if err := env.topoServ.SaveVSchema(context.Background(), ms.TargetKeyspace, &vschemapb.Keyspace{}); err != nil {
		t.Fatal(err)
	}
	if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, sourceVSchema); err != nil {
		t.Fatal(err)
	}

	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, "/CREATE TABLE `lkp`", &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, "update _vt.vreplication set state='Running' where db_name='vt_targetks' and workflow='lkp_vdx'", &sqltypes.Result{})

	ctx := context.Background()
	err := env.wr.CreateLookupVindex(ctx, ms.SourceKeyspace, specs, "cell", "MASTER")
	require.NoError(t, err)

	wantvschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
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
					Name:   "hash",
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
	require.Equal(t, wantvschema, vschema)

	wantvschema = &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"lkp": {},
		},
	}
	vschema, err = env.topoServ.GetVSchema(ctx, ms.TargetKeyspace)
	require.NoError(t, err)
	require.Equal(t, wantvschema, vschema)
}

func TestCreateLookupVindexCreateDDL(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
	}
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
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
					Column: "col1",
					Name:   "hash",
				}},
			},
		},
	}
	if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, vs); err != nil {
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
		err:          "unexpected number of tables returned from schema",
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

		outms, _, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, tcase.specs)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
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
				"hash": {
					Type: "hash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "hash",
						Column: "col1",
					}},
				},
			},
		},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"hash": {
					Type: "hash",
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
						Name:   "hash",
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
				"hash": {
					Type: "hash",
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
						Name:   "hash",
						Column: "col1",
					}},
				},
			},
		},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"hash": {
					Type: "hash",
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
						Name:   "hash",
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
				"hash": {
					Type: "hash",
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
						Name:   "hash",
						Column: "col1",
					}, {
						Name:   "hash",
						Column: "col2",
					}},
				},
			},
		},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"hash": {
					Type: "hash",
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
						Name:   "hash",
						Column: "col1",
					}, {
						Name:   "hash",
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
		if err := env.topoServ.SaveVSchema(context.Background(), ms.TargetKeyspace, &vschemapb.Keyspace{}); err != nil {
			t.Fatal(err)
		}
		if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, tcase.sourceVSchema); err != nil {
			t.Fatal(err)
		}

		_, got, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()
	sourcevs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "col1",
					Name:   "hash",
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
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
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
				"hash": {
					Type: "hash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"lkp": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Column: "c1",
						Name:   "hash",
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
				// Create a misleading vindex name.
				"hash": {
					Type: "hash",
				},
			},
		},
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"hash": {
					Type: "hash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"lkp": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Column: "c1",
						Name:   "hash",
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
				"hash": {
					Type: "unicode_loose_md5",
				},
			},
		},
		err: "a conflicting vindex named hash already exists in the target vschema",
	}, {
		description:     "sharded, int64, good table",
		targetTable:     "t2",
		sourceFieldType: querypb.Type_INT64,
		targetVSchema:   withTable,
		out: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"hash": {
					Type: "hash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t2": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Column: "c1",
						Name:   "hash",
					}},
				},
			},
		},
	}, {
		description:     "sharded, int64, table mismatch",
		targetTable:     "t2",
		sourceFieldType: querypb.Type_VARCHAR,
		targetVSchema:   withTable,
		err:             "a conflicting table named t2 already exists in the target vschema",
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
		if err := env.topoServ.SaveVSchema(context.Background(), ms.TargetKeyspace, tcase.targetVSchema); err != nil {
			t.Fatal(err)
		}

		_, _, got, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs)
		if tcase.err != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("prepareCreateLookup(%s) err: %v, must contain %v", tcase.description, err, tcase.err)
			}
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tcase.out, got, tcase.description)
	}
}

func TestCreateLookupVindexSameKeyspace(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "ks",
		TargetKeyspace: "ks",
	}
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
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
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Name:   "hash",
					Column: "col1",
				}},
			},
		},
	}
	want := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
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
					Name:   "hash",
					Column: "col1",
				}, {
					Name:   "v",
					Column: "col2",
				}},
			},
			"lkp": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "c1",
					Name:   "hash",
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
	if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, vschema); err != nil {
		t.Fatal(err)
	}

	_, got, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs)
	require.NoError(t, err)
	if !proto.Equal(got, want) {
		t.Errorf("same keyspace: got:\n%v, want\n%v", got, want)
	}
}

func TestCreateLookupVindexFailures(t *testing.T) {
	topoServ := memorytopo.NewServer("cell")
	wr := New(logutil.NewConsoleLogger(), topoServ, nil)

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
			"other": {
				Type: "hash",
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
					Column: "c1",
					Name:   "v",
				}},
			},
		},
	}
	if err := topoServ.SaveVSchema(context.Background(), "sourceks", vs); err != nil {
		t.Fatal(err)
	}
	if err := topoServ.SaveVSchema(context.Background(), "targetks", &vschemapb.Keyspace{}); err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		description string
		input       *vschemapb.Keyspace
		err         string
	}{{
		description: "dup vindex",
		input: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v1": {
					Type: "hash",
				},
				"v2": {
					Type: "hash",
				},
			},
		},
		err: "only one vindex must be specified in the specs",
	}, {
		description: "not a lookup",
		input: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "hash",
				},
			},
		},
		err: "vindex hash is not a lookup type",
	}, {
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
		err: "vindex 'table' must be <keyspace>.<table>",
	}, {
		description: "unique lookup should have only one from column",
		input: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table": "targetks.t",
						"from":  "c1,c2",
					},
				},
			},
		},
		err: "unique vindex 'from' should have only one column",
	}, {
		description: "non-unique lookup should have more than one column",
		input: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup",
					Params: map[string]string{
						"table": "targetks.t",
						"from":  "c1",
					},
				},
			},
		},
		err: "non-unique vindex 'from' should have more than one column",
	}, {
		description: "vindex not found",
		input: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup_noexist",
					Params: map[string]string{
						"table": "targetks.t",
						"from":  "c1,c2",
					},
				},
			},
		},
		err: `vindexType "lookup_noexist" not found`,
	}, {
		description: "only one table",
		input: &vschemapb.Keyspace{
			Vindexes: unique,
		},
		err: "exactly one table must be specified in the specs",
	}, {
		description: "only one colvindex",
		input: &vschemapb.Keyspace{
			Vindexes: unique,
			Tables: map[string]*vschemapb.Table{
				"t1": {},
			},
		},
		err: "exactly one ColumnVindex must be specified for the table",
	}, {
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
		err: "ColumnVindex name must match vindex name: other vs v",
	}, {
		description: "owner must match",
		input: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table": "targetks.t",
						"from":  "c1",
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
		err: "vindex owner must match table name: otherTable vs t1",
	}, {
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
	}, {
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
		err: "length of table columns differes from length of vindex columns",
	}, {
		description: "vindex mismatches with what's in vschema",
		input: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"other": {
					Type: "lookup_unique",
					Params: map[string]string{
						"table": "targetks.t",
						"from":  "c1",
					},
					Owner: "t1",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "other",
						Column: "col",
					}},
				},
			},
		},
		err: "a conflicting vindex named other already exists in the source vschema",
	}, {
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
		err: "source table other not found in vschema",
	}, {
		description: "colvindex already exists in vschema",
		input: &vschemapb.Keyspace{
			Vindexes: unique,
			Tables: map[string]*vschemapb.Table{
				"t1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{{
						Name:   "v",
						Column: "c1",
					}},
				},
			},
		},
		err: "ColumnVindex for table t1 already exists: c1",
	}}
	for _, tcase := range testcases {
		err := wr.CreateLookupVindex(context.Background(), "sourceks", tcase.input, "", "")
		if !strings.Contains(err.Error(), tcase.err) {
			t.Errorf("CreateLookupVindex(%s) err: %v, must contain %v", tcase.description, err, tcase.err)
		}
	}
}

func TestExternalizeVindex(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
	}
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	sourceVSchema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
			"owned": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      "targetks.lkp",
					"from":       "c1",
					"to":         "c2",
					"write_only": "true",
				},
				Owner: "t1",
			},
			"unowned": {
				Type: "lookup_unique",
				Params: map[string]string{
					"table":      "targetks.lkp",
					"from":       "c1",
					"to":         "c2",
					"write_only": "true",
				},
			},
			"bad": {
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
					Name:   "hash",
					Column: "col1",
				}, {
					Name:   "owned",
					Column: "col2",
				}},
			},
		},
	}
	fields := sqltypes.MakeTestFields(
		"id|state|message",
		"int64|varbinary|varbinary",
	)
	running := sqltypes.MakeTestResult(fields, "1|Running|msg")
	stopped := sqltypes.MakeTestResult(fields, "1|Stopped|Stopped after copy")
	testcases := []struct {
		input        string
		vrResponse   *sqltypes.Result
		expectDelete bool
		err          string
	}{{
		input:        "sourceks.owned",
		vrResponse:   stopped,
		expectDelete: true,
	}, {
		input:      "sourceks.unowned",
		vrResponse: running,
	}, {
		input: "unqualified",
		err:   "vindex name should be of the form keyspace.vindex: unqualified",
	}, {
		input: "sourceks.absent",
		err:   "vindex sourceks.absent not found in vschema",
	}, {
		input: "sourceks.bad",
		err:   "table name in vindex should be of the form keyspace.table: unqualified",
	}, {
		input:      "sourceks.owned",
		vrResponse: running,
		err:        "is not in Stopped after copy state",
	}, {
		input:      "sourceks.unowned",
		vrResponse: stopped,
		err:        "is not in Running state",
	}}
	for _, tcase := range testcases {
		// Resave the source schema for every iteration.
		if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, sourceVSchema); err != nil {
			t.Fatal(err)
		}
		if tcase.vrResponse != nil {
			validationQuery := "select id, state, message from _vt.vreplication where workflow='lkp_vdx' and db_name='vt_targetks'"
			env.tmc.expectVRQuery(200, validationQuery, tcase.vrResponse)
			env.tmc.expectVRQuery(210, validationQuery, tcase.vrResponse)
		}

		if tcase.expectDelete {
			deleteQuery := "delete from _vt.vreplication where db_name='vt_targetks' and workflow='lkp_vdx'"
			env.tmc.expectVRQuery(200, deleteQuery, &sqltypes.Result{})
			env.tmc.expectVRQuery(210, deleteQuery, &sqltypes.Result{})
		}

		err := env.wr.ExternalizeVindex(context.Background(), tcase.input)
		if tcase.err != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("ExternalizeVindex(%s) err: %v, must contain %v", tcase.input, err, tcase.err)
			}
			continue
		}
		require.NoError(t, err)

		outvschema, err := env.topoServ.GetVSchema(context.Background(), ms.SourceKeyspace)
		require.NoError(t, err)
		vindexName := strings.Split(tcase.input, ".")[1]
		require.NotContains(t, outvschema.Vindexes[vindexName].Params, "write_only", tcase.input)
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
		Cell:        "zone1",
		TabletTypes: "master,rdonly",
	}
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\(`+
			`'workflow', `+
			(`'keyspace:\\"sourceks\\" shard:\\"0\\" `+
				`filter:<`+
				`rules:<match:\\"t1\\" filter:\\"select.*t1\\" > `+
				`rules:<match:\\"t2\\" filter:\\"select.*t3\\" > `+
				`rules:<match:\\"t4\\" > `+
				`> ', `)+
			`'', [0-9]*, [0-9]*, 'zone1', 'master,rdonly', [0-9]*, 0, 'Stopped', 'vt_targetks'`+
			`\)`+eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
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
	env := newTestMaterializerEnv(t, ms, []string{"-80", "80-"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t2")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix+`.*stop_after_copy:true`, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(context.Background(), ms)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	vs := &vschemapb.Keyspace{
		Sharded: true,
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
	require.EqualError(t, err, "target table t1 does not exist and there is no create ddl defined")
	require.Equal(t, env.tmc.getSchemaRequestCount(100), 0)
	require.Equal(t, env.tmc.getSchemaRequestCount(200), 1)

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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
	require.EqualError(t, err, "source shard must have a master for copying schema: 0")
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	delete(env.tmc.schema, "targetks.t1")
	delete(env.tmc.schema, "sourceks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
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
	env := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	defer env.close()

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
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

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(context.Background(), ms)
	require.EqualError(t, err, "could not find vindex column c1")
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
