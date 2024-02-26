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
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const mzUpdateQuery = "update _vt.vreplication set state='Running' where db_name='vt_targetks' and workflow='workflow'"
const mzSelectIDQuery = "select id from _vt.vreplication where db_name='vt_targetks' and workflow='workflow'"
const mzSelectFrozenQuery = "select 1 from _vt.vreplication where db_name='vt_targetks' and message='FROZEN' and workflow_sub_type != 1"
const mzCheckJournal = "/select val from _vt.resharding_journal where id="

var defaultOnDDL = binlogdatapb.OnDDLAction_IGNORE.String()

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
	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1", "", "", false, "", true, false, "", false, false, "", defaultOnDDL, nil, true, false)
	require.NoError(t, err)
	rr, err := env.wr.ts.GetRoutingRules(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(rr.Rules))
}

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
	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1", "", "", false, "", true, false, "", false, false, "", defaultOnDDL, nil, false, false)
	require.NoError(t, err)
	vschema, err := env.wr.ts.GetSrvVSchema(ctx, env.cell)
	require.NoError(t, err)
	got := fmt.Sprintf("%v", vschema)
	want := []string{
		`keyspaces:{key:"sourceks" value:{}} keyspaces:{key:"targetks" value:{tables:{key:"t1" value:{}}}}`,
		`rules:{from_table:"t1" to_tables:"sourceks.t1"}`,
		`rules:{from_table:"targetks.t1" to_tables:"sourceks.t1"}`,
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1,tyt", "", "", false, "", true, false, "", false, false, "", defaultOnDDL, nil, false, false)
	require.EqualError(t, err, "table(s) not found in source keyspace sourceks: tyt")
	err = env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1,tyt,t2,txt", "", "", false, "", true, false, "", false, false, "", defaultOnDDL, nil, false, false)
	require.EqualError(t, err, "table(s) not found in source keyspace sourceks: tyt,txt")
	err = env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1", "", "", false, "", true, false, "", false, false, "", defaultOnDDL, nil, false, false)
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

	var err error

	var targetTables = func(ctx context.Context, env *testMaterializerEnv) []string {
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
			env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
			env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
			err = env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "", "", "", tcase.allTables, tcase.excludeTables, true, false, "", false, false, "", defaultOnDDL, nil, false, false)
			require.NoError(t, err)
			require.EqualValues(t, tcase.want, targetTables(ctx, env))
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

	var err error
	t.Run("StopStartedAndStopAfterCopyFlags", func(t *testing.T) {
		env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
		env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
		env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
		// insert expects flag stop_after_copy to be true
		insert := `/insert into _vt.vreplication\(workflow, source, pos, max_tps, max_replication_lag, cell, tablet_types.*stop_after_copy:true.*`

		env.tmc.expectVRQuery(200, insert, &sqltypes.Result{})
		env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
		// -auto_start=false is tested by NOT expecting the update query which sets state to RUNNING
		err = env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1", "",
			"", false, "", false, true, "", false, false, "", defaultOnDDL, nil, false, false)
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
	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", `{"t1":{}}`, "", "", false, "", true, false, "", false, false, "", defaultOnDDL, nil, false, false)
	require.NoError(t, err)
	vschema, err := env.wr.ts.GetSrvVSchema(ctx, env.cell)
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

func TestCreateLookupVindexFull(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "lkp_vdx",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
	}

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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

	err := env.wr.CreateLookupVindex(ctx, ms.SourceKeyspace, specs, "cell", "PRIMARY", false)
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
	}
	vschema, err := env.topoServ.GetVSchema(ctx, ms.SourceKeyspace)
	require.NoError(t, err)
	utils.MustMatch(t, wantvschema, vschema)

	wantvschema = &vschemapb.Keyspace{
		Tables: map[string]*vschemapb.Table{
			"lkp": {},
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

	env, _ := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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

		outms, _, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, tcase.specs, false)
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

	env, _ := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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
		if err := env.topoServ.SaveVSchema(context.Background(), ms.TargetKeyspace, &vschemapb.Keyspace{}); err != nil {
			t.Fatal(err)
		}
		if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, tcase.sourceVSchema); err != nil {
			t.Fatal(err)
		}

		_, got, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs, false)
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

	env, _ := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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
		err: "a conflicting vindex named xxhash already exists in the target vschema",
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

		_, _, got, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs, false)
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

	env, _ := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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
	if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, vschema); err != nil {
		t.Fatal(err)
	}

	_, got, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs, false)
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

	env, _ := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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
	if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, vschema); err != nil {
		t.Fatal(err)
	}

	_, got, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs, false)
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

	env, _ := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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
	if err := env.topoServ.SaveVSchema(context.Background(), ms.SourceKeyspace, vschema); err != nil {
		t.Fatal(err)
	}

	ms, ks, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs, false)
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

	env, _ := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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

	ms1, _, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs, false)
	require.NoError(t, err)
	require.Equal(t, ms1.StopAfterCopy, true)

	ms2, _, _, err := env.wr.prepareCreateLookup(context.Background(), ms.SourceKeyspace, specs, true)
	require.NoError(t, err)
	require.Equal(t, ms2.StopAfterCopy, false)
}

func TestCreateLookupVindexFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topoServ := memorytopo.NewServer(ctx, "cell")
	wr := New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), topoServ, nil)

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
					Type: "xxhash",
				},
				"v2": {
					Type: "xxhash",
				},
			},
		},
		err: "only one vindex must be specified in the specs",
	}, {
		description: "not a lookup",
		input: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"v": {
					Type: "xxhash",
				},
			},
		},
		err: "vindex xxhash is not a lookup type",
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
		err: "vindex table name must be in the form <keyspace>.<table>",
	}, {
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
	}, {
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
	}, {
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
						"to":    "c2",
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
		err := wr.CreateLookupVindex(context.Background(), "sourceks", tcase.input, "", "", false)
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

	env, _ := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

	sourceVSchema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"xxhash": {
				Type: "xxhash",
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
					Name:   "xxhash",
					Column: "col1",
				}, {
					Name:   "owned",
					Column: "col2",
				}},
			},
		},
	}
	fields := sqltypes.MakeTestFields(
		"id|state|message|source",
		"int64|varbinary|varbinary|blob",
	)
	sourceStopAfterCopy := `keyspace:"sourceKs",shard:"0",filter:{rules:{match:"owned" filter:"select * from t1 where in_keyrange(col1, 'sourceKs.hash', '-80')"}} stop_after_copy:true`
	sourceKeepRunningAfterCopy := `keyspace:"sourceKs",shard:"0",filter:{rules:{match:"owned" filter:"select * from t1 where in_keyrange(col1, 'sourceKs.hash', '-80')"}}`
	running := sqltypes.MakeTestResult(fields, "1|Running|msg|"+sourceKeepRunningAfterCopy)
	stopped := sqltypes.MakeTestResult(fields, "1|Stopped|Stopped after copy|"+sourceStopAfterCopy)
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
		err:   "vindex table name must be in the form <keyspace>.<table>. Got: unqualified",
	}, {
		input:        "sourceks.owned",
		vrResponse:   running,
		expectDelete: true,
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
			validationQuery := "select id, state, message, source from _vt.vreplication where workflow='lkp_vdx' and db_name='vt_targetks'"
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
		Cell: "zone1",
		TabletTypes: topoproto.MakeStringTypeCSV([]topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_RDONLY,
		}),
	}

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"-80", "80-"}, []string{"0"})

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

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

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

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"-40", "40-"}, []string{"-80", "80-"})

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
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

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

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

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

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

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

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

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

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix+`.*stop_after_copy:true`, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

	vs := &vschemapb.Keyspace{
		Sharded: true,
	}

	if err := env.topoServ.SaveVSchema(context.Background(), "targetks", vs); err != nil {
		t.Fatal(err)
	}
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(210, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(ctx, ms)
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
	env.wr = New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), env.topoServ, env.tmc)

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
	err := env.wr.Materialize(ctx, ms)
	require.EqualError(t, err, "source shard must have a primary for copying schema: 0")
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

	delete(env.tmc.schema, "targetks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

	delete(env.tmc.schema, "targetks.t1")
	delete(env.tmc.schema, "sourceks.t1")

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})

	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

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
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

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
	err := env.wr.Materialize(ctx, ms)
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

	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"-80", "80-"})

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
	err := env.wr.Materialize(ctx, ms)
	require.EqualError(t, err, "could not find vindex column c1")
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

func TestMaterializerSourceShardSelection(t *testing.T) {
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

	getStreamInsert := func(sourceShard, sourceColumn, targetVindex, targetShard string) string {
		return fmt.Sprintf(`.*shard:\\"%s\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1 where in_keyrange\(%s.*targetks\.%s.*%s.*`, sourceShard, sourceColumn, targetVindex, targetShard)
	}

	targetVSchema := &vschemapb.Keyspace{
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

	type testcase struct {
		name                         string
		targetShards, sourceShards   []string
		sourceColumn                 string
		targetVindex                 string
		insertMap                    map[string][]string
		targetVSchema, sourceVSchema *vschemapb.Keyspace
		getStreamInsert              func(sourceShard, sourceColumn, targetVindexName, targetShard string) string
	}
	testcases := []testcase{
		{
			targetShards:    []string{"-40", "40-80", "80-c0", "c0-"},
			sourceShards:    []string{"-80", "80-"},
			sourceColumn:    "c1",
			targetVindex:    "xxhash",
			insertMap:       map[string][]string{"-40": {"-80"}, "40-80": {"-80"}, "80-c0": {"80-"}, "c0-": {"80-"}},
			targetVSchema:   targetVSchema,
			getStreamInsert: getStreamInsert,
		},
		{
			targetShards:    []string{"-20", "20-40", "40-a0", "a0-f0", "f0-"},
			sourceShards:    []string{"-40", "40-80", "80-c0", "c0-"},
			sourceColumn:    "c1",
			targetVindex:    "xxhash",
			insertMap:       map[string][]string{"-20": {"-40"}, "20-40": {"-40"}, "40-a0": {"40-80", "80-c0"}, "a0-f0": {"80-c0", "c0-"}, "f0-": {"c0-"}},
			targetVSchema:   targetVSchema,
			getStreamInsert: getStreamInsert,
		},
		{
			targetShards:    []string{"-40", "40-80", "80-"},
			sourceShards:    []string{"-80", "80-"},
			sourceColumn:    "c1",
			targetVindex:    "xxhash",
			insertMap:       map[string][]string{"-40": {"-80"}, "40-80": {"-80"}, "80-": {"80-"}},
			targetVSchema:   targetVSchema,
			getStreamInsert: getStreamInsert,
		},
		{
			targetShards:    []string{"-80", "80-"},
			sourceShards:    []string{"-40", "40-80", "80-c0", "c0-"},
			sourceColumn:    "c1",
			targetVindex:    "xxhash",
			insertMap:       map[string][]string{"-80": {"-40", "40-80"}, "80-": {"80-c0", "c0-"}},
			targetVSchema:   targetVSchema,
			getStreamInsert: getStreamInsert,
		},
		{
			targetShards:    []string{"0"},
			sourceShards:    []string{"-80", "80-"},
			sourceColumn:    "c1",
			targetVindex:    "xxhash",
			insertMap:       map[string][]string{"0": {"-80", "80-"}},
			targetVSchema:   targetVSchema,
			getStreamInsert: getStreamInsert,
		},
		{
			targetShards:    []string{"-80", "80-"},
			sourceShards:    []string{"0"},
			sourceColumn:    "c1",
			targetVindex:    "xxhash",
			insertMap:       map[string][]string{"-80": {"0"}, "80-": {"0"}},
			targetVSchema:   targetVSchema,
			getStreamInsert: getStreamInsert,
		},
		{
			name:         "different primary vindex type, use all source shards",
			targetShards: []string{"-80", "80-"},
			sourceShards: []string{"-40", "40-80", "80-c0", "c0-"},
			sourceColumn: "c1",
			targetVindex: "hash",
			insertMap: map[string][]string{
				"-80": {"-40", "40-80", "80-c0", "c0-"},
				"80-": {"-40", "40-80", "80-c0", "c0-"},
			},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "xxhash",
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
			},
			getStreamInsert: getStreamInsert,
		},
		{
			name:         "different vindex type and name, use all source shards",
			targetShards: []string{"-80", "80-"},
			sourceShards: []string{"-40", "40-80", "80-c0", "c0-"},
			sourceColumn: "c1",
			targetVindex: "xxhash",
			insertMap: map[string][]string{
				"-80": {"-40", "40-80", "80-c0", "c0-"},
				"80-": {"-40", "40-80", "80-c0", "c0-"},
			},
			targetVSchema: &vschemapb.Keyspace{
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
			},
			getStreamInsert: getStreamInsert,
		},
		{
			name:         "same vindex type but different name, use intersecting source shards",
			targetShards: []string{"-80", "80-"},
			sourceShards: []string{"-40", "40-80", "80-c0", "c0-"},
			sourceColumn: "c1",
			targetVindex: "hash",
			insertMap:    map[string][]string{"-80": {"-40", "40-80"}, "80-": {"80-c0", "c0-"}},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash_vdx": {
						Type: "hash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Column: "c1",
							Name:   "hash_vdx",
						}},
					},
				},
			},
			getStreamInsert: getStreamInsert,
		},
		{
			name:         "unsharded source, sharded target, use all source shards",
			targetShards: []string{"-80", "80-"},
			sourceShards: []string{"-"},
			targetVindex: "xxhash",
			insertMap: map[string][]string{
				"-80": {"-"},
				"80-": {"-"},
			},
			sourceVSchema: &vschemapb.Keyspace{
				Sharded: false,
			},
			targetVSchema:   targetVSchema,
			getStreamInsert: getStreamInsert,
		},
		{
			name:         "sharded source, unsharded target, use all source shards",
			targetShards: []string{"-"},
			sourceShards: []string{"-80", "80-"},
			insertMap: map[string][]string{
				"-": {"-80", "80-"},
			},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: false,
			},
			// The single target shard streams all data from each source shard
			// without any keyrange filtering.
			getStreamInsert: func(sourceShard, _, _, targetShard string) string {
				return fmt.Sprintf(`.*shard:\\"%s\\" filter:{rules:{match:\\"t1\\" filter:\\"select.*t1`, sourceShard)
			},
		},
		{
			name:         "target secondary vindexes, use intersecting source shards",
			targetShards: []string{"-80", "80-"},
			sourceShards: []string{"-40", "40-80", "80-c0", "c0-"},
			sourceColumn: "c1",
			targetVindex: "hash",
			insertMap:    map[string][]string{"-80": {"-40", "40-80"}, "80-": {"80-c0", "c0-"}},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"lookup_vdx": {
						Type: "lookup",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "c1",
								Name:   "hash",
							},
							{
								Column: "c2",
								Name:   "lookup_vdx",
							},
						},
					},
				},
			},
			getStreamInsert: getStreamInsert,
		},
		{
			name:         "same vindex type but different cols, use all source shards",
			targetShards: []string{"-80", "80-"},
			sourceShards: []string{"-40", "40-80", "80-c0", "c0-"},
			sourceColumn: "c2",
			targetVindex: "hash",
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
							Column: "c1",
							Name:   "hash",
						}},
					},
				},
			},
			targetVSchema: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Column: "c2",
							Name:   "hash",
						}},
					},
				},
			},
			getStreamInsert: getStreamInsert,
		},
	}

	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			env, ctx := newTestMaterializerEnv(t, ms, tcase.sourceShards, tcase.targetShards)
			if err := env.topoServ.SaveVSchema(ctx, "targetks", tcase.targetVSchema); err != nil {
				t.Fatal(err)
			}
			if tcase.sourceVSchema != nil {
				if err := env.topoServ.SaveVSchema(context.Background(), "sourceks", tcase.sourceVSchema); err != nil {
					t.Fatal(err)
				}
			}

			for i, targetShard := range tcase.targetShards {
				tabletID := 200 + i*10
				env.tmc.expectVRQuery(tabletID, mzSelectFrozenQuery, &sqltypes.Result{})

				streamsInsert := ""
				sourceShards := tcase.insertMap[targetShard]
				for _, sourceShard := range sourceShards {
					streamsInsert += tcase.getStreamInsert(sourceShard, tcase.sourceColumn, tcase.targetVindex, targetShard)
				}
				env.tmc.expectVRQuery(
					tabletID,
					insertPrefix+streamsInsert,
					&sqltypes.Result{},
				)
				env.tmc.expectVRQuery(tabletID, mzUpdateQuery, &sqltypes.Result{})
			}
			err := env.wr.Materialize(ctx, ms)
			require.NoError(t, err)
			env.tmc.verifyQueries(t)
		})
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
			env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
			ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			env.tmc.expectVRQuery(100, mzCheckJournal, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
			if onDDLAction == binlogdatapb.OnDDLAction_IGNORE.String() {
				// This is the default and go does not marshal defaults
				// for prototext fields so we use the default insert stmt.
				env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
			} else {
				env.tmc.expectVRQuery(200, fmt.Sprintf(`/insert into _vt.vreplication\(.*on_ddl:%s.*`, onDDLAction),
					&sqltypes.Result{})
			}
			env.tmc.expectVRQuery(200, mzSelectIDQuery, &sqltypes.Result{})
			env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})

			err := env.wr.MoveTables(ctx, "workflow", "sourceks", "targetks", "t1", "",
				"", false, "", false, true, "", false, false, "", onDDLAction, nil, false, false)
			require.NoError(t, err)
		})
	}
}

func TestAddTablesToVSchema(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ts := memorytopo.NewServer(ctx, "zone1")
	srcks := "source"
	wr := &Wrangler{
		logger:   logutil.NewMemoryLogger(),
		ts:       ts,
		sourceTs: ts,
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
								Name:   "xxhash",
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
			name: "target vschema; copy source vschema",
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
					"t4": {
						ColumnVindexes: []*vschemapb.ColumnVindex{ // Should be stripped on target
							{
								Column: "c1",
								Name:   "xxhash",
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
			ts.SaveVSchema(ctx, srcks, tt.sourceVSchema)
			err := wr.addTablesToVSchema(ctx, srcks, tt.inTargetVSchema, tt.tables, tt.copyVSchema)
			require.NoError(t, err)
			require.Equal(t, tt.wantTargetVSchema, tt.inTargetVSchema)
		})
	}
}

// TestKeyRangesEqualOptimization tests that we optimize the source
// filtering when there's only one source shard for the stream and
// its keyrange is equal to the target shard for the stream. This
// means that even if the target keyspace is sharded, the source
// does not need to perform the in_keyrange filtering.
func TestKeyRangesEqualOptimization(t *testing.T) {
	workflow := "testwf"
	sourceKs := "sourceks"
	targetKs := "targetks"
	table := "t1"
	mzi := vtctldatapb.MaterializationIntent_MOVETABLES
	tableMaterializeSettings := []*vtctldatapb.TableMaterializeSettings{
		{
			TargetTable:      table,
			SourceExpression: fmt.Sprintf("select * from %s", table),
		},
	}
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
		name         string
		ms           *vtctldatapb.MaterializeSettings
		sourceShards []string
		targetShards []string
		wantBls      map[string]*binlogdatapb.BinlogSource
	}{
		{
			name: "no in_keyrange filter -- partial, one equal shard",
			ms: &vtctldatapb.MaterializeSettings{
				MaterializationIntent: mzi,
				Workflow:              workflow,
				TargetKeyspace:        targetKs,
				SourceKeyspace:        sourceKs,
				Cell:                  "cell",
				SourceShards:          []string{"-80"}, // Partial MoveTables just for this shard
				TableSettings:         tableMaterializeSettings,
			},
			sourceShards: []string{"-80", "80-"},
			targetShards: []string{"-80", "80-"},
			wantBls: map[string]*binlogdatapb.BinlogSource{
				"-80": {
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
		{
			name: "in_keyrange filter -- unequal shards",
			ms: &vtctldatapb.MaterializeSettings{
				MaterializationIntent: mzi,
				Workflow:              workflow,
				TargetKeyspace:        targetKs,
				SourceKeyspace:        sourceKs,
				Cell:                  "cell",
				TableSettings:         tableMaterializeSettings,
			},
			sourceShards: []string{"-"},
			targetShards: []string{"-80", "80-"},
			wantBls: map[string]*binlogdatapb.BinlogSource{
				"-80": {
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
				"80-": {
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
		{
			name: "in_keyrange filter -- unequal shards on merge",
			ms: &vtctldatapb.MaterializeSettings{
				MaterializationIntent: mzi,
				Workflow:              workflow,
				TargetKeyspace:        targetKs,
				SourceKeyspace:        sourceKs,
				Cell:                  "cell",
				TableSettings:         tableMaterializeSettings,
			},
			sourceShards: []string{"-80", "80-"},
			targetShards: []string{"-"},
			wantBls: map[string]*binlogdatapb.BinlogSource{
				"-": {
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
			},
		},
		{
			name: "no in_keyrange filter -- all equal shards",
			ms: &vtctldatapb.MaterializeSettings{
				MaterializationIntent: mzi,
				Workflow:              workflow,
				TargetKeyspace:        targetKs,
				SourceKeyspace:        sourceKs,
				Cell:                  "cell",
				TableSettings:         tableMaterializeSettings,
			},
			sourceShards: []string{"-80", "80-"},
			targetShards: []string{"-80", "80-"},
			wantBls: map[string]*binlogdatapb.BinlogSource{
				"-80": {
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
				"80-": {
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env, ctx := newTestMaterializerEnv(t, tc.ms, tc.sourceShards, tc.targetShards)
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			// Target is always sharded.
			err := env.wr.ts.SaveVSchema(ctx, targetKs, targetVSchema)
			require.NoError(t, err, "SaveVSchema failed: %v", err)

			for _, tablet := range env.tablets {
				// Queries will only be executed on primary tablets in the target keyspace.
				if tablet.Keyspace != targetKs || tablet.Type != topodatapb.TabletType_PRIMARY {
					continue
				}
				env.tmc.expectVRQuery(int(tablet.Alias.Uid), mzSelectFrozenQuery, &sqltypes.Result{})
				// If we are doing a partial MoveTables, we will only perform the workflow
				// stream creation / INSERT statment on the shard(s) we're migrating.
				if len(tc.ms.SourceShards) > 0 && !slices.Contains(tc.ms.SourceShards, tablet.Shard) {
					continue
				}
				bls := tc.wantBls[tablet.Shard]
				require.NotNil(t, bls, "no binlog source defined for tablet %+v", tablet)
				if bls.Filter != nil {
					for i, rule := range bls.Filter.Rules {
						// It's escaped in the SQL statement.
						bls.Filter.Rules[i].Filter = strings.ReplaceAll(rule.Filter, `'`, `\'`)
					}
				}
				blsBytes, err := prototext.Marshal(bls)
				require.NoError(t, err, "failed to marshal binlog source: %v", err)
				// This is also escaped in the SQL statement.
				blsStr := strings.ReplaceAll(string(blsBytes), `"`, `\"`)
				// Escape the string for the regexp comparison.
				blsStr = regexp.QuoteMeta(blsStr)
				// For some reason we end up with an extra slash added by QuoteMeta for the
				// escaped single quotes in the filter.
				blsStr = strings.ReplaceAll(blsStr, `\\\\`, `\\\`)
				expectedQuery := fmt.Sprintf(`/insert into _vt.vreplication.* values \('%s', '%s'`, workflow, blsStr)
				env.tmc.expectVRQuery(int(tablet.Alias.Uid), expectedQuery, &sqltypes.Result{})
			}

			_, err = env.wr.prepareMaterializerStreams(ctx, tc.ms)
			require.NoError(t, err, "prepareMaterializerStreams failed: %v", err)
		})
	}
}
