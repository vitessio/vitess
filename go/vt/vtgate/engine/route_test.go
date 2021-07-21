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

package engine

import (
	"errors"
	"testing"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var defaultSelectResult = sqltypes.MakeTestResult(
	sqltypes.MakeTestFields(
		"id",
		"int64",
	),
	"1",
)

func TestSelectUnsharded(t *testing.T) {
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{
		shards:  []string{"0"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.0: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.0: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectInformationSchemaWithTableAndSchemaWithRoutedTables(t *testing.T) {
	stringToExpr := func(in string) evalengine.Expr {
		var schema evalengine.Expr
		if in != "" {
			schema = evalengine.NewLiteralString([]byte(in))
		}
		return schema
	}

	type testCase struct {
		tableSchema, tableName, testName string
		expectedLog                      []string
		routed                           bool
	}
	tests := []testCase{{
		testName:    "both schema and table predicates - routed table",
		tableSchema: "schema",
		tableName:   "table",
		routed:      true,
		expectedLog: []string{
			"FindTable(`schema`.`table`)",
			"ResolveDestinations routedKeyspace [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard routedKeyspace.1: dummy_select {__replacevtschemaname: type:INT64 value:\"1\" __vttablename: type:VARBINARY value:\"routedTable\" } false false"},
	}, {
		testName:    "both schema and table predicates - not routed",
		tableSchema: "schema",
		tableName:   "table",
		routed:      false,
		expectedLog: []string{
			"FindTable(`schema`.`table`)",
			"ResolveDestinations schema [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard schema.1: dummy_select {__replacevtschemaname: type:INT64 value:\"1\" __vttablename: type:VARBINARY value:\"table\" } false false"},
	}, {
		testName:  "table name predicate - routed table",
		tableName: "tableName",
		routed:    true,
		expectedLog: []string{
			"FindTable(tableName)",
			"ResolveDestinations routedKeyspace [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard routedKeyspace.1: dummy_select {__vttablename: type:VARBINARY value:\"routedTable\" } false false"},
	}, {
		testName:  "table name predicate - not routed",
		tableName: "tableName",
		routed:    false,
		expectedLog: []string{
			"FindTable(tableName)",
			"ResolveDestinations ks [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard ks.1: dummy_select {__vttablename: type:VARBINARY value:\"tableName\" } false false"},
	}, {
		testName:    "schema predicate",
		tableSchema: "myKeyspace",
		expectedLog: []string{
			"ResolveDestinations myKeyspace [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard myKeyspace.1: dummy_select {__replacevtschemaname: type:INT64 value:\"1\" } false false"},
	}, {
		testName: "no predicates",
		expectedLog: []string{
			"ResolveDestinations ks [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard ks.1: dummy_select {} false false"},
	}}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			sel := &Route{
				Opcode: SelectDBA,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: false,
				},
				Query:               "dummy_select",
				FieldQuery:          "dummy_select_field",
				SysTableTableSchema: stringToExpr(tc.tableSchema),
				SysTableTableName:   stringToExpr(tc.tableName),
			}
			vc := &loggingVCursor{
				shards:  []string{"1"},
				results: []*sqltypes.Result{defaultSelectResult},
			}
			if tc.routed {
				vc.tableRoutes = tableRoutes{
					tbl: &vindexes.Table{
						Name:     sqlparser.NewTableIdent("routedTable"),
						Keyspace: &vindexes.Keyspace{Name: "routedKeyspace"},
					}}
			}
			_, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			vc.ExpectLog(t, tc.expectedLog)
		})
	}
}

func TestSelectScatter(t *testing.T) {
	sel := NewRoute(
		SelectScatter,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectEqualUnique(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	sel := NewRoute(
		SelectEqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectNone(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	sel := NewRoute(
		SelectNone,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = nil

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	require.Empty(t, vc.log)
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	require.Empty(t, vc.log)
	expectResult(t, "sel.StreamExecute", result, nil)
}

func TestSelectEqualUniqueScatter(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	sel := NewRoute(
		SelectEqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyRange(-)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyRange(-)`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectEqual(t *testing.T) {
	vindex, _ := vindexes.NewLookup("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := NewRoute(
		SelectEqual,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}}

	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"fromc|toc",
					"int64|varbinary",
				),
				"1|\x00",
				"1|\x80",
			),
			defaultSelectResult,
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" >  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyspaceIDs(00,80)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" >  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyspaceIDs(00,80)`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectEqualNoRoute(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := NewRoute(
		SelectEqual,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" >  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationNone()`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" >  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationNone()`,
	})
	expectResult(t, "sel.StreamExecute", result, nil)
}

func TestSelectINUnique(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	sel := NewRoute(
		SelectIN,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{
		Values: []sqltypes.PlanValue{{
			Value: sqltypes.NewInt64(1),
		}, {
			Value: sqltypes.NewInt64(2),
		}, {
			Value: sqltypes.NewInt64(4),
		}},
	}}

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "-20", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"  type:INT64 value:"2"  type:INT64 value:"4" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_select {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"2" > } ` +
			`ks.20-: dummy_select {__vals: type:TUPLE values:<type:INT64 value:"4" > } ` +
			`false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"  type:INT64 value:"2"  type:INT64 value:"4" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`StreamExecuteMulti dummy_select ks.-20: {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"2" > } ks.20-: {__vals: type:TUPLE values:<type:INT64 value:"4" > } `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectINNonUnique(t *testing.T) {
	vindex, _ := vindexes.NewLookup("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := NewRoute(
		SelectIN,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{
		Values: []sqltypes.PlanValue{{
			Value: sqltypes.NewInt64(1),
		}, {
			Value: sqltypes.NewInt64(2),
		}, {
			Value: sqltypes.NewInt64(4),
		}},
	}}

	fields := sqltypes.MakeTestFields(
		"fromc|toc",
		"int64|varbinary",
	)
	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
		results: []*sqltypes.Result{
			// 1 will be sent to both shards.
			// 2 will go to -20.
			// 4 will go to 20-.
			sqltypes.MakeTestResult(
				fields,
				"1|\x00",
				"1|\x80",
				"2|\x00",
				"4|\x80",
			),
			defaultSelectResult,
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"2" > values:<type:INT64 value:"4" >  false`,
		`ResolveDestinations ks [type:INT64 value:"1"  type:INT64 value:"2"  type:INT64 value:"4" ] Destinations:DestinationKeyspaceIDs(00,80),DestinationKeyspaceIDs(00),DestinationKeyspaceIDs(80)`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_select {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"2" > } ` +
			`ks.20-: dummy_select {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"4" > } ` +
			`false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"2" > values:<type:INT64 value:"4" >  false`,
		`ResolveDestinations ks [type:INT64 value:"1"  type:INT64 value:"2"  type:INT64 value:"4" ] Destinations:DestinationKeyspaceIDs(00,80),DestinationKeyspaceIDs(00),DestinationKeyspaceIDs(80)`,
		`StreamExecuteMulti dummy_select ks.-20: {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"2" > } ks.20-: {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"4" > } `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectMultiEqual(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	sel := NewRoute(
		SelectMultiEqual,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{
		Values: []sqltypes.PlanValue{{
			Value: sqltypes.NewInt64(1),
		}, {
			Value: sqltypes.NewInt64(2),
		}, {
			Value: sqltypes.NewInt64(4),
		}},
	}}

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "-20", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"  type:INT64 value:"2"  type:INT64 value:"4" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"  type:INT64 value:"2"  type:INT64 value:"4" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectNext(t *testing.T) {
	sel := NewRoute(
		SelectNext,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectDBA(t *testing.T) {
	sel := NewRoute(
		SelectDBA,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectReference(t *testing.T) {
	sel := NewRoute(
		SelectReference,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestRouteGetFields(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := NewRoute(
		SelectEqual,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" >  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select_field {} false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" >  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select_field {} false false`,
	})
	expectResult(t, "sel.StreamExecute", result, &sqltypes.Result{})
}

func TestRouteSort(t *testing.T) {
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.OrderBy = []OrderbyParams{{
		Col:             0,
		WeightStringCol: -1,
	}}

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"int64",
				),
				"1",
				"1",
				"3",
				"2",
			),
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.0: dummy_select {} false false`,
	})
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		"1",
		"1",
		"2",
		"3",
	)
	expectResult(t, "sel.Execute", result, wantResult)

	sel.OrderBy[0].Desc = true
	vc.Rewind()
	result, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	wantResult = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		"3",
		"2",
		"1",
		"1",
	)
	expectResult(t, "sel.Execute", result, wantResult)

	vc = &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id",
					"varchar",
				),
				"1",
				"2",
				"3",
			),
		},
	}
	_, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `types are not comparable: VARCHAR vs VARCHAR`)
}

func TestRouteSortWeightStrings(t *testing.T) {
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.OrderBy = []OrderbyParams{{
		Col:             1,
		WeightStringCol: 0,
	}}

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"weightString|normal",
					"varbinary|varchar",
				),
				"v|x",
				"g|d",
				"a|a",
				"c|t",
				"f|p",
			),
		},
	}

	var result *sqltypes.Result
	var wantResult *sqltypes.Result
	var err error
	t.Run("Sort using Weight Strings", func(t *testing.T) {
		result, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
			`ExecuteMultiShard ks.0: dummy_select {} false false`,
		})
		wantResult = sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"weightString|normal",
				"varbinary|varchar",
			),
			"a|a",
			"c|t",
			"f|p",
			"g|d",
			"v|x",
		)
		expectResult(t, "sel.Execute", result, wantResult)
	})

	t.Run("Descending ordering using weighted strings", func(t *testing.T) {
		sel.OrderBy[0].Desc = true
		vc.Rewind()
		result, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		wantResult = sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"weightString|normal",
				"varbinary|varchar",
			),
			"v|x",
			"g|d",
			"f|p",
			"c|t",
			"a|a",
		)
		expectResult(t, "sel.Execute", result, wantResult)
	})

	t.Run("Error when no weight string set", func(t *testing.T) {
		sel.OrderBy = []OrderbyParams{{
			Col:             1,
			WeightStringCol: -1,
		}}

		vc = &loggingVCursor{
			shards: []string{"0"},
			results: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"weightString|normal",
						"varbinary|varchar",
					),
					"v|x",
					"g|d",
					"a|a",
					"c|t",
					"f|p",
				),
			},
		}
		_, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
		require.EqualError(t, err, `types are not comparable: VARCHAR vs VARCHAR`)
	})
}

func TestRouteSortTruncate(t *testing.T) {
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.OrderBy = []OrderbyParams{{
		Col: 0,
	}}
	sel.TruncateColumnCount = 1

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|col",
					"int64|int64",
				),
				"1|1",
				"1|1",
				"3|1",
				"2|1",
			),
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.0: dummy_select {} false false`,
	})
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		"1",
		"1",
		"2",
		"3",
	)
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestRouteStreamTruncate(t *testing.T) {
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.TruncateColumnCount = 1

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|col",
					"int64|int64",
				),
				"1|1",
				"2|1",
			),
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.0: dummy_select {} false false`,
	})
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		"1",
		"2",
	)
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestRouteStreamSortTruncate(t *testing.T) {
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.OrderBy = []OrderbyParams{{
		Col: 0,
	}}
	sel.TruncateColumnCount = 1

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"id|col",
					"int64|int64",
				),
				"1|1",
				"2|1",
			),
		},
	}
	result, err := wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.0: {} `,
	})

	// We're not really testing sort functionality here because that part is tested
	// in merge_sort_test. We're only testing that truncation happens even if sort
	// is enabled, which is a different code path from unsorted truncate.
	wantResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id",
			"int64",
		),
		"1",
		"2",
	)
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestParamsFail(t *testing.T) {
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{shardErr: errors.New("shard error")}
	_, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `shard error`)

	vc.Rewind()
	_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `shard error`)
}

func TestExecFail(t *testing.T) {
	// Unsharded error
	sel := NewRoute(
		SelectUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{shards: []string{"0"}, resultErr: vterrors.NewErrorf(vtrpcpb.Code_CANCELED, vterrors.QueryInterrupted, "query timeout")}
	_, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `query timeout`)
	vc.ExpectWarnings(t, nil)

	vc.Rewind()
	_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `query timeout`)

	// Scatter fails if one of N fails without ScatterErrorsAsWarnings
	sel = NewRoute(
		SelectScatter,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc = &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
		multiShardErrs: []error{
			errors.New("result error -20"),
		},
	}
	_, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `result error -20`)
	vc.ExpectWarnings(t, nil)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})

	vc.Rewind()
	// Scatter succeeds if all shards fail with ScatterErrorsAsWarnings
	sel = NewRoute(
		SelectScatter,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.ScatterErrorsAsWarnings = true

	vc = &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
		multiShardErrs: []error{
			mysql.NewSQLError(mysql.ERQueryInterrupted, "", "query timeout -20"),
			errors.New("not a sql error 20-"),
		},
	}
	_, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err, "unexpected ScatterErrorsAsWarnings error %v", err)

	// Ensure that the error code is preserved from SQLErrors and that it
	// turns into ERUnknownError for all others
	vc.ExpectWarnings(t, []*querypb.QueryWarning{
		{Code: mysql.ERQueryInterrupted, Message: "query timeout -20 (errno 1317) (sqlstate HY000)"},
		{Code: mysql.ERUnknownError, Message: "not a sql error 20-"},
	})
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})

	vc.Rewind()

	// Scatter succeeds if one of N fails with ScatterErrorsAsWarnings
	sel = NewRoute(
		SelectScatter,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.ScatterErrorsAsWarnings = true

	vc = &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
		multiShardErrs: []error{
			errors.New("result error -20"),
			nil,
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err, "unexpected ScatterErrorsAsWarnings error %v", err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
}
