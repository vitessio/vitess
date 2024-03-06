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
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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
		Unsharded,
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select ks.0: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestInformationSchemaWithTableAndSchemaWithRoutedTables(t *testing.T) {
	stringListToExprList := func(in []string) []evalengine.Expr {
		var schema []evalengine.Expr
		for _, s := range in {
			schema = append(schema, evalengine.NewLiteralString([]byte(s), collations.SystemCollation))
		}
		return schema
	}

	type testCase struct {
		tableSchema []string
		tableName   map[string]evalengine.Expr
		testName    string
		expectedLog []string
		routed      bool
	}
	tests := []testCase{{
		testName:    "both schema and table predicates - routed table",
		tableSchema: []string{"schema"},
		tableName:   map[string]evalengine.Expr{"table_name": evalengine.NewLiteralString([]byte("table"), collations.SystemCollation)},
		routed:      true,
		expectedLog: []string{
			"FindTable(`schema`.`table`)",
			"ResolveDestinations routedKeyspace [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard routedKeyspace.1: dummy_select {__replacevtschemaname: type:INT64 value:\"1\" table_name: type:VARCHAR value:\"routedTable\"} false false"},
	}, {
		testName:    "both schema and table predicates - not routed",
		tableSchema: []string{"schema"},
		tableName:   map[string]evalengine.Expr{"table_name": evalengine.NewLiteralString([]byte("table"), collations.SystemCollation)},
		routed:      false,
		expectedLog: []string{
			"FindTable(`schema`.`table`)",
			"ResolveDestinations schema [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard schema.1: dummy_select {__replacevtschemaname: type:INT64 value:\"1\" table_name: type:VARCHAR value:\"table\"} false false"},
	}, {
		testName:    "multiple schema and table predicates",
		tableSchema: []string{"schema", "schema", "schema"},
		tableName:   map[string]evalengine.Expr{"t1": evalengine.NewLiteralString([]byte("table"), collations.SystemCollation), "t2": evalengine.NewLiteralString([]byte("table"), collations.SystemCollation), "t3": evalengine.NewLiteralString([]byte("table"), collations.SystemCollation)},
		routed:      false,
		expectedLog: []string{
			"FindTable(`schema`.`table`)",
			"FindTable(`schema`.`table`)",
			"FindTable(`schema`.`table`)",
			"ResolveDestinations schema [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard schema.1: dummy_select {__replacevtschemaname: type:INT64 value:\"1\" t1: type:VARCHAR value:\"table\" t2: type:VARCHAR value:\"table\" t3: type:VARCHAR value:\"table\"} false false"},
	}, {
		testName:  "table name predicate - routed table",
		tableName: map[string]evalengine.Expr{"table_name": evalengine.NewLiteralString([]byte("tableName"), collations.SystemCollation)},
		routed:    true,
		expectedLog: []string{
			"FindTable(tableName)",
			"ResolveDestinations routedKeyspace [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard routedKeyspace.1: dummy_select {table_name: type:VARCHAR value:\"routedTable\"} false false"},
	}, {
		testName:  "table name predicate - not routed",
		tableName: map[string]evalengine.Expr{"table_name": evalengine.NewLiteralString([]byte("tableName"), collations.SystemCollation)},
		routed:    false,
		expectedLog: []string{
			"FindTable(tableName)",
			"ResolveDestinations ks [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard ks.1: dummy_select {table_name: type:VARCHAR value:\"tableName\"} false false"},
	}, {
		testName:    "schema predicate",
		tableSchema: []string{"myKeyspace"},
		expectedLog: []string{
			"ResolveDestinations myKeyspace [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard myKeyspace.1: dummy_select {__replacevtschemaname: type:INT64 value:\"1\"} false false"},
	}, {
		testName:    "multiple schema predicates",
		tableSchema: []string{"myKeyspace", "myKeyspace", "myKeyspace", "myKeyspace"},
		expectedLog: []string{
			"ResolveDestinations myKeyspace [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard myKeyspace.1: dummy_select {__replacevtschemaname: type:INT64 value:\"1\"} false false"},
	}, {
		testName: "no predicates",
		expectedLog: []string{
			"ResolveDestinations ks [] Destinations:DestinationAnyShard()",
			"ExecuteMultiShard ks.1: dummy_select {} false false"},
	}}
	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			sel := &Route{
				RoutingParameters: &RoutingParameters{
					Opcode: DBA,
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: false,
					},
					SysTableTableSchema: stringListToExprList(tc.tableSchema),
					SysTableTableName:   tc.tableName,
				},
				Query:      "dummy_select",
				FieldQuery: "dummy_select_field",
			}
			vc := &loggingVCursor{
				shards:  []string{"1"},
				results: []*sqltypes.Result{defaultSelectResult},
			}
			if tc.routed {
				vc.tableRoutes = tableRoutes{
					tbl: &vindexes.Table{
						Name:     sqlparser.NewIdentifierCS("routedTable"),
						Keyspace: &vindexes.Keyspace{Name: "routedKeyspace"},
					}}
			}
			_, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
			require.NoError(t, err)
			vc.ExpectLog(t, tc.expectedLog)
		})
	}
}

func TestSelectScatter(t *testing.T) {
	sel := NewRoute(
		Scatter,
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestSelectEqualUnique(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	sel := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)

	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}
	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestSelectNone(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	sel := NewRoute(
		None,
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	require.Empty(t, vc.log)
	expectResult(t, result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	require.Empty(t, vc.log)
	require.Nil(t, result)

	vc.Rewind()

	// test with special no-routes handling
	sel.NoRoutesSpecialHandling = true
	result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, &sqltypes.Result{})
}

func TestSelectEqualUniqueScatter(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("lookup_unique", "", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	sel := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}
	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyRange(-)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyRange(-)`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestSelectEqual(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("lookup", "", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := NewRoute(
		Equal,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceIDs(00,80)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceIDs(00,80)`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestSelectEqualNoRoute(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("lookup_unique", "", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := NewRoute(
		Equal,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
	})
	expectResult(t, result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
	})
	require.Nil(t, result)

	// test with special no-routes handling
	sel.NoRoutesSpecialHandling = true
	vc.Rewind()

	result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, &sqltypes.Result{})
}

func TestINUnique(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	sel := NewRoute(
		IN,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.TupleExpr{
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(4),
		},
	}
	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "-20", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"4"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_select {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"}} ` +
			`ks.20-: dummy_select {__vals: type:TUPLE values:{type:INT64 value:"4"}} ` +
			`false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"4"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`StreamExecuteMulti dummy_select ks.-20: {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"}} ks.20-: {__vals: type:TUPLE values:{type:INT64 value:"4"}} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestINNonUnique(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("lookup", "", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := NewRoute(
		IN,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.TupleExpr{
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(4),
		},
	}

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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"4"} false`,
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"4"] Destinations:DestinationKeyspaceIDs(00,80),DestinationKeyspaceIDs(00),DestinationKeyspaceIDs(80)`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_select {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"}} ` +
			`ks.20-: dummy_select {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"4"}} ` +
			`false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"4"} false`,
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"4"] Destinations:DestinationKeyspaceIDs(00,80),DestinationKeyspaceIDs(00),DestinationKeyspaceIDs(80)`,
		`StreamExecuteMulti dummy_select ks.-20: {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"}} ks.20-: {__vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"4"}} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestMultiEqual(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	sel := NewRoute(
		MultiEqual,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.TupleExpr{
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(4),
		},
	}

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "-20", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"4"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" type:INT64 value:"2" type:INT64 value:"4"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestSelectLike(t *testing.T) {
	subshard, _ := vindexes.CreateVindex("cfc", "cfc", map[string]string{"hash": "md5", "offsets": "[1,2]"})
	vindex := subshard.(*vindexes.CFC).PrefixVindex()
	vc := &loggingVCursor{
		// we have shards '-0c80', '0c80-0d', '0d-40', '40-80', '80-'
		shards:  []string{"\x0c\x80", "\x0d", "\x40", "\x80"},
		results: []*sqltypes.Result{defaultSelectResult},
	}

	sel := NewRoute(
		Equal,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)

	sel.Vindex = vindex
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralString([]byte("a%"), collations.SystemCollation),
	}
	// md5("a") = 0cc175b9c0f1b6a831c399e269772661
	// keyspace id prefix for "a" is 0x0c
	vc.shardForKsid = []string{"-0c80", "0c80-0d"}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	// range 0c-0d hits 2 shards ks.-0c80 ks.0c80-0d;
	// note that 0c-0d should not hit ks.0d-40
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:VARCHAR value:"a%"] Destinations:DestinationKeyRange(0c-0d)`,
		`ExecuteMultiShard ks.-0c80: dummy_select {} ks.0c80-0d: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()

	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:VARCHAR value:"a%"] Destinations:DestinationKeyRange(0c-0d)`,
		`StreamExecuteMulti dummy_select ks.-0c80: {} ks.0c80-0d: {} `,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()

	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralString([]byte("ab%"), collations.SystemCollation),
	}
	// md5("b") = 92eb5ffee6ae2fec3ad71c777531578f
	// keyspace id prefix for "ab" is 0x0c92
	// adding one byte to the prefix just hit one shard
	vc.shardForKsid = []string{"0c80-0d"}
	result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:VARCHAR value:"ab%"] Destinations:DestinationKeyRange(0c92-0c93)`,
		`ExecuteMultiShard ks.0c80-0d: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()

	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:VARCHAR value:"ab%"] Destinations:DestinationKeyRange(0c92-0c93)`,
		`StreamExecuteMulti dummy_select ks.0c80-0d: {} `,
	})
	expectResult(t, result, defaultSelectResult)

}

func TestSelectNext(t *testing.T) {
	sel := NewRoute(
		Next,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{
		shards:  []string{"-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select ks.-: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestSelectDBA(t *testing.T) {
	sel := NewRoute(
		DBA,
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestSelectReference(t *testing.T) {
	sel := NewRoute(
		Reference,
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestRouteGetFields(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("lookup_unique", "", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := NewRoute(
		Equal,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex.(vindexes.SingleColumn)
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select_field {} false false`,
	})
	expectResult(t, result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select_field {} false false`,
	})
	expectResult(t, result, &sqltypes.Result{})
	vc.Rewind()

	// test with special no-routes handling
	sel.NoRoutesSpecialHandling = true
	result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, &sqltypes.Result{})
}

func TestRouteSort(t *testing.T) {
	sel := NewRoute(
		Unsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.OrderBy = []evalengine.OrderByParams{{
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
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
	expectResult(t, result, wantResult)

	sel.OrderBy[0].Desc = true
	vc.Rewind()
	result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
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
	expectResult(t, result, wantResult)

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
	_, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `cannot compare strings, collation is unknown or unsupported (collation ID: 0)`)
}

func TestRouteSortWeightStrings(t *testing.T) {
	sel := NewRoute(
		Unsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.OrderBy = []evalengine.OrderByParams{{
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
		result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
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
		expectResult(t, result, wantResult)
	})

	t.Run("Descending ordering using weighted strings", func(t *testing.T) {
		sel.OrderBy[0].Desc = true
		vc.Rewind()
		result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
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
		expectResult(t, result, wantResult)
	})

	t.Run("Error when no weight string set", func(t *testing.T) {
		sel.OrderBy = []evalengine.OrderByParams{{
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
		_, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.EqualError(t, err, `cannot compare strings, collation is unknown or unsupported (collation ID: 0)`)
	})
}

func TestRouteSortCollation(t *testing.T) {
	sel := NewRoute(
		Unsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)

	collationID, _ := collations.MySQL8().LookupID("utf8mb4_hu_0900_ai_ci")

	sel.OrderBy = []evalengine.OrderByParams{{
		Col:  0,
		Type: evalengine.NewType(sqltypes.VarChar, collationID),
	}}

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"normal",
					"varchar",
				),
				"c",
				"d",
				"cs",
				"cs",
				"c",
			),
		},
	}

	var result *sqltypes.Result
	var wantResult *sqltypes.Result
	var err error
	t.Run("Sort using Collation", func(t *testing.T) {
		result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: dummy_select {} false false`,
		})
		wantResult = sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"normal",
				"varchar",
			),
			"c",
			"c",
			"cs",
			"cs",
			"d",
		)
		expectResult(t, result, wantResult)
	})

	t.Run("Descending ordering using Collation", func(t *testing.T) {
		sel.OrderBy[0].Desc = true
		vc.Rewind()
		result, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err)
		wantResult = sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"normal",
				"varchar",
			),
			"d",
			"cs",
			"cs",
			"c",
			"c",
		)
		expectResult(t, result, wantResult)
	})

	t.Run("Error when Unknown Collation", func(t *testing.T) {
		sel.OrderBy = []evalengine.OrderByParams{{
			Col: 0,
		}}

		vc := &loggingVCursor{
			shards: []string{"0"},
			results: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"normal",
						"varchar",
					),
					"c",
					"d",
					"cs",
					"cs",
					"c",
				),
			},
		}
		_, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.EqualError(t, err, "cannot compare strings, collation is unknown or unsupported (collation ID: 0)")
	})

	t.Run("Error when Unsupported Collation", func(t *testing.T) {
		sel.OrderBy = []evalengine.OrderByParams{{
			Col:  0,
			Type: evalengine.NewType(sqltypes.Unknown, 1111),
		}}

		vc := &loggingVCursor{
			shards: []string{"0"},
			results: []*sqltypes.Result{
				sqltypes.MakeTestResult(
					sqltypes.MakeTestFields(
						"normal",
						"varchar",
					),
					"c",
					"d",
					"cs",
					"cs",
					"c",
				),
			},
		}
		_, err = sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.EqualError(t, err, "cannot compare strings, collation is unknown or unsupported (collation ID: 1111)")
	})
}

func TestRouteSortTruncate(t *testing.T) {
	sel := NewRoute(
		Unsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.OrderBy = []evalengine.OrderByParams{{
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
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
	expectResult(t, result, wantResult)
}

func TestRouteStreamTruncate(t *testing.T) {
	sel := NewRoute(
		Unsharded,
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
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
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
	expectResult(t, result, wantResult)
}

func TestRouteStreamSortTruncate(t *testing.T) {
	sel := NewRoute(
		Unsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.OrderBy = []evalengine.OrderByParams{{
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
	result, err := wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
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
	expectResult(t, result, wantResult)
}

func TestParamsFail(t *testing.T) {
	sel := NewRoute(
		Unsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_select",
		"dummy_select_field",
	)

	vc := &loggingVCursor{shardErr: errors.New("shard error")}
	_, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `shard error`)

	vc.Rewind()
	_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `shard error`)
}

func TestExecFail(t *testing.T) {

	t.Run("unsharded", func(t *testing.T) {
		// Unsharded error
		sel := NewRoute(
			Unsharded,
			&vindexes.Keyspace{
				Name:    "ks",
				Sharded: false,
			},
			"dummy_select",
			"dummy_select_field",
		)

		vc := &loggingVCursor{shards: []string{"0"}, resultErr: vterrors.NewErrorf(vtrpcpb.Code_CANCELED, vterrors.QueryInterrupted, "query timeout")}
		_, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.EqualError(t, err, `query timeout`)
		assert.Empty(t, vc.warnings)

		vc.Rewind()
		_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
		require.EqualError(t, err, `query timeout`)
	})

	t.Run("normal route with no scatter errors as warnings", func(t *testing.T) {
		// Scatter fails if one of N fails without ScatterErrorsAsWarnings
		sel := NewRoute(
			Scatter,
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
			multiShardErrs: []error{
				errors.New("result error -20"),
			},
		}
		_, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.EqualError(t, err, `result error -20`)
		vc.ExpectWarnings(t, nil)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
		})
	})

	t.Run("ScatterErrorsAsWarnings", func(t *testing.T) {
		// Scatter succeeds if one of N fails with ScatterErrorsAsWarnings
		sel := NewRoute(
			Scatter,
			&vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			"dummy_select",
			"dummy_select_field",
		)
		sel.ScatterErrorsAsWarnings = true

		vc := &loggingVCursor{
			shards:  []string{"-20", "20-"},
			results: []*sqltypes.Result{defaultSelectResult},
			multiShardErrs: []error{
				errors.New("result error -20"),
				nil,
			},
		}
		result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err, "unexpected ScatterErrorsAsWarnings error %v", err)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
		})
		expectResult(t, result, defaultSelectResult)

		vc.Rewind()
		vc.resultErr = sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, "", "query timeout -20")
		// test when there is order by column
		sel.OrderBy = []evalengine.OrderByParams{{
			WeightStringCol: -1,
			Col:             0,
		}}
		_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
		require.NoError(t, err, "unexpected ScatterErrorsAsWarnings error %v", err)
		vc.ExpectWarnings(t, []*querypb.QueryWarning{{Code: uint32(sqlerror.ERQueryInterrupted), Message: "query timeout -20 (errno 1317) (sqlstate HY000)"}})
	})
}

func TestSelectEqualUniqueMultiColumnVindex(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("region_experimental", "", map[string]string{"region_bytes": "1"})
	sel := NewRoute(
		EqualUnique,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
		evalengine.NewLiteralInt(2),
	}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestSelectEqualMultiColumnVindex(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("region_experimental", "", map[string]string{"region_bytes": "1"})
	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	sel := NewRoute(
		Equal,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex
	sel.Values = []evalengine.Expr{evalengine.NewLiteralInt(32)}

	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(32)]] Destinations:DestinationKeyRange(20-21)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(32)]] Destinations:DestinationKeyRange(20-21)`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestINMultiColumnVindex(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("region_experimental", "", map[string]string{"region_bytes": "1"})
	sel := NewRoute(
		IN,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex
	sel.Values = []evalengine.Expr{
		evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(2),
		),
		evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(3),
			evalengine.NewLiteralInt(4),
		),
	}

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "20-", "20-", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(3)] [INT64(1) INT64(4)] [INT64(2) INT64(3)] [INT64(2) INT64(4)]] Destinations:DestinationKeyspaceID(014eb190c9a2fa169c),DestinationKeyspaceID(01d2fd8867d50d2dfe),DestinationKeyspaceID(024eb190c9a2fa169c),DestinationKeyspaceID(02d2fd8867d50d2dfe)`,
		`ExecuteMultiShard ks.-20: dummy_select {__vals0: type:TUPLE values:{type:INT64 value:"1"} __vals1: type:TUPLE values:{type:INT64 value:"3"}} ks.20-: dummy_select {__vals0: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} __vals1: type:TUPLE values:{type:INT64 value:"4"} values:{type:INT64 value:"3"}} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(3)] [INT64(1) INT64(4)] [INT64(2) INT64(3)] [INT64(2) INT64(4)]] Destinations:DestinationKeyspaceID(014eb190c9a2fa169c),DestinationKeyspaceID(01d2fd8867d50d2dfe),DestinationKeyspaceID(024eb190c9a2fa169c),DestinationKeyspaceID(02d2fd8867d50d2dfe)`,
		`StreamExecuteMulti dummy_select ks.-20: {__vals0: type:TUPLE values:{type:INT64 value:"1"} __vals1: type:TUPLE values:{type:INT64 value:"3"}} ks.20-: {__vals0: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} __vals1: type:TUPLE values:{type:INT64 value:"4"} values:{type:INT64 value:"3"}} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestINMixedMultiColumnComparision(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("region_experimental", "", map[string]string{"region_bytes": "1"})
	sel := NewRoute(
		IN,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex
	sel.Values = []evalengine.Expr{
		evalengine.NewLiteralInt(1),
		evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(3),
			evalengine.NewLiteralInt(4),
		),
	}

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(3)] [INT64(1) INT64(4)]] Destinations:DestinationKeyspaceID(014eb190c9a2fa169c),DestinationKeyspaceID(01d2fd8867d50d2dfe)`,
		`ExecuteMultiShard ks.-20: dummy_select {__vals1: type:TUPLE values:{type:INT64 value:"3"}} ks.20-: dummy_select {__vals1: type:TUPLE values:{type:INT64 value:"4"}} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(3)] [INT64(1) INT64(4)]] Destinations:DestinationKeyspaceID(014eb190c9a2fa169c),DestinationKeyspaceID(01d2fd8867d50d2dfe)`,
		`StreamExecuteMulti dummy_select ks.-20: {__vals1: type:TUPLE values:{type:INT64 value:"3"}} ks.20-: {__vals1: type:TUPLE values:{type:INT64 value:"4"}} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestMultiEqualMultiCol(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("region_experimental", "", map[string]string{"region_bytes": "1"})
	sel := NewRoute(
		MultiEqual,
		&vindexes.Keyspace{Name: "ks", Sharded: true},
		"dummy_select",
		"dummy_select_field",
	)
	sel.Vindex = vindex
	sel.Values = []evalengine.Expr{
		evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(3),
		),
		evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(4),
		),
	}

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-40", "40-"},
		shardForKsid: []string{"-20", "40-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(2)] [INT64(3) INT64(4)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f),DestinationKeyspaceID(03d2fd8867d50d2dfe)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.40-: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(2)] [INT64(3) INT64(4)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f),DestinationKeyspaceID(03d2fd8867d50d2dfe)`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.40-: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestBuildRowColValues(t *testing.T) {
	out := buildRowColValues([][]sqltypes.Value{
		{sqltypes.NewInt64(1), sqltypes.NewInt64(10)},
		{sqltypes.NewInt64(2), sqltypes.NewInt64(20)},
	}, []sqltypes.Value{
		sqltypes.NewInt64(3),
		sqltypes.NewInt64(4),
	})

	require.Len(t, out, 4)
	require.EqualValues(t, "[INT64(1) INT64(10) INT64(3)]", fmt.Sprintf("%s", out[0]))
	require.EqualValues(t, "[INT64(1) INT64(10) INT64(4)]", fmt.Sprintf("%s", out[1]))
	require.EqualValues(t, "[INT64(2) INT64(20) INT64(3)]", fmt.Sprintf("%s", out[2]))
	require.EqualValues(t, "[INT64(2) INT64(20) INT64(4)]", fmt.Sprintf("%s", out[3]))
}

func TestBuildMultiColumnVindexValues(t *testing.T) {
	testcases := []struct {
		input  [][][]sqltypes.Value
		output [][][]*querypb.Value
	}{
		{
			input: [][][]sqltypes.Value{
				{
					{sqltypes.NewInt64(1), sqltypes.NewInt64(10)},
					{sqltypes.NewInt64(2), sqltypes.NewInt64(20)},
				}, {
					{sqltypes.NewInt64(10), sqltypes.NewInt64(10)},
					{sqltypes.NewInt64(20), sqltypes.NewInt64(20)},
				},
			},
			output: [][][]*querypb.Value{
				{
					{sqltypes.ValueToProto(sqltypes.NewInt64(1)), sqltypes.ValueToProto(sqltypes.NewInt64(2))},
					{sqltypes.ValueToProto(sqltypes.NewInt64(10)), sqltypes.ValueToProto(sqltypes.NewInt64(20))},
				}, {
					{sqltypes.ValueToProto(sqltypes.NewInt64(10)), sqltypes.ValueToProto(sqltypes.NewInt64(20))},
					{sqltypes.ValueToProto(sqltypes.NewInt64(10)), sqltypes.ValueToProto(sqltypes.NewInt64(20))},
				},
			},
		}, {
			input: [][][]sqltypes.Value{{
				{sqltypes.NewInt64(10), sqltypes.NewInt64(10), sqltypes.NewInt64(1)},
				{sqltypes.NewInt64(20), sqltypes.NewInt64(20), sqltypes.NewInt64(1)},
			},
			},
			output: [][][]*querypb.Value{{
				{sqltypes.ValueToProto(sqltypes.NewInt64(10)), sqltypes.ValueToProto(sqltypes.NewInt64(20))},
				{sqltypes.ValueToProto(sqltypes.NewInt64(10)), sqltypes.ValueToProto(sqltypes.NewInt64(20))},
				{sqltypes.ValueToProto(sqltypes.NewInt64(1))},
			},
			},
		},
	}

	for idx, testcase := range testcases {
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			out := buildMultiColumnVindexValues(testcase.input)
			require.EqualValues(t, testcase.output, out)
		})
	}
}

// TestSelectTupleMultiCol tests route execution having bind variable with multi column tuple.
func TestSelectTupleMultiCol(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("multicol", "", map[string]string{
		"column_count":  "2",
		"column_vindex": "hash,binary",
	})

	sel := NewRoute(
		MultiEqual,
		&vindexes.Keyspace{Name: "user", Sharded: true},
		"select 1 from multicol_tbl where (colb, colx, cola) in ::vals",
		"select 1 from multicol_tbl where 1 != 1",
	)
	sel.Vindex = vindex
	sel.Values = []evalengine.Expr{
		&evalengine.TupleBindVariable{Key: "vals", Index: 0},
		&evalengine.TupleBindVariable{Key: "vals", Index: 1},
	}

	v1 := sqltypes.TestTuple(sqltypes.NewInt64(1), sqltypes.NewVarChar("a"))
	v2 := sqltypes.TestTuple(sqltypes.NewInt64(4), sqltypes.NewVarChar("b"))
	tupleBV := &querypb.BindVariable{
		Type:   querypb.Type_TUPLE,
		Values: append([]*querypb.Value{sqltypes.ValueToProto(v1)}, sqltypes.ValueToProto(v2)),
	}
	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
	}
	_, err := sel.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{"vals": tupleBV}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol user [[INT64(1) VARCHAR("a")] [INT64(4) VARCHAR("b")]] Destinations:DestinationKeyspaceID(166b40b461),DestinationKeyspaceID(d2fd886762)`,
		`ExecuteMultiShard user.-20: select 1 from multicol_tbl where (colb, colx, cola) in ::vals {vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x011\x950\x01a"} values:{type:TUPLE value:"\x89\x02\x014\x950\x01b"}} false false`,
	})

	vc.Rewind()
	_, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{"vals": tupleBV}, false)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol user [[INT64(1) VARCHAR("a")] [INT64(4) VARCHAR("b")]] Destinations:DestinationKeyspaceID(166b40b461),DestinationKeyspaceID(d2fd886762)`,
		`StreamExecuteMulti select 1 from multicol_tbl where (colb, colx, cola) in ::vals user.-20: {vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x011\x950\x01a"} values:{type:TUPLE value:"\x89\x02\x014\x950\x01b"}} `,
	})
}
