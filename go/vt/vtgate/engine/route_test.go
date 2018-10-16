/*
Copyright 2018 Google Inc.

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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var defaultSelectResult = sqltypes.MakeTestResult(
	sqltypes.MakeTestFields(
		"id",
		"int64",
	),
	"1",
)

func TestSelectUnsharded(t *testing.T) {
	sel := &Route{
		Opcode: SelectUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
	}

	vc := &loggingVCursor{
		shards:  []string{"0"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select ks.0: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectScatter(t *testing.T) {
	sel := &Route{
		Opcode: SelectScatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
	}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectEqualUnique(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	sel := &Route{
		Opcode: SelectEqualUnique,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		Vindex:     vindex,
		Values:     []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectEqualUniqueScatter(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	sel := &Route{
		Opcode: SelectEqualUnique,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		Vindex:     vindex,
		Values:     []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyRange(-)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
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
	sel := &Route{
		Opcode: SelectEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		Vindex:     vindex,
		Values:     []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"toc",
					"varbinary",
				),
				"\x00",
				"\x80",
			),
			defaultSelectResult,
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationKeyspaceIDs(00,80)`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
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
	sel := &Route{
		Opcode: SelectEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		Vindex:     vindex,
		Values:     []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationNone()`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationNone()`,
	})
	expectResult(t, "sel.StreamExecute", result, nil)
}

func TestSelectINUnique(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	sel := &Route{
		Opcode: SelectIN,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		Vindex:     vindex,
		Values: []sqltypes.PlanValue{{
			Values: []sqltypes.PlanValue{{
				Value: sqltypes.NewInt64(1),
			}, {
				Value: sqltypes.NewInt64(2),
			}, {
				Value: sqltypes.NewInt64(4),
			}},
		}},
	}

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "-20", "20-"},
		results:      []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	sel := &Route{
		Opcode: SelectIN,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		Vindex:     vindex,
		Values: []sqltypes.PlanValue{{
			Values: []sqltypes.PlanValue{{
				Value: sqltypes.NewInt64(1),
			}, {
				Value: sqltypes.NewInt64(2),
			}, {
				Value: sqltypes.NewInt64(4),
			}},
		}},
	}

	fields := sqltypes.MakeTestFields(
		"toc",
		"varbinary",
	)
	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
		results: []*sqltypes.Result{
			// 1 will be sent to both shards.
			sqltypes.MakeTestResult(
				fields,
				"\x00",
				"\x80",
			),
			// 2 will go to -20.
			sqltypes.MakeTestResult(
				fields,
				"\x00",
			),
			// 4 will go to 20-.
			sqltypes.MakeTestResult(
				fields,
				"\x80",
			),
			defaultSelectResult,
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
		`Execute select toc from lkp where from = :from from: type:INT64 value:"2"  false`,
		`Execute select toc from lkp where from = :from from: type:INT64 value:"4"  false`,
		`ResolveDestinations ks [type:INT64 value:"1"  type:INT64 value:"2"  type:INT64 value:"4" ] Destinations:DestinationKeyspaceIDs(00,80),DestinationKeyspaceIDs(00),DestinationKeyspaceIDs(80)`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_select {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"2" > } ` +
			`ks.20-: dummy_select {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"4" > } ` +
			`false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
		`Execute select toc from lkp where from = :from from: type:INT64 value:"2"  false`,
		`Execute select toc from lkp where from = :from from: type:INT64 value:"4"  false`,
		`ResolveDestinations ks [type:INT64 value:"1"  type:INT64 value:"2"  type:INT64 value:"4" ] Destinations:DestinationKeyspaceIDs(00,80),DestinationKeyspaceIDs(00),DestinationKeyspaceIDs(80)`,
		`StreamExecuteMulti dummy_select ks.-20: {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"2" > } ks.20-: {__vals: type:TUPLE values:<type:INT64 value:"1" > values:<type:INT64 value:"4" > } `,
	})
	expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
}

func TestSelectNext(t *testing.T) {
	sel := &Route{
		Opcode: SelectNext,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
	}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone dummy_select  ks -20`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "sel.StreamExecute", err, `query "dummy_select" cannot be used for streaming`)
}

func TestSelectDBA(t *testing.T) {
	sel := &Route{
		Opcode: SelectDBA,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
	}

	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone dummy_select  ks -20`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
	_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "sel.StreamExecute", err, `query "dummy_select" cannot be used for streaming`)
}

func TestRouteGetFields(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	sel := &Route{
		Opcode: SelectEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		Vindex:     vindex,
		Values:     []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, true)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select_field {} false false`,
	})
	expectResult(t, "sel.Execute", result, &sqltypes.Result{})

	vc.Rewind()
	result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, true)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
		`ResolveDestinations ks [type:INT64 value:"1" ] Destinations:DestinationNone()`,
		`ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
		`ExecuteMultiShard ks.-20: dummy_select_field {} false false`,
	})
	expectResult(t, "sel.StreamExecute", result, &sqltypes.Result{})
}

func TestRouteSort(t *testing.T) {
	sel := &Route{
		Opcode: SelectUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		OrderBy: []OrderbyParams{{
			Col: 0,
		}},
	}

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
	if err != nil {
		t.Fatal(err)
	}
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
	expectResult(t, "sel.Execute", result, wantResult)

	sel.OrderBy[0].Desc = true
	vc.Rewind()
	result, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
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
	expectError(t, "sel.Execute", err, "types are not comparable: VARCHAR vs VARCHAR")
}

func TestRouteSortTruncate(t *testing.T) {
	sel := &Route{
		Opcode: SelectUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		OrderBy: []OrderbyParams{{
			Col: 0,
		}},
		TruncateColumnCount: 1,
	}

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
	if err != nil {
		t.Fatal(err)
	}
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
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestRouteStreamTruncate(t *testing.T) {
	sel := &Route{
		Opcode: SelectUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:               "dummy_select",
		FieldQuery:          "dummy_select_field",
		TruncateColumnCount: 1,
	}

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
	if err != nil {
		t.Fatal(err)
	}
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
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestRouteStreamSortTruncate(t *testing.T) {
	sel := &Route{
		Opcode: SelectUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
		OrderBy: []OrderbyParams{{
			Col: 0,
		}},
		TruncateColumnCount: 1,
	}

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
	if err != nil {
		t.Fatal(err)
	}
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
	expectResult(t, "sel.Execute", result, wantResult)
}

func TestParamsFail(t *testing.T) {
	sel := &Route{
		Opcode: SelectUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
	}

	vc := &loggingVCursor{shardErr: errors.New("shard error")}
	_, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "sel.Execute err", err, "paramsAllShards: shard error")

	vc.Rewind()
	_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "sel.StreamExecute err", err, "paramsAllShards: shard error")
}

func TestExecFail(t *testing.T) {
	// Unsharded error
	sel := &Route{
		Opcode: SelectUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
	}

	vc := &loggingVCursor{shards: []string{"0"}, resultErr: errors.New("result error")}
	_, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "sel.Execute err", err, "result error")

	vc.Rewind()
	_, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "sel.StreamExecute err", err, "result error")

	// Scatter fails if one of N fails without ShardPartial
	sel = &Route{
		Opcode: SelectScatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:      "dummy_select",
		FieldQuery: "dummy_select_field",
	}

	vc = &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
		multiShardErrs: []error{
			errors.New("result error -20"),
		},
	}
	_, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "sel.Execute err", err, "result error -20")
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})

	vc.Rewind()

	// Scatter succeeds if all shards fail with ShardPartial
	sel = &Route{
		Opcode: SelectScatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:        "dummy_select",
		FieldQuery:   "dummy_select_field",
		ShardPartial: true,
	}

	vc = &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
		multiShardErrs: []error{
			errors.New("result error -20"),
			errors.New("result error 20-"),
		},
	}
	_, err = sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Errorf("unexpected ShardPartial error %v", err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})

	vc.Rewind()

	// Scatter succeeds if one of N fails with ShardPartial
	sel = &Route{
		Opcode: SelectScatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:        "dummy_select",
		FieldQuery:   "dummy_select_field",
		ShardPartial: true,
	}

	vc = &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: []*sqltypes.Result{defaultSelectResult},
		multiShardErrs: []error{
			errors.New("result error -20"),
			nil,
		},
	}
	result, err := sel.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Errorf("unexpected ShardPartial error %v", err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_select {} ks.20-: dummy_select {} false false`,
	})
	expectResult(t, "sel.Execute", result, defaultSelectResult)

	vc.Rewind()
}
