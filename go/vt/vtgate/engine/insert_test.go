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
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestInsertUnsharded(t *testing.T) {
	ins := NewQueryInsert(
		InsertUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_insert",
	)

	vc := newDMLTestVCursor("0")
	vc.results = []*sqltypes.Result{{
		InsertID: 4,
	}}

	result, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_insert {} true true`,
	})
	expectResult(t, "Execute", result, &sqltypes.Result{InsertID: 4})

	// Failure cases
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `shard_error`)

	vc = &loggingVCursor{}
	_, err = ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `Keyspace does not have exactly one shard: []`)
}

func TestInsertUnshardedGenerate(t *testing.T) {
	ins := NewQueryInsert(
		InsertUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_insert",
	)
	ins.Generate = &Generate{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks2",
			Sharded: false,
		},
		Query: "dummy_generate",
		Values: evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(1),
			evalengine.NullExpr,
			evalengine.NewLiteralInt(2),
			evalengine.NullExpr,
			evalengine.NewLiteralInt(3),
		),
	}

	vc := newDMLTestVCursor("0")
	vc.results = []*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"nextval",
				"int64",
			),
			"4",
		),
		{InsertID: 1},
	}

	result, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Fetch two sequence value.
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone dummy_generate n: type:INT64 value:"2" ks2 0`,
		// Fill those values into the insert.
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_insert {__seq0: type:INT64 value:"1" __seq1: type:INT64 value:"4" __seq2: type:INT64 value:"2" __seq3: type:INT64 value:"5" __seq4: type:INT64 value:"3"} true true`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerateFromValues.
	expectResult(t, "Execute", result, &sqltypes.Result{InsertID: 4})
}

func TestInsertUnshardedGenerate_Zeros(t *testing.T) {
	ins := NewQueryInsert(
		InsertUnsharded,
		&vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		"dummy_insert",
	)
	ins.Generate = &Generate{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks2",
			Sharded: false,
		},
		Query: "dummy_generate",
		Values: evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(1),
			evalengine.NewLiteralInt(0),
			evalengine.NewLiteralInt(2),
			evalengine.NewLiteralInt(0),
			evalengine.NewLiteralInt(3),
		),
	}

	vc := newDMLTestVCursor("0")
	vc.results = []*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"nextval",
				"int64",
			),
			"4",
		),
		{InsertID: 1},
	}

	result, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Fetch two sequence value.
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone dummy_generate n: type:INT64 value:"2" ks2 0`,
		// Fill those values into the insert.
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_insert {__seq0: type:INT64 value:"1" __seq1: type:INT64 value:"4" __seq2: type:INT64 value:"2" __seq3: type:INT64 value:"5" __seq4: type:INT64 value:"3"} true true`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerateFromValues.
	expectResult(t, "Execute", result, &sqltypes.Result{InsertID: 4})
}

func TestInsertShardedSimple(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	// A single row insert should be autocommitted
	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				evalengine.NewLiteralInt(1),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1"},
		" suffix",
	)
	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Based on shardForKsid, values returned will be 20-.
		`ResolveDestinations sharded [value:"0"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1 suffix {_id_0: type:INT64 value:"1"} ` +
			`true true`,
	})

	// Multiple rows are not autocommitted by default
	ins = NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			// 3 rows.
			{
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)
	vc = newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}

	_, err = ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix {_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`sharded.-20: prefix mid2 suffix {_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`true false`,
	})

	// Optional flag overrides autocommit
	ins = NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			// 3 rows.
			{
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}},

		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)
	ins.MultiShardAutocommit = true

	vc = newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}

	_, err = ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix {_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`sharded.-20: prefix mid2 suffix {_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`true true`,
	})
}

func TestInsertShardedFail(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"primary": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "prim",
							"from":  "from1",
							"to":    "toc",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "primary",
							Columns: []string{"id"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				evalengine.NewLiteralInt(1),
			},
		}},

		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := &loggingVCursor{}

	// The lookup will fail to map to a keyspace id.
	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `could not map [INT64(1)] to a keyspace id`)
}

func TestInsertShardedGenerate(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// 3 rows.
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	ins.Generate = &Generate{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks2",
			Sharded: false,
		},
		Query: "dummy_generate",
		Values: evalengine.NewTupleExpr(
			evalengine.NewLiteralInt(1),
			evalengine.NullExpr,
			evalengine.NewLiteralInt(2),
		),
	}

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"nextval",
				"int64",
			),
			"2",
		),
		{InsertID: 1},
	}

	result, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone dummy_generate n: type:INT64 value:"1" ks2 -20`,
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix ` +
			`{__seq0: type:INT64 value:"1" __seq1: type:INT64 value:"2" __seq2: type:INT64 value:"2" ` +
			`_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`sharded.-20: prefix mid2 suffix ` +
			`{__seq0: type:INT64 value:"1" __seq1: type:INT64 value:"2" __seq2: type:INT64 value:"2" ` +
			`_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`true false`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerateFromValues.
	expectResult(t, "Execute", result, &sqltypes.Result{InsertID: 2})
}

func TestInsertShardedOwned(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"twocol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp2",
							"from":  "from1,from2",
							"to":    "toc",
						},
						Owner: "t1",
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc",
						},
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}, {
							Name:    "twocol",
							Columns: []string{"c1", "c2"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}, {
			// colVindex columns: c1, c2
			{
				evalengine.NewLiteralInt(4),
				evalengine.NewLiteralInt(5),
				evalengine.NewLiteralInt(6),
			},
			{
				evalengine.NewLiteralInt(7),
				evalengine.NewLiteralInt(8),
				evalengine.NewLiteralInt(9),
			},
		}, {
			// colVindex columns: c3
			{
				evalengine.NewLiteralInt(10),
				evalengine.NewLiteralInt(11),
				evalengine.NewLiteralInt(12),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0), (:from1_1, :from2_1, :toc_1), (:from1_2, :from2_2, :toc_2) ` +
			`from1_0: type:INT64 value:"4" from1_1: type:INT64 value:"5" from1_2: type:INT64 value:"6" ` +
			`from2_0: type:INT64 value:"7" from2_1: type:INT64 value:"8" from2_2: type:INT64 value:"9" ` +
			`toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" toc_1: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" toc_2: type:VARBINARY value:"N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0), (:from_1, :toc_1), (:from_2, :toc_2) ` +
			`from_0: type:INT64 value:"10" from_1: type:INT64 value:"11" from_2: type:INT64 value:"12" ` +
			`toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" toc_1: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" toc_2: type:VARBINARY value:"N\xb1\x90ɢ\xfa\x16\x9c" true`,
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix ` +
			`{_c1_0: type:INT64 value:"4" _c1_1: type:INT64 value:"5" _c1_2: type:INT64 value:"6" ` +
			`_c2_0: type:INT64 value:"7" _c2_1: type:INT64 value:"8" _c2_2: type:INT64 value:"9" ` +
			`_c3_0: type:INT64 value:"10" _c3_1: type:INT64 value:"11" _c3_2: type:INT64 value:"12" ` +
			`_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`sharded.-20: prefix mid2 suffix ` +
			`{_c1_0: type:INT64 value:"4" _c1_1: type:INT64 value:"5" _c1_2: type:INT64 value:"6" ` +
			`_c2_0: type:INT64 value:"7" _c2_1: type:INT64 value:"8" _c2_2: type:INT64 value:"9" ` +
			`_c3_0: type:INT64 value:"10" _c3_1: type:INT64 value:"11" _c3_2: type:INT64 value:"12" ` +
			`_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`true false`,
	})
}

func TestInsertShardedOwnedWithNull(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table":        "lkp1",
							"from":         "from",
							"to":           "toc",
							"ignore_nulls": "true",
						},
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
			},
		}, {
			// colVindex columns: c3
			{
				evalengine.NullExpr,
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [value:"0"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard sharded.20-: prefix mid1 suffix ` +
			`{_c3_0:  _id_0: type:INT64 value:"1"} true true`,
	})
}

func TestInsertShardedGeo(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"geo": {
						Type: "region_experimental",
						Params: map[string]string{
							"region_bytes": "1",
						},
					},
					"lookup": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "id_idx",
							"from":  "id",
							"to":    "keyspace_id",
						},
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "geo",
							Columns: []string{"region", "id"},
						}, {
							Name:    "lookup",
							Columns: []string{"id"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: region, id
			{
				// rows for region
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(255),
			},
			{
				// rows for id
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(1),
			},
		}, {
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(1),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2"},
		" suffix",
	)
	for _, colVindex := range ks.Tables["t1"].ColumnVindexes {
		if colVindex.IsPartialVindex() {
			continue
		}
		ins.ColVindexes = append(ins.ColVindexes, colVindex)
	}

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20"}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute insert into id_idx(id, keyspace_id) values(:id_0, :keyspace_id_0), (:id_1, :keyspace_id_1) ` +
			`id_0: type:INT64 value:"1" id_1: type:INT64 value:"1" ` +
			`keyspace_id_0: type:VARBINARY value:"\x01\x16k@\xb4J\xbaK\xd6" keyspace_id_1: type:VARBINARY value:"\xff\x16k@\xb4J\xbaK\xd6" true`,
		`ResolveDestinations sharded [value:"0" value:"1"] Destinations:DestinationKeyspaceID(01166b40b44aba4bd6),DestinationKeyspaceID(ff166b40b44aba4bd6)`,
		`ExecuteMultiShard sharded.20-: prefix mid1 suffix ` +
			`{_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"1" ` +
			`_region_0: type:INT64 value:"1" _region_1: type:INT64 value:"255"} ` +
			`sharded.-20: prefix mid2 suffix ` +
			`{_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"1" ` +
			`_region_0: type:INT64 value:"1" _region_1: type:INT64 value:"255"} ` +
			`true false`,
	})
}

func TestInsertShardedIgnoreOwned(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"primary": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "prim",
							"from":  "from1",
							"to":    "toc",
						},
					},
					"twocol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp2",
							"from":  "from1,from2",
							"to":    "toc",
						},
						Owner: "t1",
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc",
						},
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "primary",
							Columns: []string{"id"},
						}, {
							Name:    "twocol",
							Columns: []string{"c1", "c2"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		true,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id

				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
				evalengine.NewLiteralInt(4),
			},
		}, {
			// colVindex columns: c1, c2
			{
				// rows for c1
				evalengine.NewLiteralInt(5),
				evalengine.NewLiteralInt(6),
				evalengine.NewLiteralInt(7),
				evalengine.NewLiteralInt(8),
			},
			{
				// rows for c2
				evalengine.NewLiteralInt(9),
				evalengine.NewLiteralInt(10),
				evalengine.NewLiteralInt(11),
				evalengine.NewLiteralInt(12),
			},
		}, {
			// colVindex columns: c3
			{
				// rows for c3
				evalengine.NewLiteralInt(13),
				evalengine.NewLiteralInt(14),
				evalengine.NewLiteralInt(15),
				evalengine.NewLiteralInt(16),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3", " mid4"},
		" suffix",
	)

	ksid0Lookup := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"from|to",
			"int64|varbinary",
		),
		"1|\x00",
		"3|\x00",
		"4|\x00",
	)
	ksid0 := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"to",
			"varbinary",
		),
		"\x00",
	)
	noresult := &sqltypes.Result{}
	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20"}
	vc.results = []*sqltypes.Result{
		// primary vindex lookups: fail row 2.
		ksid0Lookup,
		// insert lkp2
		noresult,
		// fail one verification (row 3)
		ksid0,
		noresult,
		ksid0,
		// insert lkp1
		noresult,
		// verify lkp1 (only two rows to verify)
		ksid0,
		ksid0,
	}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select from1, toc from prim where from1 in ::from1 from1: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"} values:{type:INT64 value:"4"} false`,
		`Execute insert ignore into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0), (:from1_1, :from2_1, :toc_1), (:from1_2, :from2_2, :toc_2) ` +
			`from1_0: type:INT64 value:"5" from1_1: type:INT64 value:"7" from1_2: type:INT64 value:"8" ` +
			`from2_0: type:INT64 value:"9" from2_1: type:INT64 value:"11" from2_2: type:INT64 value:"12" ` +
			`toc_0: type:VARBINARY value:"\x00" toc_1: type:VARBINARY value:"\x00" toc_2: type:VARBINARY value:"\x00" ` +
			`true`,
		// row 2 is out because it didn't map to a ksid.
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"5" toc: type:VARBINARY value:"\x00" false`,
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"7" toc: type:VARBINARY value:"\x00" false`,
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"8" toc: type:VARBINARY value:"\x00" false`,
		`Execute insert ignore into lkp1(from, toc) values(:from_0, :toc_0), (:from_1, :toc_1) ` +
			`from_0: type:INT64 value:"13" from_1: type:INT64 value:"16" ` +
			`toc_0: type:VARBINARY value:"\x00" toc_1: type:VARBINARY value:"\x00" ` +
			`true`,
		// row 3 is out because it failed Verify. Only two verifications from lkp1.
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"13" toc: type:VARBINARY value:"\x00" false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"16" toc: type:VARBINARY value:"\x00" false`,
		`ResolveDestinations sharded [value:"0" value:"3"] Destinations:DestinationKeyspaceID(00),DestinationKeyspaceID(00)`,
		// Bind vars for rows 2 & 3 may be missing because they were not sent.
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1 suffix ` +
			`{_c1_0: type:INT64 value:"5" _c1_3: type:INT64 value:"8" ` +
			`_c2_0: type:INT64 value:"9" _c2_3: type:INT64 value:"12" ` +
			`_c3_0: type:INT64 value:"13" _c3_3: type:INT64 value:"16" ` +
			`_id_0: type:INT64 value:"1" _id_3: type:INT64 value:"4"} ` +
			`sharded.-20: prefix mid4 suffix ` +
			`{_c1_0: type:INT64 value:"5" _c1_3: type:INT64 value:"8" ` +
			`_c2_0: type:INT64 value:"9" _c2_3: type:INT64 value:"12" ` +
			`_c3_0: type:INT64 value:"13" _c3_3: type:INT64 value:"16" ` +
			`_id_0: type:INT64 value:"1" _id_3: type:INT64 value:"4"} ` +
			`true false`,
	})
}

func TestInsertShardedIgnoreOwnedWithNull(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"primary": {
						Type: "hash",
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table":        "lkp1",
							"from":         "from",
							"to":           "toc",
							"ignore_nulls": "true",
						},
						Owner: "t1",
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "primary",
							Columns: []string{"id"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		true,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
			},
		}, {
			// colVindex columns: c3
			{
				// rows for c3
				evalengine.NullExpr,
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3", " mid4"},
		" suffix",
	)

	ksid0 := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"to",
			"varbinary",
		),
		"\x00",
	)
	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"-20", "20-"}
	vc.results = []*sqltypes.Result{
		ksid0,
		ksid0,
		ksid0,
	}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select from from lkp1 where from = :from and toc = :toc from:  toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" false`,
		`ResolveDestinations sharded [value:"0"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard sharded.-20: prefix mid1 suffix ` +
			`{_c3_0:  _id_0: type:INT64 value:"1"} true true`,
	})
}

func TestInsertShardedUnownedVerify(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"twocol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp2",
							"from":  "from1,from2",
							"to":    "toc",
						},
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}, {
							Name:    "twocol",
							Columns: []string{"c1", "c2"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}, {
			// colVindex columns: c1, c2
			{
				// rows for c1
				evalengine.NewLiteralInt(4),
				evalengine.NewLiteralInt(5),
				evalengine.NewLiteralInt(6),
			},
			{
				// rows for c2
				evalengine.NewLiteralInt(7),
				evalengine.NewLiteralInt(8),
				evalengine.NewLiteralInt(9),
			},
		}, {
			// colVindex columns: c3
			{
				// rows for c3
				evalengine.NewLiteralInt(10),
				evalengine.NewLiteralInt(11),
				evalengine.NewLiteralInt(12),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	// nonemptyResult will cause the lookup verify queries to succeed.
	nonemptyResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1",
			"int64",
		),
		"1",
	)

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		nonemptyResult,
		nonemptyResult,
		nonemptyResult,
		nonemptyResult,
		nonemptyResult,
		nonemptyResult,
	}
	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Perform verification for each colvindex.
		// Note that only first column of each colvindex is used.
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"4" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" false`,
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"5" toc: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" false`,
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"6" toc: type:VARBINARY value:"N\xb1\x90ɢ\xfa\x16\x9c" false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"10" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"11" toc: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"12" toc: type:VARBINARY value:"N\xb1\x90ɢ\xfa\x16\x9c" false`,
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix ` +
			`{_c1_0: type:INT64 value:"4" _c1_1: type:INT64 value:"5" _c1_2: type:INT64 value:"6" ` +
			`_c2_0: type:INT64 value:"7" _c2_1: type:INT64 value:"8" _c2_2: type:INT64 value:"9" ` +
			`_c3_0: type:INT64 value:"10" _c3_1: type:INT64 value:"11" _c3_2: type:INT64 value:"12" ` +
			`_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`sharded.-20: prefix mid2 suffix ` +
			`{_c1_0: type:INT64 value:"4" _c1_1: type:INT64 value:"5" _c1_2: type:INT64 value:"6" ` +
			`_c2_0: type:INT64 value:"7" _c2_1: type:INT64 value:"8" _c2_2: type:INT64 value:"9" ` +
			`_c3_0: type:INT64 value:"10" _c3_1: type:INT64 value:"11" _c3_2: type:INT64 value:"12" ` +
			`_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`true false`,
	})
}

func TestInsertShardedIgnoreUnownedVerify(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		true,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}, {
			// colVindex columns: c3
			{
				// rows for c3
				evalengine.NewLiteralInt(10),
				evalengine.NewLiteralInt(11),
				evalengine.NewLiteralInt(12),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	// nonemptyResult will cause the lookup verify queries to succeed.
	nonemptyResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1",
			"int64",
		),
		"1",
	)

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20"}
	vc.results = []*sqltypes.Result{
		nonemptyResult,
		// fail verification of second row.
		{},
		nonemptyResult,
	}
	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Perform verification for each colvindex.
		// Note that only first column of each colvindex is used.
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"10" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"11" toc: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"12" toc: type:VARBINARY value:"N\xb1\x90ɢ\xfa\x16\x9c" false`,
		// Based on shardForKsid, values returned will be 20-, -20.
		`ResolveDestinations sharded [value:"0" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1 suffix ` +
			`{_c3_0: type:INT64 value:"10" _c3_2: type:INT64 value:"12" ` +
			`_id_0: type:INT64 value:"1" _id_2: type:INT64 value:"3"} ` +
			`sharded.-20: prefix mid3 suffix ` +
			`{_c3_0: type:INT64 value:"10" _c3_2: type:INT64 value:"12" ` +
			`_id_0: type:INT64 value:"1" _id_2: type:INT64 value:"3"} ` +
			`true false`,
	})
}

func TestInsertShardedIgnoreUnownedVerifyFail(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
			},
		}, {
			// colVindex columns: c3
			{
				// rows for c3
				evalengine.NewLiteralInt(2),
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := newDMLTestVCursor("-20", "20-")

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `values [[INT64(2)]] for column [c3] does not map to keyspace ids`)
}

func TestInsertShardedUnownedReverseMap(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"twocol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp2",
							"from":  "from1,from2",
							"to":    "toc",
						},
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}, {
							Name:    "hash",
							Columns: []string{"c1", "c2"},
						}, {
							Name:    "hash",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
				evalengine.NewLiteralInt(3),
			},
		}, {
			// colVindex columns: c1, c2
			{
				// rows for c1
				evalengine.NullExpr,
				evalengine.NullExpr,
				evalengine.NullExpr,
			},
			{
				// rows for c2
				evalengine.NullExpr,
				evalengine.NullExpr,
				evalengine.NullExpr,
			},
		}, {
			// colVindex columns: c3
			{
				// rows for c3
				evalengine.NullExpr,
				evalengine.NullExpr,
				evalengine.NullExpr,
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	// nonemptyResult will cause the lookup verify queries to succeed.
	nonemptyResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1",
			"int64",
		),
		"1",
	)

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		nonemptyResult,
	}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix ` +
			`{_c1_0: type:UINT64 value:"1" _c1_1: type:UINT64 value:"2" _c1_2: type:UINT64 value:"3" ` +
			`_c2_0:  _c2_1:  _c2_2:  ` +
			`_c3_0: type:UINT64 value:"1" _c3_1: type:UINT64 value:"2" _c3_2: type:UINT64 value:"3" ` +
			`_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`sharded.-20: prefix mid2 suffix ` +
			`{_c1_0: type:UINT64 value:"1" _c1_1: type:UINT64 value:"2" _c1_2: type:UINT64 value:"3" ` +
			`_c2_0:  _c2_1:  _c2_2:  ` +
			`_c3_0: type:UINT64 value:"1" _c3_1: type:UINT64 value:"2" _c3_2: type:UINT64 value:"3" ` +
			`_id_0: type:INT64 value:"1" _id_1: type:INT64 value:"2" _id_2: type:INT64 value:"3"} ` +
			`true false`,
	})
}

func TestInsertShardedUnownedReverseMapSuccess(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc",
						},
					},
				},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}, {
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		false,
		ks.Keyspace,
		[][][]evalengine.Expr{{
			// colVindex columns: id
			{
				// rows for id
				evalengine.NewLiteralInt(1),
			},
		}, {
			// colVindex columns: c3
			{
				// rows for c3
				evalengine.NullExpr,
			},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := newDMLTestVCursor("-20", "20-")

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
}

func TestInsertSelectSimple(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {Type: "hash"}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"}}}}}}}}

	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	// A single row insert should be autocommitted
	ins := &Insert{
		Opcode:            InsertSelect,
		Keyspace:          ks.Keyspace,
		Query:             "dummy_insert",
		Table:             ks.Tables["t1"],
		VindexValueOffset: [][]int{{1}},
		Input: &Route{
			Query:      "dummy_select",
			FieldQuery: "dummy_field_query",
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace}}}

	ins.ColVindexes = append(ins.ColVindexes, ks.Tables["t1"].ColumnVindexes...)
	ins.Prefix = "prefix "
	ins.Suffix = " suffix"

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"name|id",
				"varchar|int64"),
			"a|1",
			"a|3",
			"b|2")}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,

		// the select query
		`ExecuteMultiShard sharded.-20: dummy_select {} sharded.20-: dummy_select {} false false`,
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(4eb190c9a2fa169c),DestinationKeyspaceID(06e7ea22ce92708f)`,

		// two rows go to the 20- shard, and one row go to the -20 shard
		`ExecuteMultiShard ` +
			`sharded.20-: prefix values (:_c0_0, :_c0_1), (:_c2_0, :_c2_1) suffix ` +
			`{_c0_0: type:VARCHAR value:"a" _c0_1: type:INT64 value:"1"` +
			` _c2_0: type:VARCHAR value:"b" _c2_1: type:INT64 value:"2"} ` +
			`sharded.-20: prefix values (:_c1_0, :_c1_1) suffix` +
			` {_c1_0: type:VARCHAR value:"a" _c1_1: type:INT64 value:"3"} true false`})

	vc.Rewind()
	err = ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,

		// the select query
		`StreamExecuteMulti dummy_select sharded.-20: {} sharded.20-: {} `,
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(4eb190c9a2fa169c),DestinationKeyspaceID(06e7ea22ce92708f)`,

		// two rows go to the 20- shard, and one row go to the -20 shard
		`ExecuteMultiShard ` +
			`sharded.20-: prefix values (:_c0_0, :_c0_1), (:_c2_0, :_c2_1) suffix ` +
			`{_c0_0: type:VARCHAR value:"a" _c0_1: type:INT64 value:"1"` +
			` _c2_0: type:VARCHAR value:"b" _c2_1: type:INT64 value:"2"} ` +
			`sharded.-20: prefix values (:_c1_0, :_c1_1) suffix` +
			` {_c1_0: type:VARCHAR value:"a" _c1_1: type:INT64 value:"3"} true false`})
}

func TestInsertSelectOwned(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {Type: "hash"},
					"onecol": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc"},
						Owner: "t1"}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"}}, {
							Name:    "onecol",
							Columns: []string{"c3"}}}}}}}}

	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := &Insert{
		Opcode:   InsertSelect,
		Keyspace: ks.Keyspace,
		Query:    "dummy_insert",
		Table:    ks.Tables["t1"],
		VindexValueOffset: [][]int{
			{1},  // The primary vindex has a single column as sharding key
			{0}}, // the onecol vindex uses the 'name' column
		Input: &Route{
			Query:      "dummy_select",
			FieldQuery: "dummy_field_query",
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace}}}

	ins.ColVindexes = append(ins.ColVindexes, ks.Tables["t1"].ColumnVindexes...)
	ins.Prefix = "prefix "
	ins.Suffix = " suffix"

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"name|id",
				"varchar|int64"),
			"a|1",
			"a|3",
			"b|2")}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,

		// the select query
		`ExecuteMultiShard sharded.-20: dummy_select {} sharded.20-: dummy_select {} false false`,

		// insert values into the owned lookup vindex
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0), (:from_1, :toc_1), (:from_2, :toc_2) from_0: type:VARCHAR value:"a" from_1: type:VARCHAR value:"a" from_2: type:VARCHAR value:"b" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" toc_1: type:VARBINARY value:"N\xb1\x90ɢ\xfa\x16\x9c" toc_2: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,

		// Values 0 1 2 come from the id column
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(4eb190c9a2fa169c),DestinationKeyspaceID(06e7ea22ce92708f)`,

		// insert values into the main table
		`ExecuteMultiShard ` +
			// first we insert two rows on the 20- shard
			`sharded.20-: prefix values (:_c0_0, :_c0_1), (:_c2_0, :_c2_1) suffix ` +
			`{_c0_0: type:VARCHAR value:"a" _c0_1: type:INT64 value:"1" _c2_0: type:VARCHAR value:"b" _c2_1: type:INT64 value:"2"} ` +

			// next we insert one row on the -20 shard
			`sharded.-20: prefix values (:_c1_0, :_c1_1) suffix ` +
			`{_c1_0: type:VARCHAR value:"a" _c1_1: type:INT64 value:"3"} ` +
			`true false`})

	vc.Rewind()
	err = ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,

		// the select query
		`StreamExecuteMulti dummy_select sharded.-20: {} sharded.20-: {} `,

		// insert values into the owned lookup vindex
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0), (:from_1, :toc_1), (:from_2, :toc_2) from_0: type:VARCHAR value:"a" from_1: type:VARCHAR value:"a" from_2: type:VARCHAR value:"b" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" toc_1: type:VARBINARY value:"N\xb1\x90ɢ\xfa\x16\x9c" toc_2: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,

		// Values 0 1 2 come from the id column
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(4eb190c9a2fa169c),DestinationKeyspaceID(06e7ea22ce92708f)`,

		// insert values into the main table
		`ExecuteMultiShard ` +
			// first we insert two rows on the 20- shard
			`sharded.20-: prefix values (:_c0_0, :_c0_1), (:_c2_0, :_c2_1) suffix ` +
			`{_c0_0: type:VARCHAR value:"a" _c0_1: type:INT64 value:"1" _c2_0: type:VARCHAR value:"b" _c2_1: type:INT64 value:"2"} ` +

			// next we insert one row on the -20 shard
			`sharded.-20: prefix values (:_c1_0, :_c1_1) suffix ` +
			`{_c1_0: type:VARCHAR value:"a" _c1_1: type:INT64 value:"3"} ` +
			`true false`})
}

func TestInsertSelectGenerate(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash"}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"}}}}}}}}

	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := &Insert{
		Opcode:   InsertSelect,
		Keyspace: ks.Keyspace,
		Query:    "dummy_insert",
		Table:    ks.Tables["t1"],
		VindexValueOffset: [][]int{
			{1}}, // The primary vindex has a single column as sharding key
		Input: &Route{
			Query:      "dummy_select",
			FieldQuery: "dummy_field_query",
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace}}}

	ins.Generate = &Generate{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks2",
			Sharded: false,
		},
		Query:  "dummy_generate",
		Offset: 1,
	}
	ins.Prefix = "prefix "
	ins.Suffix = " suffix"

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		// This is the result from the input SELECT
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"name|id",
				"varchar|int64"),
			"a|1",
			"a|null",
			"b|null"),
		// This is the result for the sequence query
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"nextval",
				"int64",
			),
			"2",
		),
		{InsertID: 1},
	}

	result, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// this is the input query
		`ExecuteMultiShard sharded.-20: dummy_select {} sharded.20-: dummy_select {} false false`,
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,

		// this is the sequence table query
		`ExecuteStandalone dummy_generate n: type:INT64 value:"2" ks2 -20`,
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			// first we send the insert to the 20- shard
			`sharded.20-: prefix values (:_c0_0, :_c0_1), (:_c2_0, :_c2_1) suffix ` +
			`{_c0_0: type:VARCHAR value:"a" ` +
			`_c0_1: type:INT64 value:"1" ` +
			`_c2_0: type:VARCHAR value:"b" ` +
			`_c2_1: type:INT64 value:"3"} ` +
			// next we send the insert to the -20 shard
			`sharded.-20: prefix values (:_c1_0, :_c1_1) suffix ` +
			`{_c1_0: type:VARCHAR value:"a" _c1_1: type:INT64 value:"2"} ` +
			`true false`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerateFromValues.
	expectResult(t, "Execute", result, &sqltypes.Result{InsertID: 2})
}

func TestStreamingInsertSelectGenerate(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash"}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"}}}}}}}}

	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := &Insert{
		Opcode:   InsertSelect,
		Keyspace: ks.Keyspace,
		Query:    "dummy_insert",
		Table:    ks.Tables["t1"],
		VindexValueOffset: [][]int{
			{1}}, // The primary vindex has a single column as sharding key
		Input: &Route{
			Query:      "dummy_select",
			FieldQuery: "dummy_field_query",
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace}}}

	ins.Generate = &Generate{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks2",
			Sharded: false,
		},
		Query:  "dummy_generate",
		Offset: 1,
	}
	ins.Prefix = "prefix "
	ins.Suffix = " suffix"

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		// This is the result from the input SELECT
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"name|id",
				"varchar|int64"),
			"a|1",
			"a|null",
			"b|null"),
		// This is the result for the sequence query
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"nextval",
				"int64",
			),
			"2",
		),
		{InsertID: 1},
	}

	var output *sqltypes.Result
	err := ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		output = result
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// this is the input query
		`StreamExecuteMulti dummy_select sharded.-20: {} sharded.20-: {} `,
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,

		// this is the sequence table query
		`ExecuteStandalone dummy_generate n: type:INT64 value:"2" ks2 -20`,
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			// first we send the insert to the 20- shard
			`sharded.20-: prefix values (:_c0_0, :_c0_1), (:_c2_0, :_c2_1) suffix ` +
			`{_c0_0: type:VARCHAR value:"a" ` +
			`_c0_1: type:INT64 value:"1" ` +
			`_c2_0: type:VARCHAR value:"b" ` +
			`_c2_1: type:INT64 value:"3"} ` +
			// next we send the insert to the -20 shard
			`sharded.-20: prefix values (:_c1_0, :_c1_1) suffix ` +
			`{_c1_0: type:VARCHAR value:"a" _c1_1: type:INT64 value:"2"} ` +
			`true false`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerateFromValues.
	expectResult(t, "Execute", output, &sqltypes.Result{InsertID: 2})
}

func TestInsertSelectGenerateNotProvided(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash"}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"}}}}}}}}

	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := &Insert{
		Opcode:   InsertSelect,
		Keyspace: ks.Keyspace,
		Query:    "dummy_insert",
		Table:    ks.Tables["t1"],
		VindexValueOffset: [][]int{
			{1}}, // The primary vindex has a single column as sharding key
		Input: &Route{
			Query:      "dummy_select",
			FieldQuery: "dummy_field_query",
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace}}}

	ins.Generate = &Generate{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks2",
			Sharded: false,
		},
		Query:  "dummy_generate",
		Offset: 2,
	}
	ins.Prefix = "prefix "
	ins.Suffix = " suffix"

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		// This is the result from the input SELECT
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"name|id",
				"varchar|int64"),
			"a|1",
			"a|2",
			"b|3"),
		// This is the result for the sequence query
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"nextval",
				"int64",
			),
			"10",
		),
		{InsertID: 1},
	}

	result, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// this is the input query
		`ExecuteMultiShard sharded.-20: dummy_select {} sharded.20-: dummy_select {} false false`,
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,

		// this is the sequence table query
		`ExecuteStandalone dummy_generate n: type:INT64 value:"3" ks2 -20`,
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix values (:_c0_0, :_c0_1, :_c0_2), (:_c2_0, :_c2_1, :_c2_2) suffix ` +
			`{_c0_0: type:VARCHAR value:"a" _c0_1: type:INT64 value:"1" _c0_2: type:INT64 value:"10" ` +
			`_c2_0: type:VARCHAR value:"b" _c2_1: type:INT64 value:"3" _c2_2: type:INT64 value:"12"} ` +
			`sharded.-20: prefix values (:_c1_0, :_c1_1, :_c1_2) suffix ` +
			`{_c1_0: type:VARCHAR value:"a" _c1_1: type:INT64 value:"2" _c1_2: type:INT64 value:"11"} ` +
			`true false`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerateFromValues.
	expectResult(t, "Execute", result, &sqltypes.Result{InsertID: 10})
}

func TestStreamingInsertSelectGenerateNotProvided(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash"}},
				Tables: map[string]*vschemapb.Table{
					"t1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"}}}}}}}}

	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := &Insert{
		Opcode:   InsertSelect,
		Keyspace: ks.Keyspace,
		Query:    "dummy_insert",
		Table:    ks.Tables["t1"],
		VindexValueOffset: [][]int{
			{1}}, // The primary vindex has a single column as sharding key
		Input: &Route{
			Query:      "dummy_select",
			FieldQuery: "dummy_field_query",
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace}}}

	ins.Generate = &Generate{
		Keyspace: &vindexes.Keyspace{
			Name:    "ks2",
			Sharded: false,
		},
		Query:  "dummy_generate",
		Offset: 2,
	}
	ins.Prefix = "prefix "
	ins.Suffix = " suffix"

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		// This is the result from the input SELECT
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"name|id",
				"varchar|int64"),
			"a|1",
			"a|2",
			"b|3"),
		// This is the result for the sequence query
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"nextval",
				"int64",
			),
			"10",
		),
		{InsertID: 1},
	}

	var output *sqltypes.Result
	err := ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		output = result
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// this is the input query
		`StreamExecuteMulti dummy_select sharded.-20: {} sharded.20-: {} `,
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,

		// this is the sequence table query
		`ExecuteStandalone dummy_generate n: type:INT64 value:"3" ks2 -20`,
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix values (:_c0_0, :_c0_1, :_c0_2), (:_c2_0, :_c2_1, :_c2_2) suffix ` +
			`{_c0_0: type:VARCHAR value:"a" _c0_1: type:INT64 value:"1" _c0_2: type:INT64 value:"10" ` +
			`_c2_0: type:VARCHAR value:"b" _c2_1: type:INT64 value:"3" _c2_2: type:INT64 value:"12"} ` +
			`sharded.-20: prefix values (:_c1_0, :_c1_1, :_c1_2) suffix ` +
			`{_c1_0: type:VARCHAR value:"a" _c1_1: type:INT64 value:"2" _c1_2: type:INT64 value:"11"} ` +
			`true false`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerateFromValues.
	expectResult(t, "Execute", output, &sqltypes.Result{InsertID: 10})
}

func TestInsertSelectUnowned(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {Type: "hash"},
					"onecol": {
						Type: "lookup_unique",
						Params: map[string]string{
							"table": "lkp1",
							"from":  "from",
							"to":    "toc"},
						Owner: "t1"}},
				Tables: map[string]*vschemapb.Table{
					"t2": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "onecol",
							Columns: []string{"id"}}}}}}}}

	vs := vindexes.BuildVSchema(invschema)
	ks := vs.Keyspaces["sharded"]

	ins := &Insert{
		Opcode:   InsertSelect,
		Keyspace: ks.Keyspace,
		Query:    "dummy_insert",
		Table:    ks.Tables["t2"],
		VindexValueOffset: [][]int{
			{0}}, // the onecol vindex as unowned lookup sharding column
		Input: &Route{
			Query:      "dummy_select",
			FieldQuery: "dummy_field_query",
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace}}}

	ins.ColVindexes = append(ins.ColVindexes, ks.Tables["t2"].ColumnVindexes...)
	ins.Prefix = "prefix "
	ins.Suffix = " suffix"

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"20-", "-20", "20-"}
	vc.results = []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "3", "2"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|tocol", "int64|int64"), "1|1", "3|2", "2|3"),
	}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,

		// the select query
		`ExecuteMultiShard sharded.-20: dummy_select {} sharded.20-: dummy_select {} false false`,

		// select values into the unowned lookup vindex for routing
		`Execute select from, toc from lkp1 where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"3"} values:{type:INT64 value:"2"} false`,

		// values from lookup vindex resolved to destination
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(31),DestinationKeyspaceID(32),DestinationKeyspaceID(33)`,

		// insert values into the main table
		`ExecuteMultiShard ` +
			// first we insert two rows on the 20- shard
			`sharded.20-: prefix values (:_c0_0), (:_c2_0) suffix ` +
			`{_c0_0: type:INT64 value:"1" _c2_0: type:INT64 value:"2"} ` +

			// next we insert one row on the -20 shard
			`sharded.-20: prefix values (:_c1_0) suffix ` +
			`{_c1_0: type:INT64 value:"3"} ` +
			`true false`})

	vc.Rewind()
	err = ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,

		// the select query
		`StreamExecuteMulti dummy_select sharded.-20: {} sharded.20-: {} `,

		// select values into the unowned lookup vindex for routing
		`Execute select from, toc from lkp1 where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"3"} values:{type:INT64 value:"2"} false`,

		// values from lookup vindex resolved to destination
		`ResolveDestinations sharded [value:"0" value:"1" value:"2"] Destinations:DestinationKeyspaceID(31),DestinationKeyspaceID(32),DestinationKeyspaceID(33)`,

		// insert values into the main table
		`ExecuteMultiShard ` +
			// first we insert two rows on the 20- shard
			`sharded.20-: prefix values (:_c0_0), (:_c2_0) suffix ` +
			`{_c0_0: type:INT64 value:"1" _c2_0: type:INT64 value:"2"} ` +

			// next we insert one row on the -20 shard
			`sharded.-20: prefix values (:_c1_0) suffix ` +
			`{_c1_0: type:INT64 value:"3"} ` +
			`true false`})
}

func TestInsertSelectShardingCases(t *testing.T) {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sks1": {
				Sharded:  true,
				Vindexes: map[string]*vschemapb.Vindex{"hash": {Type: "hash"}},
				Tables: map[string]*vschemapb.Table{
					"s1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"}}}}}},
			"sks2": {
				Sharded:  true,
				Vindexes: map[string]*vschemapb.Vindex{"hash": {Type: "hash"}},
				Tables: map[string]*vschemapb.Table{
					"s2": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"}}}}}},
			"uks1": {Tables: map[string]*vschemapb.Table{"u1": {}}},
			"uks2": {Tables: map[string]*vschemapb.Table{"u2": {}}},
		}}

	vs := vindexes.BuildVSchema(invschema)
	sks1 := vs.Keyspaces["sks1"]
	sks2 := vs.Keyspaces["sks2"]
	uks1 := vs.Keyspaces["uks1"]
	uks2 := vs.Keyspaces["uks2"]

	// sharded input route.
	sRoute := &Route{
		Query:             "dummy_select",
		FieldQuery:        "dummy_field_query",
		RoutingParameters: &RoutingParameters{Opcode: Scatter, Keyspace: sks2.Keyspace}}

	// unsharded input route.
	uRoute := &Route{
		Query:             "dummy_select",
		FieldQuery:        "dummy_field_query",
		RoutingParameters: &RoutingParameters{Opcode: Unsharded, Keyspace: uks2.Keyspace}}

	// sks1 and sks2
	ins := &Insert{
		Opcode:            InsertSelect,
		Keyspace:          sks1.Keyspace,
		Query:             "dummy_insert",
		Table:             sks1.Tables["s1"],
		Prefix:            "prefix ",
		Suffix:            " suffix",
		ColVindexes:       sks1.Tables["s1"].ColumnVindexes,
		VindexValueOffset: [][]int{{0}},
		Input:             sRoute,
	}

	vc := &loggingVCursor{
		resolvedTargetTabletType: topodatapb.TabletType_PRIMARY,
		ksShardMap: map[string][]string{
			"sks1": {"-20", "20-"},
			"sks2": {"-20", "20-"},
			"uks1": {"0"},
			"uks2": {"0"},
		},
	}
	vc.results = []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1")}

	_, err := ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// the select query
		`ResolveDestinations sks2 [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard sks2.-20: dummy_select {} sks2.20-: dummy_select {} false false`,

		// the query exec
		`ResolveDestinations sks1 [value:"0"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard sks1.-20: prefix values (:_c0_0) suffix {_c0_0: type:INT64 value:"1"} true true`})

	vc.Rewind()
	err = ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// the select query
		`ResolveDestinations sks2 [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select sks2.-20: {} sks2.20-: {} `,

		// the query exec
		`ResolveDestinations sks1 [value:"0"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard sks1.-20: prefix values (:_c0_0) suffix {_c0_0: type:INT64 value:"1"} true true`})

	// sks1 and uks2
	ins.Input = uRoute

	vc.Rewind()
	_, err = ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// the select query
		`ResolveDestinations uks2 [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard uks2.0: dummy_select {} false false`,

		// the query exec
		`ResolveDestinations sks1 [value:"0"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard sks1.-20: prefix values (:_c0_0) suffix {_c0_0: type:INT64 value:"1"} true true`})

	vc.Rewind()
	err = ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// the select query
		`ResolveDestinations uks2 [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select uks2.0: {} `,

		// the query exec
		`ResolveDestinations sks1 [value:"0"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard sks1.-20: prefix values (:_c0_0) suffix {_c0_0: type:INT64 value:"1"} true true`})

	// uks1 and sks2
	ins = &Insert{
		Opcode:   InsertUnsharded,
		Keyspace: uks1.Keyspace,
		Query:    "dummy_insert",
		Table:    uks1.Tables["s1"],
		Prefix:   "prefix ",
		Suffix:   " suffix",
		Input:    sRoute,
	}

	vc.Rewind()
	_, err = ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// the select query
		`ResolveDestinations sks2 [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard sks2.-20: dummy_select {} sks2.20-: dummy_select {} false false`,

		// the query exec
		`ResolveDestinations uks1 [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard uks1.0: prefix values (:_c0_0) suffix {_c0_0: type:INT64 value:"1"} true true`})

	vc.Rewind()
	err = ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// the select query
		`ResolveDestinations sks2 [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select sks2.-20: {} sks2.20-: {} `,

		// the query exec
		`ResolveDestinations uks1 [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard uks1.0: prefix values (:_c0_0) suffix {_c0_0: type:INT64 value:"1"} true true`})

	// uks1 and uks2
	ins.Input = uRoute

	vc.Rewind()
	_, err = ins.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// the select query
		`ResolveDestinations uks2 [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard uks2.0: dummy_select {} false false`,

		// the query exec
		`ResolveDestinations uks1 [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard uks1.0: prefix values (:_c0_0) suffix {_c0_0: type:INT64 value:"1"} true true`})

	vc.Rewind()
	err = ins.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// the select query
		`ResolveDestinations uks2 [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti dummy_select uks2.0: {} `,

		// the query exec
		`ResolveDestinations uks1 [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard uks1.0: prefix values (:_c0_0) suffix {_c0_0: type:INT64 value:"1"} true true`})
}
