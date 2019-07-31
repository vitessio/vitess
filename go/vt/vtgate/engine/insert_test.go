/*
Copyright 2018 The Vitess Authors.

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

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{{
			InsertID: 4,
		}},
	}
	result, err := ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
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
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execInsertUnsharded: shard_error")

	vc = &loggingVCursor{}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "Keyspace does not have exactly one shard: []")
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
		Values: sqltypes.PlanValue{
			Values: []sqltypes.PlanValue{
				{Value: sqltypes.NewInt64(1)},
				{Value: sqltypes.NULL},
				{Value: sqltypes.NewInt64(2)},
				{Value: sqltypes.NULL},
				{Value: sqltypes.NewInt64(3)},
			},
		},
	}

	vc := &loggingVCursor{
		shards: []string{"0"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"nextval",
					"int64",
				),
				"4",
			),
			{InsertID: 1},
		},
	}
	result, err := ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Fetch two sequence value.
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone dummy_generate n: type:INT64 value:"2"  ks2 0`,
		// Fill those values into the insert.
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_insert {__seq0: type:INT64 value:"1" __seq1: type:INT64 value:"4" __seq2: type:INT64 value:"2" __seq3: type:INT64 value:"5" __seq4: type:INT64 value:"3" } true true`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerate.
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	// A single row insert should be autocommitted
	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// 3 rows.
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1"},
		" suffix",
	)
	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20", "20-"},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Based on shardForKsid, values returned will be 20-.
		`ResolveDestinations sharded [value:"0" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6 */ {_id0: type:INT64 value:"1" } ` +
			`true true`,
	})

	// Multiple rows are not autocommitted by default
	ins = NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// 3 rows.
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}, {
					Value: sqltypes.NewInt64(2),
				}, {
					Value: sqltypes.NewInt64(3),
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)
	vc = &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20", "20-"},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0"  value:"1"  value:"2" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6,4eb190c9a2fa169c */ {_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`sharded.-20: prefix mid2 suffix /* vtgate:: keyspace_id:06e7ea22ce92708f */ {_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`true false`,
	})

	// Optional flag overrides autocommit
	ins = NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// 3 rows.
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}, {
					Value: sqltypes.NewInt64(2),
				}, {
					Value: sqltypes.NewInt64(3),
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)
	ins.MultiShardAutocommit = true

	vc = &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20", "20-"},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0"  value:"1"  value:"2" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6,4eb190c9a2fa169c */ {_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`sharded.-20: prefix mid2 suffix /* vtgate:: keyspace_id:06e7ea22ce92708f */ {_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// 1 row
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := &loggingVCursor{}

	// The lookup will fail to map to a keyspace id.
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execInsertSharded: getInsertShardedRoute: could not map INT64(1) to a keyspace id")
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// 3 rows.
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}, {
					Value: sqltypes.NewInt64(2),
				}, {
					Value: sqltypes.NewInt64(3),
				}},
			}},
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
		Values: sqltypes.PlanValue{
			Values: []sqltypes.PlanValue{
				{Value: sqltypes.NewInt64(1)},
				{Value: sqltypes.NULL},
				{Value: sqltypes.NewInt64(2)},
			},
		},
	}

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20", "20-"},
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields(
					"nextval",
					"int64",
				),
				"2",
			),
			{InsertID: 1},
		},
	}
	result, err := ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks2 [] Destinations:DestinationAnyShard()`,
		`ExecuteStandalone dummy_generate n: type:INT64 value:"1"  ks2 -20`,
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0"  value:"1"  value:"2" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		// Row 2 will go to -20, rows 1 & 3 will go to 20-
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6,4eb190c9a2fa169c */ ` +
			`{__seq0: type:INT64 value:"1" __seq1: type:INT64 value:"2" __seq2: type:INT64 value:"2" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`sharded.-20: prefix mid2 suffix /* vtgate:: keyspace_id:06e7ea22ce92708f */ ` +
			`{__seq0: type:INT64 value:"1" __seq1: type:INT64 value:"2" __seq2: type:INT64 value:"2" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`true false`,
	})

	// The insert id returned by ExecuteMultiShard should be overwritten by processGenerate.
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}, {
					Value: sqltypes.NewInt64(2),
				}, {
					Value: sqltypes.NewInt64(3),
				}},
			}},
		}, {
			// colVindex columns: c1, c2
			Values: []sqltypes.PlanValue{{
				// rows for c1
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(4),
				}, {
					Value: sqltypes.NewInt64(5),
				}, {
					Value: sqltypes.NewInt64(6),
				}},
			}, {
				// rows for c2
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(7),
				}, {
					Value: sqltypes.NewInt64(8),
				}, {
					Value: sqltypes.NewInt64(9),
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(10),
				}, {
					Value: sqltypes.NewInt64(11),
				}, {
					Value: sqltypes.NewInt64(12),
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20", "20-"},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute insert into lkp2(from1, from2, toc) values(:from10, :from20, :toc0), (:from11, :from21, :toc1), (:from12, :from22, :toc2) ` +
			`from10: type:INT64 value:"4" from11: type:INT64 value:"5" from12: type:INT64 value:"6" ` +
			`from20: type:INT64 value:"7" from21: type:INT64 value:"8" from22: type:INT64 value:"9" ` +
			`toc0: type:VARBINARY value:"\026k@\264J\272K\326" toc1: type:VARBINARY value:"\006\347\352\"\316\222p\217" toc2: type:VARBINARY value:"N\261\220\311\242\372\026\234"  true`,
		`Execute insert into lkp1(from, toc) values(:from0, :toc0), (:from1, :toc1), (:from2, :toc2) ` +
			`from0: type:INT64 value:"10" from1: type:INT64 value:"11" from2: type:INT64 value:"12" ` +
			`toc0: type:VARBINARY value:"\026k@\264J\272K\326" toc1: type:VARBINARY value:"\006\347\352\"\316\222p\217" toc2: type:VARBINARY value:"N\261\220\311\242\372\026\234"  true`,
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0"  value:"1"  value:"2" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6,4eb190c9a2fa169c */ ` +
			`{_c10: type:INT64 value:"4" _c11: type:INT64 value:"5" _c12: type:INT64 value:"6" ` +
			`_c20: type:INT64 value:"7" _c21: type:INT64 value:"8" _c22: type:INT64 value:"9" ` +
			`_c30: type:INT64 value:"10" _c31: type:INT64 value:"11" _c32: type:INT64 value:"12" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`sharded.-20: prefix mid2 suffix /* vtgate:: keyspace_id:06e7ea22ce92708f */ ` +
			`{_c10: type:INT64 value:"4" _c11: type:INT64 value:"5" _c12: type:INT64 value:"6" ` +
			`_c20: type:INT64 value:"7" _c21: type:INT64 value:"8" _c22: type:INT64 value:"9" ` +
			`_c30: type:INT64 value:"10" _c31: type:INT64 value:"11" _c32: type:INT64 value:"12" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
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
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NULL,
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20", "20-"},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute insert into lkp1(from, toc) values(:from0, :toc0) from0: toc0: type:VARBINARY ` +
			`value:"\026k@\264J\272K\326"  true`,
		`ResolveDestinations sharded [value:"0" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard sharded.20-: prefix mid1 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6 */ ` +
			`{_c30: _id0: type:INT64 value:"1" } true true`,
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertShardedIgnore,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}, {
					Value: sqltypes.NewInt64(2),
				}, {
					Value: sqltypes.NewInt64(3),
				}, {
					Value: sqltypes.NewInt64(4),
				}},
			}},
		}, {
			// colVindex columns: c1, c2
			Values: []sqltypes.PlanValue{{
				// rows for c1
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(5),
				}, {
					Value: sqltypes.NewInt64(6),
				}, {
					Value: sqltypes.NewInt64(7),
				}, {
					Value: sqltypes.NewInt64(8),
				}},
			}, {
				// rows for c2
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(9),
				}, {
					Value: sqltypes.NewInt64(10),
				}, {
					Value: sqltypes.NewInt64(11),
				}, {
					Value: sqltypes.NewInt64(12),
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(13),
				}, {
					Value: sqltypes.NewInt64(14),
				}, {
					Value: sqltypes.NewInt64(15),
				}, {
					Value: sqltypes.NewInt64(16),
				}},
			}},
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
	noresult := &sqltypes.Result{}
	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20"},
		results: []*sqltypes.Result{
			// primary vindex lookups: fail row 2.
			ksid0,
			noresult,
			ksid0,
			ksid0,
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
		},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute select toc from prim where from1 = :from1 from1: type:INT64 value:"1"  false`,
		`Execute select toc from prim where from1 = :from1 from1: type:INT64 value:"2"  false`,
		`Execute select toc from prim where from1 = :from1 from1: type:INT64 value:"3"  false`,
		`Execute select toc from prim where from1 = :from1 from1: type:INT64 value:"4"  false`,
		`Execute insert ignore into lkp2(from1, from2, toc) values(:from10, :from20, :toc0), (:from11, :from21, :toc1), (:from12, :from22, :toc2) ` +
			`from10: type:INT64 value:"5" from11: type:INT64 value:"7" from12: type:INT64 value:"8" ` +
			`from20: type:INT64 value:"9" from21: type:INT64 value:"11" from22: type:INT64 value:"12" ` +
			`toc0: type:VARBINARY value:"\000" toc1: type:VARBINARY value:"\000" toc2: type:VARBINARY value:"\000"  ` +
			`true`,
		// row 2 is out because it didn't map to a ksid.
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"5" toc: type:VARBINARY value:"\000"  false`,
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"7" toc: type:VARBINARY value:"\000"  false`,
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"8" toc: type:VARBINARY value:"\000"  false`,
		`Execute insert ignore into lkp1(from, toc) values(:from0, :toc0), (:from1, :toc1) ` +
			`from0: type:INT64 value:"13" from1: type:INT64 value:"16" ` +
			`toc0: type:VARBINARY value:"\000" toc1: type:VARBINARY value:"\000"  ` +
			`true`,
		// row 3 is out because it failed Verify. Only two verifications from lkp1.
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"13" toc: type:VARBINARY value:"\000"  false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"16" toc: type:VARBINARY value:"\000"  false`,
		`ResolveDestinations sharded [value:"0"  value:"3" ] Destinations:DestinationKeyspaceID(00),DestinationKeyspaceID(00)`,
		// Bind vars for rows 2 & 3 may be missing because they were not sent.
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1 suffix /* vtgate:: keyspace_id:00 */ ` +
			`{_c10: type:INT64 value:"5" _c12: type:INT64 value:"7" _c13: type:INT64 value:"8" ` +
			`_c20: type:INT64 value:"9" _c22: type:INT64 value:"11" _c23: type:INT64 value:"12" ` +
			`_c30: type:INT64 value:"13" _c33: type:INT64 value:"16" ` +
			`_id0: type:INT64 value:"1" _id2: type:INT64 value:"3" _id3: type:INT64 value:"4" } ` +
			`sharded.-20: prefix mid4 suffix /* vtgate:: keyspace_id:00 */ ` +
			`{_c10: type:INT64 value:"5" _c12: type:INT64 value:"7" _c13: type:INT64 value:"8" ` +
			`_c20: type:INT64 value:"9" _c22: type:INT64 value:"11" _c23: type:INT64 value:"12" ` +
			`_c30: type:INT64 value:"13" _c33: type:INT64 value:"16" ` +
			`_id0: type:INT64 value:"1" _id2: type:INT64 value:"3" _id3: type:INT64 value:"4" } ` +
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
							Name:    "onecol",
							Columns: []string{"c3"},
						}},
					},
				},
			},
		},
	}
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertShardedIgnore,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NULL,
				}},
			}},
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
	//noresult := &sqltypes.Result{}
	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"-20", "20-"},
		results: []*sqltypes.Result{
			ksid0,
			ksid0,
			ksid0,
		},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`Execute insert ignore into lkp1(from, toc) values(:from0, :toc0) from0: toc0: type:VARBINARY ` +
			`value:"\026k@\264J\272K\326"  true`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: toc: type:VARBINARY value:"\026k@\264J\272K\326"  false`,
		`ResolveDestinations sharded [value:"0" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard sharded.-20: prefix mid1 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6 */ ` +
			`{_c30: _id0: type:INT64 value:"1" } true true`,
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}, {
					Value: sqltypes.NewInt64(2),
				}, {
					Value: sqltypes.NewInt64(3),
				}},
			}},
		}, {
			// colVindex columns: c1, c2
			Values: []sqltypes.PlanValue{{
				// rows for c1
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(4),
				}, {
					Value: sqltypes.NewInt64(5),
				}, {
					Value: sqltypes.NewInt64(6),
				}},
			}, {
				// rows for c2
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(7),
				}, {
					Value: sqltypes.NewInt64(8),
				}, {
					Value: sqltypes.NewInt64(9),
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(10),
				}, {
					Value: sqltypes.NewInt64(11),
				}, {
					Value: sqltypes.NewInt64(12),
				}},
			}},
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

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20", "20-"},
		results: []*sqltypes.Result{
			nonemptyResult,
			nonemptyResult,
			nonemptyResult,
			nonemptyResult,
			nonemptyResult,
			nonemptyResult,
		},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Perform verification for each colvindex.
		// Note that only first column of each colvindex is used.
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"4" toc: type:VARBINARY value:"\026k@\264J\272K\326"  false`,
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"5" toc: type:VARBINARY value:"\006\347\352\"\316\222p\217"  false`,
		`Execute select from1 from lkp2 where from1 = :from1 and toc = :toc from1: type:INT64 value:"6" toc: type:VARBINARY value:"N\261\220\311\242\372\026\234"  false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"10" toc: type:VARBINARY value:"\026k@\264J\272K\326"  false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"11" toc: type:VARBINARY value:"\006\347\352\"\316\222p\217"  false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"12" toc: type:VARBINARY value:"N\261\220\311\242\372\026\234"  false`,
		// Based on shardForKsid, values returned will be 20-, -20, 20-.
		`ResolveDestinations sharded [value:"0"  value:"1"  value:"2" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6,4eb190c9a2fa169c */ ` +
			`{_c10: type:INT64 value:"4" _c11: type:INT64 value:"5" _c12: type:INT64 value:"6" ` +
			`_c20: type:INT64 value:"7" _c21: type:INT64 value:"8" _c22: type:INT64 value:"9" ` +
			`_c30: type:INT64 value:"10" _c31: type:INT64 value:"11" _c32: type:INT64 value:"12" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`sharded.-20: prefix mid2 suffix /* vtgate:: keyspace_id:06e7ea22ce92708f */ ` +
			`{_c10: type:INT64 value:"4" _c11: type:INT64 value:"5" _c12: type:INT64 value:"6" ` +
			`_c20: type:INT64 value:"7" _c21: type:INT64 value:"8" _c22: type:INT64 value:"9" ` +
			`_c30: type:INT64 value:"10" _c31: type:INT64 value:"11" _c32: type:INT64 value:"12" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertShardedIgnore,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}, {
					Value: sqltypes.NewInt64(2),
				}, {
					Value: sqltypes.NewInt64(3),
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(10),
				}, {
					Value: sqltypes.NewInt64(11),
				}, {
					Value: sqltypes.NewInt64(12),
				}},
			}},
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

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20"},
		results: []*sqltypes.Result{
			nonemptyResult,
			// fail verification of second row.
			{},
			nonemptyResult,
		},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// Perform verification for each colvindex.
		// Note that only first column of each colvindex is used.
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"10" toc: type:VARBINARY value:"\026k@\264J\272K\326"  false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"11" toc: type:VARBINARY value:"\006\347\352\"\316\222p\217"  false`,
		`Execute select from from lkp1 where from = :from and toc = :toc from: type:INT64 value:"12" toc: type:VARBINARY value:"N\261\220\311\242\372\026\234"  false`,
		// Based on shardForKsid, values returned will be 20-, -20.
		`ResolveDestinations sharded [value:"0"  value:"2" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6 */ ` +
			`{_c30: type:INT64 value:"10" _c32: type:INT64 value:"12" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`sharded.-20: prefix mid3 suffix /* vtgate:: keyspace_id:4eb190c9a2fa169c */ ` +
			`{_c30: type:INT64 value:"10" _c32: type:INT64 value:"12" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(2),
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execInsertSharded: getInsertShardedRoute: values [[INT64(2)]] for column [c3] does not map to keyspace ids")
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}, {
					Value: sqltypes.NewInt64(2),
				}, {
					Value: sqltypes.NewInt64(3),
				}},
			}},
		}, {
			// colVindex columns: c1, c2
			Values: []sqltypes.PlanValue{{
				// rows for c1
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NULL,
				}, {
					Value: sqltypes.NULL,
				}, {
					Value: sqltypes.NULL,
				}},
			}, {
				// rows for c2
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NULL,
				}, {
					Value: sqltypes.NULL,
				}, {
					Value: sqltypes.NULL,
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NULL,
				}, {
					Value: sqltypes.NULL,
				}, {
					Value: sqltypes.NULL,
				}},
			}},
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

	vc := &loggingVCursor{
		shards:       []string{"-20", "20-"},
		shardForKsid: []string{"20-", "-20", "20-"},
		results: []*sqltypes.Result{
			nonemptyResult,
		},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [value:"0"  value:"1"  value:"2" ] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(4eb190c9a2fa169c)`,
		`ExecuteMultiShard ` +
			`sharded.20-: prefix mid1, mid3 suffix /* vtgate:: keyspace_id:166b40b44aba4bd6,4eb190c9a2fa169c */ ` +
			`{_c10: type:UINT64 value:"1" _c11: type:UINT64 value:"2" _c12: type:UINT64 value:"3" ` +
			`_c20: _c21: _c22: ` +
			`_c30: type:UINT64 value:"1" _c31: type:UINT64 value:"2" _c32: type:UINT64 value:"3" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`sharded.-20: prefix mid2 suffix /* vtgate:: keyspace_id:06e7ea22ce92708f */ ` +
			`{_c10: type:UINT64 value:"1" _c11: type:UINT64 value:"2" _c12: type:UINT64 value:"3" ` +
			`_c20: _c21: _c22: ` +
			`_c30: type:UINT64 value:"1" _c31: type:UINT64 value:"2" _c32: type:UINT64 value:"3" ` +
			`_id0: type:INT64 value:"1" _id1: type:INT64 value:"2" _id2: type:INT64 value:"3" } ` +
			`true false`,
	})
}

func TestInsertShardedUnownedReverseMapFail(t *testing.T) {
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
	vs, err := vindexes.BuildVSchema(invschema)
	if err != nil {
		t.Fatal(err)
	}
	ks := vs.Keyspaces["sharded"]

	ins := NewInsert(
		InsertSharded,
		ks.Keyspace,
		[]sqltypes.PlanValue{{
			// colVindex columns: id
			Values: []sqltypes.PlanValue{{
				// rows for id
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NewInt64(1),
				}},
			}},
		}, {
			// colVindex columns: c3
			Values: []sqltypes.PlanValue{{
				// rows for c3
				Values: []sqltypes.PlanValue{{
					Value: sqltypes.NULL,
				}},
			}},
		}},
		ks.Tables["t1"],
		"prefix",
		[]string{" mid1", " mid2", " mid3"},
		" suffix",
	)

	vc := &loggingVCursor{
		shards: []string{"-20", "20-"},
	}
	_, err = ins.Execute(vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execInsertSharded: getInsertShardedRoute: value must be supplied for column [c3]")
}
