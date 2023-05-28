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

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestUpdateUnsharded(t *testing.T) {
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: Unsharded,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: false,
				},
			},

			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_update {} true true`,
	})

	// Failure cases
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `shard_error`)

	vc = &loggingVCursor{}
	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `Keyspace 'ks' does not have exactly one shard: []`)
}

func TestUpdateEqual(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: Equal,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_update {} true true`,
	})

	// Failure case
	upd.Values = []evalengine.Expr{evalengine.NewBindVar("aa")}
	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `query arguments missing for aa`)
}

func TestUpdateEqualMultiCol(t *testing.T) {
	vindex, _ := vindexes.NewRegionExperimental("", map[string]string{"region_bytes": "1"})
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: Equal,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1), evalengine.NewLiteralInt(2)},
			},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		`ExecuteMultiShard ks.-20: dummy_update {} true true`,
	})
}

func TestUpdateScatter(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: Scatter,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_update {} ks.20-: dummy_update {} true false`,
	})

	// works with multishard autocommit
	upd = &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: Scatter,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			Query:                "dummy_update",
			MultiShardAutocommit: true,
		},
	}

	vc = newDMLTestVCursor("-20", "20-")
	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_update {} ks.20-: dummy_update {} true true`,
	})
}

func TestUpdateEqualNoRoute(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: Equal,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// This lookup query will return no rows. So, the DML will not be sent anywhere.
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
	})
}

func TestUpdateEqualNoScatter(t *testing.T) {
	t.Skip("planner does not produces this plan anymore")
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: Equal,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `cannot map vindex to unique keyspace id: DestinationKeyRange(-)`)
}

func TestUpdateEqualChangedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   Equal,
				Keyspace: ks.Keyspace,
				Vindex:   ks.Vindexes["hash"],
				Values:   []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			Query: "dummy_update",
			Table: []*vindexes.Table{
				ks.Tables["t1"],
			},
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["hash"],
			KsidLength:       1,
		},
		ChangedVindexValues: map[string]*VindexValues{
			"twocol": {
				PvMap: map[string]evalengine.Expr{
					"c1": evalengine.NewLiteralInt(1),
					"c2": evalengine.NewLiteralInt(2),
				},
				Offset: 4,
			},
			"onecol": {
				PvMap: map[string]evalengine.Expr{
					"c3": evalengine.NewLiteralInt(3),
				},
				Offset: 5,
			},
		},
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3|twocol|onecol",
			"int64|int64|int64|int64|int64|int64",
		),
		"1|4|5|6|0|0",
	)}
	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// No rows changing
	vc = newDMLTestVCursor("-20", "20-")

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are updated. We still pass-through the original update.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// multiple rows changing.
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3|twocol|onecol",
			"int64|int64|int64|int64|int64|int64",
		),
		"1|4|5|6|0|0",
		"1|7|8|9|0|0",
	)}
	vc = newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 7,8 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 9 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// multiple rows changing, but only some vindex actually changes
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3|twocol|onecol",
			"int64|int64|int64|int64|int64|int64",
		),
		"1|4|5|6|0|1", // twocol changes
		"1|7|8|9|1|0", // onecol changes
	)}
	vc = newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 9 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

}

func TestUpdateEqualMultiColChangedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   Equal,
				Keyspace: ks.Keyspace,
				Vindex:   ks.Vindexes["rg_vdx"],
				Values:   []evalengine.Expr{evalengine.NewLiteralInt(1), evalengine.NewLiteralInt(2)},
			},
			Query: "dummy_update",
			Table: []*vindexes.Table{
				ks.Tables["rg_tbl"],
			},
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["rg_vdx"],
			KsidLength:       2,
		},
		ChangedVindexValues: map[string]*VindexValues{
			"lkp_rg": {
				PvMap: map[string]evalengine.Expr{
					"colc": evalengine.NewLiteralInt(5),
				},
				Offset: 3,
			},
		},
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"cola|colb|colc|colc=5",
			"int64|int64|int64|int64",
		),
		"1|2|4|0",
	)}
	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// 4 has to be replaced by 5.
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"4" toc: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		`Execute insert into lkp_rg_tbl(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"5" toc_0: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// No rows changing
	vc.Rewind()
	vc.results = nil
	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are updated. We still pass-through the original update.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// multiple rows changing.
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"cola|colb|colc|colc=5",
			"int64|int64|int64|int64",
		),
		"1|2|4|0",
		"1|2|6|0",
	)}
	vc.Rewind()
	vc.results = results

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// 4 has to be replaced by 5.
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"4" toc: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		`Execute insert into lkp_rg_tbl(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"5" toc_0: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		// 6 has to be replaced by 5.
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		`Execute insert into lkp_rg_tbl(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"5" toc_0: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// multiple rows changing, but only some rows actually changes
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"cola|colb|colc|colc=5",
			"int64|int64|int64|int64",
		),
		"1|2|5|1",
		"1|2|7|0",
	)}
	vc.Rewind()
	vc.results = results

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// 7 has to be replaced by 5.
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"7" toc: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		`Execute insert into lkp_rg_tbl(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"5" toc_0: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestUpdateScatterChangedVindex(t *testing.T) {
	// update t1 set c1 = 1, c2 = 2, c3 = 3
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace,
			},
			Query: "dummy_update",
			Table: []*vindexes.Table{
				ks.Tables["t1"],
			},
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["hash"],
			KsidLength:       1,
		},
		ChangedVindexValues: map[string]*VindexValues{
			"twocol": {
				PvMap: map[string]evalengine.Expr{
					"c1": evalengine.NewLiteralInt(1),
					"c2": evalengine.NewLiteralInt(2),
				},
				Offset: 4,
			},
			"onecol": {
				PvMap: map[string]evalengine.Expr{
					"c3": evalengine.NewLiteralInt(3),
				},
				Offset: 5,
			},
		},
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3|twocol|onecol",
			"int64|int64|int64|int64|int64|int64",
		),
		"1|4|5|6|0|0",
	)}
	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard sharded.-20: dummy_subquery {} sharded.20-: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} sharded.20-: dummy_update {} true false`,
	})

	// No rows changing
	vc = newDMLTestVCursor("-20", "20-")

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} sharded.20-: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are deleted. We still pass-through the original delete.
		`ExecuteMultiShard sharded.-20: dummy_update {} sharded.20-: dummy_update {} true false`,
	})

	// Update can affect multiple rows
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3|twocol|onecol",
			"int64|int64|int64|int64|int64|int64",
		),
		"1|4|5|6|0|0",
		"1|7|8|9|0|0",
	)}
	vc = newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} sharded.20-: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Those values are returned as 7,8 for twocol and 9 for onecol.
		// 7,8 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 9 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} sharded.20-: dummy_update {} true false`,
	})

}

func TestUpdateIn(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   IN,
				Keyspace: ks.Keyspace,
				Vindex:   ks.Vindexes["hash"],
				Values: []evalengine.Expr{evalengine.TupleExpr{
					evalengine.NewLiteralInt(1),
					evalengine.NewLiteralInt(2),
				}}},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1" type:INT64 value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestUpdateInStreamExecute(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{DML: &DML{
		RoutingParameters: &RoutingParameters{
			Opcode:   IN,
			Keyspace: ks.Keyspace,
			Vindex:   ks.Vindexes["hash"],
			Values: []evalengine.Expr{evalengine.TupleExpr{
				evalengine.NewLiteralInt(1),
				evalengine.NewLiteralInt(2),
			}}},
		Query: "dummy_update",
	}}

	vc := newDMLTestVCursor("-20", "20-")
	err := upd.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1" type:INT64 value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestUpdateInMultiCol(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{DML: &DML{
		RoutingParameters: &RoutingParameters{
			Opcode:   IN,
			Keyspace: ks.Keyspace,
			Vindex:   ks.Vindexes["rg_vdx"],
			Values: []evalengine.Expr{
				evalengine.TupleExpr{evalengine.NewLiteralInt(1), evalengine.NewLiteralInt(2)},
				evalengine.TupleExpr{evalengine.NewLiteralInt(3), evalengine.NewLiteralInt(4)},
			},
		},
		Query: "dummy_update",
	}}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(3)] [INT64(1) INT64(4)] [INT64(2) INT64(3)] [INT64(2) INT64(4)]] Destinations:DestinationKeyspaceID(014eb190c9a2fa169c),DestinationKeyspaceID(01d2fd8867d50d2dfe),DestinationKeyspaceID(024eb190c9a2fa169c),DestinationKeyspaceID(02d2fd8867d50d2dfe)`,
		// ResolveDestinations is hard-coded to return -20.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestUpdateInChangedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   IN,
				Keyspace: ks.Keyspace,
				Vindex:   ks.Vindexes["hash"],
				Values: []evalengine.Expr{evalengine.TupleExpr{
					evalengine.NewLiteralInt(1),
					evalengine.NewLiteralInt(2),
				}},
			},
			Query: "dummy_update",
			Table: []*vindexes.Table{
				ks.Tables["t1"],
			},
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["hash"],
			KsidLength:       1,
		},
		ChangedVindexValues: map[string]*VindexValues{
			"twocol": {
				PvMap: map[string]evalengine.Expr{
					"c1": evalengine.NewLiteralInt(1),
					"c2": evalengine.NewLiteralInt(2),
				},
				Offset: 4,
			},
			"onecol": {
				PvMap: map[string]evalengine.Expr{
					"c3": evalengine.NewLiteralInt(3),
				},
				Offset: 5,
			},
		},
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3|twocol|onecol",
			"int64|int64|int64|int64|int64|int64",
		),
		"1|4|5|6|0|0",
		"2|21|22|23|0|0",
	)}
	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1" type:INT64 value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 21,22 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"21" from2: type:INT64 value:"22" toc: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,
		// 23 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"23" toc: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// No rows changing
	vc = newDMLTestVCursor("-20", "20-")

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1" type:INT64 value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are updated. We still pass-through the original update.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// multiple rows changing.
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3|twocol|onecol",
			"int64|int64|int64|int64|int64|int64",
		),
		"1|4|5|6|0|0",
		"1|7|8|9|0|0",
		"2|21|22|23|0|0",
	)}
	vc = newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1" type:INT64 value:"2"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 7,8 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 9 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// 21,22 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"21" from2: type:INT64 value:"22" toc: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,
		// 23 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"23" toc: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\x06\xe7\xea\"Βp\x8f" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestUpdateInChangedVindexMultiCol(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   IN,
				Keyspace: ks.Keyspace,
				Vindex:   ks.Vindexes["rg_vdx"],
				Values: []evalengine.Expr{
					evalengine.TupleExpr{evalengine.NewLiteralInt(1), evalengine.NewLiteralInt(2)},
					evalengine.NewLiteralInt(3),
				},
			},
			Query: "dummy_update",
			Table: []*vindexes.Table{
				ks.Tables["rg_tbl"],
			},
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["rg_vdx"],
			KsidLength:       2,
		},
		ChangedVindexValues: map[string]*VindexValues{
			"lkp_rg": {
				PvMap: map[string]evalengine.Expr{
					"colc": evalengine.NewLiteralInt(5),
				},
				Offset: 3,
			},
		},
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"cola|colb|colc|colc=5",
			"int64|int64|int64|int64",
		),
		"1|3|4|0",
		"2|3|5|1",
		"1|3|6|0",
		"2|3|7|0",
	)}
	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(3)] [INT64(2) INT64(3)]] Destinations:DestinationKeyspaceID(014eb190c9a2fa169c),DestinationKeyspaceID(024eb190c9a2fa169c)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5,6 and 7 for colc, but 5 is unchanged so only 3 rows will be updated.
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"4" toc: type:VARBINARY value:"\x01N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute insert into lkp_rg_tbl(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"5" toc_0: type:VARBINARY value:"\x01N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x01N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute insert into lkp_rg_tbl(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"5" toc_0: type:VARBINARY value:"\x01N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"7" toc: type:VARBINARY value:"\x02N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute insert into lkp_rg_tbl(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"5" toc_0: type:VARBINARY value:"\x02N\xb1\x90ɢ\xfa\x16\x9c" true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestUpdateEqualSubshard(t *testing.T) {
	vindex, _ := vindexes.NewRegionExperimental("", map[string]string{"region_bytes": "1"})
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: SubShard,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: true,
				},
				Vindex: vindex,
				Values: []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"-20", "20-"}
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1)]] Destinations:DestinationKeyRange(01-02)`,
		`ExecuteMultiShard ks.-20: dummy_update {} ks.20-: dummy_update {} true false`,
	})

	vc.Rewind()
	// as it is single shard so autocommit should be allowed.
	vc.shardForKsid = []string{"-20"}
	_, err = upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1)]] Destinations:DestinationKeyRange(01-02)`,
		`ExecuteMultiShard ks.-20: dummy_update {} true true`,
	})
}

func TestUpdateMultiEqual(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   MultiEqual,
				Keyspace: ks.Keyspace,
				Vindex:   ks.Vindexes["hash"],
				Values: []evalengine.Expr{evalengine.NewTupleExpr(
					evalengine.NewLiteralInt(1),
					evalengine.NewLiteralInt(5),
				)},
			},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"-20", "20-"}
	_, err := upd.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1" type:INT64 value:"5"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(70bb023c810ca87a)`,
		`ExecuteMultiShard sharded.-20: dummy_update {} sharded.20-: dummy_update {} true false`,
	})
}

func buildTestVSchema() *vindexes.VSchema {
	invschema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"sharded": {
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"hash": {
						Type: "hash",
					},
					"rg_vdx": {
						Type: "region_experimental",
						Params: map[string]string{
							"region_bytes": "1",
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
					"lkp_rg": {
						Type: "lookup",
						Params: map[string]string{
							"table": "lkp_rg_tbl",
							"from":  "from",
							"to":    "toc",
						},
						Owner: "rg_tbl",
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
					"t2": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "hash",
							Columns: []string{"id"},
						}},
					},
					"rg_tbl": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{
							Name:    "rg_vdx",
							Columns: []string{"cola", "colb"},
						}, {
							Name:    "lkp_rg",
							Columns: []string{"colc"},
						}},
					},
				},
			},
		},
	}
	vs := vindexes.BuildVSchema(invschema)
	return vs
}

func newDMLTestVCursor(shards ...string) *loggingVCursor {
	return &loggingVCursor{shards: shards, resolvedTargetTabletType: topodatapb.TabletType_PRIMARY}
}
