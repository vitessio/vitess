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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestDeleteUnsharded(t *testing.T) {
	del := &Delete{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode: Unsharded,
				Keyspace: &vindexes.Keyspace{
					Name:    "ks",
					Sharded: false,
				},
			},
			Query: "dummy_delete",
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_delete {} true true`,
	})

	// Failure cases
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "shard_error")

	vc = &loggingVCursor{}
	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "Keyspace 'ks' does not have exactly one shard: []")
}

func TestDeleteEqual(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("hash", "", nil)
	del := &Delete{
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
			Query: "dummy_delete",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_delete {} true true`,
	})

	// Failure case
	expr := evalengine.NewBindVar("aa", sqltypes.Unknown, collations.Unknown)
	del.Values = []evalengine.Expr{expr}
	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "query arguments missing for aa")
}

func TestDeleteEqualMultiCol(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("region_experimental", "", map[string]string{"region_bytes": "1"})
	del := &Delete{
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
			Query: "dummy_delete",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		`ExecuteMultiShard ks.-20: dummy_delete {} true true`,
	})

	// Failure case
	expr := evalengine.NewBindVar("aa", sqltypes.Unknown, collations.Unknown)
	del.Values = []evalengine.Expr{expr}
	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "query arguments missing for aa")
}

func TestDeleteEqualNoRoute(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("lookup_unique", "", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	del := &Delete{
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
			Query: "dummy_delete",
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// This lookup query will return no rows. So, the DML will not be sent anywhere.
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:{type:INT64 value:"1"} false`,
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationNone()`,
	})
}

func TestDeleteEqualNoScatter(t *testing.T) {
	t.Skip("planner does not produces this plan anymore")
	vindex, _ := vindexes.CreateVindex("lookup_unique", "", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	del := &Delete{
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
			Query: "dummy_delete",
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "cannot map vindex to unique keyspace id: DestinationKeyRange(-)")
}

func TestDeleteOwnedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	del := &Delete{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   Equal,
				Keyspace: ks.Keyspace,
				Vindex:   ks.Vindexes["hash"],
				Values:   []evalengine.Expr{evalengine.NewLiteralInt(1)},
			},
			Query: "dummy_delete",
			Table: []*vindexes.Table{
				ks.Tables["t1"],
			},
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["hash"],
			KsidLength:       1,
		},
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3",
			"int64|int64|int64|int64",
		),
		"1|4|5|6",
	)}

	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Finally, the actual delete, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_delete {} true true`,
	})

	// No rows changing
	vc = newDMLTestVCursor("-20", "20-")
	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are deleted. We still pass-through the original delete.
		`ExecuteMultiShard sharded.-20: dummy_delete {} true true`,
	})

	// Delete can affect multiple rows
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3",
			"int64|int64|int64|int64",
		),
		"1|4|5|6",
		"1|7|8|9",
	)}
	vc = newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Delete 4,5 and 7,8 from lkp2.
		// Delete 6 and 8 from lkp1.
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Send the DML.
		`ExecuteMultiShard sharded.-20: dummy_delete {} true true`,
	})
}

func TestDeleteOwnedVindexMultiCol(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	del := &Delete{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   Equal,
				Keyspace: ks.Keyspace,
				Vindex:   ks.Vindexes["rg_vdx"],
				Values:   []evalengine.Expr{evalengine.NewLiteralInt(1), evalengine.NewLiteralInt(2)},
			},
			Query: "dummy_delete",
			Table: []*vindexes.Table{
				ks.Tables["rg_tbl"],
			},
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["rg_vdx"],
			KsidLength:       2,
		},
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"cola|colb|colc",
			"int64|int64|int64",
		),
		"1|2|4",
	)}

	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// 4 returned for lkp_rg.
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"4" toc: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		// Finally, the actual delete, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_delete {} true true`,
	})

	// No rows changing
	vc.Rewind()
	vc.results = nil
	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are deleted. We still pass-through the original delete.
		`ExecuteMultiShard sharded.-20: dummy_delete {} true true`,
	})

	// Delete can affect multiple rows
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"cola|colb|colc",
			"int64|int64|int64",
		),
		"1|2|4",
		"1|2|6",
	)}
	vc.Rewind()
	vc.results = results

	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(2)]] Destinations:DestinationKeyspaceID(0106e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Delete 4 and 6 from lkp_rg.
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"4" toc: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x01\x06\xe7\xea\"Βp\x8f" true`,
		// Send the DML.
		`ExecuteMultiShard sharded.-20: dummy_delete {} true true`,
	})
}

func TestDeleteSharded(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	del := &Delete{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace,
			},
			Query: "dummy_delete",
			Table: []*vindexes.Table{
				ks.Tables["t2"],
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard sharded.-20: dummy_delete {} sharded.20-: dummy_delete {} true false`,
	})

	// Failure case
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, "shard_error")
}

func TestDeleteShardedStreaming(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	del := &Delete{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace,
			},
			Query: "dummy_delete",
			Table: []*vindexes.Table{
				ks.Tables["t2"],
			},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	err := del.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error {
		return nil
	})
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard sharded.-20: dummy_delete {} sharded.20-: dummy_delete {} true false`,
	})
}

func TestDeleteScatterOwnedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	del := &Delete{
		DML: &DML{
			RoutingParameters: &RoutingParameters{
				Opcode:   Scatter,
				Keyspace: ks.Keyspace,
			},
			Query: "dummy_delete",
			Table: []*vindexes.Table{
				ks.Tables["t1"],
			},
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["hash"],
			KsidLength:       1,
		},
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3",
			"int64|int64|int64|int64",
		),
		"1|4|5|6",
	)}

	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} sharded.20-: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Finally, the actual delete, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_delete {} sharded.20-: dummy_delete {} true false`,
	})

	// No rows changing
	vc = newDMLTestVCursor("-20", "20-")

	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} sharded.20-: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are deleted. We still pass-through the original delete.
		`ExecuteMultiShard sharded.-20: dummy_delete {} sharded.20-: dummy_delete {} true false`,
	})

	// Delete can affect multiple rows
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"id|c1|c2|c3",
			"int64|int64|int64|int64",
		),
		"1|4|5|6",
		"1|7|8|9",
	)}
	vc = newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} sharded.20-: dummy_subquery {} false false`,
		// Delete 4,5 and 7,8 from lkp2.
		// Delete 6 and 8 from lkp1.
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\x16k@\xb4J\xbaK\xd6" true`,
		// Send the DML.
		`ExecuteMultiShard sharded.-20: dummy_delete {} sharded.20-: dummy_delete {} true false`,
	})
}

func TestDeleteInChangedVindexMultiCol(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	del := &Delete{
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
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"cola|colb|colc",
			"int64|int64|int64",
		),
		"1|3|4",
		"2|3|5",
		"1|3|6",
		"2|3|7",
	)}
	vc := newDMLTestVCursor("-20", "20-")
	vc.results = results

	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol sharded [[INT64(1) INT64(3)] [INT64(2) INT64(3)]] Destinations:DestinationKeyspaceID(014eb190c9a2fa169c),DestinationKeyspaceID(024eb190c9a2fa169c)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5,6 and 7 for colc
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"4" toc: type:VARBINARY value:"\x01N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"5" toc: type:VARBINARY value:"\x02N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\x01N\xb1\x90ɢ\xfa\x16\x9c" true`,
		`Execute delete from lkp_rg_tbl where from = :from and toc = :toc from: type:INT64 value:"7" toc: type:VARBINARY value:"\x02N\xb1\x90ɢ\xfa\x16\x9c" true`,
		// Finally, the actual delete, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestDeleteEqualSubshard(t *testing.T) {
	vindex, _ := vindexes.CreateVindex("region_experimental", "", map[string]string{"region_bytes": "1"})
	del := &Delete{
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
			Query: "dummy_delete",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"-20", "20-"}
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1)]] Destinations:DestinationKeyRange(01-02)`,
		`ExecuteMultiShard ks.-20: dummy_delete {} ks.20-: dummy_delete {} true false`,
	})

	vc.Rewind()
	// as it is single shard so autocommit should be allowed.
	vc.shardForKsid = []string{"-20"}
	_, err = del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinationsMultiCol ks [[INT64(1)]] Destinations:DestinationKeyRange(01-02)`,
		`ExecuteMultiShard ks.-20: dummy_delete {} true true`,
	})
}

func TestDeleteMultiEqual(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	del := &Delete{
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
			Query: "dummy_delete",
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	vc.shardForKsid = []string{"-20", "20-"}
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [type:INT64 value:"1" type:INT64 value:"5"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(70bb023c810ca87a)`,
		`ExecuteMultiShard sharded.-20: dummy_delete {} sharded.20-: dummy_delete {} true false`,
	})
}
