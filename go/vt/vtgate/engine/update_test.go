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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func TestUpdateUnsharded(t *testing.T) {
	upd := &Update{
		DML: DML{
			Opcode: Unsharded,
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: false,
			},
			Query: "dummy_update",
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_update {} true true`,
	})

	// Failure cases
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `shard_error`)

	vc = &loggingVCursor{}
	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `Keyspace does not have exactly one shard: []`)
}

func TestUpdateEqual(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	upd := &Update{
		DML: DML{
			Opcode: Equal,
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			Query:  "dummy_update",
			Vindex: vindex.(vindexes.SingleColumn),
			Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_update {} true true`,
	})

	// Failure case
	upd.Values = []sqltypes.PlanValue{{Key: "aa"}}
	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `missing bind var aa`)
}

func TestUpdateScatter(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	upd := &Update{
		DML: DML{
			Opcode: Scatter,
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			Query:  "dummy_update",
			Vindex: vindex.(vindexes.SingleColumn),
			Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
		},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_update {} ks.20-: dummy_update {} true false`,
	})

	// works with multishard autocommit
	upd = &Update{
		DML: DML{
			Opcode: Scatter,
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			Query:                "dummy_update",
			Vindex:               vindex.(vindexes.SingleColumn),
			Values:               []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
			MultiShardAutocommit: true,
		},
	}

	vc = newDMLTestVCursor("-20", "20-")
	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
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
		DML: DML{
			Opcode: Equal,
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			Query:  "dummy_update",
			Vindex: vindex.(vindexes.SingleColumn),
			Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		// This lookup query will return no rows. So, the DML will not be sent anywhere.
		`Execute select from, toc from lkp where from in ::from from: type:TUPLE values:<type:INT64 value:"1" >  false`,
	})
}

func TestUpdateEqualNoScatter(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	upd := &Update{
		DML: DML{
			Opcode: Equal,
			Keyspace: &vindexes.Keyspace{
				Name:    "ks",
				Sharded: true,
			},
			Query:  "dummy_update",
			Vindex: vindex.(vindexes.SingleColumn),
			Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
		},
	}

	vc := newDMLTestVCursor("0")
	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.EqualError(t, err, `cannot map vindex to unique keyspace id: DestinationKeyRange(-)`)
}

func TestUpdateEqualChangedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: DML{
			Opcode:           Equal,
			Keyspace:         ks.Keyspace,
			Query:            "dummy_update",
			Vindex:           ks.Vindexes["hash"].(vindexes.SingleColumn),
			Values:           []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
			Table:            ks.Tables["t1"],
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["hash"].(vindexes.SingleColumn),
		},
		ChangedVindexValues: map[string]*VindexValues{
			"twocol": {
				PvMap: map[string]sqltypes.PlanValue{
					"c1": {Value: sqltypes.NewInt64(1)},
					"c2": {Value: sqltypes.NewInt64(2)},
				},
				Offset: 4,
			},
			"onecol": {
				PvMap: map[string]sqltypes.PlanValue{
					"c3": {Value: sqltypes.NewInt64(3)},
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

	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// No rows changing
	vc = newDMLTestVCursor("-20", "20-")

	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
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

	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 7,8 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 9 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
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

	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 9 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

}

func TestUpdateScatterChangedVindex(t *testing.T) {
	// update t1 set c1 = 1, c2 = 2, c3 = 3
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: DML{
			Opcode:           Scatter,
			Keyspace:         ks.Keyspace,
			Query:            "dummy_update",
			Table:            ks.Tables["t1"],
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["hash"].(vindexes.SingleColumn),
		},
		ChangedVindexValues: map[string]*VindexValues{
			"twocol": {
				PvMap: map[string]sqltypes.PlanValue{
					"c1": {Value: sqltypes.NewInt64(1)},
					"c2": {Value: sqltypes.NewInt64(2)},
				},
				Offset: 4,
			},
			"onecol": {
				PvMap: map[string]sqltypes.PlanValue{
					"c3": {Value: sqltypes.NewInt64(3)},
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

	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard sharded.-20: dummy_subquery {} sharded.20-: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} sharded.20-: dummy_update {} true false`,
	})

	// No rows changing
	vc = newDMLTestVCursor("-20", "20-")

	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
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

	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationAllShards()`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} sharded.20-: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Those values are returned as 7,8 for twocol and 9 for onecol.
		// 7,8 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 9 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} sharded.20-: dummy_update {} true false`,
	})

}

func TestUpdateIn(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{DML: DML{
		Opcode:   In,
		Keyspace: ks.Keyspace,
		Query:    "dummy_update",
		Vindex:   ks.Vindexes["hash"].(vindexes.SingleColumn),
		Values: []sqltypes.PlanValue{{
			Values: []sqltypes.PlanValue{
				{Value: sqltypes.NewInt64(1)},
				{Value: sqltypes.NewInt64(2)},
			}},
		}},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestUpdateInChangedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	upd := &Update{
		DML: DML{
			Opcode:   In,
			Keyspace: ks.Keyspace,
			Query:    "dummy_update",
			Vindex:   ks.Vindexes["hash"].(vindexes.SingleColumn),
			Values: []sqltypes.PlanValue{{
				Values: []sqltypes.PlanValue{
					{Value: sqltypes.NewInt64(1)},
					{Value: sqltypes.NewInt64(2)},
				}},
			},
			Table:            ks.Tables["t1"],
			OwnedVindexQuery: "dummy_subquery",
			KsidVindex:       ks.Vindexes["hash"].(vindexes.SingleColumn),
		},
		ChangedVindexValues: map[string]*VindexValues{
			"twocol": {
				PvMap: map[string]sqltypes.PlanValue{
					"c1": {Value: sqltypes.NewInt64(1)},
					"c2": {Value: sqltypes.NewInt64(2)},
				},
				Offset: 4,
			},
			"onecol": {
				PvMap: map[string]sqltypes.PlanValue{
					"c3": {Value: sqltypes.NewInt64(3)},
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

	_, err := upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 21,22 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"21" from2: type:INT64 value:"22" toc: type:VARBINARY value:"\006\347\352\"\316\222p\217"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\006\347\352\"\316\222p\217"  true`,
		// 23 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"23" toc: type:VARBINARY value:"\006\347\352\"\316\222p\217"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\006\347\352\"\316\222p\217"  true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})

	// No rows changing
	vc = newDMLTestVCursor("-20", "20-")

	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
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

	_, err = upd.Execute(vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		// 4,5 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 6 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 7,8 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 9 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// 21,22 have to be replaced by 1,2 (the new values).
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"21" from2: type:INT64 value:"22" toc: type:VARBINARY value:"\006\347\352\"\316\222p\217"  true`,
		`Execute insert into lkp2(from1, from2, toc) values(:from1_0, :from2_0, :toc_0) from1_0: type:INT64 value:"1" from2_0: type:INT64 value:"2" toc_0: type:VARBINARY value:"\006\347\352\"\316\222p\217"  true`,
		// 23 has to be replaced by 3.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"23" toc: type:VARBINARY value:"\006\347\352\"\316\222p\217"  true`,
		`Execute insert into lkp1(from, toc) values(:from_0, :toc_0) from_0: type:INT64 value:"3" toc_0: type:VARBINARY value:"\006\347\352\"\316\222p\217"  true`,
		// Finally, the actual update, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_update {} true true`,
	})
}

func TestUpdateNoStream(t *testing.T) {
	upd := &Update{}
	err := upd.StreamExecute(nil, nil, false, nil)
	require.EqualError(t, err, `query "" cannot be used for streaming`)
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
					"t2": {
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
		panic(err)
	}
	return vs
}

func newDMLTestVCursor(shards ...string) *loggingVCursor {
	return &loggingVCursor{shards: shards, resolvedTargetTabletType: topodatapb.TabletType_MASTER}
}
