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
	"context"
	"errors"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestDeleteUnsharded(t *testing.T) {
	del := &Delete{
		Opcode: DeleteUnsharded,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: false,
		},
		Query: "dummy_delete",
	}

	vc := &loggingVCursor{shards: []string{"0"}}
	_, err := del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: dummy_delete {} true true`,
	})

	// Failure cases
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execDeleteUnsharded: shard_error")

	vc = &loggingVCursor{}
	_, err = del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "Keyspace does not have exactly one shard: []")
}

func TestDeleteEqual(t *testing.T) {
	vindex, _ := vindexes.NewHash("", nil)
	del := &Delete{
		Opcode: DeleteEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:  "dummy_delete",
		Vindex: vindex,
		Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	_, err := del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		`ExecuteMultiShard ks.-20: dummy_delete /* vtgate:: keyspace_id:166b40b44aba4bd6 */ {} true true`,
	})

	// Failure case
	del.Values = []sqltypes.PlanValue{{Key: "aa"}}
	_, err = del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execDeleteEqual: missing bind var aa")
}

func TestDeleteEqualNoRoute(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table": "lkp",
		"from":  "from",
		"to":    "toc",
	})
	del := &Delete{
		Opcode: DeleteEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:  "dummy_delete",
		Vindex: vindex,
		Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"0"}}
	_, err := del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		// This lookup query will return no rows. So, the DML will not be sent anywhere.
		`Execute select toc from lkp where from = :from from: type:INT64 value:"1"  false`,
	})
}

func TestDeleteEqualNoScatter(t *testing.T) {
	vindex, _ := vindexes.NewLookupUnique("", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	del := &Delete{
		Opcode: DeleteEqual,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query:  "dummy_delete",
		Vindex: vindex,
		Values: []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
	}

	vc := &loggingVCursor{shards: []string{"0"}}
	_, err := del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execDeleteEqual: cannot map vindex to unique keyspace id: DestinationKeyRange(-)")
}

func TestDeleteOwnedVindex(t *testing.T) {
	ks := buildTestVSchema().Keyspaces["sharded"]
	del := &Delete{
		Opcode:           DeleteEqual,
		Keyspace:         ks.Keyspace,
		Query:            "dummy_delete",
		Vindex:           ks.Vindexes["hash"],
		Values:           []sqltypes.PlanValue{{Value: sqltypes.NewInt64(1)}},
		Table:            ks.Tables["t1"],
		OwnedVindexQuery: "dummy_subquery",
	}

	results := []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|c2|c3",
			"int64|int64|int64",
		),
		"4|5|6",
	)}
	vc := &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: results,
	}

	_, err := del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Those values are returned as 4,5 for twocol and 6 for onecol.
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Finally, the actual delete, which is also sent to -20, same route as the subquery.
		`ExecuteMultiShard sharded.-20: dummy_delete /* vtgate:: keyspace_id:166b40b44aba4bd6 */ {} true true`,
	})

	// No rows changing
	vc = &loggingVCursor{
		shards: []string{"-20", "20-"},
	}
	_, err = del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Subquery returns no rows. So, no vindexes are deleted. We still pass-through the original delete.
		`ExecuteMultiShard sharded.-20: dummy_delete /* vtgate:: keyspace_id:166b40b44aba4bd6 */ {} true true`,
	})

	// Delete can affect multiple rows
	results = []*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"c1|c2|c3",
			"int64|int64|int64",
		),
		"4|5|6",
		"7|8|9",
	)}
	vc = &loggingVCursor{
		shards:  []string{"-20", "20-"},
		results: results,
	}
	_, err = del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations sharded [] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
		// ResolveDestinations is hard-coded to return -20.
		// It gets used to perform the subquery to fetch the changing column values.
		`ExecuteMultiShard sharded.-20: dummy_subquery {} false false`,
		// Delete 4,5 and 7,8 from lkp2.
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"4" from2: type:INT64 value:"5" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute delete from lkp2 where from1 = :from1 and from2 = :from2 and toc = :toc from1: type:INT64 value:"7" from2: type:INT64 value:"8" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Delete 6 and 8 from lkp1.
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"6" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		`Execute delete from lkp1 where from = :from and toc = :toc from: type:INT64 value:"9" toc: type:VARBINARY value:"\026k@\264J\272K\326"  true`,
		// Send the DML.
		`ExecuteMultiShard sharded.-20: dummy_delete /* vtgate:: keyspace_id:166b40b44aba4bd6 */ {} true true`,
	})
}

func TestDeleteSharded(t *testing.T) {
	del := &Delete{
		Opcode: DeleteScatter,
		Keyspace: &vindexes.Keyspace{
			Name:    "ks",
			Sharded: true,
		},
		Query: "dummy_delete",
	}

	vc := &loggingVCursor{shards: []string{"-20", "20-"}}
	_, err := del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	if err != nil {
		t.Fatal(err)
	}
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: dummy_delete {} ks.20-: dummy_delete {} true false`,
	})

	// Failure case
	vc = &loggingVCursor{shardErr: errors.New("shard_error")}
	_, err = del.Execute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	expectError(t, "Execute", err, "execDeleteScatter: shard_error")
}

func TestDeleteNoStream(t *testing.T) {
	del := &Delete{}
	err := del.StreamExecute(context.Background(), nil, nil, false, nil)
	expectError(t, "StreamExecute", err, `query "" cannot be used for streaming`)
}
