/*
Copyright 2023 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestDeleteCascade tests that FkCascade executes the child and parent primitives for a delete cascade.
func TestDeleteCascade(t *testing.T) {
	fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("cola|colb", "int64|varchar"), "1|a", "2|b")

	inputP := &Route{
		Query: "select cola, colb from parent where foo = 48",
		RoutingParameters: &RoutingParameters{
			Opcode:   Unsharded,
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	}
	childP := &Delete{
		DML: &DML{
			Query: "delete from child where (ca, cb) in ::__vals",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	parentP := &Delete{
		DML: &DML{
			Query: "delete from parent where foo = 48",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	fkc := &FkCascade{
		Selection: inputP,
		Children:  []*FkChild{{BVName: "__vals", Cols: []int{0, 1}, Exec: childP}},
		Parent:    parentP,
	}

	vc := newDMLTestVCursor("0")
	vc.results = []*sqltypes.Result{fakeRes}
	_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: delete from child where (ca, cb) in ::__vals {__vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x011\x950\x01a"} values:{type:TUPLE value:"\x89\x02\x012\x950\x01b"}} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: delete from parent where foo = 48 {} true true`,
	})

	vc.Rewind()
	err = fkc.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: delete from child where (ca, cb) in ::__vals {__vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x011\x950\x01a"} values:{type:TUPLE value:"\x89\x02\x012\x950\x01b"}} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: delete from parent where foo = 48 {} true true`,
	})
}

// TestUpdateCascade tests that FkCascade executes the child and parent primitives for an update cascade.
func TestUpdateCascade(t *testing.T) {
	fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("cola|colb", "int64|varchar"), "1|a", "2|b")

	inputP := &Route{
		Query: "select cola, colb from parent where foo = 48",
		RoutingParameters: &RoutingParameters{
			Opcode:   Unsharded,
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	}
	childP := &Update{
		DML: &DML{
			Query: "update child set ca = :vtg1 where (ca, cb) in ::__vals",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	parentP := &Update{
		DML: &DML{
			Query: "update parent set cola = 1 where foo = 48",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	fkc := &FkCascade{
		Selection: inputP,
		Children:  []*FkChild{{BVName: "__vals", Cols: []int{0, 1}, Exec: childP}},
		Parent:    parentP,
	}

	vc := newDMLTestVCursor("0")
	vc.results = []*sqltypes.Result{fakeRes}
	_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update child set ca = :vtg1 where (ca, cb) in ::__vals {__vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x011\x950\x01a"} values:{type:TUPLE value:"\x89\x02\x012\x950\x01b"}} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set cola = 1 where foo = 48 {} true true`,
	})

	vc.Rewind()
	err = fkc.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update child set ca = :vtg1 where (ca, cb) in ::__vals {__vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x011\x950\x01a"} values:{type:TUPLE value:"\x89\x02\x012\x950\x01b"}} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set cola = 1 where foo = 48 {} true true`,
	})
}

// TestNonLiteralUpdateCascade tests that FkCascade executes the child and parent primitives for a non-literal update cascade.
func TestNonLiteralUpdateCascade(t *testing.T) {
	fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("cola|cola <=> colb + 2|colb + 2", "int64|int64|int64"), "1|1|3", "2|0|5", "3|0|7")

	inputP := &Route{
		Query: "select cola, cola <=> colb + 2, colb + 2, from parent where foo = 48",
		RoutingParameters: &RoutingParameters{
			Opcode:   Unsharded,
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	}
	childP := &Update{
		DML: &DML{
			Query: "update child set ca = :fkc_upd where (ca) in ::__vals",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	parentP := &Update{
		DML: &DML{
			Query: "update parent set cola = colb + 2 where foo = 48",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	fkc := &FkCascade{
		Selection: inputP,
		Children: []*FkChild{{
			BVName: "__vals",
			Cols:   []int{0},
			NonLiteralInfo: []NonLiteralUpdateInfo{
				{
					UpdateExprBvName: "fkc_upd",
					UpdateExprCol:    2,
					CompExprCol:      1,
				},
			},
			Exec: childP,
		}},
		Parent: parentP,
	}

	vc := newDMLTestVCursor("0")
	vc.results = []*sqltypes.Result{fakeRes}
	_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, cola <=> colb + 2, colb + 2, from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update child set ca = :fkc_upd where (ca) in ::__vals {__vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x012"} fkc_upd: type:INT64 value:"5"} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update child set ca = :fkc_upd where (ca) in ::__vals {__vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x013"} fkc_upd: type:INT64 value:"7"} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set cola = colb + 2 where foo = 48 {} true true`,
	})

	vc.Rewind()
	err = fkc.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, cola <=> colb + 2, colb + 2, from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update child set ca = :fkc_upd where (ca) in ::__vals {__vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x012"} fkc_upd: type:INT64 value:"5"} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update child set ca = :fkc_upd where (ca) in ::__vals {__vals: type:TUPLE values:{type:TUPLE value:"\x89\x02\x013"} fkc_upd: type:INT64 value:"7"} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set cola = colb + 2 where foo = 48 {} true true`,
	})
}

// TestNeedsTransactionInExecPrepared tests that if we have a foreign key cascade inside an ExecStmt plan, then we do mark the plan to require a transaction.
func TestNeedsTransactionInExecPrepared(t *testing.T) {
	// Even if FkCascade is wrapped in ExecStmt, the plan should be marked such that it requires a transaction.
	// This is necessary because if we don't run the cascades for DMLs in a transaction, we might end up committing partial writes that should eventually be rolled back.
	execPrepared := &ExecStmt{
		Input: &FkCascade{},
	}
	require.True(t, execPrepared.NeedsTransaction())
}
