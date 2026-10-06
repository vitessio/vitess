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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
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

	vc := newTestVCursor("0")
	vc.results = []*sqltypes.Result{fakeRes}
	_, err := fkc.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		fmt.Sprintf(`ExecuteMultiShard ks.0: delete from child where (ca, cb) in ::__vals {__vals: %v} true true`, &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x011\x950\x01a")}, {Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x012\x950\x01b")}}}),
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: delete from parent where foo = 48 {} true true`,
	})

	vc.Rewind()
	err = fkc.TryStreamExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		fmt.Sprintf(`ExecuteMultiShard ks.0: delete from child where (ca, cb) in ::__vals {__vals: %v} true true`, &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x011\x950\x01a")}, {Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x012\x950\x01b")}}}),
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

	vc := newTestVCursor("0")
	vc.results = []*sqltypes.Result{fakeRes}
	_, err := fkc.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		fmt.Sprintf(`ExecuteMultiShard ks.0: update child set ca = :vtg1 where (ca, cb) in ::__vals {__vals: %v} true true`, &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x011\x950\x01a")}, {Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x012\x950\x01b")}}}),
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set cola = 1 where foo = 48 {} true true`,
	})

	vc.Rewind()
	err = fkc.TryStreamExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		fmt.Sprintf(`ExecuteMultiShard ks.0: update child set ca = :vtg1 where (ca, cb) in ::__vals {__vals: %v} true true`, &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x011\x950\x01a")}, {Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x012\x950\x01b")}}}),
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

	vc := newTestVCursor("0")
	vc.results = []*sqltypes.Result{fakeRes}
	_, err := fkc.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, cola <=> colb + 2, colb + 2, from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		fmt.Sprintf(`ExecuteMultiShard ks.0: update child set ca = :fkc_upd where (ca) in ::__vals {__vals: %v fkc_upd: %v} true true`, &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x012")}}}, &querypb.BindVariable{Type: querypb.Type_INT64, Value: []byte("5")}),
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		fmt.Sprintf(`ExecuteMultiShard ks.0: update child set ca = :fkc_upd where (ca) in ::__vals {__vals: %v fkc_upd: %v} true true`, &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x013")}}}, &querypb.BindVariable{Type: querypb.Type_INT64, Value: []byte("7")}),
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set cola = colb + 2 where foo = 48 {} true true`,
	})

	vc.Rewind()
	err = fkc.TryStreamExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, cola <=> colb + 2, colb + 2, from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		fmt.Sprintf(`ExecuteMultiShard ks.0: update child set ca = :fkc_upd where (ca) in ::__vals {__vals: %v fkc_upd: %v} true true`, &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x012")}}}, &querypb.BindVariable{Type: querypb.Type_INT64, Value: []byte("5")}),
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		fmt.Sprintf(`ExecuteMultiShard ks.0: update child set ca = :fkc_upd where (ca) in ::__vals {__vals: %v fkc_upd: %v} true true`, &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{{Type: querypb.Type_TUPLE, Value: []byte("\x89\x02\x013")}}}, &querypb.BindVariable{Type: querypb.Type_INT64, Value: []byte("7")}),
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set cola = colb + 2 where foo = 48 {} true true`,
	})
}

// newNonLiteralCascade returns an FkCascade for `update parent set k = <expr>` whose Selection
// returns rows of (k, k <=> <expr>, <expr>), cascading to child(ck) ON UPDATE CASCADE. k is a
// unique key of parent.
func newNonLiteralCascade(typ string, colTypes []evalengine.Type, rows ...string) (*FkCascade, *sqltypes.Result) {
	selection := sqltypes.MakeTestResult(sqltypes.MakeTestFields("k|k <=> expr|expr", typ+"|int64|"+typ), rows...)
	unsharded := &RoutingParameters{Opcode: Unsharded, Keyspace: &vindexes.Keyspace{Name: "ks"}}
	return &FkCascade{
		Selection: &Route{Query: "select k, k <=> expr, expr from parent for update", RoutingParameters: unsharded},
		Children: []*FkChild{{
			BVName:          "fkc_vals",
			Cols:            []int{0},
			NonLiteralInfo:  []NonLiteralUpdateInfo{{CompExprCol: 1, UpdateExprCol: 2, UpdateExprBvName: "fkc_upd", FkColIdx: 0}},
			ParentKeyUnique: true,
			ColTypes:        colTypes,
			Exec: &Update{DML: &DML{
				Query:             "update child set ck = :fkc_upd where (ck) in ::fkc_vals",
				RoutingParameters: unsharded,
			}},
		}},
		Parent: &Update{DML: &DML{Query: "update parent set k = expr", RoutingParameters: unsharded}},
	}, selection
}

// childUpdateLog is the log line of the child update that moves child rows from old to new.
func childUpdateLog(old, new sqltypes.Value) string {
	vals := &querypb.BindVariable{Type: querypb.Type_TUPLE, Values: []*querypb.Value{sqltypes.TupleToProto([]sqltypes.Value{old})}}
	return fmt.Sprintf(`ExecuteMultiShard ks.0: update child set ck = :fkc_upd where (ck) in ::fkc_vals {fkc_upd: %v fkc_vals: %v} true true`, sqltypes.ValueBindVariable(new), vals)
}

// childUpdateOrder returns the old key of each child update, in the order they ran.
func childUpdateOrder(t *testing.T, fkc *FkCascade, selection *sqltypes.Result) ([]string, error) {
	vc := newTestVCursor("0")
	vc.results = []*sqltypes.Result{selection}
	_, err := fkc.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true)
	var order []string
	for _, line := range vc.log {
		for _, row := range selection.Rows {
			if line == childUpdateLog(row[0], row[2]) {
				order = append(order, row[0].String())
			}
		}
	}
	return order, err
}

// TestNonLiteralUpdateCascadeOrder tests that the child updates of a non-literal update run in an
// order where each child row is moved once. For `update parent set k = k + 1` over keys 1 and 2,
// the children of 2 must move to 3 before the children of 1 move to 2. Otherwise the second child
// update also matches the children that the first one moved to 2.
func TestNonLiteralUpdateCascadeOrder(t *testing.T) {
	fkc, selection := newNonLiteralCascade("int64", nil, "1|0|2", "2|0|3")

	vc := newTestVCursor("0")
	vc.results = []*sqltypes.Result{selection}
	_, err := fkc.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select k, k <=> expr, expr from parent for update {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		childUpdateLog(sqltypes.NewInt64(2), sqltypes.NewInt64(3)),
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		childUpdateLog(sqltypes.NewInt64(1), sqltypes.NewInt64(2)),
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set k = expr {} true true`,
	})
}

// TestNonLiteralUpdateCascadeOrderWithNull tests the ordering when keys are NULL. A NULL key
// matches no child rows, so a row whose new key is NULL must not have to wait for a row whose old
// key is NULL. Treating NULL as equal to NULL here would report a cycle.
func TestNonLiteralUpdateCascadeOrderWithNull(t *testing.T) {
	// 1 -> 2 must wait for 2 -> NULL; NULL -> 1 must wait for 1 -> 2.
	fkc, selection := newNonLiteralCascade("int64", nil, "1|0|2", "2|0|null", "null|0|1")
	order, err := childUpdateOrder(t, fkc, selection)
	require.NoError(t, err)
	assert.Equal(t, []string{"INT64(2)", "INT64(1)", "NULL"}, order)
}

// TestNonLiteralUpdateCascadeOrderUsesCollation tests that keys are compared with the column's
// collation, as MySQL compares them in the child update's IN clause.
func TestNonLiteralUpdateCascadeOrderUsesCollation(t *testing.T) {
	env := collations.MySQL8()
	caseInsensitive := evalengine.NewType(sqltypes.VarChar, env.LookupByName("utf8mb4_0900_ai_ci"))
	caseSensitive := evalengine.NewType(sqltypes.VarChar, env.LookupByName("utf8mb4_0900_as_cs"))

	// Under a case-insensitive collation 'B' is the old key 'b', so 'b' -> 'c' must run first.
	fkc, selection := newNonLiteralCascade("varchar", []evalengine.Type{caseInsensitive}, "a|0|B", "b|0|c")
	order, err := childUpdateOrder(t, fkc, selection)
	require.NoError(t, err)
	assert.Equal(t, []string{`VARCHAR("b")`, `VARCHAR("a")`}, order)

	// Under a case-sensitive collation the keys differ, so the Selection order is kept.
	fkc, selection = newNonLiteralCascade("varchar", []evalengine.Type{caseSensitive}, "a|0|B", "b|0|c")
	order, err = childUpdateOrder(t, fkc, selection)
	require.NoError(t, err)
	assert.Equal(t, []string{`VARCHAR("a")`, `VARCHAR("b")`}, order)
}

// TestNonLiteralUpdateCascadeCycle tests that an update that moves keys in a cycle fails with a
// duplicate key error before any child update runs, as MySQL fails the parent update.
func TestNonLiteralUpdateCascadeCycle(t *testing.T) {
	fkc, selection := newNonLiteralCascade("int64", nil, "1|0|2", "2|0|1")

	vc := newTestVCursor("0")
	vc.results = []*sqltypes.Result{selection}
	_, err := fkc.TryExecute(t.Context(), vc, map[string]*querypb.BindVariable{}, true)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_ALREADY_EXISTS, vterrors.Code(err))
	assert.Equal(t, vterrors.DupEntry, vterrors.ErrState(err))
	assert.Equal(t, sqlerror.ERDupEntry, sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError).Number())
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select k, k <=> expr, expr from parent for update {} false false`,
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
