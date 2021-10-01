/*
Copyright 2020 The Vitess Authors.

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

package planbuilder

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func unsharded(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routeTree {
	return &routeTree{
		routeOpCode: engine.SelectUnsharded,
		solved:      solved,
		keyspace:    keyspace,
	}
}
func selectDBA(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routeTree {
	return &routeTree{
		routeOpCode: engine.SelectDBA,
		solved:      solved,
		keyspace:    keyspace,
	}
}

func selectScatter(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routeTree {
	return &routeTree{
		routeOpCode: engine.SelectScatter,
		solved:      solved,
		keyspace:    keyspace,
	}
}

var ts = semantics.TableSetFromIds

func TestMergeJoins(t *testing.T) {
	t.Skip("?")
	ks := &vindexes.Keyspace{Name: "apa", Sharded: false}
	ks2 := &vindexes.Keyspace{Name: "banan", Sharded: false}

	type testCase struct {
		l, r, expected queryTree
		predicates     []sqlparser.Expr
	}

	tests := []testCase{{
		l:        unsharded(ts(0), ks),
		r:        unsharded(ts(1), ks),
		expected: unsharded(ts(0, 1), ks),
	}, {
		l:        unsharded(ts(0), ks),
		r:        unsharded(ts(1), ks2),
		expected: nil,
	}, {
		l:        unsharded(ts(1), ks),
		r:        unsharded(ts(0), ks2),
		expected: nil,
	}, {
		l:        selectDBA(ts(0), ks),
		r:        selectDBA(ts(1), ks),
		expected: selectDBA(ts(0, 1), ks),
	}, {
		l:        selectDBA(ts(0), ks),
		r:        selectDBA(ts(1), ks2),
		expected: selectDBA(ts(0, 1), ks),
	}, {
		l:        selectDBA(ts(1), ks),
		r:        selectDBA(ts(0), ks2),
		expected: selectDBA(ts(0, 1), ks),
	}, {
		l:        unsharded(ts(0), ks),
		r:        selectDBA(ts(1), ks),
		expected: nil,
	}, {
		l: selectScatter(ts(0), ks),
		r: selectScatter(ts(1), ks),
		predicates: []sqlparser.Expr{
			equals(colName("t1", "id"), colName("t2", "id")),
		},
		expected: nil,
	}}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			result, _ := tryMerge(&planningContext{semTable: semantics.NewSemTable()}, tc.l, tc.r, tc.predicates, nil) // fakeMerger ? how to test this
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestClone(t *testing.T) {
	original := &routeTree{
		routeOpCode: engine.SelectEqualUnique,
		vindexPreds: []*vindexPlusPredicates{{}},
	}

	clone := original.clone()

	clonedRP := clone.(*routeTree)
	clonedRP.routeOpCode = engine.SelectDBA
	assert.Equal(t, clonedRP.routeOpCode, engine.SelectDBA)
	assert.Equal(t, original.routeOpCode, engine.SelectEqualUnique)

	clonedRP.vindexPreds[0].colVindex = &vindexes.ColumnVindex{}
	assert.NotNil(t, clonedRP.vindexPreds[0].colVindex)
	assert.Nil(t, original.vindexPreds[0].colVindex)
}

func TestCreateRoutePlanForOuter(t *testing.T) {
	assert := assert.New(t)
	m1 := &routeTable{
		qtable: &abstract.QueryTable{
			TableID: ts(0),
			Table:   sqlparser.TableName{},
		},
		vtable: &vindexes.Table{},
	}
	m2 := &routeTable{
		qtable: &abstract.QueryTable{
			TableID: ts(1),
			Table:   sqlparser.TableName{},
		},
		vtable: &vindexes.Table{},
	}
	m3 := &routeTable{
		qtable: &abstract.QueryTable{
			TableID: ts(2),
			Table:   sqlparser.TableName{},
		},
		vtable: &vindexes.Table{},
	}
	a := &routeTree{
		routeOpCode: engine.SelectUnsharded,
		solved:      ts(0),
		tables:      []relation{m1},
	}
	col1 := sqlparser.NewColNameWithQualifier("id", sqlparser.TableName{
		Name: sqlparser.NewTableIdent("m1"),
	})
	col2 := sqlparser.NewColNameWithQualifier("id", sqlparser.TableName{
		Name: sqlparser.NewTableIdent("m2"),
	})
	b := &routeTree{
		routeOpCode: engine.SelectUnsharded,
		solved:      ts(1, 2),
		tables:      []relation{m2, m3},
		predicates:  []sqlparser.Expr{equals(col1, col2)},
	}
	semTable := semantics.NewSemTable()
	merge, _ := tryMerge(&planningContext{semTable: semTable}, a, b, []sqlparser.Expr{}, fakeMerger)
	assert.NotNil(merge)
}

func fakeMerger(a, _ *routeTree) (*routeTree, error) {
	return a, nil
}

func equals(left, right sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     left,
		Right:    right,
	}
}

func colName(table, column string) *sqlparser.ColName {
	return &sqlparser.ColName{Name: sqlparser.NewColIdent(column), Qualifier: tableName(table)}
}

func tableName(name string) sqlparser.TableName {
	return sqlparser.TableName{Name: sqlparser.NewTableIdent(name)}
}
