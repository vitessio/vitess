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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func unsharded(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routePlan {
	return &routePlan{
		routeOpCode: engine.SelectUnsharded,
		solved:      solved,
		keyspace:    keyspace,
	}
}
func selectDBA(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routePlan {
	return &routePlan{
		routeOpCode: engine.SelectDBA,
		solved:      solved,
		keyspace:    keyspace,
	}
}

func selectScatter(solved semantics.TableSet, keyspace *vindexes.Keyspace) *routePlan {
	return &routePlan{
		routeOpCode: engine.SelectScatter,
		solved:      solved,
		keyspace:    keyspace,
	}
}

func TestMergeJoins(t *testing.T) {
	ks := &vindexes.Keyspace{Name: "apa", Sharded: false}
	ks2 := &vindexes.Keyspace{Name: "banan", Sharded: false}

	type testCase struct {
		l, r, expected joinTree
		predicates     []sqlparser.Expr
	}

	tests := []testCase{{
		l:        unsharded(1, ks),
		r:        unsharded(2, ks),
		expected: unsharded(1|2, ks),
	}, {
		l:        unsharded(1, ks),
		r:        unsharded(2, ks2),
		expected: nil,
	}, {
		l:        unsharded(2, ks),
		r:        unsharded(1, ks2),
		expected: nil,
	}, {
		l:        selectDBA(1, ks),
		r:        selectDBA(2, ks),
		expected: selectDBA(1|2, ks),
	}, {
		l:        selectDBA(1, ks),
		r:        selectDBA(2, ks2),
		expected: nil,
	}, {
		l:        selectDBA(2, ks),
		r:        selectDBA(1, ks2),
		expected: nil,
	}, {
		l:        unsharded(1, ks),
		r:        selectDBA(2, ks),
		expected: nil,
	}, {
		l: selectScatter(1, ks),
		r: selectScatter(2, ks),
		predicates: []sqlparser.Expr{
			equals(colName("t1", "id"), colName("t2", "id")),
		},
		expected: nil,
	}}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			result := tryMerge(tc.l, tc.r, tc.predicates, semantics.NewSemTable())
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestClone(t *testing.T) {
	original := &routePlan{
		routeOpCode: engine.SelectEqualUnique,
		vindexPreds: []*vindexPlusPredicates{{}},
	}

	clone := original.clone()

	clonedRP := clone.(*routePlan)
	clonedRP.routeOpCode = engine.SelectDBA
	assert.Equal(t, clonedRP.routeOpCode, engine.SelectDBA)
	assert.Equal(t, original.routeOpCode, engine.SelectEqualUnique)

	clonedRP.vindexPreds[0].foundVindex = &vindexes.Null{}
	assert.NotNil(t, clonedRP.vindexPreds[0].foundVindex)
	assert.Nil(t, original.vindexPreds[0].foundVindex)
}

func TestExpandStar(t *testing.T) {
	schemaInfo := &fakeSI{
		tables: map[string]*vindexes.Table{
			"t1": {
				Name: sqlparser.NewTableIdent("t1"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewColIdent("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewColIdent("b"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewColIdent("c"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"t2": {
				Name: sqlparser.NewTableIdent("t2"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewColIdent("c1"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewColIdent("c2"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"t3": { // non authoritative table.
				Name: sqlparser.NewTableIdent("t3"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewColIdent("col"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: false,
			},
		},
	}
	cDB := "db"
	tcases := []struct {
		sql    string
		expSQL string
	}{{
		sql:    "select * from t1",
		expSQL: "select t1.a, t1.b, t1.c from t1",
	}, {
		sql:    "select t1.* from t1",
		expSQL: "select t1.a, t1.b, t1.c from t1",
	}, {
		sql:    "select *, 42, t1.* from t1",
		expSQL: "select t1.a, t1.b, t1.c, 42, t1.a, t1.b, t1.c from t1",
	}, {
		sql:    "select 42, t1.* from t1",
		expSQL: "select 42, t1.a, t1.b, t1.c from t1",
	}, {
		sql:    "select * from t1, t2",
		expSQL: "select t1.a, t1.b, t1.c, t2.c1, t2.c2 from t1, t2",
	}, {
		sql:    "select t1.* from t1, t2",
		expSQL: "select t1.a, t1.b, t1.c from t1, t2",
	}, {
		sql:    "select *, t1.* from t1, t2",
		expSQL: "select t1.a, t1.b, t1.c, t2.c1, t2.c2, t1.a, t1.b, t1.c from t1, t2",
	}, { // t3 is non-authoritative table
		sql:    "select * from t3",
		expSQL: "select * from t3",
	}, { // t3 is non-authoritative table
		sql:    "select * from t1, t2, t3",
		expSQL: "select * from t1, t2, t3",
	}, { // t3 is non-authoritative table
		sql:    "select t1.*, t2.*, t3.* from t1, t2, t3",
		expSQL: "select t1.a, t1.b, t1.c, t2.c1, t2.c2, t3.* from t1, t2, t3",
	}, { // TODO: This should fail on analyze step and should not reach down.
		sql:    "select foo.* from t1, t2",
		expSQL: "select foo.* from t1, t2",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.Parse(tcase.sql)
			require.NoError(t, err)
			semState, err := semantics.Analyze(ast, cDB, schemaInfo)
			require.NoError(t, err)
			expanded := expandStar(ast.(*sqlparser.Select), semState)
			assert.Equal(t, tcase.expSQL, sqlparser.String(expanded))
		})
	}
}
