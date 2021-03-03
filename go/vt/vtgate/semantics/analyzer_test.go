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

package semantics

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	// Just here to make outputs more readable
	T0 TableSet = 1 << iota
	T1
	T2
	_ // T3 is not used in the tests
	T4
)

func extract(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}

func TestScopeForSubqueries(t *testing.T) {
	t.Skip("subqueries not yet supported")
	query := `
select t.col1, (
	select t.col2 from z as t) 
from x as t`
	stmt, semTable := parseAndAnalyze(t, query)

	sel, _ := stmt.(*sqlparser.Select)

	// extract the `t.col2` expression from the subquery
	sel2 := sel.SelectExprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.Subquery).Select.(*sqlparser.Select)
	s1 := semTable.Dependencies(extract(sel2, 0))

	// if scoping works as expected, we should be able to see the inner table being used by the inner expression
	assert.Equal(t, T1, s1)
}

func TestBindingSingleTable(t *testing.T) {
	queries := []string{
		"select col from tabl",
		"select tabl.col from tabl",
		"select d.tabl.col from tabl",
		"select col from d.tabl",
		"select tabl.col from d.tabl",
		"select d.tabl.col from d.tabl",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query)
			sel, _ := stmt.(*sqlparser.Select)
			t1 := sel.From[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.EqualValues(t, 1, ts)

			d := semTable.Dependencies(extract(sel, 0))
			require.Equal(t, T0, d, query)
		})
	}
}

func TestUnion(t *testing.T) {
	query := "select col1 from tabl1 union select col2 from tabl2"

	stmt, semTable := parseAndAnalyze(t, query)
	union, _ := stmt.(*sqlparser.Union)
	sel1 := union.FirstStatement.(*sqlparser.Select)
	sel2 := union.UnionSelects[0].Statement.(*sqlparser.Select)

	t1 := sel1.From[0].(*sqlparser.AliasedTableExpr)
	t2 := sel2.From[0].(*sqlparser.AliasedTableExpr)
	ts1 := semTable.TableSetFor(t1)
	ts2 := semTable.TableSetFor(t2)
	assert.EqualValues(t, 1, ts1)
	assert.EqualValues(t, 2, ts2)

	d1 := semTable.Dependencies(extract(sel1, 0))
	d2 := semTable.Dependencies(extract(sel2, 0))
	assert.Equal(t, T0, d1)
	assert.Equal(t, T1, d2)
}

func TestBindingMultiTable(t *testing.T) {
	type testCase struct {
		query string
		deps  TableSet
	}
	queries := []testCase{{
		query: "select t.col from t, s",
		deps:  T0,
	}, {
		query: "select t.col from t join s",
		deps:  T0,
	}, {
		query: "select max(t.col+s.col) from t, s",
		deps:  T0 | T1,
	}, {
		query: "select max(t.col+s.col) from t join s",
		deps:  T0 | T1,
	}, {
		query: "select case t.col when s.col then r.col else u.col end from t, s, r, w, u",
		deps:  T0 | T1 | T2 | T4,
		//}, {
		//	// make sure that we don't let sub-query Dependencies leak out by mistake
		//	query: "select t.col + (select 42 from s) from t",
		//	deps:  T0,
		//}, {
		//	query: "select (select 42 from s where r.id = s.id) from r",
		//	deps:  T0 | T1,
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query.query)
			sel, _ := stmt.(*sqlparser.Select)
			assert.Equal(t, query.deps, semTable.Dependencies(extract(sel, 0)), query.query)
		})
	}
}

func TestBindingSingleDepPerTable(t *testing.T) {
	query := "select t.col + t.col2 from t"
	stmt, semTable := parseAndAnalyze(t, query)
	sel, _ := stmt.(*sqlparser.Select)

	d := semTable.Dependencies(extract(sel, 0))
	assert.Equal(t, 1, d.NumberOfTables(), "size wrong")
	assert.Equal(t, T0, d)
}

func TestNotUniqueTableName(t *testing.T) {
	queries := []string{
		"select * from t, t",
		"select * from t, (select 1 from x) as t",
		"select * from t join t",
		"select * from t join (select 1 from x) as t",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			if strings.Contains(query, "as") {
				t.Skip("table alias not implemented")
			}
			parse, _ := sqlparser.Parse(query)
			_, err := Analyse(parse)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Not unique table/alias")
		})
	}
}

func TestMissingTable(t *testing.T) {
	queries := []string{
		"select t.col from a",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, _ := sqlparser.Parse(query)
			_, err := Analyse(parse)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Unknown table")
		})
	}
}

func parseAndAnalyze(t *testing.T, query string) (sqlparser.Statement, *SemTable) {
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)
	semTable, err := Analyse(parse)
	require.NoError(t, err)
	return parse, semTable
}
