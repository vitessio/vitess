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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

func extract(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}

func TestScope(t *testing.T) {
	query := `
select t.col1, (
	select t.col2 from z as t) 
from x as t`
	stmt, semTable := parseAndAnalyze(t, query)

	sel, _ := stmt.(*sqlparser.Select)

	// extract the `t.col2` expression from the subquery
	sel2 := sel.SelectExprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.Subquery).Select.(*sqlparser.Select)
	s1 := semTable.dependencies(extract(sel2, 0))

	// if scoping works as expected, we should be able to see the inner table being used by the inner expression
	assert.Equal(t, []string{"z as t"}, sortDeps(s1))
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

			d := semTable.dependencies(extract(sel, 0))
			require.NotEmpty(t, d)
			require.NotNil(t, d[0])
			require.Contains(t, sqlparser.String(d[0]), "tabl")
		})
	}
}

func TestUnion(t *testing.T) {
	query := "select col1 from tabl1 union select col2 from tabl2"

	stmt, semTable := parseAndAnalyze(t, query)
	union, _ := stmt.(*sqlparser.Union)
	sel1 := union.FirstStatement.(*sqlparser.Select)
	sel2 := union.UnionSelects[0].Statement.(*sqlparser.Select)

	d1 := semTable.dependencies(extract(sel1, 0))
	d2 := semTable.dependencies(extract(sel2, 0))
	require.Contains(t, sortDeps(d1), "tabl1")
	require.Contains(t, sortDeps(d2), "tabl2")
}

func TestBindingMultiTable(t *testing.T) {
	type testCase struct {
		query string
		deps  []string
	}
	d := func(i ...string) []string { return i }
	queries := []testCase{{
		query: "select t.col from t, s",
		deps:  d("t"),
	}, {
		query: "select max(t.col+s.col) from t, s",
		deps:  d("s", "t"),
	}, {
		query: "select case t.col when s.col then r.col else w.col end from t, s, r, w, u",
		deps:  d("r", "s", "t", "w"),
	}, {
		// make sure that we don't let sub-query dependencies leak out by mistake
		query: "select t.col + (select 42 from s) from t",
		deps:  d("t"),
	}, {
		query: "select (select 42 from s where r.id = s.id) from r",
		deps:  d("r", "s"),
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query.query)
			sel, _ := stmt.(*sqlparser.Select)

			d := semTable.dependencies(extract(sel, 0))
			assert.Equal(t, query.deps, sortDeps(d))
		})
	}
}

func sortDeps(d []table) []string {
	var deps []string
	for _, t2 := range d {
		deps = append(deps, sqlparser.String(t2))
	}
	sort.Strings(deps)
	return deps
}

func TestBindingSingleDepPerTable(t *testing.T) {
	query := "select t.col + t.col2 from t"
	stmt, semTable := parseAndAnalyze(t, query)
	sel, _ := stmt.(*sqlparser.Select)

	d := semTable.dependencies(extract(sel, 0))
	assert.Equal(t, 1, len(d), "size wrong")
	assert.Equal(t, "t", sqlparser.String(d[0]))
}

func TestNotUniqueTableName(t *testing.T) {
	queries := []string{
		"select * from t, t",
		"select * from t, (select 1 from x) as t",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, _ := sqlparser.Parse(query)
			_, err := Analyse(parse, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "Not unique table/alias")
		})
	}

}

func parseAndAnalyze(t *testing.T, query string) (sqlparser.Statement, *SemTable) {
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)
	semTable, err := Analyse(parse, nil)
	require.NoError(t, err)
	return parse, semTable
}
