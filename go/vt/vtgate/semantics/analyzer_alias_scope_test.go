/*
Copyright 2026 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestSubqueryParentAliasResolution checks that a subquery column reference
// that matches a parent SELECT alias is reported as depending on the alias
// expression's tables. Literal aliases (no table dependencies of their own)
// are reported as correlated to the outer scope.
func TestSubqueryParentAliasResolution(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		sql:  "select id as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		sql:  "select id+1 as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		sql:  "select 1 as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		sql:  "select id as foobar, (select foobar from t2) from t1 where id = 1",
		deps: TS0,
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel := stmt.(*sqlparser.Select)

			subq := extract(sel, 1).(*sqlparser.Subquery).Select.(*sqlparser.Select)
			innerCol := extract(subq, 0)

			require.NoError(t, semTable.NotSingleRouteErr)
			assert.Equal(t, tc.deps, semTable.RecursiveDeps(innerCol))
		})
	}
}

// TestSubqueryParentAliasFromlessSubquery covers subqueries with no FROM clause
// (DUAL is a reserved keyword, so `(select x)` and `(select x from dual)` both
// parse to From: nil). The inner column reference resolves against a parent
// SELECT alias and inherits the alias expression's dependencies; literal aliases
// fall back to the outer scope's tables so single-route merging still works.
func TestSubqueryParentAliasFromlessSubquery(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		// outer has no FROM and the alias is a literal — no tables anywhere.
		sql:  "select 1 as x, (select x)",
		deps: EmptyTableSet(),
	}, {
		// alias expression references t1 — inner col inherits that dep.
		sql:  "select id as foobar, (select foobar) from t1",
		deps: TS0,
	}, {
		// literal alias falls back to the outer scope's tables (t1).
		sql:  "select 1 as foobar, (select foobar) from t1",
		deps: TS0,
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel := stmt.(*sqlparser.Select)

			subq := extract(sel, 1).(*sqlparser.Subquery).Select.(*sqlparser.Select)
			innerCol := extract(subq, 0)

			require.NoError(t, semTable.NotSingleRouteErr)
			assert.Equal(t, tc.deps, semTable.RecursiveDeps(innerCol))
		})
	}
}

// TestSubqueryParentAliasNestedResolution checks that an alias defined two
// scope levels above the reference is still reachable.
func TestSubqueryParentAliasNestedResolution(t *testing.T) {
	stmt, semTable := parseAndAnalyze(t, "select 1 as foobar, (select (select foobar from t2) as barbaz from t3) from t1", "d")
	sel := stmt.(*sqlparser.Select)

	middle := extract(sel, 1).(*sqlparser.Subquery).Select.(*sqlparser.Select)
	inner := extract(middle, 0).(*sqlparser.Subquery).Select.(*sqlparser.Select)
	innerCol := extract(inner, 0)

	require.NoError(t, semTable.NotSingleRouteErr)
	assert.Equal(t, TS0, semTable.RecursiveDeps(innerCol))
}

// TestSubqueryParentAliasErrors covers the cases that match MySQL by failing
// rather than resolving: forward alias references, qualified column refs,
// same-scope references, and ambiguous duplicate aliases.
func TestSubqueryParentAliasErrors(t *testing.T) {
	tcases := []struct {
		name      string
		sql       string
		errSubstr string
	}{{
		name:      "forward reference rejected",
		sql:       "select (select foobar from t2), id as foobar from t1",
		errSubstr: "column 'foobar' not found",
	}, {
		name:      "qualified column does not match alias",
		sql:       "select id as foobar, (select x.foobar from t2) from t1 as x",
		errSubstr: "column 'x.foobar' not found",
	}, {
		name:      "same-scope alias reference is not resolved as alias",
		sql:       "select 1 as x, x from t1",
		errSubstr: "column 'x' not found",
	}, {
		name:      "duplicate bare-column aliases are ambiguous",
		sql:       "select id as foobar, uid as foobar, (select foobar from t3) from t1, t2",
		errSubstr: "Column 'foobar' in field list is ambiguous",
	}}
	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(tc.sql)
			require.NoError(t, err)

			st, err := Analyze(parse, "d", fakeSchemaInfo())
			if err != nil {
				assert.ErrorContains(t, err, tc.errSubstr)
				return
			}
			require.NotNil(t, st.NotUnshardedErr, "expected an error from Analyze or via NotUnshardedErr")
			assert.ErrorContains(t, st.NotUnshardedErr, tc.errSubstr)
		})
	}
}

// TestSubqueryParentAliasDuplicateResolution covers the duplicate-alias cases
// that resolve cleanly: the same column repeated under the same alias, a
// non-column expression sharing an alias with a bare column (the non-column
// wins regardless of order), and the same alias on three or more expressions
// where at least one is a non-column.
func TestSubqueryParentAliasDuplicateResolution(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		sql:  "select id as foobar, id as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		sql:  "select id as foobar, 99 as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		sql:  "select 99 as foobar, id as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		sql:  "select 99 as foobar, id as foobar, uid as foobar, (select foobar from t3) from t1, t2",
		deps: MergeTableSets(TS0, TS1),
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel := stmt.(*sqlparser.Select)

			var subq *sqlparser.Subquery
			for _, se := range sel.SelectExprs.Exprs {
				ae, ok := se.(*sqlparser.AliasedExpr)
				if !ok {
					continue
				}
				if s, ok := ae.Expr.(*sqlparser.Subquery); ok {
					subq = s
				}
			}
			require.NotNil(t, subq, "no subquery found in: %s", tc.sql)

			innerSel := subq.Select.(*sqlparser.Select)
			innerCol := extract(innerSel, 0)

			require.NoError(t, semTable.NotSingleRouteErr)
			assert.Equal(t, tc.deps, semTable.RecursiveDeps(innerCol))
		})
	}
}
