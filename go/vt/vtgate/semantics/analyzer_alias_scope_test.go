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

// Each test in this file uses a subquery with its own authoritative FROM
// table that does NOT contain the alias name as a column. That forces the
// inner resolveColumnInScope to return `nothing` (empty deps), which in turn
// triggers the parent-alias path on the outer scope — exactly the behaviour
// this PR adds. If the subquery had no FROM, the parser would inject `dual`,
// which is non-authoritative and short-circuits with an uncertain dep before
// the alias path ever runs.

// TestSubqueryParentAliasResolution covers the resolveColumn parent-alias path:
// a subquery column reference that resolves against a parent SELECT alias must
// take on the alias expression's table dependencies — or, when the alias has
// no table deps (literal alias), be marked as correlated to the outer scope.
func TestSubqueryParentAliasResolution(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		// Column alias propagates the outer table dep.
		sql:  "select id as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		// Compound alias still propagates the outer table dep.
		sql:  "select id+1 as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		// Literal alias has no table deps; the reference is correlated to the
		// outer scope's tables.
		sql:  "select 1 as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		// An outer WHERE clause does not change the resolution.
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

// TestSubqueryParentAliasDualFallback covers the path where the subquery has
// no explicit FROM and the parser injects `dual`. `dual` is non-authoritative,
// so resolveColumnInScope returns an uncertain dep at the inner scope and the
// loop short-circuits before ever reaching the parent-alias check. The end-
// user query still works (the dual subquery merges with the outer route at
// planbuilder time and MySQL resolves the alias itself), but the binder
// resolves the inner column reference to the inner subquery's `dual`, not to
// the outer alias. This test pins that behaviour so future refactors don't
// silently change which path handles these shapes.
func TestSubqueryParentAliasDualFallback(t *testing.T) {
	tcases := []struct {
		sql string
	}{{
		// Both outer and inner have no FROM — each gets its own implicit dual.
		sql: "select 1 as x, (select x)",
	}, {
		// Outer has a real authoritative FROM, inner gets an implicit dual.
		sql: "select id as foobar, (select foobar) from t1",
	}, {
		// Same as above but with a literal alias.
		sql: "select 1 as foobar, (select foobar) from t1",
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel := stmt.(*sqlparser.Select)

			subq := extract(sel, 1).(*sqlparser.Subquery).Select.(*sqlparser.Select)
			innerDual := subq.From[0].(*sqlparser.AliasedTableExpr)
			innerCol := extract(subq, 0)

			require.NoError(t, semTable.NotSingleRouteErr)
			// The inner column's deps point to the inner subquery's own dual,
			// not to the outer scope — i.e. the alias path was not used.
			assert.Equal(t, semTable.TableSetFor(innerDual), semTable.RecursiveDeps(innerCol))
		})
	}
}

// TestSubqueryParentAliasNestedResolution covers the nested case: an inner
// subquery references an alias defined two scope levels above. Both inner
// subqueries use authoritative FROM tables that don't contain `foobar`.
func TestSubqueryParentAliasNestedResolution(t *testing.T) {
	stmt, semTable := parseAndAnalyze(t, "select 1 as foobar, (select (select foobar from t2) as barbaz from t3) from t1", "d")
	sel := stmt.(*sqlparser.Select)

	middle := extract(sel, 1).(*sqlparser.Subquery).Select.(*sqlparser.Select)
	inner := extract(middle, 0).(*sqlparser.Subquery).Select.(*sqlparser.Select)
	innerCol := extract(inner, 0)

	require.NoError(t, semTable.NotSingleRouteErr)
	assert.Equal(t, TS0, semTable.RecursiveDeps(innerCol))
}

// TestSubqueryParentAliasErrors covers the gates that reject invalid alias
// references: forward references, qualified column refs, same-scope refs, and
// duplicate-alias ambiguity.
func TestSubqueryParentAliasErrors(t *testing.T) {
	tcases := []struct {
		name string
		sql  string
		// errSubstr is matched against either the analyze error or
		// SemTable.NotUnshardedErr, whichever surfaces first.
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

// TestSubqueryParentAliasDuplicateResolution covers the non-error cases of the
// duplicate-alias logic: same-column-same-alias, non-column expression beats
// bare column, and shielded ambiguity.
func TestSubqueryParentAliasDuplicateResolution(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		// Repeated same column under same alias is not ambiguous.
		sql:  "select id as foobar, id as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		// Non-column expression beats bare column when both share an alias.
		sql:  "select id as foobar, 99 as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		// Order doesn't matter — non-column still wins.
		sql:  "select 99 as foobar, id as foobar, (select foobar from t2) from t1",
		deps: TS0,
	}, {
		// Shielded ambiguity: a non-column already in the match buffer
		// protects against a later column-vs-column collision. The literal
		// alias has empty deps, so the reference is correlated to the union
		// of the outer scope's tables.
		sql:  "select 99 as foobar, id as foobar, uid as foobar, (select foobar from t3) from t1, t2",
		deps: MergeTableSets(TS0, TS1),
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel := stmt.(*sqlparser.Select)

			// Locate the subquery — it's the last AliasedExpr whose Expr is a
			// Subquery.
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
