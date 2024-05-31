/*
Copyright 2024 The Vitess Authors.

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

func TestScopingWDerivedTables(t *testing.T) {
	queries := []struct {
		query         string
		errorMessage  string
		recursiveDeps TableSet
		directDeps    TableSet
	}{
		{
			query:         "select id from (select x as id from user) as t",
			recursiveDeps: TS0,
			directDeps:    TS1,
		}, {
			query:         "select id from (select foo as id from user) as t",
			recursiveDeps: TS0,
			directDeps:    TS1,
		}, {
			query:         "select id from (select foo as id from (select x as foo from user) as c) as t",
			recursiveDeps: TS0,
			directDeps:    TS2,
		}, {
			query:         "select t.id from (select foo as id from user) as t",
			recursiveDeps: TS0,
			directDeps:    TS1,
		}, {
			query:        "select t.id2 from (select foo as id from user) as t",
			errorMessage: "column 't.id2' not found",
		}, {
			query:         "select id from (select 42 as id) as t",
			recursiveDeps: NoTables,
			directDeps:    TS1,
		}, {
			query:         "select t.id from (select 42 as id) as t",
			recursiveDeps: NoTables,
			directDeps:    TS1,
		}, {
			query:        "select ks.t.id from (select 42 as id) as t",
			errorMessage: "column 'ks.t.id' not found",
		}, {
			query:        "select * from (select id, id from user) as t",
			errorMessage: "Duplicate column name 'id'",
		}, {
			query:         "select t.baz = 1 from (select id as baz from user) as t",
			directDeps:    TS1,
			recursiveDeps: TS0,
		}, {
			query:         "select t.id from (select * from user, music) as t",
			directDeps:    TS2,
			recursiveDeps: MergeTableSets(TS0, TS1),
		}, {
			query:         "select t.id from (select * from user, music) as t order by t.id",
			directDeps:    TS2,
			recursiveDeps: MergeTableSets(TS0, TS1),
		}, {
			query:         "select t.id from (select * from user) as t join user as u on t.id = u.id",
			directDeps:    TS2,
			recursiveDeps: TS0,
		}, {
			query:         "select t.textcol from t3 ua join (select t3.uid, t3.textcol from t3 join t2) as t",
			directDeps:    TS3,
			recursiveDeps: TS1,
		}, {
			query:        "select uu.test from (select id from t1) uu",
			errorMessage: "column 'uu.test' not found",
		}, {
			query:        "select uu.id from (select id as col from t1) uu",
			errorMessage: "column 'uu.id' not found",
		}, {
			query:        "select uu.id from (select id as col from t1) uu",
			errorMessage: "column 'uu.id' not found",
		}, {
			query:         "select uu.id from (select id from t1) as uu where exists (select * from t2 as uu where uu.id = uu.uid)",
			directDeps:    TS2,
			recursiveDeps: TS0,
		}, {
			query:         "select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
			directDeps:    NoTables,
			recursiveDeps: NoTables,
		}, {
			query:         "select uu.count from (select count(*) as `count` from t1) uu",
			directDeps:    TS1,
			recursiveDeps: TS0,
		}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, st, err := parseAndAnalyzeStrictAllowErr(t, query.query, "d")
			sel := stmt.(*sqlparser.Select)
			if query.errorMessage != "" {
				require.EqualError(t, err, query.errorMessage)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, query.recursiveDeps, st.RecursiveDeps(extract(sel, 0)), "RecursiveDeps")
			assert.Equal(t, query.directDeps, st.DirectDeps(extract(sel, 0)), "DirectDeps")
		})
	}
}

func TestDerivedTablesOrderClause(t *testing.T) {
	queries := []struct {
		query         string
		recursiveDeps TableSet
		directDeps    TableSet
	}{{
		query:         "select 1 from (select id from user) as t order by id",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}, {
		query:         "select id from (select id from user) as t order by id",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}, {
		query:         "select id from (select id from user) as t order by t.id",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}, {
		query:         "select id as foo from (select id from user) as t order by foo",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}, {
		query:         "select bar from (select id as bar from user) as t order by bar",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}, {
		query:         "select bar as foo from (select id as bar from user) as t order by bar",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}, {
		query:         "select bar as foo from (select id as bar from user) as t order by foo",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}, {
		query:         "select bar as foo from (select id as bar, oo from user) as t order by oo",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}, {
		query:         "select bar as foo from (select id, oo from user) as t(bar,oo) order by bar",
		recursiveDeps: TS0,
		directDeps:    TS1,
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, st := parseAndAnalyzeStrict(t, query.query, "d")

			sel := stmt.(*sqlparser.Select)
			assert.Equal(t, query.recursiveDeps, st.RecursiveDeps(sel.OrderBy[0].Expr), "RecursiveDeps")
			assert.Equal(t, query.directDeps, st.DirectDeps(sel.OrderBy[0].Expr), "DirectDeps")
		})
	}
}

func TestScopingWComplexDerivedTables(t *testing.T) {
	queries := []struct {
		query     string
		rightDeps TableSet
		leftDeps  TableSet
	}{
		{
			query:     "select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
			rightDeps: TS0,
			leftDeps:  TS0,
		},
		{
			query:     "select 1 from user.user uu where exists (select 1 from user.user as uu where exists (select 1 from (select 1 from user.t1) uu where uu.user_id = uu.id))",
			rightDeps: TS1,
			leftDeps:  TS1,
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, st := parseAndAnalyzeStrict(t, query.query, "d")
			sel := parse.(*sqlparser.Select)
			comparisonExpr := sel.Where.Expr.(*sqlparser.ExistsExpr).Subquery.Select.(*sqlparser.Select).Where.Expr.(*sqlparser.ExistsExpr).Subquery.Select.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr)
			left := comparisonExpr.Left
			right := comparisonExpr.Right
			assert.Equal(t, query.leftDeps, st.RecursiveDeps(left), "Left RecursiveDeps")
			assert.Equal(t, query.rightDeps, st.RecursiveDeps(right), "Right RecursiveDeps")
		})
	}
}

func BenchmarkAnalyzeDerivedTableQueries(b *testing.B) {
	queries := []string{
		"select id from (select x as id from user) as t",
		"select id from (select foo as id from user) as t",
		"select id from (select foo as id from (select x as foo from user) as c) as t",
		"select t.id from (select foo as id from user) as t",
		"select t.id2 from (select foo as id from user) as t",
		"select id from (select 42 as id) as t",
		"select t.id from (select 42 as id) as t",
		"select ks.t.id from (select 42 as id) as t",
		"select * from (select id, id from user) as t",
		"select t.baz = 1 from (select id as baz from user) as t",
		"select t.id from (select * from user, music) as t",
		"select t.id from (select * from user, music) as t order by t.id",
		"select t.id from (select * from user) as t join user as u on t.id = u.id",
		"select t.col1 from t3 ua join (select t1.id, t1.col1 from t1 join t2) as t",
		"select uu.id from (select id from t1) as uu where exists (select * from t2 as uu where uu.id = uu.uid)",
		"select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
	}

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			parse, err := sqlparser.NewTestParser().Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo(), nil)
		}
	}
}
