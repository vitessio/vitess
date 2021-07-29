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

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

const T0 TableSet = 0

const (
	// Just here to make outputs more readable
	T1 TableSet = 1 << iota
	T2
	T3
	_ // T4 is not used in the tests
	T5
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
	stmt, semTable := parseAndAnalyze(t, query, "")

	sel, _ := stmt.(*sqlparser.Select)

	// extract the `t.col2` expression from the subquery
	sel2 := sel.SelectExprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.Subquery).Select.(*sqlparser.Select)
	s1 := semTable.BaseTableDependencies(extract(sel2, 0))

	// if scoping works as expected, we should be able to see the inner table being used by the inner expression
	assert.Equal(t, T2, s1)
}

func TestBindingSingleTablePositive(t *testing.T) {
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
			stmt, semTable := parseAndAnalyze(t, query, "d")
			sel, _ := stmt.(*sqlparser.Select)
			t1 := sel.From[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.EqualValues(t, 1, ts)

			assert.Equal(t, T1, semTable.BaseTableDependencies(extract(sel, 0)), query)
			assert.Equal(t, T1, semTable.Dependencies(extract(sel, 0)), query)
		})
	}
}

func TestBindingSingleTableNegative(t *testing.T) {
	queries := []string{
		"select foo.col from tabl",
		"select ks.tabl.col from tabl",
		"select ks.tabl.col from d.tabl",
		"select d.tabl.col from ks.tabl",
		"select foo.col from d.tabl",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)
			_, err = Analyze(parse.(sqlparser.SelectStatement), "d", &FakeSI{})
			require.Error(t, err)
		})
	}
}

func TestOrderByBindingSingleTable(t *testing.T) {
	t.Run("positive tests", func(t *testing.T) {
		tcases := []struct {
			sql  string
			deps TableSet
		}{{
			"select col from tabl order by col",
			T1,
		}, {
			"select col from tabl order by tabl.col",
			T1,
		}, {

			"select col from tabl order by d.tabl.col",
			T1,
		}, {
			"select col from tabl order by 1",
			T1,
		}, {
			"select col as c from tabl order by c",
			T1,
		}, {
			"select 1 as c from tabl order by c",
			T0,
		}}
		for _, tc := range tcases {
			t.Run(tc.sql, func(t *testing.T) {
				stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
				sel, _ := stmt.(*sqlparser.Select)
				order := sel.OrderBy[0].Expr
				d := semTable.BaseTableDependencies(order)
				require.Equal(t, tc.deps, d, tc.sql)
			})
		}
	})
}

func TestOrderByBindingMultiTable(t *testing.T) {
	t.Run("positive tests", func(t *testing.T) {
		tcases := []struct {
			sql  string
			deps TableSet
		}{{
			"select name, name from t1, t2 order by name",
			T2,
		}}
		for _, tc := range tcases {
			t.Run(tc.sql, func(t *testing.T) {
				stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
				sel, _ := stmt.(*sqlparser.Select)
				order := sel.OrderBy[0].Expr
				d := semTable.BaseTableDependencies(order)
				require.Equal(t, tc.deps, d, tc.sql)
			})
		}
	})
}

func TestGroupByBindingSingleTable(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		"select col from tabl group by col",
		T1,
	}, {
		"select col from tabl group by tabl.col",
		T1,
	}, {
		"select col from tabl group by d.tabl.col",
		T1,
	}, {
		"select tabl.col as x from tabl group by x",
		T1,
	}, {
		"select tabl.col as x from tabl group by col",
		T1,
	}, {
		"select col from tabl group by 1",
		T1,
	}, {
		"select col as c from tabl group by c",
		T1,
	}, {
		"select 1 as c from tabl group by c",
		T0,
	}, {
		"select t1.id from t1, t2 group by id",
		T1,
	}, {
		"select t.id from t, t1 group by id",
		T1,
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel, _ := stmt.(*sqlparser.Select)
			grp := sel.GroupBy[0]
			d := semTable.BaseTableDependencies(grp)
			require.Equal(t, tc.deps, d, tc.sql)
		})
	}
}

func TestBindingSingleAliasedTable(t *testing.T) {
	t.Run("positive tests", func(t *testing.T) {
		queries := []string{
			"select col from tabl as X",
			"select tabl.col from X as tabl",
			"select col from d.X as tabl",
			"select tabl.col from d.X as tabl",
		}
		for _, query := range queries {
			t.Run(query, func(t *testing.T) {
				stmt, semTable := parseAndAnalyze(t, query, "")
				sel, _ := stmt.(*sqlparser.Select)
				t1 := sel.From[0].(*sqlparser.AliasedTableExpr)
				ts := semTable.TableSetFor(t1)
				assert.EqualValues(t, 1, ts)

				d := semTable.BaseTableDependencies(extract(sel, 0))
				require.Equal(t, T1, d, query)
			})
		}
	})
	t.Run("negative tests", func(t *testing.T) {
		queries := []string{
			"select tabl.col from tabl as X",
			"select d.X.col from d.X as tabl",
			"select d.tabl.col from X as tabl",
			"select d.tabl.col from ks.X as tabl",
			"select d.tabl.col from d.X as tabl",
		}
		for _, query := range queries {
			t.Run(query, func(t *testing.T) {
				parse, err := sqlparser.Parse(query)
				require.NoError(t, err)
				_, err = Analyze(parse.(sqlparser.SelectStatement), "", &FakeSI{
					Tables: map[string]*vindexes.Table{
						"t": {Name: sqlparser.NewTableIdent("t")},
					},
				})
				require.Error(t, err)
			})
		}
	})
}

func TestUnion(t *testing.T) {
	query := "select col1 from tabl1 union select col2 from tabl2"

	stmt, semTable := parseAndAnalyze(t, query, "")
	union, _ := stmt.(*sqlparser.Union)
	sel1 := union.FirstStatement.(*sqlparser.Select)
	sel2 := union.UnionSelects[0].Statement.(*sqlparser.Select)

	t1 := sel1.From[0].(*sqlparser.AliasedTableExpr)
	t2 := sel2.From[0].(*sqlparser.AliasedTableExpr)
	ts1 := semTable.TableSetFor(t1)
	ts2 := semTable.TableSetFor(t2)
	assert.EqualValues(t, 1, ts1)
	assert.EqualValues(t, 2, ts2)

	d1 := semTable.BaseTableDependencies(extract(sel1, 0))
	d2 := semTable.BaseTableDependencies(extract(sel2, 0))
	assert.Equal(t, T1, d1)
	assert.Equal(t, T2, d2)
}

func TestBindingMultiTable(t *testing.T) {
	t.Run("positive tests", func(t *testing.T) {

		type testCase struct {
			query string
			deps  TableSet
		}
		queries := []testCase{{
			query: "select t.col from t, s",
			deps:  T1,
		}, {
			query: "select s.col from t join s",
			deps:  T2,
		}, {
			query: "select max(t.col+s.col) from t, s",
			deps:  T1 | T2,
		}, {
			query: "select max(t.col+s.col) from t join s",
			deps:  T1 | T2,
		}, {
			query: "select case t.col when s.col then r.col else u.col end from t, s, r, w, u",
			deps:  T1 | T2 | T3 | T5,
			// }, {
			//	// make sure that we don't let sub-query Dependencies leak out by mistake
			//	query: "select t.col + (select 42 from s) from t",
			//	deps:  T1,
			// }, {
			//	query: "select (select 42 from s where r.id = s.id) from r",
			//	deps:  T1 | T2,
		}, {
			query: "select X.col from t as X, s as S",
			deps:  T1,
		}, {
			query: "select X.col+S.col from t as X, s as S",
			deps:  T1 | T2,
		}, {
			query: "select max(X.col+S.col) from t as X, s as S",
			deps:  T1 | T2,
		}, {
			query: "select max(X.col+s.col) from t as X, s",
			deps:  T1 | T2,
		}, {
			query: "select b.t.col from b.t, t",
			deps:  T1,
		}, {
			query: "select u1.a + u2.a from u1, u2",
			deps:  T1 | T2,
		}}
		for _, query := range queries {
			t.Run(query.query, func(t *testing.T) {
				stmt, semTable := parseAndAnalyze(t, query.query, "user")
				sel, _ := stmt.(*sqlparser.Select)
				assert.Equal(t, query.deps, semTable.BaseTableDependencies(extract(sel, 0)), query.query)
			})
		}
	})

	t.Run("negative tests", func(t *testing.T) {
		queries := []string{
			"select 1 from d.tabl, d.tabl",
			"select 1 from d.tabl, tabl",
			"select 1 from user join user_extra user",
		}
		for _, query := range queries {
			t.Run(query, func(t *testing.T) {
				parse, err := sqlparser.Parse(query)
				require.NoError(t, err)
				_, err = Analyze(parse.(sqlparser.SelectStatement), "d", &FakeSI{
					Tables: map[string]*vindexes.Table{
						"tabl": {Name: sqlparser.NewTableIdent("tabl")},
						"foo":  {Name: sqlparser.NewTableIdent("foo")},
					},
				})
				require.Error(t, err)
			})
		}
	})
}

func TestBindingSingleDepPerTable(t *testing.T) {
	query := "select t.col + t.col2 from t"
	stmt, semTable := parseAndAnalyze(t, query, "")
	sel, _ := stmt.(*sqlparser.Select)

	d := semTable.BaseTableDependencies(extract(sel, 0))
	assert.Equal(t, 1, d.NumberOfTables(), "size wrong")
	assert.Equal(t, T1, d)
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
			if strings.Contains(query, ") as") {
				t.Skip("derived tables not implemented")
			}
			parse, _ := sqlparser.Parse(query)
			_, err := Analyze(parse.(sqlparser.SelectStatement), "test", &FakeSI{})
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
			_, err := Analyze(parse.(sqlparser.SelectStatement), "", &FakeSI{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "symbol t.col not found")
		})
	}
}

func TestUnknownColumnMap2(t *testing.T) {
	query := "select col from a, b"
	authoritativeTblA := vindexes.Table{
		Name: sqlparser.NewTableIdent("a"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewColIdent("col2"),
			Type: querypb.Type_VARCHAR,
		}},
		ColumnListAuthoritative: true,
	}
	authoritativeTblB := vindexes.Table{
		Name: sqlparser.NewTableIdent("b"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewColIdent("col"),
			Type: querypb.Type_VARCHAR,
		}},
		ColumnListAuthoritative: true,
	}
	nonAuthoritativeTblA := authoritativeTblA
	nonAuthoritativeTblA.ColumnListAuthoritative = false
	nonAuthoritativeTblB := authoritativeTblB
	nonAuthoritativeTblB.ColumnListAuthoritative = false
	authoritativeTblAWithConflict := vindexes.Table{
		Name: sqlparser.NewTableIdent("a"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewColIdent("col"),
			Type: querypb.Type_INT32,
		}},
		ColumnListAuthoritative: true,
	}
	authoritativeTblBWithInt := vindexes.Table{
		Name: sqlparser.NewTableIdent("b"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewColIdent("col"),
			Type: querypb.Type_INT32,
		}},
		ColumnListAuthoritative: true,
	}

	varchar := querypb.Type_VARCHAR
	int := querypb.Type_INT32

	parse, _ := sqlparser.Parse(query)
	expr := extract(parse.(*sqlparser.Select), 0)
	tests := []struct {
		name   string
		schema map[string]*vindexes.Table
		err    bool
		typ    *querypb.Type
	}{
		{
			name:   "no info about tables",
			schema: map[string]*vindexes.Table{"a": {}, "b": {}},
			err:    true,
		},
		{
			name:   "non authoritative columns",
			schema: map[string]*vindexes.Table{"a": &nonAuthoritativeTblA, "b": &nonAuthoritativeTblA},
			err:    true,
		},
		{
			name:   "non authoritative columns - one authoritative and one not",
			schema: map[string]*vindexes.Table{"a": &nonAuthoritativeTblA, "b": &authoritativeTblB},
			err:    false,
			typ:    &varchar,
		},
		{
			name:   "non authoritative columns - one authoritative and one not",
			schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &nonAuthoritativeTblB},
			err:    false,
			typ:    &varchar,
		},
		{
			name:   "authoritative columns",
			schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &authoritativeTblB},
			err:    false,
			typ:    &varchar,
		},
		{
			name:   "authoritative columns",
			schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &authoritativeTblBWithInt},
			err:    false,
			typ:    &int,
		},
		{
			name:   "authoritative columns with overlap",
			schema: map[string]*vindexes.Table{"a": &authoritativeTblAWithConflict, "b": &authoritativeTblB},
			err:    true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			si := &FakeSI{Tables: test.schema}
			tbl, err := Analyze(parse.(sqlparser.SelectStatement), "", si)
			require.NoError(t, err)

			if test.err {
				require.Error(t, tbl.ProjectionErr)
			} else {
				require.NoError(t, tbl.ProjectionErr)
				typ := tbl.TypeFor(expr)
				assert.Equal(t, test.typ, typ)
			}
		})
	}
}

func TestUnknownPredicate(t *testing.T) {
	query := "select 1 from a, b where col = 1"
	authoritativeTblA := &vindexes.Table{
		Name: sqlparser.NewTableIdent("a"),
	}
	authoritativeTblB := &vindexes.Table{
		Name: sqlparser.NewTableIdent("b"),
	}

	parse, _ := sqlparser.Parse(query)

	tests := []struct {
		name   string
		schema map[string]*vindexes.Table
		err    bool
	}{
		{
			name:   "no info about tables",
			schema: map[string]*vindexes.Table{"a": authoritativeTblA, "b": authoritativeTblB},
			err:    false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			si := &FakeSI{Tables: test.schema}
			_, err := Analyze(parse.(sqlparser.SelectStatement), "", si)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestScoping(t *testing.T) {
	queries := []struct {
		query        string
		errorMessage string
	}{
		{
			query:        "select 1 from u1, u2 left join u3 on u1.a = u2.a",
			errorMessage: "symbol u1.a not found",
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			_, err = Analyze(parse.(sqlparser.SelectStatement), "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewTableIdent("t")},
				},
			})
			if query.errorMessage == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, query.errorMessage)
			}
		})
	}
}

func TestScopingWDerivedTables(t *testing.T) {
	queries := []struct {
		query                string
		errorMessage         string
		recursiveExpectation TableSet
		expectation          TableSet
	}{
		{
			query:                "select id from (select id from user where id = 5) as t",
			recursiveExpectation: T1,
			expectation:          T2,
		}, {
			query:                "select id from (select foo as id from user) as t",
			recursiveExpectation: T1,
			expectation:          T2,
		}, {
			query:                "select id from (select foo as id from (select x as foo from user) as c) as t",
			recursiveExpectation: T1,
			expectation:          T3,
		}, {
			query:                "select t.id from (select foo as id from user) as t",
			recursiveExpectation: T1,
			expectation:          T2,
		}, {
			query:        "select t.id2 from (select foo as id from user) as t",
			errorMessage: "symbol t.id2 not found",
		}, {
			query:                "select id from (select 42 as id) as t",
			recursiveExpectation: T0,
			expectation:          T2,
		}, {
			query:                "select t.id from (select 42 as id) as t",
			recursiveExpectation: T0,
			expectation:          T2,
		}, {
			query:        "select ks.t.id from (select 42 as id) as t",
			errorMessage: "symbol ks.t.id not found",
		}, {
			query:        "select * from (select id, id from user) as t",
			errorMessage: "Duplicate column name 'id'",
		}, {
			query:                "select t.baz = 1 from (select id as baz from user) as t",
			expectation:          T2,
			recursiveExpectation: T1,
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			st, err := Analyze(parse.(sqlparser.SelectStatement), "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewTableIdent("t")},
				},
			})
			if query.errorMessage != "" {
				require.EqualError(t, err, query.errorMessage)
			} else {
				require.NoError(t, err)
				sel := parse.(*sqlparser.Select)
				assert.Equal(t, query.recursiveExpectation, st.BaseTableDependencies(extract(sel, 0)))
				assert.Equal(t, query.expectation, st.Dependencies(extract(sel, 0)))
			}
		})
	}
}

func parseAndAnalyze(t *testing.T, query, dbName string) (sqlparser.Statement, *SemTable) {
	t.Helper()
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)

	cols1 := []vindexes.Column{{
		Name: sqlparser.NewColIdent("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewColIdent("uid"),
		Type: querypb.Type_INT64,
	}, {
		Name: sqlparser.NewColIdent("name"),
		Type: querypb.Type_VARCHAR,
	}}

	semTable, err := Analyze(parse.(sqlparser.SelectStatement), dbName, &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t":  {Name: sqlparser.NewTableIdent("t")},
			"t1": {Name: sqlparser.NewTableIdent("t1"), Columns: cols1, ColumnListAuthoritative: true},
			"t2": {Name: sqlparser.NewTableIdent("t2"), Columns: cols2, ColumnListAuthoritative: true},
		},
	})
	require.NoError(t, err)
	return parse, semTable
}
