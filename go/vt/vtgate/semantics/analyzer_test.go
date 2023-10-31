/*
Copyright 2021 The Vitess Authors.

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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var T0 TableSet

var (
	// Just here to make outputs more readable
	None = EmptyTableSet()
	TS0  = SingleTableSet(0)
	TS1  = SingleTableSet(1)
	TS2  = SingleTableSet(2)
	TS3  = SingleTableSet(3)
	TS4  = SingleTableSet(4)
)

func extract(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}

func TestBindingSingleTablePositive(t *testing.T) {
	queries := []string{
		"select col from tabl",
		"select uid from t2",
		"select tabl.col from tabl",
		"select d.tabl.col from tabl",
		"select col from d.tabl",
		"select tabl.col from d.tabl",
		"select d.tabl.col from d.tabl",
		"select col+col from tabl",
		"select max(col1+col2) from d.tabl",
		"select max(id) from t1",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query, "d")
			sel, _ := stmt.(*sqlparser.Select)
			t1 := sel.From[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.Equal(t, SingleTableSet(0), ts)

			recursiveDeps := semTable.RecursiveDeps(extract(sel, 0))
			assert.Equal(t, TS0, recursiveDeps, query)
			assert.Equal(t, TS0, semTable.DirectDeps(extract(sel, 0)), query)
			assert.Equal(t, 1, recursiveDeps.NumberOfTables(), "number of tables is wrong")
		})
	}
}

func TestInformationSchemaColumnInfo(t *testing.T) {
	stmt, semTable := parseAndAnalyze(t, "select table_comment, file_name from information_schema.`TABLES`, information_schema.`FILES`", "d")

	sel, _ := stmt.(*sqlparser.Select)
	tables := SingleTableSet(0)
	files := SingleTableSet(1)

	assert.Equal(t, tables, semTable.RecursiveDeps(extract(sel, 0)))
	assert.Equal(t, files, semTable.DirectDeps(extract(sel, 1)))
}

func TestBindingSingleAliasedTablePositive(t *testing.T) {
	queries := []string{
		"select col from tabl as X",
		"select tabl.col from X as tabl",
		"select col from d.X as tabl",
		"select tabl.col from d.X as tabl",
		"select col+col from tabl as X",
		"select max(tabl.col1 + tabl.col2) from d.X as tabl",
		"select max(t.id) from t1 as t",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query, "")
			sel, _ := stmt.(*sqlparser.Select)
			t1 := sel.From[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.Equal(t, SingleTableSet(0), ts)

			recursiveDeps := semTable.RecursiveDeps(extract(sel, 0))
			require.Equal(t, TS0, recursiveDeps, query)
			assert.Equal(t, 1, recursiveDeps.NumberOfTables(), "number of tables is wrong")
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
		"select tabl.col from d.tabl as t",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)
			st, err := Analyze(parse, "d", &FakeSI{})
			require.NoError(t, err)
			require.ErrorContains(t, st.NotUnshardedErr, "column")
			require.ErrorContains(t, st.NotUnshardedErr, "not found")
		})
	}
}

func TestBindingSingleAliasedTableNegative(t *testing.T) {
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
			st, err := Analyze(parse, "", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewIdentifierCS("t")},
				},
			})
			require.NoError(t, err)
			require.Error(t, st.NotUnshardedErr)
		})
	}
}

func TestBindingMultiTablePositive(t *testing.T) {
	type testCase struct {
		query          string
		deps           TableSet
		numberOfTables int
	}
	queries := []testCase{{
		query:          "select t.col from t, s",
		deps:           TS0,
		numberOfTables: 1,
	}, {
		query:          "select s.col from t join s",
		deps:           TS1,
		numberOfTables: 1,
	}, {
		query:          "select max(t.col+s.col) from t, s",
		deps:           MergeTableSets(TS0, TS1),
		numberOfTables: 2,
	}, {
		query:          "select max(t.col+s.col) from t join s",
		deps:           MergeTableSets(TS0, TS1),
		numberOfTables: 2,
	}, {
		query:          "select case t.col when s.col then r.col else u.col end from t, s, r, w, u",
		deps:           MergeTableSets(TS0, TS1, TS2, TS4),
		numberOfTables: 4,
	}, {
		query:          "select u1.a + u2.a from u1, u2",
		deps:           MergeTableSets(TS0, TS1),
		numberOfTables: 2,
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query.query, "user")
			sel, _ := stmt.(*sqlparser.Select)
			recursiveDeps := semTable.RecursiveDeps(extract(sel, 0))
			assert.Equal(t, query.deps, recursiveDeps, query.query)
			assert.Equal(t, query.numberOfTables, recursiveDeps.NumberOfTables(), "number of tables is wrong")
		})
	}
}

func TestBindingMultiAliasedTablePositive(t *testing.T) {
	type testCase struct {
		query          string
		deps           TableSet
		numberOfTables int
	}
	queries := []testCase{{
		query:          "select X.col from t as X, s as S",
		deps:           TS0,
		numberOfTables: 1,
	}, {
		query:          "select X.col+S.col from t as X, s as S",
		deps:           MergeTableSets(TS0, TS1),
		numberOfTables: 2,
	}, {
		query:          "select max(X.col+S.col) from t as X, s as S",
		deps:           MergeTableSets(TS0, TS1),
		numberOfTables: 2,
	}, {
		query:          "select max(X.col+s.col) from t as X, s",
		deps:           MergeTableSets(TS0, TS1),
		numberOfTables: 2,
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query.query, "user")
			sel, _ := stmt.(*sqlparser.Select)
			recursiveDeps := semTable.RecursiveDeps(extract(sel, 0))
			assert.Equal(t, query.deps, recursiveDeps, query.query)
			assert.Equal(t, query.numberOfTables, recursiveDeps.NumberOfTables(), "number of tables is wrong")
		})
	}
}

func TestBindingMultiTableNegative(t *testing.T) {
	queries := []string{
		"select 1 from d.tabl, d.tabl",
		"select 1 from d.tabl, tabl",
		"select t.col from k.t, t",
		"select b.t.col from b.t, t",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)
			_, err = Analyze(parse, "d", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"tabl": {Name: sqlparser.NewIdentifierCS("tabl")},
					"foo":  {Name: sqlparser.NewIdentifierCS("foo")},
				},
			})
			require.Error(t, err)
		})
	}
}

func TestBindingMultiAliasedTableNegative(t *testing.T) {
	queries := []string{
		"select 1 from d.tabl as tabl, d.tabl",
		"select 1 from d.tabl as tabl, tabl",
		"select 1 from d.tabl as a, tabl as a",
		"select 1 from user join user_extra user",
		"select t.col from k.t as t, t",
		"select b.t.col from b.t as t, t",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query)
			require.NoError(t, err)
			_, err = Analyze(parse, "d", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"tabl": {Name: sqlparser.NewIdentifierCS("tabl")},
					"foo":  {Name: sqlparser.NewIdentifierCS("foo")},
				},
			})
			require.Error(t, err)
		})
	}
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
			parse, _ := sqlparser.Parse(query)
			_, err := Analyze(parse, "test", &FakeSI{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "VT03013: not unique table/alias")
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
			st, err := Analyze(parse, "", &FakeSI{})
			require.NoError(t, err)
			require.ErrorContains(t, st.NotUnshardedErr, "column 't.col' not found")
		})
	}
}

func TestUnknownColumnMap2(t *testing.T) {
	authoritativeTblA := vindexes.Table{
		Name: sqlparser.NewIdentifierCS("a"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("col2"),
			Type: sqltypes.VarChar,
		}},
		ColumnListAuthoritative: true,
	}
	authoritativeTblB := vindexes.Table{
		Name: sqlparser.NewIdentifierCS("b"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("col"),
			Type: sqltypes.VarChar,
		}},
		ColumnListAuthoritative: true,
	}
	nonAuthoritativeTblA := authoritativeTblA
	nonAuthoritativeTblA.ColumnListAuthoritative = false
	nonAuthoritativeTblB := authoritativeTblB
	nonAuthoritativeTblB.ColumnListAuthoritative = false
	authoritativeTblAWithConflict := vindexes.Table{
		Name: sqlparser.NewIdentifierCS("a"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("col"),
			Type: sqltypes.Int64,
		}},
		ColumnListAuthoritative: true,
	}
	authoritativeTblBWithInt := vindexes.Table{
		Name: sqlparser.NewIdentifierCS("b"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("col"),
			Type: sqltypes.Int64,
		}},
		ColumnListAuthoritative: true,
	}

	tests := []struct {
		name   string
		schema map[string]*vindexes.Table
		err    bool
		typ    querypb.Type
	}{{
		name:   "no info about tables",
		schema: map[string]*vindexes.Table{"a": {}, "b": {}},
		err:    true,
	}, {
		name:   "non authoritative columns",
		schema: map[string]*vindexes.Table{"a": &nonAuthoritativeTblA, "b": &nonAuthoritativeTblA},
		err:    true,
	}, {
		name:   "non authoritative columns - one authoritative and one not",
		schema: map[string]*vindexes.Table{"a": &nonAuthoritativeTblA, "b": &authoritativeTblB},
		err:    false,
		typ:    sqltypes.VarChar,
	}, {
		name:   "non authoritative columns - one authoritative and one not",
		schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &nonAuthoritativeTblB},
		err:    false,
		typ:    sqltypes.VarChar,
	}, {
		name:   "authoritative columns",
		schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &authoritativeTblB},
		err:    false,
		typ:    sqltypes.VarChar,
	}, {
		name:   "authoritative columns",
		schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &authoritativeTblBWithInt},
		err:    false,
		typ:    sqltypes.Int64,
	}, {
		name:   "authoritative columns with overlap",
		schema: map[string]*vindexes.Table{"a": &authoritativeTblAWithConflict, "b": &authoritativeTblB},
		err:    true,
	}}

	queries := []string{"select col from a, b", "select col from a as user, b as extra"}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			parse, _ := sqlparser.Parse(query)
			expr := extract(parse.(*sqlparser.Select), 0)

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					si := &FakeSI{Tables: test.schema}
					tbl, err := Analyze(parse, "", si)
					if test.err {
						require.True(t, err != nil || tbl.NotSingleRouteErr != nil)
					} else {
						require.NoError(t, err)
						require.NoError(t, tbl.NotSingleRouteErr)
						typ, found := tbl.TypeForExpr(expr)
						assert.True(t, found)
						assert.Equal(t, test.typ, typ.Type)
					}
				})
			}
		})
	}
}

func TestUnknownPredicate(t *testing.T) {
	query := "select 1 from a, b where col = 1"
	authoritativeTblA := &vindexes.Table{
		Name: sqlparser.NewIdentifierCS("a"),
	}
	authoritativeTblB := &vindexes.Table{
		Name: sqlparser.NewIdentifierCS("b"),
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
			_, err := Analyze(parse, "", si)
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
			errorMessage: "column 'u1.a' not found",
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			st, err := Analyze(parse, "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewIdentifierCS("t")},
				},
			})
			require.NoError(t, err)
			require.EqualError(t, st.NotUnshardedErr, query.errorMessage)
		})
	}
}

func TestScopeForSubqueries(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{
		{
			sql:  `select t.col1, (select t.col2 from z as t) from x as t`,
			deps: TS1,
		}, {
			sql:  `select t.col1, (select t.col2 from z) from x as t`,
			deps: TS0,
		}, {
			sql:  `select t.col1, (select (select z.col2 from y) from z) from x as t`,
			deps: TS1,
		}, {
			sql:  `select t.col1, (select (select y.col2 from y) from z) from x as t`,
			deps: None,
		}, {
			sql:  `select t.col1, (select (select (select (select w.col2 from w) from x) from y) from z) from x as t`,
			deps: None,
		}, {
			sql:  `select t.col1, (select id from t) from x as t`,
			deps: TS1,
		},
	}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel, _ := stmt.(*sqlparser.Select)

			// extract the first expression from the subquery (which should be the second expression in the outer query)
			sel2 := sel.SelectExprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.Subquery).Select.(*sqlparser.Select)
			exp := extract(sel2, 0)
			s1 := semTable.RecursiveDeps(exp)
			require.NoError(t, semTable.NotSingleRouteErr)
			// if scoping works as expected, we should be able to see the inner table being used by the inner expression
			assert.Equal(t, tc.deps, s1)
		})
	}
}

func TestSubqueryOrderByBinding(t *testing.T) {
	queries := []struct {
		query    string
		expected TableSet
	}{{
		query:    "select * from user u where exists (select * from user order by col)",
		expected: TS1,
	}, {
		query:    "select * from user u where exists (select * from user order by user.col)",
		expected: TS1,
	}, {
		query:    "select * from user u where exists (select * from user order by u.col)",
		expected: TS0,
	}, {
		query:    "select * from dbName.user as u where exists (select * from dbName.user order by u.col)",
		expected: TS0,
	}, {
		query:    "select * from dbName.user where exists (select * from otherDb.user order by dbName.user.col)",
		expected: TS0,
	}, {
		query:    "select id from dbName.t1 where exists (select * from dbName.t2 order by dbName.t1.id)",
		expected: TS0,
	}}

	for _, tc := range queries {
		t.Run(tc.query, func(t *testing.T) {
			ast, err := sqlparser.Parse(tc.query)
			require.NoError(t, err)

			sel := ast.(*sqlparser.Select)
			st, err := Analyze(sel, "dbName", fakeSchemaInfo())
			require.NoError(t, err)
			exists := sel.Where.Expr.(*sqlparser.ExistsExpr)
			expr := exists.Subquery.Select.(*sqlparser.Select).OrderBy[0].Expr
			require.Equal(t, tc.expected, st.DirectDeps(expr))
			require.Equal(t, tc.expected, st.RecursiveDeps(expr))
		})
	}
}

func TestOrderByBindingTable(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		"select col from tabl order by col",
		TS0,
	}, {
		"select tabl.col from d.tabl order by col",
		TS0,
	}, {
		"select d.tabl.col from d.tabl order by col",
		TS0,
	}, {
		"select col from tabl order by tabl.col",
		TS0,
	}, {
		"select col from tabl order by d.tabl.col",
		TS0,
	}, {
		"select col from tabl order by 1",
		TS0,
	}, {
		"select col as c from tabl order by c",
		TS0,
	}, {
		"select 1 as c from tabl order by c",
		T0,
	}, {
		"select name, name from t1, t2 order by name",
		TS1,
	}, {
		"(select id from t1) union (select uid from t2) order by id",
		MergeTableSets(TS0, TS1),
	}, {
		"select id from t1 union (select uid from t2) order by 1",
		MergeTableSets(TS0, TS1),
	}, {
		"select id from t1 union select uid from t2 union (select name from t) order by 1",
		MergeTableSets(TS0, TS1, TS2),
	}, {
		"select a.id from t1 as a union (select uid from t2) order by 1",
		MergeTableSets(TS0, TS1),
	}, {
		"select b.id as a from t1 as b union (select uid as c from t2) order by 1",
		MergeTableSets(TS0, TS1),
	}, {
		"select a.id from t1 as a union (select uid from t2, t union (select name from t) order by 1) order by 1",
		MergeTableSets(TS0, TS1, TS3),
	}, {
		"select a.id from t1 as a union (select uid from t2, t union (select name from t) order by 1) order by id",
		MergeTableSets(TS0, TS1, TS3),
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")

			var order sqlparser.Expr
			switch stmt := stmt.(type) {
			case *sqlparser.Select:
				order = stmt.OrderBy[0].Expr
			case *sqlparser.Union:
				order = stmt.OrderBy[0].Expr
			default:
				t.Fail()
			}
			d := semTable.RecursiveDeps(order)
			require.Equal(t, tc.deps, d, tc.sql)
		})
	}
}

func TestGroupByBinding(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		"select col from tabl group by col",
		TS0,
	}, {
		"select col from tabl group by tabl.col",
		TS0,
	}, {
		"select col from tabl group by d.tabl.col",
		TS0,
	}, {
		"select tabl.col as x from tabl group by x",
		TS0,
	}, {
		"select tabl.col as x from tabl group by col",
		TS0,
	}, {
		"select d.tabl.col as x from tabl group by x",
		TS0,
	}, {
		"select d.tabl.col as x from tabl group by col",
		TS0,
	}, {
		"select col from tabl group by 1",
		TS0,
	}, {
		"select col as c from tabl group by c",
		TS0,
	}, {
		"select 1 as c from tabl group by c",
		T0,
	}, {
		"select t1.id from t1, t2 group by id",
		TS0,
	}, {
		"select id from t, t1 group by id",
		TS1,
	}, {
		"select id from t, t1 group by id",
		TS1,
	}, {
		"select a.id from t as a, t1 group by id",
		TS0,
	}, {
		"select a.id from t, t1 as a group by id",
		TS1,
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel, _ := stmt.(*sqlparser.Select)
			grp := sel.GroupBy[0]
			d := semTable.RecursiveDeps(grp)
			require.Equal(t, tc.deps, d, tc.sql)
		})
	}
}

func TestHavingBinding(t *testing.T) {
	tcases := []struct {
		sql  string
		deps TableSet
	}{{
		"select col from tabl having col = 1",
		TS0,
	}, {
		"select col from tabl having tabl.col = 1",
		TS0,
	}, {
		"select col from tabl having d.tabl.col = 1",
		TS0,
	}, {
		"select tabl.col as x from tabl having x = 1",
		TS0,
	}, {
		"select tabl.col as x from tabl having col",
		TS0,
	}, {
		"select col from tabl having 1 = 1",
		T0,
	}, {
		"select col as c from tabl having c = 1",
		TS0,
	}, {
		"select 1 as c from tabl having c = 1",
		T0,
	}, {
		"select t1.id from t1, t2 having id = 1",
		TS0,
	}, {
		"select t.id from t, t1 having id = 1",
		TS0,
	}, {
		"select t.id, count(*) as a from t, t1 group by t.id having a = 1",
		MergeTableSets(TS0, TS1),
	}, {
		"select t.id, sum(t2.name) as a from t, t2 group by t.id having a = 1",
		TS1,
	}, {
		sql:  "select u2.a, u1.a from u1, u2 having u2.a = 2",
		deps: TS1,
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, tc.sql, "d")
			sel, _ := stmt.(*sqlparser.Select)
			hvng := sel.Having.Expr
			d := semTable.RecursiveDeps(hvng)
			require.Equal(t, tc.deps, d, tc.sql)
		})
	}
}

func TestUnionCheckFirstAndLastSelectsDeps(t *testing.T) {
	query := "select col1 from tabl1 union select col2 from tabl2"

	stmt, semTable := parseAndAnalyze(t, query, "")
	union, _ := stmt.(*sqlparser.Union)
	sel1 := union.Left.(*sqlparser.Select)
	sel2 := union.Right.(*sqlparser.Select)

	t1 := sel1.From[0].(*sqlparser.AliasedTableExpr)
	t2 := sel2.From[0].(*sqlparser.AliasedTableExpr)
	ts1 := semTable.TableSetFor(t1)
	ts2 := semTable.TableSetFor(t2)
	assert.Equal(t, SingleTableSet(0), ts1)
	assert.Equal(t, SingleTableSet(1), ts2)

	d1 := semTable.RecursiveDeps(extract(sel1, 0))
	d2 := semTable.RecursiveDeps(extract(sel2, 0))
	assert.Equal(t, TS0, d1)
	assert.Equal(t, TS1, d2)
}

func TestUnionOrderByRewrite(t *testing.T) {
	query := "select tabl1.id from tabl1 union select 1 order by 1"

	stmt, _ := parseAndAnalyze(t, query, "")
	assert.Equal(t, "select tabl1.id from tabl1 union select 1 from dual order by id asc", sqlparser.String(stmt))
}

func TestInvalidQueries(t *testing.T) {
	tcases := []struct {
		sql             string
		serr            string
		err             error
		notUnshardedErr string
	}{{
		sql: "select t1.id, t1.col1 from t1 union select t2.uid from t2",
		err: &UnionColumnsDoNotMatchError{FirstProj: 2, SecondProj: 1},
	}, {
		sql: "select t1.id from t1 union select t2.uid, t2.price from t2",
		err: &UnionColumnsDoNotMatchError{FirstProj: 1, SecondProj: 2},
	}, {
		sql: "select t1.id from t1 union select t2.uid, t2.price from t2",
		err: &UnionColumnsDoNotMatchError{FirstProj: 1, SecondProj: 2},
	}, {
		sql: "(select 1,2 union select 3,4) union (select 5,6 union select 7)",
		err: &UnionColumnsDoNotMatchError{FirstProj: 2, SecondProj: 1},
	}, {
		sql:  "select id from a union select 3 order by a.id",
		err:  &QualifiedOrderInUnionError{Table: "a"},
		serr: "Table `a` from one of the SELECTs cannot be used in global ORDER clause",
	}, {
		sql:  "select a.id, b.id from a, b union select 1, 2 order by id",
		serr: "Column 'id' in field list is ambiguous",
	}, {
		sql:  "select sql_calc_found_rows id from a union select 1 limit 109",
		err:  &UnionWithSQLCalcFoundRowsError{},
		serr: "VT12001: unsupported: SQL_CALC_FOUND_ROWS not supported with union",
	}, {
		sql:  "select * from (select sql_calc_found_rows id from a) as t",
		serr: "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'",
	}, {
		sql:  "select (select sql_calc_found_rows id from a) as t",
		serr: "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'",
	}, {
		sql:  "select id from t1 natural join t2",
		serr: "VT12001: unsupported: natural join",
	}, {
		sql: "select * from music where user_id IN (select sql_calc_found_rows * from music limit 10)",
		err: &SQLCalcFoundRowsUsageError{},
	}, {
		sql:  "select is_free_lock('xyz') from user",
		serr: "is_free_lock('xyz') allowed only with dual",
	}, {
		sql: "SELECT * FROM JSON_TABLE('[ {\"c1\": null} ]','$[*]' COLUMNS( c1 INT PATH '$.c1' ERROR ON ERROR )) as jt",
		err: &JSONTablesError{},
	}, {
		sql:             "select does_not_exist from t1",
		notUnshardedErr: "column 'does_not_exist' not found in table 't1'",
	}, {
		sql:             "select t1.does_not_exist from t1, t2",
		notUnshardedErr: "column 't1.does_not_exist' not found",
	}, {
		sql:  "select 1 from t1 where id = (select 1, 2)",
		serr: "Operand should contain 1 column(s)",
	}, {
		sql:  "select 1 from t1 where (id, id) in (select 1, 2, 3)",
		serr: "Operand should contain 2 column(s)",
	}, {
		sql:  "WITH RECURSIVE cte (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte",
		serr: "VT12001: unsupported: recursive common table expression",
	}, {
		sql:  "with x as (select 1), x as (select 1) select * from x",
		serr: "VT03013: not unique table/alias: 'x'",
	}, {
		// should not fail, same name is valid as long as it's not in the same scope
		sql: "with x as (with x as (select 1) select * from x) select * from x",
	}}

	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			parse, err := sqlparser.Parse(tc.sql)
			require.NoError(t, err)

			st, err := Analyze(parse, "dbName", fakeSchemaInfo())

			switch {
			case tc.err != nil:
				require.Error(t, err)
				require.Equal(t, tc.err, err)
			case tc.serr != "":
				require.EqualError(t, err, tc.serr)
			case tc.notUnshardedErr != "":
				require.EqualError(t, st.NotUnshardedErr, tc.notUnshardedErr)
			}
		})
	}
}

func TestUnionWithOrderBy(t *testing.T) {
	query := "select col1 from tabl1 union (select col2 from tabl2) order by 1"

	stmt, semTable := parseAndAnalyze(t, query, "")
	union, _ := stmt.(*sqlparser.Union)
	sel1 := sqlparser.GetFirstSelect(union)
	sel2 := sqlparser.GetFirstSelect(union.Right)

	t1 := sel1.From[0].(*sqlparser.AliasedTableExpr)
	t2 := sel2.From[0].(*sqlparser.AliasedTableExpr)
	ts1 := semTable.TableSetFor(t1)
	ts2 := semTable.TableSetFor(t2)
	assert.Equal(t, SingleTableSet(0), ts1)
	assert.Equal(t, SingleTableSet(1), ts2)

	d1 := semTable.RecursiveDeps(extract(sel1, 0))
	d2 := semTable.RecursiveDeps(extract(sel2, 0))
	assert.Equal(t, TS0, d1)
	assert.Equal(t, TS1, d2)
}

func TestScopingWDerivedTables(t *testing.T) {
	queries := []struct {
		query                string
		errorMessage         string
		recursiveExpectation TableSet
		expectation          TableSet
	}{
		{
			query:                "select id from (select x as id from user) as t",
			recursiveExpectation: TS0,
			expectation:          TS1,
		}, {
			query:                "select id from (select foo as id from user) as t",
			recursiveExpectation: TS0,
			expectation:          TS1,
		}, {
			query:                "select id from (select foo as id from (select x as foo from user) as c) as t",
			recursiveExpectation: TS0,
			expectation:          TS2,
		}, {
			query:                "select t.id from (select foo as id from user) as t",
			recursiveExpectation: TS0,
			expectation:          TS1,
		}, {
			query:        "select t.id2 from (select foo as id from user) as t",
			errorMessage: "column 't.id2' not found",
		}, {
			query:                "select id from (select 42 as id) as t",
			recursiveExpectation: T0,
			expectation:          TS1,
		}, {
			query:                "select t.id from (select 42 as id) as t",
			recursiveExpectation: T0,
			expectation:          TS1,
		}, {
			query:        "select ks.t.id from (select 42 as id) as t",
			errorMessage: "column 'ks.t.id' not found",
		}, {
			query:        "select * from (select id, id from user) as t",
			errorMessage: "Duplicate column name 'id'",
		}, {
			query:                "select t.baz = 1 from (select id as baz from user) as t",
			expectation:          TS1,
			recursiveExpectation: TS0,
		}, {
			query:                "select t.id from (select * from user, music) as t",
			expectation:          TS2,
			recursiveExpectation: MergeTableSets(TS0, TS1),
		}, {
			query:                "select t.id from (select * from user, music) as t order by t.id",
			expectation:          TS2,
			recursiveExpectation: MergeTableSets(TS0, TS1),
		}, {
			query:                "select t.id from (select * from user) as t join user as u on t.id = u.id",
			expectation:          TS1,
			recursiveExpectation: TS0,
		}, {
			query:                "select t.col1 from t3 ua join (select t1.id, t1.col1 from t1 join t2) as t",
			expectation:          TS3,
			recursiveExpectation: TS1,
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
			query:                "select uu.id from (select id from t1) as uu where exists (select * from t2 as uu where uu.id = uu.uid)",
			expectation:          TS1,
			recursiveExpectation: TS0,
		}, {
			query:                "select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
			expectation:          T0,
			recursiveExpectation: T0,
		}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			st, err := Analyze(parse, "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewIdentifierCS("t")},
				},
			})

			switch {
			case query.errorMessage != "" && err != nil:
				require.EqualError(t, err, query.errorMessage)
			case query.errorMessage != "":
				require.EqualError(t, st.NotUnshardedErr, query.errorMessage)
			default:
				require.NoError(t, err)
				sel := parse.(*sqlparser.Select)
				assert.Equal(t, query.recursiveExpectation, st.RecursiveDeps(extract(sel, 0)), "RecursiveDeps")
				assert.Equal(t, query.expectation, st.DirectDeps(extract(sel, 0)), "DirectDeps")
			}
		})
	}
}

func TestScopingWithWITH(t *testing.T) {
	queries := []struct {
		query             string
		errorMessage      string
		recursive, direct TableSet
	}{
		{
			query:     "with t as (select x as id from user) select id from t",
			recursive: TS0,
			direct:    TS1,
		}, {
			query:     "with t as (select foo as id from user) select id from t",
			recursive: TS0,
			direct:    TS1,
		}, {
			query:     "with c as (select x as foo from user), t as (select foo as id from c) select id from t",
			recursive: TS0,
			direct:    TS2,
		}, {
			query:     "with t as (select foo as id from user) select t.id from t",
			recursive: TS0,
			direct:    TS1,
		}, {
			query:        "select t.id2 from (select foo as id from user) as t",
			errorMessage: "column 't.id2' not found",
		}, {
			query:     "with t as (select 42 as id) select id from t",
			recursive: T0,
			direct:    TS1,
		}, {
			query:     "with t as (select 42 as id) select t.id from t",
			recursive: T0,
			direct:    TS1,
		}, {
			query:        "with t as (select 42 as id) select ks.t.id from t",
			errorMessage: "column 'ks.t.id' not found",
		}, {
			query:        "with t as (select id, id from user)  select * from t",
			errorMessage: "Duplicate column name 'id'",
		}, {
			query:     "with t as (select id as baz from user) select t.baz = 1 from t",
			direct:    TS1,
			recursive: TS0,
		}, {
			query:     "with t as (select * from user, music) select t.id from  t",
			direct:    TS2,
			recursive: MergeTableSets(TS0, TS1),
		}, {
			query:     "with t as (select * from user, music) select t.id from t order by t.id",
			direct:    TS2,
			recursive: MergeTableSets(TS0, TS1),
		}, {
			query:     "with t as (select * from user) select t.id from t join user as u on t.id = u.id",
			direct:    TS1,
			recursive: TS0,
		}, {
			query:     "with t as (select t1.id, t1.col1 from t1 join t2) select t.col1 from t3 ua join t",
			direct:    TS3,
			recursive: TS1,
		}, {
			query:        "with uu as (select id from t1) select uu.test from uu",
			errorMessage: "column 'uu.test' not found",
		}, {
			query:        "with uu as (select id as col from t1) select uu.id from uu",
			errorMessage: "column 'uu.id' not found",
		}, {
			query:        "select uu.id from (select id as col from t1) uu",
			errorMessage: "column 'uu.id' not found",
		}, {
			query:     "select uu.id from (select id from t1) as uu where exists (select * from t2 as uu where uu.id = uu.uid)",
			direct:    TS1,
			recursive: TS0,
		}, {
			query:     "select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
			direct:    T0,
			recursive: T0,
		}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			st, err := Analyze(parse, "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewIdentifierCS("t")},
				},
			})

			switch {
			case query.errorMessage != "" && err != nil:
				require.EqualError(t, err, query.errorMessage)
			case query.errorMessage != "":
				require.EqualError(t, st.NotUnshardedErr, query.errorMessage)
			default:
				require.NoError(t, err)
				sel := parse.(*sqlparser.Select)
				assert.Equal(t, query.recursive, st.RecursiveDeps(extract(sel, 0)), "RecursiveDeps")
				assert.Equal(t, query.direct, st.DirectDeps(extract(sel, 0)), "DirectDeps")
			}
		})
	}
}

func TestJoinPredicateDependencies(t *testing.T) {
	// create table t(<no column info>)
	// create table t1(id bigint)
	// create table t2(uid bigint, name varchar(255))

	queries := []struct {
		query           string
		recursiveExpect TableSet
		directExpect    TableSet
	}{{
		query:           "select 1 from t1 join t2 on t1.id = t2.uid",
		recursiveExpect: MergeTableSets(TS0, TS1),
		directExpect:    MergeTableSets(TS0, TS1),
	}, {
		query:           "select 1 from (select * from t1) x join t2 on x.id = t2.uid",
		recursiveExpect: MergeTableSets(TS0, TS2),
		directExpect:    MergeTableSets(TS1, TS2),
	}, {
		query:           "select 1 from (select id from t1) x join t2 on x.id = t2.uid",
		recursiveExpect: MergeTableSets(TS0, TS2),
		directExpect:    MergeTableSets(TS1, TS2),
	}, {
		query:           "select 1 from (select id from t1 union select id from t) x join t2 on x.id = t2.uid",
		recursiveExpect: MergeTableSets(TS0, TS1, TS3),
		directExpect:    MergeTableSets(TS2, TS3),
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)

			st, err := Analyze(parse, "user", fakeSchemaInfo())
			require.NoError(t, err)

			sel := parse.(*sqlparser.Select)
			expr := sel.From[0].(*sqlparser.JoinTableExpr).Condition.On
			assert.Equal(t, query.recursiveExpect, st.RecursiveDeps(expr), "RecursiveDeps")
			assert.Equal(t, query.directExpect, st.DirectDeps(expr), "DirectDeps")
		})
	}
}

func TestDerivedTablesOrderClause(t *testing.T) {
	queries := []struct {
		query                string
		recursiveExpectation TableSet
		expectation          TableSet
	}{{
		query:                "select 1 from (select id from user) as t order by id",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}, {
		query:                "select id from (select id from user) as t order by id",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}, {
		query:                "select id from (select id from user) as t order by t.id",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}, {
		query:                "select id as foo from (select id from user) as t order by foo",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}, {
		query:                "select bar from (select id as bar from user) as t order by bar",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}, {
		query:                "select bar as foo from (select id as bar from user) as t order by bar",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}, {
		query:                "select bar as foo from (select id as bar from user) as t order by foo",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}, {
		query:                "select bar as foo from (select id as bar, oo from user) as t order by oo",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}, {
		query:                "select bar as foo from (select id, oo from user) as t(bar,oo) order by bar",
		recursiveExpectation: TS0,
		expectation:          TS1,
	}}
	si := &FakeSI{Tables: map[string]*vindexes.Table{"t": {Name: sqlparser.NewIdentifierCS("t")}}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)

			st, err := Analyze(parse, "user", si)
			require.NoError(t, err)

			sel := parse.(*sqlparser.Select)
			assert.Equal(t, query.recursiveExpectation, st.RecursiveDeps(sel.OrderBy[0].Expr), "RecursiveDeps")
			assert.Equal(t, query.expectation, st.DirectDeps(sel.OrderBy[0].Expr), "DirectDeps")

		})
	}
}

func TestScopingWComplexDerivedTables(t *testing.T) {
	queries := []struct {
		query            string
		errorMessage     string
		rightExpectation TableSet
		leftExpectation  TableSet
	}{
		{
			query:            "select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
			rightExpectation: TS0,
			leftExpectation:  TS0,
		},
		{
			query:            "select 1 from user.user uu where exists (select 1 from user.user as uu where exists (select 1 from (select 1 from user.t1) uu where uu.user_id = uu.id))",
			rightExpectation: TS1,
			leftExpectation:  TS1,
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			st, err := Analyze(parse, "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewIdentifierCS("t")},
				},
			})
			if query.errorMessage != "" {
				require.EqualError(t, err, query.errorMessage)
			} else {
				require.NoError(t, err)
				sel := parse.(*sqlparser.Select)
				comparisonExpr := sel.Where.Expr.(*sqlparser.ExistsExpr).Subquery.Select.(*sqlparser.Select).Where.Expr.(*sqlparser.ExistsExpr).Subquery.Select.(*sqlparser.Select).Where.Expr.(*sqlparser.ComparisonExpr)
				left := comparisonExpr.Left
				right := comparisonExpr.Right
				assert.Equal(t, query.leftExpectation, st.RecursiveDeps(left), "Left RecursiveDeps")
				assert.Equal(t, query.rightExpectation, st.RecursiveDeps(right), "Right RecursiveDeps")
			}
		})
	}
}

func TestScopingWVindexTables(t *testing.T) {
	queries := []struct {
		query                string
		errorMessage         string
		recursiveExpectation TableSet
		expectation          TableSet
	}{
		{
			query:                "select id from user_index where id = 1",
			recursiveExpectation: TS0,
			expectation:          TS0,
		}, {
			query:                "select u.id + t.id from t as t join user_index as u where u.id = 1 and u.id = t.id",
			recursiveExpectation: MergeTableSets(TS0, TS1),
			expectation:          MergeTableSets(TS0, TS1),
		},
	}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(query.query)
			require.NoError(t, err)
			hash, _ := vindexes.CreateVindex("hash", "user_index", nil)
			st, err := Analyze(parse, "user", &FakeSI{
				Tables: map[string]*vindexes.Table{
					"t": {Name: sqlparser.NewIdentifierCS("t")},
				},
				VindexTables: map[string]vindexes.Vindex{
					"user_index": hash,
				},
			})
			if query.errorMessage != "" {
				require.EqualError(t, err, query.errorMessage)
			} else {
				require.NoError(t, err)
				sel := parse.(*sqlparser.Select)
				assert.Equal(t, query.recursiveExpectation, st.RecursiveDeps(extract(sel, 0)))
				assert.Equal(t, query.expectation, st.DirectDeps(extract(sel, 0)))
			}
		})
	}
}

func BenchmarkAnalyzeMultipleDifferentQueries(b *testing.B) {
	queries := []string{
		"select col from tabl",
		"select t.col from t, s",
		"select max(tabl.col1 + tabl.col2) from d.X as tabl",
		"select max(X.col + S.col) from t as X, s as S",
		"select case t.col when s.col then r.col else u.col end from t, s, r, w, u",
		"select t.col1, (select t.col2 from z as t) from x as t",
		"select * from user u where exists (select * from user order by col)",
		"select id from dbName.t1 where exists (select * from dbName.t2 order by dbName.t1.id)",
		"select d.tabl.col from d.tabl order by col",
		"select a.id from t1 as a union (select uid from t2, t union (select name from t) order by 1) order by 1",
		"select a.id from t, t1 as a group by id",
		"select tabl.col as x from tabl having x = 1",
		"select id from (select foo as id from (select x as foo from user) as c) as t",
	}

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			parse, err := sqlparser.Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo())
		}
	}
}

func BenchmarkAnalyzeUnionQueries(b *testing.B) {
	queries := []string{
		"select id from t1 union select uid from t2",
		"select col1 from tabl1 union (select col2 from tabl2)",
		"select t1.id, t1.col1 from t1 union select t2.uid from t2",
		"select a.id from t1 as a union (select uid from t2, t union (select name from t) order by 1) order by 1",
		"select b.id as a from t1 as b union (select uid as c from t2) order by 1",
		"select a.id from t1 as a union (select uid from t2) order by 1",
		"select id from t1 union select uid from t2 union (select name from t)",
		"select id from t1 union (select uid from t2) order by 1",
		"(select id from t1) union (select uid from t2) order by id",
		"select a.id from t1 as a union (select uid from t2, t union (select name from t) order by 1) order by 1",
	}

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			parse, err := sqlparser.Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo())
		}
	}
}

func BenchmarkAnalyzeSubQueries(b *testing.B) {
	queries := []string{
		"select * from user u where exists (select * from user order by col)",
		"select * from user u where exists (select * from user order by user.col)",
		"select * from user u where exists (select * from user order by u.col)",
		"select * from dbName.user as u where exists (select * from dbName.user order by u.col)",
		"select * from dbName.user where exists (select * from otherDb.user order by dbName.user.col)",
		"select id from dbName.t1 where exists (select * from dbName.t2 order by dbName.t1.id)",
		"select t.col1, (select t.col2 from z as t) from x as t",
		"select t.col1, (select t.col2 from z) from x as t",
		"select t.col1, (select (select z.col2 from y) from z) from x as t",
		"select t.col1, (select (select y.col2 from y) from z) from x as t",
		"select t.col1, (select (select (select (select w.col2 from w) from x) from y) from z) from x as t",
		"select t.col1, (select id from t) from x as t",
	}

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			parse, err := sqlparser.Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo())
		}
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
			parse, err := sqlparser.Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo())
		}
	}
}

func BenchmarkAnalyzeHavingQueries(b *testing.B) {
	queries := []string{
		"select col from tabl having col = 1",
		"select col from tabl having tabl.col = 1",
		"select col from tabl having d.tabl.col = 1",
		"select tabl.col as x from tabl having x = 1",
		"select tabl.col as x from tabl having col",
		"select col from tabl having 1 = 1",
		"select col as c from tabl having c = 1",
		"select 1 as c from tabl having c = 1",
		"select t1.id from t1, t2 having id = 1",
		"select t.id from t, t1 having id = 1",
		"select t.id, count(*) as a from t, t1 group by t.id having a = 1",
		"select u2.a, u1.a from u1, u2 having u2.a = 2",
	}

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			parse, err := sqlparser.Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo())
		}
	}
}

func BenchmarkAnalyzeGroupByQueries(b *testing.B) {
	queries := []string{
		"select col from tabl group by col",
		"select col from tabl group by tabl.col",
		"select col from tabl group by d.tabl.col",
		"select tabl.col as x from tabl group by x",
		"select tabl.col as x from tabl group by col",
		"select d.tabl.col as x from tabl group by x",
		"select d.tabl.col as x from tabl group by col",
		"select col from tabl group by 1",
		"select col as c from tabl group by c",
		"select 1 as c from tabl group by c",
		"select t1.id from t1, t2 group by id",
		"select id from t, t1 group by id",
		"select id from t, t1 group by id",
		"select a.id from t as a, t1 group by id",
		"select a.id from t, t1 as a group by id",
	}

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			parse, err := sqlparser.Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo())
		}
	}
}

func BenchmarkAnalyzeOrderByQueries(b *testing.B) {
	queries := []string{
		"select col from tabl order by col",
		"select tabl.col from d.tabl order by col",
		"select d.tabl.col from d.tabl order by col",
		"select col from tabl order by tabl.col",
		"select col from tabl order by d.tabl.col",
		"select col from tabl order by 1",
		"select col as c from tabl order by c",
		"select 1 as c from tabl order by c",
		"select name, name from t1, t2 order by name",
	}

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			parse, err := sqlparser.Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo())
		}
	}
}

func parseAndAnalyze(t *testing.T, query, dbName string) (sqlparser.Statement, *SemTable) {
	t.Helper()
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)

	semTable, err := Analyze(parse, dbName, fakeSchemaInfo())
	require.NoError(t, err)
	return parse, semTable
}

func TestSingleUnshardedKeyspace(t *testing.T) {
	tests := []struct {
		query     string
		unsharded *vindexes.Keyspace
		tables    []*vindexes.Table
	}{
		{
			query:     "select 1 from t, t1",
			unsharded: nil, // both tables are unsharded, but from different keyspaces
			tables:    nil,
		}, {
			query:     "select 1 from t2",
			unsharded: nil,
			tables:    nil,
		}, {
			query:     "select 1 from t, t2",
			unsharded: nil,
			tables:    nil,
		}, {
			query:     "select 1 from t as A, t as B",
			unsharded: ks1,
			tables: []*vindexes.Table{
				{Keyspace: ks1, Name: sqlparser.NewIdentifierCS("t")},
				{Keyspace: ks1, Name: sqlparser.NewIdentifierCS("t")},
			},
		}, {
			query:     "insert into t select * from t",
			unsharded: ks1,
			tables: []*vindexes.Table{
				{Keyspace: ks1, Name: sqlparser.NewIdentifierCS("t")},
				{Keyspace: ks1, Name: sqlparser.NewIdentifierCS("t")},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			_, semTable := parseAndAnalyze(t, test.query, "d")
			queryIsUnsharded, tables := semTable.SingleUnshardedKeyspace()
			assert.Equal(t, test.unsharded, queryIsUnsharded)
			assert.Equal(t, test.tables, tables)
		})
	}
}

func TestNextErrors(t *testing.T) {
	tests := []struct {
		query, expectedError string
	}{
		{
			query:         "select next 2 values from dual",
			expectedError: "Table information is not provided in vschema for table `dual`",
		}, {
			query:         "select next 2 values from t1",
			expectedError: "NEXT used on a non-sequence table `t1`",
		}, {
			query:         "select * from (select next 2 values from t1) dt",
			expectedError: "Incorrect usage/placement of 'NEXT'",
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(test.query)
			require.NoError(t, err)

			_, err = Analyze(parse, "d", fakeSchemaInfo())
			assert.EqualError(t, err, test.expectedError)
		})
	}
}

func TestUpdateErrors(t *testing.T) {
	tests := []struct {
		query, expectedError string
	}{
		{
			query:         "update t1, t2 set id = 12",
			expectedError: "VT12001: unsupported: multiple (2) tables in update",
		}, {
			query:         "update (select 1 from dual) dt set id = 1",
			expectedError: "The target table dt of the UPDATE is not updatable",
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(test.query)
			require.NoError(t, err)

			st, err := Analyze(parse, "d", fakeSchemaInfo())
			if err == nil {
				err = st.NotUnshardedErr
			}
			assert.EqualError(t, err, test.expectedError)
		})
	}
}

// TestScopingSubQueryJoinClause tests the scoping behavior of a subquery containing a join clause.
// The test ensures that the scoping analysis correctly identifies and handles the relationships
// between the tables involved in the join operation with the outer query.
func TestScopingSubQueryJoinClause(t *testing.T) {
	query := "select (select 1 from u1 join u2 on u1.id = u2.id and u2.id = u3.id) x from u3"

	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)

	st, err := Analyze(parse, "user", &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t": {Name: sqlparser.NewIdentifierCS("t")},
		},
	})
	require.NoError(t, err)
	require.NoError(t, st.NotUnshardedErr)

	tb := st.DirectDeps(parse.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.Subquery).Select.(*sqlparser.Select).From[0].(*sqlparser.JoinTableExpr).Condition.On)
	require.Equal(t, 3, tb.NumberOfTables())

}

var ks1 = &vindexes.Keyspace{
	Name:    "ks1",
	Sharded: false,
}
var ks2 = &vindexes.Keyspace{
	Name:    "ks2",
	Sharded: false,
}
var ks3 = &vindexes.Keyspace{
	Name:    "ks3",
	Sharded: true,
}

// create table t(<no column info>)
// create table t1(id bigint)
// create table t2(uid bigint, name varchar(255))
func fakeSchemaInfo() *FakeSI {
	cols1 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("id"),
		Type: querypb.Type_INT64,
	}}
	cols2 := []vindexes.Column{{
		Name: sqlparser.NewIdentifierCI("uid"),
		Type: querypb.Type_INT64,
	}, {
		Name: sqlparser.NewIdentifierCI("name"),
		Type: querypb.Type_VARCHAR,
	}}

	si := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t":  {Name: sqlparser.NewIdentifierCS("t"), Keyspace: ks1},
			"t1": {Name: sqlparser.NewIdentifierCS("t1"), Columns: cols1, ColumnListAuthoritative: true, Keyspace: ks2},
			"t2": {Name: sqlparser.NewIdentifierCS("t2"), Columns: cols2, ColumnListAuthoritative: true, Keyspace: ks3},
		},
	}
	return si
}

var tbl = map[string]TableInfo{
	"t0": &RealTable{
		Table: &vindexes.Table{
			Keyspace: &vindexes.Keyspace{Name: "ks"},
			ChildForeignKeys: []vindexes.ChildFKInfo{
				ckInfo(nil, []string{"col"}, []string{"col"}, sqlparser.Restrict),
				ckInfo(nil, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
			},
			ParentForeignKeys: []vindexes.ParentFKInfo{
				pkInfo(nil, []string{"colb"}, []string{"colb"}),
				pkInfo(nil, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
			},
		},
	},
	"t1": &RealTable{
		Table: &vindexes.Table{
			Keyspace: &vindexes.Keyspace{Name: "ks_unmanaged", Sharded: true},
			ChildForeignKeys: []vindexes.ChildFKInfo{
				ckInfo(nil, []string{"cola"}, []string{"cola"}, sqlparser.Restrict),
				ckInfo(nil, []string{"cola1", "cola2"}, []string{"ccola1", "ccola2"}, sqlparser.SetNull),
			},
		},
	},
	"t2": &RealTable{
		Table: &vindexes.Table{
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	},
	"t3": &RealTable{
		Table: &vindexes.Table{
			Keyspace: &vindexes.Keyspace{Name: "undefined_ks", Sharded: true},
		},
	},
}

// TestGetAllManagedForeignKeys tests the functionality of getAllManagedForeignKeys.
func TestGetAllManagedForeignKeys(t *testing.T) {
	tests := []struct {
		name           string
		analyzer       *analyzer
		childFkWanted  map[TableSet][]vindexes.ChildFKInfo
		parentFkWanted map[TableSet][]vindexes.ParentFKInfo
		expectedErr    string
	}{
		{
			name: "Collect all foreign key constraints",
			analyzer: &analyzer{
				tables: &tableCollector{
					Tables: []TableInfo{tbl["t0"], tbl["t1"],
						&DerivedTable{},
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks":           vschemapb.Keyspace_managed,
							"ks_unmanaged": vschemapb.Keyspace_unmanaged,
						},
					},
				},
			},
			childFkWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(nil, []string{"col"}, []string{"col"}, sqlparser.Restrict),
					ckInfo(nil, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
				},
			},
			parentFkWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(nil, []string{"colb"}, []string{"colb"}),
					pkInfo(nil, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
				},
			},
		},
		{
			name: "keyspace not found in schema information",
			analyzer: &analyzer{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t2"],
						tbl["t3"],
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks": vschemapb.Keyspace_managed,
						},
					},
				},
			},
			expectedErr: "undefined_ks keyspace not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childFk, parentFk, err := tt.analyzer.getAllManagedForeignKeys()
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.EqualValues(t, tt.childFkWanted, childFk)
			require.EqualValues(t, tt.parentFkWanted, parentFk)
		})
	}
}

// TestFilterForeignKeysUsingUpdateExpressions tests the functionality of filterForeignKeysUsingUpdateExpressions.
func TestFilterForeignKeysUsingUpdateExpressions(t *testing.T) {
	cola := sqlparser.NewColName("cola")
	colb := sqlparser.NewColName("colb")
	colc := sqlparser.NewColName("colc")
	cold := sqlparser.NewColName("cold")
	a := &analyzer{
		binder: &binder{
			direct: map[sqlparser.Expr]TableSet{
				cola: SingleTableSet(0),
				colb: SingleTableSet(0),
				colc: SingleTableSet(1),
				cold: SingleTableSet(1),
			},
		},
	}
	updateExprs := sqlparser.UpdateExprs{
		&sqlparser.UpdateExpr{Name: cola, Expr: sqlparser.NewIntLiteral("1")},
		&sqlparser.UpdateExpr{Name: colb, Expr: &sqlparser.NullVal{}},
		&sqlparser.UpdateExpr{Name: colc, Expr: sqlparser.NewIntLiteral("1")},
		&sqlparser.UpdateExpr{Name: cold, Expr: &sqlparser.NullVal{}},
	}
	tests := []struct {
		name            string
		analyzer        *analyzer
		allChildFks     map[TableSet][]vindexes.ChildFKInfo
		allParentFks    map[TableSet][]vindexes.ParentFKInfo
		updExprs        sqlparser.UpdateExprs
		childFksWanted  map[TableSet][]vindexes.ChildFKInfo
		parentFksWanted map[TableSet][]vindexes.ParentFKInfo
	}{
		{
			name:         "Child Foreign Keys Filtering",
			analyzer:     a,
			allParentFks: nil,
			allChildFks: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
					ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
					ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
					ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
				},
				SingleTableSet(1): {
					ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
					ckInfo(nil, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
					ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
				},
			},
			updExprs: updateExprs,
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
					ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				},
				SingleTableSet(1): {
					ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
					ckInfo(nil, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
				},
			},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{},
		}, {
			name:     "Parent Foreign Keys Filtering",
			analyzer: a,
			allParentFks: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(nil, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
					pkInfo(nil, []string{"pcolc"}, []string{"colc"}),
					pkInfo(nil, []string{"pcolb", "pcola"}, []string{"colb", "cola"}),
					pkInfo(nil, []string{"pcolb"}, []string{"colb"}),
					pkInfo(nil, []string{"pcola"}, []string{"cola"}),
					pkInfo(nil, []string{"pcolb", "pcolx"}, []string{"colb", "colx"}),
				},
				SingleTableSet(1): {
					pkInfo(nil, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
					pkInfo(nil, []string{"pcola"}, []string{"cola"}),
					pkInfo(nil, []string{"pcold", "pcolc"}, []string{"cold", "colc"}),
					pkInfo(nil, []string{"pcold"}, []string{"cold"}),
					pkInfo(nil, []string{"pcold", "pcolx"}, []string{"cold", "colx"}),
				},
			},
			allChildFks:    nil,
			updExprs:       updateExprs,
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(nil, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
					pkInfo(nil, []string{"pcola"}, []string{"cola"}),
				},
				SingleTableSet(1): {
					pkInfo(nil, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childFks, parentFks := tt.analyzer.filterForeignKeysUsingUpdateExpressions(tt.allChildFks, tt.allParentFks, tt.updExprs)
			require.EqualValues(t, tt.childFksWanted, childFks)
			require.EqualValues(t, tt.parentFksWanted, parentFks)
		})
	}
}

// TestGetInvolvedForeignKeys tests the functionality of getInvolvedForeignKeys.
func TestGetInvolvedForeignKeys(t *testing.T) {
	cola := sqlparser.NewColName("cola")
	colb := sqlparser.NewColName("colb")
	colc := sqlparser.NewColName("colc")
	cold := sqlparser.NewColName("cold")
	tests := []struct {
		name            string
		stmt            sqlparser.Statement
		analyzer        *analyzer
		childFksWanted  map[TableSet][]vindexes.ChildFKInfo
		parentFksWanted map[TableSet][]vindexes.ParentFKInfo
		expectedErr     string
	}{
		{
			name: "Delete Query",
			stmt: &sqlparser.Delete{},
			analyzer: &analyzer{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t0"],
						tbl["t1"],
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks":           vschemapb.Keyspace_managed,
							"ks_unmanaged": vschemapb.Keyspace_unmanaged,
						},
					},
				},
			},
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(nil, []string{"col"}, []string{"col"}, sqlparser.Restrict),
					ckInfo(nil, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
				},
			},
		},
		{
			name: "Update statement",
			stmt: &sqlparser.Update{
				Exprs: sqlparser.UpdateExprs{
					&sqlparser.UpdateExpr{
						Name: cola,
						Expr: sqlparser.NewIntLiteral("1"),
					},
					&sqlparser.UpdateExpr{
						Name: colb,
						Expr: &sqlparser.NullVal{},
					},
					&sqlparser.UpdateExpr{
						Name: colc,
						Expr: sqlparser.NewIntLiteral("1"),
					},
					&sqlparser.UpdateExpr{
						Name: cold,
						Expr: &sqlparser.NullVal{},
					},
				},
			},
			analyzer: &analyzer{
				binder: &binder{
					direct: map[sqlparser.Expr]TableSet{
						cola: SingleTableSet(0),
						colb: SingleTableSet(0),
						colc: SingleTableSet(1),
						cold: SingleTableSet(1),
					},
				},
				tables: &tableCollector{
					Tables: []TableInfo{
						&RealTable{
							Table: &vindexes.Table{
								Keyspace: &vindexes.Keyspace{Name: "ks"},
								ChildForeignKeys: []vindexes.ChildFKInfo{
									ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
									ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
									ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
									ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
								},
								ParentForeignKeys: []vindexes.ParentFKInfo{
									pkInfo(nil, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
									pkInfo(nil, []string{"pcolc"}, []string{"colc"}),
									pkInfo(nil, []string{"pcolb", "pcola"}, []string{"colb", "cola"}),
									pkInfo(nil, []string{"pcolb"}, []string{"colb"}),
									pkInfo(nil, []string{"pcola"}, []string{"cola"}),
									pkInfo(nil, []string{"pcolb", "pcolx"}, []string{"colb", "colx"}),
								},
							},
						},
						&RealTable{
							Table: &vindexes.Table{
								Keyspace: &vindexes.Keyspace{Name: "ks"},
								ChildForeignKeys: []vindexes.ChildFKInfo{
									ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
									ckInfo(nil, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
									ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
								},
								ParentForeignKeys: []vindexes.ParentFKInfo{
									pkInfo(nil, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
									pkInfo(nil, []string{"pcola"}, []string{"cola"}),
									pkInfo(nil, []string{"pcold", "pcolc"}, []string{"cold", "colc"}),
									pkInfo(nil, []string{"pcold"}, []string{"cold"}),
									pkInfo(nil, []string{"pcold", "pcolx"}, []string{"cold", "colx"}),
								},
							},
						},
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks": vschemapb.Keyspace_managed,
						},
					},
				},
			},
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
					ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				},
				SingleTableSet(1): {
					ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
					ckInfo(nil, []string{"colc", "colx"}, []string{"child_colc", "child_colx"}, sqlparser.SetNull),
				},
			},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(nil, []string{"pcola", "pcolx"}, []string{"cola", "colx"}),
					pkInfo(nil, []string{"pcola"}, []string{"cola"}),
				},
				SingleTableSet(1): {
					pkInfo(nil, []string{"pcolc", "pcolx"}, []string{"colc", "colx"}),
				},
			},
		},
		{
			name: "Replace Query",
			stmt: &sqlparser.Insert{
				Action: sqlparser.ReplaceAct,
			},
			analyzer: &analyzer{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t0"],
						tbl["t1"],
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks":           vschemapb.Keyspace_managed,
							"ks_unmanaged": vschemapb.Keyspace_unmanaged,
						},
					},
				},
			},
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(nil, []string{"col"}, []string{"col"}, sqlparser.Restrict),
					ckInfo(nil, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
				},
			},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(nil, []string{"colb"}, []string{"colb"}),
					pkInfo(nil, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
				},
			},
		},
		{
			name: "Insert Query",
			stmt: &sqlparser.Insert{
				Action: sqlparser.InsertAct,
			},
			analyzer: &analyzer{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t0"],
						tbl["t1"],
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks":           vschemapb.Keyspace_managed,
							"ks_unmanaged": vschemapb.Keyspace_unmanaged,
						},
					},
				},
			},
			childFksWanted: nil,
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(nil, []string{"colb"}, []string{"colb"}),
					pkInfo(nil, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
				},
			},
		},
		{
			name: "Insert Query with On Duplicate",
			stmt: &sqlparser.Insert{
				Action: sqlparser.InsertAct,
				OnDup: sqlparser.OnDup{
					&sqlparser.UpdateExpr{
						Name: cola,
						Expr: sqlparser.NewIntLiteral("1"),
					},
					&sqlparser.UpdateExpr{
						Name: colb,
						Expr: &sqlparser.NullVal{},
					},
				},
			},
			analyzer: &analyzer{
				binder: &binder{
					direct: map[sqlparser.Expr]TableSet{
						cola: SingleTableSet(0),
						colb: SingleTableSet(0),
					},
				},
				tables: &tableCollector{
					Tables: []TableInfo{
						&RealTable{
							Table: &vindexes.Table{
								Keyspace: &vindexes.Keyspace{Name: "ks"},
								ChildForeignKeys: []vindexes.ChildFKInfo{
									ckInfo(nil, []string{"col"}, []string{"col"}, sqlparser.Restrict),
									ckInfo(nil, []string{"col1", "col2"}, []string{"ccol1", "ccol2"}, sqlparser.SetNull),
									ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
									ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
									ckInfo(nil, []string{"colx", "coly"}, []string{"child_colx", "child_coly"}, sqlparser.Cascade),
									ckInfo(nil, []string{"cold"}, []string{"child_cold"}, sqlparser.Restrict),
								},
								ParentForeignKeys: []vindexes.ParentFKInfo{
									pkInfo(nil, []string{"colb"}, []string{"colb"}),
									pkInfo(nil, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
								},
							},
						},
						tbl["t1"],
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks":           vschemapb.Keyspace_managed,
							"ks_unmanaged": vschemapb.Keyspace_unmanaged,
						},
					},
				},
			},
			childFksWanted: map[TableSet][]vindexes.ChildFKInfo{
				SingleTableSet(0): {
					ckInfo(nil, []string{"colb"}, []string{"child_colb"}, sqlparser.Restrict),
					ckInfo(nil, []string{"cola", "colx"}, []string{"child_cola", "child_colx"}, sqlparser.SetNull),
				},
			},
			parentFksWanted: map[TableSet][]vindexes.ParentFKInfo{
				SingleTableSet(0): {
					pkInfo(nil, []string{"colb"}, []string{"colb"}),
					pkInfo(nil, []string{"colb1", "colb2"}, []string{"ccolb1", "ccolb2"}),
				},
			},
		},
		{
			name: "Insert error",
			stmt: &sqlparser.Insert{},
			analyzer: &analyzer{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t2"],
						tbl["t3"],
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks": vschemapb.Keyspace_managed,
						},
					},
				},
			},
			expectedErr: "undefined_ks keyspace not found",
		},
		{
			name: "Update error",
			stmt: &sqlparser.Update{},
			analyzer: &analyzer{
				tables: &tableCollector{
					Tables: []TableInfo{
						tbl["t2"],
						tbl["t3"],
					},
					si: &FakeSI{
						KsForeignKeyMode: map[string]vschemapb.Keyspace_ForeignKeyMode{
							"ks": vschemapb.Keyspace_managed,
						},
					},
				},
			},
			expectedErr: "undefined_ks keyspace not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			childFks, parentFks, err := tt.analyzer.getInvolvedForeignKeys(tt.stmt)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.EqualValues(t, tt.childFksWanted, childFks)
			require.EqualValues(t, tt.parentFksWanted, parentFks)
		})
	}
}

func ckInfo(cTable *vindexes.Table, pCols []string, cCols []string, refAction sqlparser.ReferenceAction) vindexes.ChildFKInfo {
	return vindexes.ChildFKInfo{
		Table:         cTable,
		ParentColumns: sqlparser.MakeColumns(pCols...),
		ChildColumns:  sqlparser.MakeColumns(cCols...),
		OnDelete:      refAction,
	}
}

func pkInfo(parentTable *vindexes.Table, pCols []string, cCols []string) vindexes.ParentFKInfo {
	return vindexes.ParentFKInfo{
		Table:         parentTable,
		ParentColumns: sqlparser.MakeColumns(pCols...),
		ChildColumns:  sqlparser.MakeColumns(cCols...),
	}
}
