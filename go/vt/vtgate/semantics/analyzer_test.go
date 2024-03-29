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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var NoTables TableSet

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
			parse, err := sqlparser.NewTestParser().Parse(query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
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

func TestBindingDelete(t *testing.T) {
	queries := []string{
		"delete tbl from tbl",
		"delete from tbl",
		"delete t1 from t1, t2",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			stmt, semTable := parseAndAnalyze(t, query, "d")
			del := stmt.(*sqlparser.Delete)
			t1 := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
			ts := semTable.TableSetFor(t1)
			assert.Equal(t, SingleTableSet(0), ts)

			actualTs, err := semTable.GetTargetTableSetForTableName(del.Targets[0])
			require.NoError(t, err)
			assert.Equal(t, ts, actualTs)
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
			parse, _ := sqlparser.NewTestParser().Parse(query)
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
			parse, _ := sqlparser.NewTestParser().Parse(query)
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
			parse, _ := sqlparser.NewTestParser().Parse(query)
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
						assert.Equal(t, test.typ, typ.Type())
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

	parse, _ := sqlparser.NewTestParser().Parse(query)

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
			parse, err := sqlparser.NewTestParser().Parse(query.query)
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
			ast, err := sqlparser.NewTestParser().Parse(tc.query)
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
		NoTables,
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
	}, {
		"select * from (SELECT c1, c2 FROM a UNION SELECT c1, c2 FROM b) AS u ORDER BY u.c1",
		MergeTableSets(TS0, TS1),
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

func TestVindexHints(t *testing.T) {
	// tests that vindex hints point to existing vindexes, or an error should be returned
	tcases := []struct {
		sql         string
		expectedErr string
	}{{
		sql:         "select col from t1 use vindex (does_not_exist)",
		expectedErr: "Vindex 'does_not_exist' does not exist in table 'ks2.t1'",
	}, {
		sql:         "select col from t1 ignore vindex (does_not_exist)",
		expectedErr: "Vindex 'does_not_exist' does not exist in table 'ks2.t1'",
	}, {
		sql: "select id from t1 use vindex (id_vindex)",
	}, {
		sql: "select id from t1 ignore vindex (id_vindex)",
	}}
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(tc.sql)
			require.NoError(t, err)

			_, err = AnalyzeStrict(parse, "d", fakeSchemaInfo())
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErr)
			}
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
		NoTables,
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
		// since we have authoritative info on t1, we know that it does have an `id` column,
		// and we are missing column info for `t`, we just assume this is coming from t1.
		// we really need schema tracking here
		TS1,
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
		sql, err string
		deps     TableSet
	}{{
		sql:  "select col from tabl having col = 1",
		deps: TS0,
	}, {
		sql:  "select col from tabl having tabl.col = 1",
		deps: TS0,
	}, {
		sql:  "select col from tabl having d.tabl.col = 1",
		deps: TS0,
	}, {
		sql:  "select tabl.col as x from tabl having col = 1",
		deps: TS0,
	}, {
		sql:  "select tabl.col as x from tabl having x = 1",
		deps: TS0,
	}, {
		sql:  "select tabl.col as x from tabl having col",
		deps: TS0,
	}, {
		sql:  "select col from tabl having 1 = 1",
		deps: NoTables,
	}, {
		sql:  "select col as c from tabl having c = 1",
		deps: TS0,
	}, {
		sql:  "select 1 as c from tabl having c = 1",
		deps: NoTables,
	}, {
		sql:  "select t1.id from t1, t2 having id = 1",
		deps: TS0,
	}, {
		sql:  "select t.id from t, t1 having id = 1",
		deps: TS0,
	}, {
		sql:  "select t.id, count(*) as a from t, t1 group by t.id having a = 1",
		deps: MergeTableSets(TS0, TS1),
	}, {
		sql:  "select t.id, sum(t2.name) as a from t, t2 group by t.id having a = 1",
		deps: TS1,
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
			parse, err := sqlparser.NewTestParser().Parse(tc.sql)
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
			direct:    TS3,
		}, {
			query:     "with t as (select foo as id from user) select t.id from t",
			recursive: TS0,
			direct:    TS1,
		}, {
			query:        "select t.id2 from (select foo as id from user) as t",
			errorMessage: "column 't.id2' not found",
		}, {
			query:     "with t as (select 42 as id) select id from t",
			recursive: NoTables,
			direct:    TS1,
		}, {
			query:     "with t as (select 42 as id) select t.id from t",
			recursive: NoTables,
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
			direct:    TS2,
			recursive: TS0,
		}, {
			query:     "with t as (select t1.id, t1.col1 from t1 join t2) select t.col1 from t3 ua join t",
			direct:    TS3,
			recursive: TS0,
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
			direct:    TS2,
			recursive: TS0,
		}, {
			query:     "select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))",
			direct:    NoTables,
			recursive: NoTables,
		}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(query.query)
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
		recursiveExpect: MergeTableSets(TS0, TS1),
		directExpect:    MergeTableSets(TS1, TS2),
	}, {
		query:           "select 1 from (select id from t1) x join t2 on x.id = t2.uid",
		recursiveExpect: MergeTableSets(TS0, TS1),
		directExpect:    MergeTableSets(TS1, TS2),
	}, {
		query:           "select 1 from (select id from t1 union select id from t) x join t2 on x.id = t2.uid",
		recursiveExpect: MergeTableSets(TS0, TS1, TS2),
		directExpect:    MergeTableSets(TS2, TS3),
	}}
	for _, query := range queries {
		t.Run(query.query, func(t *testing.T) {
			parse, err := sqlparser.NewTestParser().Parse(query.query)
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
			parse, err := sqlparser.NewTestParser().Parse(query.query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
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
			parse, err := sqlparser.NewTestParser().Parse(query)
			require.NoError(b, err)

			_, _ = Analyze(parse, "d", fakeSchemaInfo())
		}
	}
}

func parseAndAnalyze(t *testing.T, query, dbName string) (sqlparser.Statement, *SemTable) {
	t.Helper()
	parse, err := sqlparser.NewTestParser().Parse(query)
	require.NoError(t, err)

	semTable, err := Analyze(parse, dbName, fakeSchemaInfo())
	require.NoError(t, err)
	return parse, semTable
}

func TestSingleUnshardedKeyspace(t *testing.T) {
	tests := []struct {
		query     string
		unsharded *vindexes.Keyspace
	}{
		{
			query:     "select 1 from t, t1",
			unsharded: nil, // both tables are unsharded, but from different keyspaces
		}, {
			query:     "select 1 from t2",
			unsharded: nil,
		}, {
			query:     "select 1 from t, t2",
			unsharded: nil,
		}, {
			query:     "select 1 from t as A, t as B",
			unsharded: unsharded,
		}, {
			query:     "insert into t select * from t",
			unsharded: unsharded,
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			_, semTable := parseAndAnalyze(t, test.query, "d")
			queryIsUnsharded, _ := semTable.SingleUnshardedKeyspace()
			assert.Equal(t, test.unsharded, queryIsUnsharded)
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
			parse, err := sqlparser.NewTestParser().Parse(test.query)
			require.NoError(t, err)

			_, err = Analyze(parse, "d", fakeSchemaInfo())
			assert.EqualError(t, err, test.expectedError)
		})
	}
}

// TestScopingSubQueryJoinClause tests the scoping behavior of a subquery containing a join clause.
// The test ensures that the scoping analysis correctly identifies and handles the relationships
// between the tables involved in the join operation with the outer query.
func TestScopingSubQueryJoinClause(t *testing.T) {
	query := "select (select 1 from u1 join u2 on u1.id = u2.id and u2.id = u3.id) x from u3"

	parse, err := sqlparser.NewTestParser().Parse(query)
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

var unsharded = &vindexes.Keyspace{
	Name:    "unsharded",
	Sharded: false,
}
var ks2 = &vindexes.Keyspace{
	Name:    "ks2",
	Sharded: true,
}
var ks3 = &vindexes.Keyspace{
	Name:    "ks3",
	Sharded: true,
}

// create table t(<no column info>)
// create table t1(id bigint)
// create table t2(uid bigint, name varchar(255))
func fakeSchemaInfo() *FakeSI {
	si := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t":  tableT(),
			"t1": tableT1(),
			"t2": tableT2(),
		},
	}
	return si
}

func tableT() *vindexes.Table {
	return &vindexes.Table{
		Name:     sqlparser.NewIdentifierCS("t"),
		Keyspace: unsharded,
	}
}
func tableT1() *vindexes.Table {
	return &vindexes.Table{
		Name: sqlparser.NewIdentifierCS("t1"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("id"),
			Type: querypb.Type_INT64,
		}},
		ColumnListAuthoritative: true,
		ColumnVindexes: []*vindexes.ColumnVindex{
			{Name: "id_vindex"},
		},
		Keyspace: ks2,
	}
}
func tableT2() *vindexes.Table {
	return &vindexes.Table{
		Name: sqlparser.NewIdentifierCS("t2"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("uid"),
			Type: querypb.Type_INT64,
		}, {
			Name:          sqlparser.NewIdentifierCI("name"),
			Type:          querypb.Type_VARCHAR,
			CollationName: "utf8_bin",
		}, {
			Name:          sqlparser.NewIdentifierCI("textcol"),
			Type:          querypb.Type_VARCHAR,
			CollationName: "big5_bin",
		}},
		ColumnListAuthoritative: true,
		Keyspace:                ks3,
	}
}
