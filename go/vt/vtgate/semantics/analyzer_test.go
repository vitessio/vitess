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

	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

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
	stmt, semTable := parseAndAnalyze(t, query, "")

	sel, _ := stmt.(*sqlparser.Select)

	// extract the `t.col2` expression from the subquery
	sel2 := sel.SelectExprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.Subquery).Select.(*sqlparser.Select)
	s1 := semTable.Dependencies(extract(sel2, 0))

	// if scoping works as expected, we should be able to see the inner table being used by the inner expression
	assert.Equal(t, T1, s1)
}

func TestBindingSingleTable(t *testing.T) {
	t.Run("positive tests", func(t *testing.T) {
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

				d := semTable.Dependencies(extract(sel, 0))
				require.Equal(t, T0, d, query)
			})
		}
	})
	t.Run("negative tests", func(t *testing.T) {
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
				_, err = Analyze(parse, "d", &fakeSI{})
				require.Error(t, err)
			})
		}
	})
}

func TestBindingSingleAliasedTable(t *testing.T) {
	t.Run("positive tests", func(t *testing.T) {
		queries := []string{
			"select col from tabl as X",
			"select tabl.col from X as tabl",
			"select col from d.X as tabl",
			"select tabl.col from d.X as tabl",
			"select d.tabl.col from d.X as tabl",
		}
		for _, query := range queries {
			t.Run(query, func(t *testing.T) {
				stmt, semTable := parseAndAnalyze(t, query, "")
				sel, _ := stmt.(*sqlparser.Select)
				t1 := sel.From[0].(*sqlparser.AliasedTableExpr)
				ts := semTable.TableSetFor(t1)
				assert.EqualValues(t, 1, ts)

				d := semTable.Dependencies(extract(sel, 0))
				require.Equal(t, T0, d, query)
			})
		}
	})
	t.Run("negative tests", func(t *testing.T) {
		queries := []string{
			"select tabl.col from tabl as X",
			"select d.X.col from d.X as tabl",
			"select d.tabl.col from X as tabl",
			"select d.tabl.col from ks.X as tabl",
		}
		for _, query := range queries {
			t.Run(query, func(t *testing.T) {
				parse, err := sqlparser.Parse(query)
				require.NoError(t, err)
				_, err = Analyze(parse, "", &fakeSI{
					tables: map[string]*vindexes.Table{
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

	d1 := semTable.Dependencies(extract(sel1, 0))
	d2 := semTable.Dependencies(extract(sel2, 0))
	assert.Equal(t, T0, d1)
	assert.Equal(t, T1, d2)
}

func TestBindingMultiTable(t *testing.T) {
	t.Run("positive tests", func(t *testing.T) {

		type testCase struct {
			query string
			deps  TableSet
		}
		queries := []testCase{{
			query: "select t.col from t, s",
			deps:  T0,
		}, {
			query: "select s.col from t join s",
			deps:  T1,
		}, {
			query: "select max(t.col+s.col) from t, s",
			deps:  T0 | T1,
		}, {
			query: "select max(t.col+s.col) from t join s",
			deps:  T0 | T1,
		}, {
			query: "select case t.col when s.col then r.col else u.col end from t, s, r, w, u",
			deps:  T0 | T1 | T2 | T4,
			// }, {
			//	// make sure that we don't let sub-query Dependencies leak out by mistake
			//	query: "select t.col + (select 42 from s) from t",
			//	deps:  T0,
			// }, {
			//	query: "select (select 42 from s where r.id = s.id) from r",
			//	deps:  T0 | T1,
		}, {
			query: "select X.col from t as X, s as S",
			deps:  T0,
		}, {
			query: "select X.col+S.col from t as X, s as S",
			deps:  T0 | T1,
		}, {
			query: "select max(X.col+S.col) from t as X, s as S",
			deps:  T0 | T1,
		}, {
			query: "select max(X.col+s.col) from t as X, s",
			deps:  T0 | T1,
		}, {
			query: "select b.t.col from b.t, t",
			deps:  T0,
		}}
		for _, query := range queries {
			t.Run(query.query, func(t *testing.T) {
				stmt, semTable := parseAndAnalyze(t, query.query, "user")
				sel, _ := stmt.(*sqlparser.Select)
				assert.Equal(t, query.deps, semTable.Dependencies(extract(sel, 0)), query.query)
			})
		}
	})

	t.Run("negative tests", func(t *testing.T) {
		queries := []string{
			"select 1 from d.tabl, d.foo as tabl",
			"select 1 from d.tabl, d.tabl",
			"select 1 from d.tabl, tabl",
		}
		for _, query := range queries {
			t.Run(query, func(t *testing.T) {
				parse, err := sqlparser.Parse(query)
				require.NoError(t, err)
				_, err = Analyze(parse, "d", &fakeSI{
					tables: map[string]*vindexes.Table{
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
			if strings.Contains(query, ") as") {
				t.Skip("derived tables not implemented")
			}
			parse, _ := sqlparser.Parse(query)
			_, err := Analyze(parse, "test", &fakeSI{})
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
			_, err := Analyze(parse, "", &fakeSI{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "Unknown table")
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
			Type: querypb.Type_VARCHAR,
		}},
		ColumnListAuthoritative: true,
	}

	parse, _ := sqlparser.Parse(query)

	tests := []struct {
		name   string
		schema map[string]*vindexes.Table
		err    bool
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
			err:    true,
		},
		{
			name:   "non authoritative columns - one authoritative and one not",
			schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &nonAuthoritativeTblB},
			err:    true,
		},
		{
			name:   "authoritative columns",
			schema: map[string]*vindexes.Table{"a": &authoritativeTblA, "b": &authoritativeTblB},
			err:    false,
		},
		{
			name:   "authoritative columns with overlap",
			schema: map[string]*vindexes.Table{"a": &authoritativeTblAWithConflict, "b": &authoritativeTblB},
			err:    true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			si := &fakeSI{tables: test.schema}
			_, err := Analyze(parse, "", si)
			if test.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func parseAndAnalyze(t *testing.T, query, dbName string) (sqlparser.Statement, *SemTable) {
	t.Helper()
	parse, err := sqlparser.Parse(query)
	require.NoError(t, err)
	semTable, err := Analyze(parse, dbName, &fakeSI{
		tables: map[string]*vindexes.Table{
			"t": {Name: sqlparser.NewTableIdent("t")},
		},
	})
	require.NoError(t, err)
	return parse, semTable
}

var _ SchemaInformation = (*fakeSI)(nil)

type fakeSI struct {
	tables map[string]*vindexes.Table
}

func (s *fakeSI) FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	return s.tables[sqlparser.String(tablename)], nil, "", 0, nil, nil
}
