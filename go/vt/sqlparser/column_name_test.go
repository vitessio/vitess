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

package sqlparser

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type columnNameCase struct {
	ID       string            `json:"id"`
	Category string            `json:"category"`
	Setup    []string          `json:"setup"`
	Query    string            `json:"query"`
	MySQL84  *columnNameResult `json:"mysql84"`
	MySQL80  *columnNameResult `json:"mysql80"`
}

type columnNameResult struct {
	Names    []string `json:"names"`
	NamesHex []string `json:"names_hex"`
	Error    string   `json:"error"`
}

// TestMySQLColumnNameCorpus checks MySQLColumnName against the names MySQL
// returns for the corpus in testdata/column_names.json. It covers every case
// whose names follow from the query text alone.
func TestMySQLColumnNameCorpus(t *testing.T) {
	data, err := os.ReadFile("testdata/column_names.json")
	require.NoError(t, err)
	var cases []columnNameCase
	require.NoError(t, json.Unmarshal(data, &cases))

	for _, version := range []string{"8.4.6", "8.0.43"} {
		parser, err := New(Options{MySQLServerVersion: version})
		require.NoError(t, err)
		checked := 0
		for _, c := range cases {
			want := c.MySQL84
			if strings.HasPrefix(version, "8.0.") && c.MySQL80 != nil {
				want = c.MySQL80
			}
			exprs := textNamedSelectExprs(parser, c, want)
			if exprs == nil {
				continue
			}
			checked++
			var got []string
			for _, ae := range exprs {
				got = append(got, ae.MySQLColumnName(ColumnNameEnv{}))
			}
			assert.Equal(t, want.Names, got, "%s %s: %s", version, c.ID, c.Query)
		}
		t.Logf("MySQL %s: checked %d cases", version, checked)
	}
}

// textNamedSelectExprs returns the select expressions of a corpus case whose
// names MySQL derives from the query text alone. It returns nil when a case
// does not qualify: when it depends on the schema (stars, and references into
// derived tables, CTEs, views or information_schema), on session settings, or
// on syntax the parser does not support.
func textNamedSelectExprs(parser *Parser, c columnNameCase, want *columnNameResult) []*AliasedExpr {
	if want == nil || want.Error != "" || want.NamesHex != nil {
		return nil
	}
	for _, stmt := range c.Setup {
		if !strings.HasPrefix(strings.ToLower(stmt), "set @") {
			return nil
		}
	}
	stmt, err := parser.Parse(c.Query)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(*Select)
	if !ok {
		return nil
	}
	var exprs []*AliasedExpr
	for _, se := range sel.SelectExprs.Exprs {
		ae, ok := se.(*AliasedExpr)
		if !ok {
			return nil
		}
		if _, isCol := ae.Expr.(*ColName); isCol && !ae._name.aliased && (sel.With != nil || !onlyBaseTables(sel.From)) {
			return nil
		}
		exprs = append(exprs, ae)
	}
	if len(exprs) != len(want.Names) {
		return nil
	}
	return exprs
}

// onlyBaseTables reports whether a FROM clause refers only to tables whose
// columns MySQL names as typed: no derived tables, views or
// information_schema tables.
func onlyBaseTables(from []TableExpr) bool {
	ok := true
	for _, te := range from {
		_ = Walk(func(node SQLNode) (bool, error) {
			switch node := node.(type) {
			case *DerivedTable:
				ok = false
			case TableName:
				name := strings.ToLower(node.Name.String())
				qualifier := strings.ToLower(node.Qualifier.String())
				if strings.HasPrefix(name, "v") || qualifier == "information_schema" {
					ok = false
				}
			}
			return ok, nil
		}, te)
	}
	return ok
}

func TestMySQLColumnName(t *testing.T) {
	tests := []struct {
		name  string
		query string
		env   ColumnNameEnv
		want  []string
	}{
		{name: "raw text keeps inner whitespace and comments", query: "select 1 /* c */ +  1, Count( * ) from t", want: []string{"1 /* c */ +  1", "Count( * )"}},
		{name: "leading and trailing comments are not part of the name", query: "select /* a */ 1+1 /* b */", want: []string{"1+1"}},
		{name: "versioned comment markers are removed", query: "select 1 /*!+ 2*/, 1 /*!80000 +1 */", want: []string{"1 + 2", "1  +1"}},
		{name: "a space replaces the end of a versioned comment", query: "select 1/*!+2*/+3", want: []string{"1+2 +3"}},
		{name: "unexecuted versioned comments are removed", query: "select 1 /*!99999 +1 */ + 1", want: []string{"1  + 1"}},
		{name: "column references are named as typed", query: "select t.ID, `a``b` from t", want: []string{"ID", "a`b"}},
		{name: "text literals are named after their first fragment", query: "select 'a' 'b', '  x', 'a\\0b', ''", want: []string{"a", "x", "a", ""}},
		{name: "introducers", query: "select _utf8mb4'z', N'n', _latin1'é', _binary'\xffabc'", want: []string{"z", "n", "Ã©", "abc"}},
		{name: "characters outside the BMP become question marks", query: "select '😀', concat('😀')", want: []string{"?", "concat('?')"}},
		{name: "NULL, parameters and wrappers", query: "select NuLl, (null), ?, (1), +1, +(1+2), -1", want: []string{"NULL", "NULL", "?", "1", "1", "+(1+2)", "-1"}},
		{name: "number tokens", query: "select 01, 1.50, 1E3", want: []string{"01", "1.50", "1E3"}},
		{name: "aliases", query: "select 1 as ' a', 1 as '', 2 as `x  `, 3 as 'x\\0y'", want: []string{"a", "", "x  ", "x"}},
		{name: "raw text is truncated to 255 bytes", query: "select concat('" + strings.Repeat("a", 300) + "')", want: []string{"concat('" + strings.Repeat("a", 247)}},
		{name: "integers are never truncated", query: "select " + strings.Repeat("0", 300) + "7", want: []string{strings.Repeat("0", 300) + "7"}},
		{name: "unsigned integers are truncated to 256 bytes", query: "select " + strings.Repeat("0", 300) + "18446744073709551615", want: []string{strings.Repeat("0", 256)}},
		{name: "aliases are truncated to 256 bytes", query: "select 1 as `" + strings.Repeat("b", 300) + "`", want: []string{strings.Repeat("b", 256)}},
		{name: "a utf8mb3 connection keeps characters outside the BMP", query: "select '😀', concat('😀')", env: ColumnNameEnv{ClientCharset: "utf8mb3", ConnectionCharset: "utf8mb3"}, want: []string{"😀", "concat('😀')"}},
		{name: "a utf8mb3 client truncates raw text to 256 bytes", query: "select concat('" + strings.Repeat("a", 300) + "')", env: ColumnNameEnv{ClientCharset: "utf8mb3"}, want: []string{"concat('" + strings.Repeat("a", 248)}},
	}
	parser, err := New(Options{MySQLServerVersion: "8.4.6"})
	require.NoError(t, err)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err)
			var got []string
			for _, se := range stmt.(*Select).SelectExprs.Exprs {
				got = append(got, se.(*AliasedExpr).MySQLColumnName(tc.env))
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMySQLColumnNameIgnoresAddedAliases(t *testing.T) {
	stmt, err := NewTestParser().Parse("select 1+1, count(*) as c from t")
	require.NoError(t, err)
	exprs := stmt.(*Select).SelectExprs.Exprs

	// An alias that Vitess adds, for example in the normalizer, does not
	// change the name MySQL gives the column.
	added := exprs[0].(*AliasedExpr)
	added.As = NewIdentifierCI("vt_alias")
	assert.Equal(t, "1+1", added.MySQLColumnName(ColumnNameEnv{}))

	// The naming input survives cloning.
	assert.Equal(t, "1+1", Clone(added).MySQLColumnName(ColumnNameEnv{}))
	assert.Equal(t, "c", exprs[1].(*AliasedExpr).MySQLColumnName(ColumnNameEnv{}))
}

func TestMySQLColumnNameOfPlannerExpressions(t *testing.T) {
	// Expressions that the parser did not create are named by ColumnName.
	ae := &AliasedExpr{Expr: NewColName("id")}
	assert.Equal(t, "id", ae.MySQLColumnName(ColumnNameEnv{}))
	ae = &AliasedExpr{Expr: &FuncExpr{Name: NewIdentifierCI("now")}, As: NewIdentifierCI("n")}
	assert.Equal(t, "n", ae.MySQLColumnName(ColumnNameEnv{}))
}

func TestColumnNameInputIsIgnoredByEquals(t *testing.T) {
	parser := NewTestParser()
	a, err := parser.Parse("select 1+1 from t")
	require.NoError(t, err)
	b, err := parser.Parse("select 1+1 from t")
	require.NoError(t, err)
	assert.True(t, Equals.SQLNode(a, b))
}

func TestSixDigitVersionedComments(t *testing.T) {
	tests := []struct {
		version string
		query   string
		want    string
	}{
		{version: "8.4.6", query: "select 1 /*!080000 +1 */", want: "select 1 + 1 from dual"},
		{version: "8.4.6", query: "select 1 /*!100000 +1 */", want: "select 1 from dual"},
		{version: "8.4.6", query: "select 1 /*!80400 +1 */", want: "select 1 + 1 from dual"},
		{version: "8.0.43", query: "select 1 /*!80400 +1 */", want: "select 1 from dual"},
		{version: "8.0.43", query: "select /*!401011 from*/ t", want: "select 1 from t"},
		{version: "8.4.6", query: "select /*!401011 from*/ t", want: "select t from dual"},
	}
	for _, tc := range tests {
		t.Run(tc.version+" "+tc.query, func(t *testing.T) {
			parser, err := New(Options{MySQLServerVersion: tc.version})
			require.NoError(t, err)
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err)
			assert.Equal(t, tc.want, String(stmt))
		})
	}
}

func TestAliasColumnNames(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{{
		// Expressions that vtgate prints differently get their MySQL names.
		query: "select 1+1, count(*), COUNT(*), id, 'a', NULL from t",
		want:  "select 1 + 1 as `1+1`, count(*), count(*) as `COUNT(*)`, id, 'a' as a, null as `NULL` from t",
	}, {
		// Aliases follow MySQL's rules too; an empty name cannot be an alias.
		query: "select 1 as ' a', 2 as `😀`, ''",
		want:  "select 1 as a, 2 as `?`, '' from dual",
	}, {
		// Planning rewrites subqueries, and names referenced by ORDER BY,
		// GROUP BY and HAVING must resolve.
		query: "select (select 1), count(*) from t having `count(*)` > 0",
		want:  "select (select 1 from dual) as `(select 1)`, count(*) as `count(*)` from t having `count(*)` > 0",
	}, {
		// Columns of derived tables, CTEs and views are named too.
		query: "with c as (select 'x') select * from (select 1+1) as d, c",
		want:  "with c as (select 'x' as x from dual) select * from (select 1 + 1 as `1+1` from dual) as d, c",
	}, {
		query: "create view v as select 1+1",
		want:  "create view v as select 1 + 1 as `1+1` from dual",
	}, {
		// A column reference into a derived table that MySQL materializes
		// keeps its own spelling; one that MySQL merges takes the column's.
		query: "select ID, x.ID from (select id from t limit 5) as x",
		want:  "select ID as ID, x.ID as ID from (select id from t limit 5) as x",
	}, {
		query: "select ID from (select id from t) as x",
		want:  "select ID from (select id from t) as x",
	}, {
		query: "select id from (select id from t limit 5) as x",
		want:  "select id from (select id from t limit 5) as x",
	}}
	parser := NewTestParser()
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err)
			AliasColumnNames(stmt, ColumnNameEnv{})
			assert.Equal(t, tc.want, String(stmt))
		})
	}
}

func TestRedactSQLQueryHidesRewrittenExpressions(t *testing.T) {
	parser := NewTestParser()
	for _, query := range []string{
		"select concat('secret', last_insert_id())",
		"select * from (select concat('secret', @@autocommit)) as t",
	} {
		redacted, err := parser.RedactSQLQuery(query)
		require.NoError(t, err)
		assert.NotContains(t, redacted, "secret", query)
	}
}
