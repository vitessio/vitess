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
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	querypb "vitess.io/vitess/go/vt/proto/query"
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
			env, results, ok := corpusSession(c.Setup)
			if !ok {
				continue
			}
			exprs := textNamedSelectExprs(parser, c, want, results != nil)
			if exprs == nil {
				continue
			}
			checked++
			var got, gotHex []string
			for _, ae := range exprs {
				name := ae.MySQLColumnName(env)
				if results != nil {
					// MySQL converts the names to character_set_results
					// when it sends them.
					encoded, _ := charset.ConvertFromUTF8(nil, results, []byte(name))
					name = string(encoded)
				}
				got = append(got, name)
				gotHex = append(gotHex, hex.EncodeToString([]byte(name)))
			}
			if want.NamesHex != nil {
				assert.Equal(t, want.NamesHex, gotHex, "%s %s: %s", version, c.ID, c.Query)
			} else {
				assert.Equal(t, want.Names, got, "%s %s: %s", version, c.ID, c.Query)
			}
		}
		t.Logf("MySQL %s: checked %d cases", version, checked)
	}
}

// corpusSession returns the charsets that the setup of a corpus case sets:
// the ones MySQL names the columns with, and the one it converts the names
// to, or nil when it sends them as they are. It reports false when the setup
// changes other session settings that names depend on.
func corpusSession(setup []string) (ColumnNameEnv, charset.Charset, bool) {
	var env ColumnNameEnv
	results := ""
	for _, stmt := range setup {
		stmt = strings.ToLower(stmt)
		switch {
		case strings.HasPrefix(stmt, "set @"):
		case stmt == "set names latin1":
			env = ColumnNameEnv{ClientCharset: "latin1", ConnectionCharset: "latin1"}
			results = "latin1"
		case strings.HasPrefix(stmt, "set character_set_results = "):
			results = strings.TrimPrefix(stmt, "set character_set_results = ")
		default:
			return env, nil, false
		}
	}
	switch results {
	case "", "utf8mb4", "utf8mb3", "binary", "null":
		return env, nil, true
	}
	coll := colldata.Lookup(collations.MySQL8().DefaultCollationForCharset(results))
	if coll == nil {
		return env, nil, false
	}
	return env, coll.Charset(), true
}

// textNamedSelectExprs returns the select expressions of a corpus case whose
// names MySQL derives from the query text alone. It returns nil when a case
// does not qualify: when it depends on the schema (stars, and references into
// derived tables, CTEs, views or information_schema), or on syntax the parser
// does not support. Names that are not valid UTF-8 qualify only when they
// were converted to another charset.
func textNamedSelectExprs(parser *Parser, c columnNameCase, want *columnNameResult, converted bool) []*AliasedExpr {
	if want == nil || want.Error != "" || (want.NamesHex != nil && !converted) {
		return nil
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
		{name: "a latin1 client's identifiers and text are read as latin1", query: "select '😀', 1 as `😀`, concat('é')", env: ColumnNameEnv{ClientCharset: "latin1", ConnectionCharset: "latin1"}, want: []string{"ðŸ˜€", "ðŸ˜€", "concat('Ã©')"}},
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
		query: "select (a, b) = (1, 2), not a = 1, not a from t",
		want:  "select (a, b) = (1, 2) as `(a, b) = (1, 2)`, not a = 1 as `not a = 1`, not a from t",
	}, {
		// Columns of derived tables, CTEs and views are named too.
		query: "with c as (select 'x') select * from (select 1+1) as d, c",
		want:  "with c as (select 'x' as x from dual) select * from (select 1 + 1 as `1+1` from dual) as d, c",
	}, {
		query: "create view v as select 1+1",
		want:  "create view v as select 1 + 1 as `1+1` from dual",
	}, {
		// MySQL names a view column Name_exp_<position> when its name would
		// be empty, longer than 64 characters or end with a space. It does
		// so itself for the concat, which vtgate sends as it is spelled.
		query: "create view v as select 1, concat('" + strings.Repeat("a", 60) + "'), 'b ', ''",
		want:  "create view v as select 1, concat('" + strings.Repeat("a", 60) + "'), 'b ' as Name_exp_3, '' as Name_exp_4 from dual",
	}, {
		// A derived table column with an empty name needs a name that the
		// SQL vtgate sends can use.
		query: "select * from (select '') as t",
		want:  "select * from (select '' as vt_unnamed_0 from dual) as t",
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
			AliasColumnNames(stmt, ColumnNameEnv{}, nil)
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

// TestColumnRenames checks that result columns named after bind variables,
// and ones with empty names, are renamed in each result instead of aliased.
func TestColumnRenames(t *testing.T) {
	stmt, err := NewTestParser().Parse("select :a + 1, 1+1, t.*, '', :b as x from t")
	require.NoError(t, err)
	bindVars := map[string]*querypb.BindVariable{"a": {}, "b": {}}
	renames := AliasColumnNames(stmt, ColumnNameEnv{}, bindVars)
	assert.Equal(t, "select :a + 1, 1 + 1 as `1+1`, t.*, '', :b as x from t", String(stmt))

	// The star expands to three columns.
	fields := []*querypb.Field{{Name: ":a + 1"}, {Name: "1+1"}, {Name: "c1"}, {Name: "c2"}, {Name: "c3"}, {Name: "''"}, {Name: "x"}}
	renamed := renames.Apply(fields)
	var names []string
	for _, f := range renamed {
		names = append(names, f.Name)
	}
	assert.Equal(t, []string{":a + 1", "1+1", "c1", "c2", "c3", "", "x"}, names)
	assert.Equal(t, "''", fields[5].Name, "the fields are not changed in place")
	assert.Same(t, fields[0], renamed[0])

	// Without a star, the fields must match the select expressions.
	stmt, err = NewTestParser().Parse("select :a + 2, ''")
	require.NoError(t, err)
	renames = AliasColumnNames(stmt, ColumnNameEnv{}, bindVars)
	fields = []*querypb.Field{{Name: "3"}, {Name: "''"}}
	assert.Equal(t, ":a + 2", renames.Apply(fields)[0].Name)
	assert.Equal(t, "", renames.Apply(fields)[1].Name)
	assert.Equal(t, fields[:1], renames.Apply(fields[:1]))

	// A star over a derived table returns the columns with empty names
	// under the names that stand in for them.
	stmt, err = NewTestParser().Parse("select * from (select '', 1) as t")
	require.NoError(t, err)
	renames = AliasColumnNames(stmt, ColumnNameEnv{}, nil)
	fields = []*querypb.Field{{Name: "vt_unnamed_0"}, {Name: "1"}}
	assert.Equal(t, "", renames.Apply(fields)[0].Name)
	assert.Equal(t, "1", renames.Apply(fields)[1].Name)
}

// BenchmarkAliasColumnNames measures the alias pass that vtgate runs on every
// normalized statement.
func BenchmarkAliasColumnNames(b *testing.B) {
	parser := NewTestParser()
	normalize := func(b *testing.B, query string) (Statement, map[string]*querypb.BindVariable) {
		stmt, known, err := parser.Parse2(query)
		require.NoError(b, err)
		bindVars := map[string]*querypb.BindVariable{}
		out, err := Normalize(stmt, NewReservedVars("vtg", known), bindVars, true, "ks", 0, "", map[string]string{}, nil, nil)
		require.NoError(b, err)
		return out.AST, bindVars
	}
	// run aliases clones of the statements, because the pass changes them.
	run := func(b *testing.B, stmts []Statement, bindVars []map[string]*querypb.BindVariable) {
		const batch = 1024
		clones := make([]Statement, 0, batch)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; {
			b.StopTimer()
			clones = clones[:0]
			for i := 0; i < batch && n+i < b.N; i++ {
				clones = append(clones, CloneStatement(stmts[(n+i)%len(stmts)]))
			}
			b.StartTimer()
			for i, stmt := range clones {
				AliasColumnNames(stmt, ColumnNameEnv{}, bindVars[(n+i)%len(stmts)])
			}
			n += len(clones)
		}
	}
	for _, query := range []string{
		"select id, name from user where id = 1",
		"select id, count(*), 1+1, 'abc' from user where id = 1",
		"select 1, 1+1, now()",
		"select u.id, u.name, o.total from user as u join orders as o on u.id = o.uid where u.id = 5 order by o.total desc limit 10",
		"select * from (select id, count(*) from t group by id) as x where id > 3",
		"insert into t(a, b, c) values (1, 'x', now())",
		"update t set a = a + 1 where id = 7",
	} {
		b.Run(query, func(b *testing.B) {
			stmt, bindVars := normalize(b, query)
			run(b, []Statement{stmt}, []map[string]*querypb.BindVariable{bindVars})
		})
	}
	for _, trace := range []string{"django_queries.txt", "lobsters.sql.gz"} {
		b.Run(trace, func(b *testing.B) {
			var stmts []Statement
			var bindVars []map[string]*querypb.BindVariable
			for _, query := range loadQueries(b, trace) {
				stmt, known, err := parser.Parse2(query)
				if err != nil {
					continue
				}
				bv := map[string]*querypb.BindVariable{}
				out, err := Normalize(stmt, NewReservedVars("vtg", known), bv, true, "ks", 0, "", map[string]string{}, nil, nil)
				if err != nil {
					continue
				}
				stmts = append(stmts, out.AST)
				bindVars = append(bindVars, bv)
				if len(stmts) == 2000 {
					break
				}
			}
			run(b, stmts, bindVars)
		})
	}
}
