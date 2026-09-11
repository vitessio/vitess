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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/config"
	"vitess.io/vitess/go/mysql/sqlmode"
)

// newIgnoreSpaceTestParser returns a parser whose sql_mode has IGNORE_SPACE.
func newIgnoreSpaceTestParser(t *testing.T) *Parser {
	t.Helper()
	parser, err := New(Options{SQLMode: sqlmode.IgnoreSpace, TruncateUILen: 512})
	require.NoError(t, err)
	return parser
}

// Without IGNORE_SPACE in its sql_mode, the parser follows MySQL's default
// reading of the whitespace-sensitive function names: the name is the
// keyword directly before '(', and an identifier anywhere else. This release
// keeps one exception, checked by TestSpacedAggrCallsWithoutIgnoreSpace.
func TestFuncNamesWithoutIgnoreSpace(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{{
		// directly before '(' it is the built-in
		input:  "select cast(1 as char), now(), now(6), sysdate(), curdate(), curtime(), substr('a', 1), extract(year from d) from t",
		output: "select cast(1 as char), now(), now(6), sysdate(), curdate(), curtime(), substr('a', 1), extract(year from d) from t",
	}, {
		// anywhere else, each name on the list is an identifier, so tables
		// and columns may be named after the functions without quoting, as
		// in MySQL
		input:  "create table CAST (a int)",
		output: "create table `CAST` (\n\ta int\n)",
	}, {
		input:  "create table NOW (a int)",
		output: "create table `NOW` (\n\ta int\n)",
	}, {
		input:  "drop table CAST",
		output: "drop table `CAST`",
	}, {
		input:  "create table t (cast int, now int, extract int, sum int)",
		output: "create table t (\n\t`cast` int,\n\t`now` int,\n\t`extract` int,\n\t`sum` int\n)",
	}, {
		input:  "select now, count, sum, position, trim, std, var_pop, extract, cast from t",
		output: "select `now`, `count`, `sum`, `position`, `trim`, `std`, `var_pop`, `extract`, `cast` from t",
	}, {
		input:  "select json_arrayagg, json_objectagg, st_collect, session_user, system_user from t",
		output: "select `json_arrayagg`, `json_objectagg`, `st_collect`, `session_user`, `system_user` from t",
	}, {
		input:  "select t.now, t.`cast` from t",
		output: "select t.`now`, t.`cast` from t",
	}, {
		// with whitespace before '(' the name is an identifier, and the call
		// is a generic function call, MySQL's stored-function call path. It
		// serializes quoted so that MySQL takes that path too, rather than
		// re-lexing the bare name as the built-in
		input:  "select now () from t",
		output: "select `now`() from t",
	}, {
		input:  "select sysdate (), curdate (), curtime (), session_user (), system_user () from t",
		output: "select `sysdate`(), `curdate`(), `curtime`(), `session_user`(), `system_user`() from t",
	}, {
		input:  "select substr ('abc', 1, 2), trim (' a '), mid ('abc', 1, 1), adddate (d, 1), subdate (d, 1) from t",
		output: "select `substr`('abc', 1, 2), `trim`(' a '), `mid`('abc', 1, 1), `adddate`(d, 1), `subdate`(d, 1) from t",
	}, {
		// a comment before the parenthesis separates the name from it, like
		// whitespace does
		input:  "select now/*c*/() from t",
		output: "select `now`() from t",
	}, {
		// quoted, the name is an identifier as well
		input:  "select `now`(), `sum`(x), `count`(1), `session_user`() from t",
		output: "select `now`(), `sum`(x), `count`(1), `session_user`() from t",
	}, {
		// a qualified name is never a keyword: it names a stored function or
		// a column in the schema, with or without whitespace before the
		// parenthesis
		input:  "select db.cast(1), db.cast (1), db.now (), t.count(1), db.sum (x) from t",
		output: "select db.`cast`(1), db.`cast`(1), db.`now`(), t.`count`(1), db.`sum`(x) from t",
	}, {
		// st_collect is an aggregate: one argument, distinct, and a window form
		input:  "select ST_Collect(DISTINCT g), st_collect(g) over (partition by id) from t group by id",
		output: "select st_collect(distinct g), st_collect(g) over (partition by id) from t group by id",
	}, {
		// session_user and system_user name tables, indexes and procedures
		// in identifier positions, as in MySQL
		input:  "create table t (a int, index session_user(a), index system_user(a))",
		output: "create table t (\n\ta int,\n\tkey `session_user` (a),\n\tkey `system_user` (a)\n)",
	}, {
		input:  "create table session_user(a int)",
		output: "create table `session_user` (\n\ta int\n)",
	}, {
		input:  "call session_user(1)",
		output: "call `session_user`(1)",
	}, {
		input:  "insert into system_user(a) values (1)",
		output: "insert into `system_user`(a) values (1)",
	}}
	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			tree, _, spaced, err := parser.ParseWithSpacedAggrCalls(tcase.input)
			require.NoError(t, err)
			assert.Equal(t, tcase.output, String(tree))
			assert.Empty(t, spaced)
		})
	}

	errors := []struct {
		input  string
		output string
	}{{
		// CAST is only a keyword when directly followed by '('; with a space
		// it is an identifier and AS is not valid in a generic argument list.
		input:  "select cast (1 as char)",
		output: "syntax error at position 18 near 'as'",
	}, {
		// the same for every non-aggregate on MySQL's list whose built-in
		// form has its own argument syntax: with whitespace, the generic
		// argument list applies
		input:  "select position ('a' in 'abc') from t",
		output: "syntax error at position 30 near 'abc'",
	}, {
		input:  "select trim (leading 'a' from 'abc') from t",
		output: "syntax error at position 21 near 'leading'",
	}, {
		input:  "select date_add (now(), interval 1 day) from t",
		output: "syntax error at position 40",
	}, {
		input:  "select substring ('abc' from 1) from t",
		output: "syntax error at position 29 near 'from'",
	}, {
		input:  "select extract (year from now()) from t",
		output: "syntax error at position 26 near 'from'",
	}, {
		// bare now (without parens) is an identifier, not the now() function
		input:  "create table t (a datetime default now)",
		output: "syntax error at position 39 near 'now'",
	}, {
		input:  "create table t (a datetime default now() on update now)",
		output: "syntax error at position 55 near 'now'",
	}, {
		// the user-information functions take no arguments; the keyword form
		// does not fall back to a generic call
		input:  "select session_user(1)",
		output: "syntax error at position 22 near '1'",
	}, {
		input:  "select system_user(1)",
		output: "syntax error at position 21 near '1'",
	}, {
		// curdate takes no argument and no precision, like current_date
		input:  "select curdate(3)",
		output: "syntax error at position 17 near '3'",
	}, {
		input:  "select current_date(3)",
		output: "syntax error at position 22 near '3'",
	}, {
		input:  "select utc_date(3)",
		output: "syntax error at position 18 near '3'",
	}, {
		// st_collect takes exactly one argument, as in MySQL
		input:  "select st_collect() from t",
		output: "syntax error at position 20",
	}, {
		input:  "select st_collect(g, g) from t",
		output: "syntax error at position 21",
	}}
	for _, tcase := range errors {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := parser.ParseStrictDDL(tcase.input)
			require.Error(t, err)
			assert.Equal(t, tcase.output, err.Error())
		})
	}
}

// Without IGNORE_SPACE in its sql_mode, the parser keeps one of Vitess's
// readings that MySQL does not have: an aggregate on the list with
// whitespace or a comment before its parenthesis stays the aggregate,
// because read as a stored-function call it could return a wrong result
// during a rolling upgrade (see mysqlAggrFuncCallKeywords). It is reported,
// since the next major release reads it as MySQL does.
func TestSpacedAggrCallsWithoutIgnoreSpace(t *testing.T) {
	testcases := []struct {
		input  string
		output string
		spaced []string
	}{{
		input:  "select count (1), sum (x), max (x), min (x) from t",
		output: "select count(1), sum(x), max(x), min(x) from t",
		spaced: []string{"count", "sum", "max", "min"},
	}, {
		input:  "select std (x), stddev (x), stddev_pop (x), stddev_samp (x), variance (x), var_pop (x), var_samp (x) from t",
		output: "select std(x), stddev(x), stddev_pop(x), stddev_samp(x), variance(x), var_pop(x), var_samp(x) from t",
		spaced: []string{"std", "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp"},
	}, {
		input:  "select bit_and (x), bit_or (x), bit_xor (x), json_arrayagg (a), json_objectagg (a, b), st_collect (g) from t",
		output: "select bit_and(x), bit_or(x), bit_xor(x), json_arrayagg(a), json_objectagg(a, b), st_collect(g) from t",
		spaced: []string{"bit_and", "bit_or", "bit_xor", "json_arrayagg", "json_objectagg", "st_collect"},
	}, {
		// the aggregate's own argument syntax is available with the whitespace
		input:  "select count (*), count (distinct a), group_concat (distinct a order by a separator ',') from t",
		output: "select count(*), count(distinct a), group_concat(distinct a order by a asc separator ',') from t",
		spaced: []string{"count", "group_concat"},
	}, {
		// a comment before the parenthesis is whitespace too
		input:  "select count/*c*/(*), sum /* c */ (x), max -- c\n(x) from t",
		output: "select count(*), sum(x), max(x) from t",
		spaced: []string{"count", "sum", "max"},
	}, {
		// each name is reported once, lowercased, in order of first appearance
		input:  "select SUM (a), Sum (b), count (c), sum(d) from t",
		output: "select sum(a), sum(b), count(c), sum(d) from t",
		spaced: []string{"sum", "count"},
	}, {
		// a spaced non-aggregate on the list is a stored-function call, as in
		// MySQL, and is not reported
		input:  "select sum (x), now (), substr (a, 1) from t",
		output: "select sum(x), `now`(), `substr`(a, 1) from t",
		spaced: []string{"sum"},
	}, {
		// a bare aggregate name is a column, as in MySQL, and is not reported
		input:  "select sum, count from t where max = 1",
		output: "select `sum`, `count` from t where `max` = 1",
	}, {
		// names that are not on MySQL's list are untouched
		input:  "select avg (x), if (a, 1, 2), left (a, 1), abs (a), database () from t",
		output: "select avg(x), if(a, 1, 2), left(a, 1), abs(a), database() from t",
	}}
	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			tree, _, spaced, err := parser.ParseWithSpacedAggrCalls(tcase.input)
			require.NoError(t, err)
			assert.Equal(t, tcase.output, String(tree))
			assert.Equal(t, tcase.spaced, spaced)
		})
	}
}

// With IGNORE_SPACE in its sql_mode, the parser follows MySQL's IGNORE_SPACE
// reading: the name is the keyword when '(' follows it, directly or after
// whitespace, and an identifier otherwise, so whitespace before the
// parenthesis is permitted and nothing is reported.
func TestFuncNamesWithIgnoreSpace(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{{
		input:  "select now (), sum (x), count (*), cast (1 as char) from t",
		output: "select now(), sum(x), count(*), cast(1 as char) from t",
	}, {
		input:  "select count\t(*), session_user\n(), substr (a, 1) from t",
		output: "select count(*), session_user(), substr(a, 1) from t",
	}, {
		// MySQL's lexer skips whitespace, not comments, before deciding
		// whether '(' follows
		input:  "select count/*c*/(1), sum /* c */ (x) from t",
		output: "select `count`(1), `sum`(x) from t",
	}, {
		// with no parenthesis following, the name is an identifier, as
		// without IGNORE_SPACE
		input:  "select now, sum, cast, extract from t",
		output: "select `now`, `sum`, `cast`, `extract` from t",
	}, {
		input:  "create table t (now int, cast int, extract int)",
		output: "create table t (\n\t`now` int,\n\t`cast` int,\n\t`extract` int\n)",
	}, {
		input:  "drop table CAST",
		output: "drop table `CAST`",
	}, {
		// a non-reserved keyword still names an index where '(' follows it
		input:  "create table t (a int, key sum (a))",
		output: "create table t (\n\ta int,\n\tkey `sum` (a)\n)",
	}, {
		// quoted and qualified names are identifiers under IGNORE_SPACE as well
		input:  "select `now`(), `sum`(x), db.cast (1), t.now from t",
		output: "select `now`(), `sum`(x), db.`cast`(1), t.`now` from t",
	}}
	parser := newIgnoreSpaceTestParser(t)
	for _, tcase := range testcases {
		t.Run(tcase.input, func(t *testing.T) {
			tree, _, spaced, err := parser.ParseWithSpacedAggrCalls(tcase.input)
			require.NoError(t, err)
			assert.Equal(t, tcase.output, String(tree))
			assert.Empty(t, spaced)
		})
	}

	// where '(' follows the name, it is the keyword, so a table named after
	// a function must be quoted there, and a bare now is not now()
	errors := []struct {
		input  string
		output string
	}{{
		input:  "create table CAST (a int)",
		output: "syntax error at position 18 near 'CAST'",
	}, {
		input:  "create table t (a datetime default now)",
		output: "syntax error at position 39 near 'now'",
	}}
	for _, tcase := range errors {
		t.Run(tcase.input, func(t *testing.T) {
			_, err := parser.ParseStrictDDL(tcase.input)
			require.Error(t, err)
			assert.Equal(t, tcase.output, err.Error())
		})
	}
}

// Only IGNORE_SPACE in the parser's sql_mode changes how the
// whitespace-sensitive function names are read; runtime modes and a zero
// mode, MySQL's default, read them without it.
func TestParserSQLMode(t *testing.T) {
	mysqlDefault, err := sqlmode.Parse(config.DefaultSQLMode)
	require.NoError(t, err)
	require.Zero(t, mysqlDefault&sqlmode.IgnoreSpace)

	testcases := []struct {
		name        string
		mode        sqlmode.Mode
		ignoreSpace bool
	}{{
		name: "zero is MySQL's default, without IGNORE_SPACE",
		mode: 0,
	}, {
		name: "MySQL's default modes",
		mode: mysqlDefault,
	}, {
		name:        "IGNORE_SPACE alone",
		mode:        sqlmode.IgnoreSpace,
		ignoreSpace: true,
	}, {
		name:        "IGNORE_SPACE among runtime modes",
		mode:        sqlmode.StrictTransTables | sqlmode.IgnoreSpace | sqlmode.NoZeroDate,
		ignoreSpace: true,
	}, {
		// the combination mode is expanded by the parser
		name:        "the ANSI combination mode has IGNORE_SPACE",
		mode:        sqlmode.Ansi,
		ignoreSpace: true,
	}}
	for _, tcase := range testcases {
		t.Run(tcase.name, func(t *testing.T) {
			parser, err := New(Options{SQLMode: tcase.mode})
			require.NoError(t, err)
			assert.Equal(t, tcase.mode.Expand(), parser.SQLMode())

			// the spaced aggregate is the aggregate either way, reported
			// without IGNORE_SPACE
			stmt, _, spaced, err := parser.ParseWithSpacedAggrCalls("select sum (x) from t")
			require.NoError(t, err)
			assert.Equal(t, "select sum(x) from t", String(stmt))
			if tcase.ignoreSpace {
				assert.Empty(t, spaced)
			} else {
				assert.Equal(t, []string{"sum"}, spaced)
			}

			// a spaced non-aggregate is the built-in with IGNORE_SPACE and a
			// stored-function call without
			stmt, _, spaced, err = parser.ParseWithSpacedAggrCalls("select now () from t")
			require.NoError(t, err)
			assert.Empty(t, spaced)
			if tcase.ignoreSpace {
				assert.Equal(t, "select now() from t", String(stmt))
			} else {
				assert.Equal(t, "select `now`() from t", String(stmt))
			}

			// a bare name is a column either way
			stmt, _, spaced, err = parser.ParseWithSpacedAggrCalls("select cast from t")
			require.NoError(t, err)
			assert.Equal(t, "select `cast` from t", String(stmt))
			assert.Empty(t, spaced)
		})
	}
	assert.Zero(t, NewTestParser().SQLMode(), "the test parser reads under MySQL's default sql_mode")
}

// Parse and Parse2 read the same SQL as ParseWithSpacedAggrCalls; only the
// report differs.
func TestParse2MatchesParseWithSpacedAggrCalls(t *testing.T) {
	parser := NewTestParser()
	in := "select sum (x) from t"
	stmt, bindVars, err := parser.Parse2(in)
	require.NoError(t, err)
	stmt2, bindVars2, spaced, err := parser.ParseWithSpacedAggrCalls(in)
	require.NoError(t, err)
	assert.Equal(t, String(stmt), String(stmt2))
	assert.Equal(t, bindVars, bindVars2)
	assert.Equal(t, []string{"sum"}, spaced)
}

// Every name MySQL lexes as a keyword only directly before '('
// (mysqlFuncCallKeywords) is a keyword of this grammar whose call form parses
// into a node of its own, so that a call by an identifier of the same spelling
// is a generic FuncExpr and prints quoted. The aggregates among them, and
// only those, are on mysqlAggrFuncCallKeywords, and keep the keyword reading
// before a detached parenthesis for this release.
func TestFuncCallKeywords(t *testing.T) {
	// arguments that satisfy each name's built-in syntax; "" means no arguments
	args := map[string]string{
		"cast": "a as char", "date_add": "a, interval 1 day", "date_sub": "a, interval 1 day", "adddate": "a, 1", "subdate": "a, 1",
		"extract": "year from a", "position": "'a' in a", "trim": "a", "substring": "a, 1", "substr": "a, 1", "mid": "a, 1, 1",
		"group_concat": "a", "json_objectagg": "a, b",
	}
	noArgs := map[string]bool{
		"now": true, "curdate": true, "curtime": true, "sysdate": true, "session_user": true, "system_user": true,
	}
	parser := NewTestParser()
	ignoreSpaceParser := newIgnoreSpaceTestParser(t)
	for _, name := range mysqlFuncCallKeywords {
		t.Run(name, func(t *testing.T) {
			id, ok := keywordLookupTable.LookupString(name)
			require.True(t, ok, "not a keyword")
			assert.True(t, isFuncCallKeyword(id))
			assert.True(t, IsFuncCallKeywordName(name))

			arg, ok := args[name]
			if !ok && !noArgs[name] {
				arg = "a"
			}
			keywordForm := fmt.Sprintf("select %s(%s) from t", name, arg)
			spacedForm := fmt.Sprintf("select %s (%s) from t", name, arg)
			spacedGeneric := fmt.Sprintf("select %s (a) from t", name)
			quoted := fmt.Sprintf("select `%s`(a) from t", name)

			// the keyword form is never a generic call; it is an aggregate
			// exactly for the names on the aggregate list
			keywordStmt, err := parser.Parse(keywordForm)
			require.NoError(t, err, keywordForm)
			expr := keywordStmt.(*Select).SelectExprs.Exprs[0].(*AliasedExpr).Expr
			assert.NotEqual(t, "*sqlparser.FuncExpr", fmt.Sprintf("%T", expr), "keyword form must not be a generic call")
			_, isAggr := expr.(AggrFunc)
			assert.Equal(t, isAggr, slices.Contains(mysqlAggrFuncCallKeywords, name), "%T", expr)
			assert.Equal(t, isAggr, isAggrFuncCallKeyword(id))

			for _, p := range []*Parser{parser, ignoreSpaceParser} {
				// quoted, the name is an identifier under either reading: the
				// call is generic, and prints quoted
				stmt, _, spaced, err := p.ParseWithSpacedAggrCalls(quoted)
				require.NoError(t, err, quoted)
				_, isGeneric := stmt.(*Select).SelectExprs.Exprs[0].(*AliasedExpr).Expr.(*FuncExpr)
				assert.True(t, isGeneric, "%s must be a generic call", quoted)
				assert.Equal(t, quoted, String(stmt), quoted)
				assert.Empty(t, spaced)
			}

			// with IGNORE_SPACE the spaced form is the keyword form, not reported
			stmt, _, spaced, err := ignoreSpaceParser.ParseWithSpacedAggrCalls(spacedForm)
			require.NoError(t, err, spacedForm)
			assert.Equal(t, String(keywordStmt), String(stmt), spacedForm)
			assert.Empty(t, spaced)

			if isAggr {
				// without IGNORE_SPACE, a spaced aggregate is the keyword form
				// too in this release, reported
				stmt, _, spaced, err = parser.ParseWithSpacedAggrCalls(spacedForm)
				require.NoError(t, err, spacedForm)
				assert.Equal(t, String(keywordStmt), String(stmt), spacedForm)
				assert.Equal(t, []string{name}, spaced, spacedForm)
			} else {
				// without IGNORE_SPACE, a spaced non-aggregate is an identifier
				// and the call takes the generic argument syntax whatever the
				// built-in's is; it prints quoted
				stmt, _, spaced, err = parser.ParseWithSpacedAggrCalls(spacedGeneric)
				require.NoError(t, err, spacedGeneric)
				_, isGeneric := stmt.(*Select).SelectExprs.Exprs[0].(*AliasedExpr).Expr.(*FuncExpr)
				assert.True(t, isGeneric, "%s must be a generic call", spacedGeneric)
				assert.Equal(t, quoted, String(stmt), spacedGeneric)
				assert.Empty(t, spaced)
			}

			// bare, the name is a column under either reading
			for _, p := range []*Parser{parser, ignoreSpaceParser} {
				stmt, _, spaced, err = p.ParseWithSpacedAggrCalls(fmt.Sprintf("select %s from t", name))
				require.NoError(t, err)
				_, isColumn := stmt.(*Select).SelectExprs.Exprs[0].(*AliasedExpr).Expr.(*ColName)
				assert.True(t, isColumn, "bare name must be a column")
				assert.Empty(t, spaced)
			}
		})
	}
	for _, kw := range keywords {
		if kw.id != UNUSED && isFuncCallKeyword(kw.id) {
			assert.Contains(t, mysqlFuncCallKeywords, kw.name)
		}
	}
	for _, name := range mysqlAggrFuncCallKeywords {
		assert.Contains(t, mysqlFuncCallKeywords, name)
	}
}
