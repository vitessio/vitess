/*
Copyright 2019 The Vitess Authors.

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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiteralID(t *testing.T) {
	testcases := []struct {
		in  string
		id  int
		out string
	}{{
		in:  "`aa`",
		id:  ID,
		out: "aa",
	}, {
		in:  "```a```",
		id:  ID,
		out: "`a`",
	}, {
		in:  "`a``b`",
		id:  ID,
		out: "a`b",
	}, {
		in:  "`a``b`c",
		id:  ID,
		out: "a`b",
	}, {
		in:  "`a``b",
		id:  LEX_ERROR,
		out: "a`b",
	}, {
		in:  "`a``b``",
		id:  LEX_ERROR,
		out: "a`b`",
	}, {
		in:  "``",
		id:  LEX_ERROR,
		out: "",
	}, {
		in:  "@x",
		id:  AT_ID,
		out: "x",
	}, {
		in:  "@@x",
		id:  AT_AT_ID,
		out: "x",
	}, {
		in:  "@@`x y`",
		id:  AT_AT_ID,
		out: "x y",
	}, {
		in:  "@@`@x @y`",
		id:  AT_AT_ID,
		out: "@x @y",
	}}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			tkn := parser.NewStringTokenizer(tcase.in)
			id, out := tkn.Scan()
			require.Equal(t, tcase.id, id)
			require.Equal(t, tcase.out, string(out))
		})
	}
}

func tokenName(id int) string {
	switch id {
	case STRING:
		return "STRING"
	case LEX_ERROR:
		return "LEX_ERROR"
	}
	return strconv.Itoa(id)
}

func TestString(t *testing.T) {
	testcases := []struct {
		in   string
		id   int
		want string
	}{{
		in:   "''",
		id:   STRING,
		want: "",
	}, {
		in:   "''''",
		id:   STRING,
		want: "'",
	}, {
		in:   "'hello'",
		id:   STRING,
		want: "hello",
	}, {
		in:   "'\\n'",
		id:   STRING,
		want: "\n",
	}, {
		in:   "'\\nhello\\n'",
		id:   STRING,
		want: "\nhello\n",
	}, {
		in:   "'a''b'",
		id:   STRING,
		want: "a'b",
	}, {
		in:   "'a\\'b'",
		id:   STRING,
		want: "a'b",
	}, {
		in:   "'\\'",
		id:   LEX_ERROR,
		want: "'",
	}, {
		in:   "'",
		id:   LEX_ERROR,
		want: "",
	}, {
		in:   "'hello\\'",
		id:   LEX_ERROR,
		want: "hello'",
	}, {
		in:   "'hello",
		id:   LEX_ERROR,
		want: "hello",
	}, {
		in:   "'hello\\",
		id:   LEX_ERROR,
		want: "hello",
	}}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			id, got := parser.NewStringTokenizer(tcase.in).Scan()
			require.Equal(t, tcase.id, id, "Scan(%q) = (%s), want (%s)", tcase.in, tokenName(id), tokenName(tcase.id))
			require.Equal(t, tcase.want, string(got))
		})
	}
}

func TestSplitStatement(t *testing.T) {
	testcases := []struct {
		in  string
		sql string
		rem string
	}{{
		in:  "select * from table",
		sql: "select * from table",
	}, {
		in:  "select * from table; ",
		sql: "select * from table",
		rem: " ",
	}, {
		in:  "select * from table; select * from table2;",
		sql: "select * from table",
		rem: " select * from table2;",
	}, {
		in:  "select * from /* comment */ table;",
		sql: "select * from /* comment */ table",
	}, {
		in:  "select * from /* comment ; */ table;",
		sql: "select * from /* comment ; */ table",
	}, {
		in:  "select * from table where semi = ';';",
		sql: "select * from table where semi = ';'",
	}, {
		in:  "-- select * from table",
		sql: "-- select * from table",
	}, {
		in:  " ",
		sql: " ",
	}, {
		in:  "",
		sql: "",
	}}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			sql, rem, err := parser.SplitStatement(tcase.in)
			require.NoErrorf(t, err, "EndOfStatementPosition(%s): ERROR: %v", tcase.in, err)

			assert.Equalf(t, tcase.sql, sql, "EndOfStatementPosition(%s) got sql \"%s\" want \"%s\"", tcase.in, sql, tcase.sql)

			assert.Equalf(t, tcase.rem, rem, "EndOfStatementPosition(%s) got remainder \"%s\" want \"%s\"", tcase.in, rem, tcase.rem)
		})
	}
}

func TestVersion(t *testing.T) {
	testcases := []struct {
		version string
		in      string
		id      []int
	}{{
		version: "5.7.9",
		in:      "/*!80102 SELECT*/ FROM IN EXISTS",
		id:      []int{FROM, IN, EXISTS, 0},
	}, {
		version: "8.1.1",
		in:      "/*!80102 SELECT*/ FROM IN EXISTS",
		id:      []int{FROM, IN, EXISTS, 0},
	}, {
		version: "8.2.1",
		in:      "/*!80102 SELECT*/ FROM IN EXISTS",
		id:      []int{SELECT, FROM, IN, EXISTS, 0},
	}, {
		version: "8.1.2",
		in:      "/*!80102 SELECT*/ FROM IN EXISTS",
		id:      []int{SELECT, FROM, IN, EXISTS, 0},
	}}

	for _, tcase := range testcases {
		t.Run(tcase.version+"_"+tcase.in, func(t *testing.T) {
			parser, err := New(Options{MySQLServerVersion: tcase.version})
			require.NoError(t, err)
			tok := parser.NewStringTokenizer(tcase.in)
			for _, expectedID := range tcase.id {
				id, _ := tok.Scan()
				require.Equal(t, expectedID, id)
			}
		})
	}
}

func scanAll(tkn *Tokenizer) []int {
	var ids []int
	for {
		id, _ := tkn.Scan()
		if id == 0 || id == LEX_ERROR {
			return ids
		}
		ids = append(ids, id)
	}
}

// The function-name rule reads its context from the input, not from the
// tokens Lex handed to the parser, so Tokenizer.Scan callers see the same
// stream the parser does: a name directly after '.' is an identifier under
// either reading, and a dot separated from the name by whitespace or a
// comment does not qualify it. Away from '(' a name is an identifier
// without IGNORE_SPACE and the keyword with it, except an aggregate, which
// stays the keyword under either reading in this release.
func TestFuncCallKeywordAfterDot(t *testing.T) {
	testcases := []struct {
		in             string
		ids            []int
		ignoreSpaceIDs []int // when different from ids
	}{{
		in:  "db.cast(1)",
		ids: []int{ID, '.', ID, '(', INTEGRAL, ')'},
	}, {
		in:  "`db`.count(1)",
		ids: []int{ID, '.', ID, '(', INTEGRAL, ')'},
	}, {
		in:  "db. cast(1)",
		ids: []int{ID, '.', CAST, '(', INTEGRAL, ')'},
	}, {
		in:  "db./*c*/cast(1)",
		ids: []int{ID, '.', COMMENT, CAST, '(', INTEGRAL, ')'},
	}, {
		in:  "cast(1)",
		ids: []int{CAST, '(', INTEGRAL, ')'},
	}, {
		in:             "cast (1)",
		ids:            []int{ID, '(', INTEGRAL, ')'},
		ignoreSpaceIDs: []int{CAST, '(', INTEGRAL, ')'},
	}, {
		in:  "sum (1)",
		ids: []int{SUM, '(', INTEGRAL, ')'},
	}, {
		in:  "db.cast (1)",
		ids: []int{ID, '.', ID, '(', INTEGRAL, ')'},
	}, {
		in:  "db.sum (1)",
		ids: []int{ID, '.', ID, '(', INTEGRAL, ')'},
	}, {
		// an aggregate name stays the keyword under either reading, which
		// the grammar reads as an identifier where one is expected, since it
		// is non-reserved
		in:  "cast, sum from t",
		ids: []int{ID, ',', SUM, FROM, ID},
	}, {
		in:  "now from t",
		ids: []int{ID, FROM, ID},
	}, {
		in:  "t.now from t",
		ids: []int{ID, '.', ID, FROM, ID},
	}}

	parser := NewTestParser()
	ignoreSpaceParser := newIgnoreSpaceTestParser(t)
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			assert.Equal(t, tcase.ids, scanAll(parser.NewStringTokenizer(tcase.in)), "without IGNORE_SPACE")
			ignoreSpaceIDs := tcase.ignoreSpaceIDs
			if ignoreSpaceIDs == nil {
				ignoreSpaceIDs = tcase.ids
			}
			assert.Equal(t, ignoreSpaceIDs, scanAll(ignoreSpaceParser.NewStringTokenizer(tcase.in)), "with IGNORE_SPACE")
		})
	}
}

// An aggregate on the whitespace-sensitive list is the keyword with or
// without whitespace before '(', and the parser reports the calls whose
// parenthesis whitespace, or a comment, separated from the name, unless its
// sql_mode has IGNORE_SPACE: those keep a meaning that MySQL's default
// reading changes. The report comes from the grammar's aggregate rules, so
// it sees the token stream the parser sees, comments of every kind and
// MySQL versioned comments included, and does not fire for a name in an
// identifier position.
func TestSpacedAggrCalls(t *testing.T) {
	spaced := func(names ...string) []SpacedAggrCall {
		calls := make([]SpacedAggrCall, 0, len(names))
		for _, name := range names {
			calls = append(calls, SpacedAggrCall{Name: name})
		}
		return calls
	}
	commented := func(names ...string) []SpacedAggrCall {
		calls := spaced(names...)
		for i := range calls {
			calls[i].Comment = true
		}
		return calls
	}
	testcases := []struct {
		in     string
		spaced []SpacedAggrCall
		err    string
	}{{
		in:     "select sum (x) from t",
		spaced: spaced("sum"),
	}, {
		in:     "select sum\t(x) from t",
		spaced: spaced("sum"),
	}, {
		in:     "select sum\n(x) from t",
		spaced: spaced("sum"),
	}, {
		in:     "select sum\r\n  (x) from t",
		spaced: spaced("sum"),
	}, {
		// a comment marks the call: MySQL reads it as a stored-function call
		// under IGNORE_SPACE as well, so it is reported under either reading
		in:     "select sum/*c*/(x) from t",
		spaced: commented("sum"),
	}, {
		in:     "select sum /* c */ /* d */ (x) from t",
		spaced: commented("sum"),
	}, {
		in:     "select sum -- c\n(x) from t",
		spaced: commented("sum"),
	}, {
		in:     "select sum # c\n(x) from t",
		spaced: commented("sum"),
	}, {
		// a versioned comment the server version satisfies is read as SQL,
		// so the call is an aggregate call with a detached parenthesis
		in:     "select count /*!50000 (id) */ from t",
		spaced: commented("count"),
	}, {
		in:     "select sum /*!50000 */ (x) from t",
		spaced: commented("sum"),
	}, {
		// one the server version does not satisfy is skipped whole
		in: "select /*!99999 sum */ (x) from t",
	}, {
		in:     "select SUM (x) from t",
		spaced: spaced("sum"),
	}, {
		in:     "select sum (a) + sum (b) from t",
		spaced: spaced("sum"),
	}, {
		in:     "select sum (a), count (b), sum (c) from t",
		spaced: spaced("sum", "count"),
	}, {
		// a name separated by whitespace once and by a comment once is
		// reported once, marked
		in:     "select sum (a), sum/*c*/(b) from t",
		spaced: commented("sum"),
	}, {
		// attached: not spaced
		in: "select sum(x) from t",
	}, {
		// in an identifier position the name means the same under either
		// reading, whatever follows it, so nothing is reported
		in: "select sum from t",
	}, {
		in: "select sum /* c */ from t",
	}, {
		in: "insert into count (a) values (1)",
	}, {
		in: "create table max (id int, key sum (id))",
	}, {
		in: "call count (1)",
	}, {
		// -- without a following blank is not a comment
		in: "select sum --(x) from t",
	}, {
		// an unterminated comment is an error, as anywhere
		in:  "select sum /* c (x) from t",
		err: "syntax error",
	}, {
		// a qualified name is a stored-function call under either reading
		in: "select db.sum (x) from t",
	}, {
		// a quoted name is an identifier
		in: "select `sum` (x) from t",
	}, {
		// names off the aggregate list are not reported
		in: "select now (), abs (x), if (a, b, c), left (a, 1) from t",
	}}

	parser := NewTestParser()
	ignoreSpaceParser := newIgnoreSpaceTestParser(t)
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			_, _, got, err := parser.ParseWithSpacedAggrCalls(tcase.in)
			if tcase.err != "" {
				require.ErrorContains(t, err, tcase.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tcase.spaced, got)

			// a parser whose sql_mode has IGNORE_SPACE reports the calls a
			// comment separates only: whitespace is the mode's own reading
			var want []SpacedAggrCall
			for _, call := range tcase.spaced {
				if call.Comment {
					want = append(want, call)
				}
			}
			_, _, got, err = ignoreSpaceParser.ParseWithSpacedAggrCalls(tcase.in)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestIntegerAndID(t *testing.T) {
	testcases := []struct {
		in  string
		id  int
		out string
	}{{
		in: "334",
		id: INTEGRAL,
	}, {
		in: "33.4",
		id: DECIMAL,
	}, {
		in: "0x33",
		id: HEXNUM,
	}, {
		in: "33e4",
		id: FLOAT,
	}, {
		in: "33.4e-3",
		id: FLOAT,
	}, {
		in: "33t4",
		id: ID,
	}, {
		in: "0x2et3",
		id: ID,
	}, {
		in:  "3e2t3",
		id:  LEX_ERROR,
		out: "3e2",
	}, {
		in:  "3.2t",
		id:  LEX_ERROR,
		out: "3.2",
	}}

	parser := NewTestParser()
	for _, tcase := range testcases {
		t.Run(tcase.in, func(t *testing.T) {
			tkn := parser.NewStringTokenizer(tcase.in)
			id, out := tkn.Scan()
			require.Equal(t, tcase.id, id)
			expectedOut := tcase.out
			if expectedOut == "" {
				expectedOut = tcase.in
			}
			require.Equal(t, expectedOut, out)
		})
	}
}
