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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sysvars"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestSplitComments(t *testing.T) {
	testCases := []struct {
		input, outSQL, outLeadingComments, outTrailingComments string
	}{{
		input:               "/",
		outSQL:              "/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "*/",
		outSQL:              "*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "/*/",
		outSQL:              "/*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "a*/",
		outSQL:              "a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "*a*/",
		outSQL:              "*a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "**a*/",
		outSQL:              "**a*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "/*b**a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*b**a*/",
	}, {
		input:               "/*a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*a*/",
	}, {
		input:               "/**/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/**/",
	}, {
		input:               "/*b*/ /*a*/",
		outSQL:              "",
		outLeadingComments:  "",
		outTrailingComments: "/*b*/ /*a*/",
	}, {
		input:               "/* before */ foo /* bar */",
		outSQL:              "foo",
		outLeadingComments:  "/* before */ ",
		outTrailingComments: " /* bar */",
	}, {
		input:               "/* before1 */ /* before2 */ foo /* after1 */ /* after2 */",
		outSQL:              "foo",
		outLeadingComments:  "/* before1 */ /* before2 */ ",
		outTrailingComments: " /* after1 */ /* after2 */",
	}, {
		input:               "/** before */ foo /** bar */",
		outSQL:              "foo",
		outLeadingComments:  "/** before */ ",
		outTrailingComments: " /** bar */",
	}, {
		input:               "/*** before */ foo /*** bar */",
		outSQL:              "foo",
		outLeadingComments:  "/*** before */ ",
		outTrailingComments: " /*** bar */",
	}, {
		input:               "/** before **/ foo /** bar **/",
		outSQL:              "foo",
		outLeadingComments:  "/** before **/ ",
		outTrailingComments: " /** bar **/",
	}, {
		input:               "/*** before ***/ foo /*** bar ***/",
		outSQL:              "foo",
		outLeadingComments:  "/*** before ***/ ",
		outTrailingComments: " /*** bar ***/",
	}, {
		input:               " /*** before ***/ foo /*** bar ***/ ",
		outSQL:              "foo",
		outLeadingComments:  "/*** before ***/ ",
		outTrailingComments: " /*** bar ***/",
	}, {
		input:               "*** bar ***/",
		outSQL:              "*** bar ***/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               " foo ",
		outSQL:              "foo",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "select 1 from t where col = '*//*'",
		outSQL:              "select 1 from t where col = '*//*'",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		input:               "/*! select 1 */",
		outSQL:              "/*! select 1 */",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// The text of a '--' line comment can end with '*/'. This must not make
		// the SQL before the line comment into a trailing comment. All of the
		// text from the client must stay in the query. The parser then reads
		// the text, and the callers authorize it.
		input:               "select id from t where id=1 /*x*/ union select authentication_string from mysql.user -- */",
		outSQL:              "select id from t where id=1 /*x*/ union select authentication_string from mysql.user -- */",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// The same rule applies to a '#' line comment.
		input:               "select id from t where id=1 /*x*/ union select 1 # */",
		outSQL:              "select id from t where id=1 /*x*/ union select 1 # */",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// The same rule applies to additional rows in an INSERT statement.
		input:               "insert into t(a) values (1) /*x*/ , (2) -- */",
		outSQL:              "insert into t(a) values (1) /*x*/ , (2) -- */",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// A '--' with no space after it does not start a comment. Therefore the
		// '*/' at the end closes no comment.
		input:               "select 1 /*a*/ union select 2 --*/",
		outSQL:              "select 1 /*a*/ union select 2 --*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// A '/*' in a string literal does not start a comment.
		input:               "select 'a /*' , 1 -- */",
		outSQL:              "select 'a /*' , 1 -- */",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// A '/*' in a quoted identifier also does not start a comment.
		input:               "select `a /*` , 1 -- */",
		outSQL:              "select `a /*` , 1 -- */",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// A line comment can end with a newline. A block comment after that
		// newline is a trailing comment. The newline stays with the trailing
		// comment, because the newline ends the line comment when we put the
		// query and the comment together again.
		input:               "select 1 -- x\n/*b*/",
		outSQL:              "select 1 -- x",
		outLeadingComments:  "",
		outTrailingComments: "\n/*b*/",
	}, {
		// A '--' with a block comment directly after it is two minus signs, and
		// it is not a comment. Therefore MySQL rejects all of this input. If we
		// remove the block comment, the query is "select 0--". That query makes
		// a good plan as "select 0". We must not authorize a statement that we
		// never send.
		input:               "select 0--/**/",
		outSQL:              "select 0--/**/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// The same with a ';' after the '--'. The trim removes the ';', so a
		// split would plan "select 0--" as "select 0". vtgate sends some
		// statements as written, with the comments added again, and then MySQL
		// reads "-- /* x" as a line comment and runs the union after it.
		input:               "select 0--; /* x\nunion select 1 /* */",
		outSQL:              "select 0--; /* x\nunion select 1 /* */",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// A ';' after a line comment. The trim removes the newline that ends
		// the line comment together with the ';', so the trailing comments get
		// a newline in front. Without it, the line comment would end inside the
		// block comment in the text that vtgate sends, and MySQL would run the
		// union after it.
		input:               "select 1 -- x\n; /* c\nunion select 2 /* */",
		outSQL:              "select 1 -- x",
		outLeadingComments:  "",
		outTrailingComments: "\n /* c\nunion select 2 /* */",
	}, {
		// A ';' after any other token is still trimmed, and the block comment
		// after it is still a trailing comment.
		input:               "select 1; /*rule-tag*/",
		outSQL:              "select 1",
		outLeadingComments:  "",
		outTrailingComments: " /*rule-tag*/",
	}, {
		// Three backticks after '@' start a name with a backtick in it, not an
		// empty name, so the block comment after the name is a trailing comment.
		input:               "select @```a` /*rule-tag*/",
		outSQL:              "select @```a`",
		outLeadingComments:  "",
		outTrailingComments: " /*rule-tag*/",
	}, {
		// A /*!...*/ comment can contain SQL that MySQL executes. Therefore it
		// stays in the query, also when a true trailing comment comes after it.
		input:               "select 1 /*!80000 union select 2 */ /*b*/",
		outSQL:              "select 1 /*!80000 union select 2 */",
		outLeadingComments:  "",
		outTrailingComments: " /*b*/",
	}, {
		// The same shape with no space between the two comments. The '*/' that
		// closes the versioned comment has to be read as part of the query: if
		// only its '*' is, the '/' that follows joins the next comment's '/*'
		// and reads as a '//' line comment, which swallows the trailing comment
		// and hides it from a query rule.
		input:               "select 1 /*!80000 union select 2 *//*b*/",
		outSQL:              "select 1 /*!80000 union select 2 */",
		outLeadingComments:  "",
		outTrailingComments: "/*b*/",
	}, {
		// A '*' inside a versioned comment that is not its closer is ordinary
		// SQL, and a '*/' outside one still ends a real comment. This pins that
		// the rule above stays narrow: this input is "2 * <comment> 3".
		input:               "select 2*/*c*/3 /*tag*/",
		outSQL:              "select 2*/*c*/3",
		outLeadingComments:  "",
		outTrailingComments: " /*tag*/",
	}, {
		// A versioned comment stays in the query whether or not its version
		// applies: the parser skips this one, but a backend can be newer than the
		// version the parser reads comments at.
		input:               "select 1 /*!99999 union select 2 */ /*tag*/",
		outSQL:              "select 1 /*!99999 union select 2 */",
		outLeadingComments:  "",
		outTrailingComments: " /*tag*/",
	}, {
		// A '*/' inside a string literal in a /*!...*/ comment does not end that
		// comment, because MySQL reads the text inside as SQL. MySQL reads this
		// input as "select '*/', 1". Therefore the block comment at the end is a
		// trailing comment, and a query rule for it must still see it.
		input:               "select /*! '*/', */ 1 /*rule-tag*/",
		outSQL:              "select /*! '*/', */ 1",
		outLeadingComments:  "",
		outTrailingComments: " /*rule-tag*/",
	}, {
		// The same shape, but with a version the parser does not apply. The
		// tokenizer skips such a comment to its first '*/' and gives quotes in it
		// no meaning, so the comment ends inside the literal, and the block at the
		// end is a trailing comment.
		input:               "select 1 /*!99999 '*/ + 2 /*rule-tag*/",
		outSQL:              "select 1 /*!99999 '*/ + 2",
		outLeadingComments:  "",
		outTrailingComments: " /*rule-tag*/",
	}, {
		// The same, with SQL after the '*/' that ends the skipped comment. The SQL
		// stays in the query, and only the block comment at the end is trailing.
		input:               "select 1 /*!99999 '*/ union select 2 /*rule-tag*/",
		outSQL:              "select 1 /*!99999 '*/ union select 2",
		outLeadingComments:  "",
		outTrailingComments: " /*rule-tag*/",
	}, {
		// A skipped comment ends at its first '*/', inside the literal here, which
		// leaves "b' */ /*rule-tag*/" with a literal that never closes. The
		// tokenizer rejects the input, so nothing is split off it.
		input:               "select 1 /*!99999 'a*/b' */ /*rule-tag*/",
		outSQL:              "select 1 /*!99999 'a*/b' */ /*rule-tag*/",
		outLeadingComments:  "",
		outTrailingComments: "",
	}, {
		// The text of a line comment can end with '--'. Those two characters are
		// already in the comment, so they cannot open a second comment. The block
		// comment after the newline is still a trailing comment. A query rule for
		// that comment reads MarginComments.Trailing, so the comment must go
		// there.
		input:               "select 1 -- body--\n/*rule-tag*/",
		outSQL:              "select 1 -- body--",
		outLeadingComments:  "",
		outTrailingComments: "\n/*rule-tag*/",
	}, {
		// A form feed is a space for MySQL, so it is a space here too. It does not
		// end the group of trailing comments, and the trim removes it. If it did
		// end the group, the block comment would stay out of the trailing
		// comments and a query rule for it would stop working.
		input:               "select 1 /*rule-tag*/\f",
		outSQL:              "select 1",
		outLeadingComments:  "",
		outTrailingComments: " /*rule-tag*/",
	}, {
		// A vertical tab is also a space for MySQL.
		input:               "select 1 /*rule-tag*/\v",
		outSQL:              "select 1",
		outLeadingComments:  "",
		outTrailingComments: " /*rule-tag*/",
	}, {
		// A space character does not end a group of trailing comments.
		input:               "select 1 /*a*/\f/*b*/",
		outSQL:              "select 1",
		outLeadingComments:  "",
		outTrailingComments: " /*a*/\f/*b*/",
	}, {
		// A no-break space is not a space for MySQL. MySQL reads it as part of an
		// unquoted identifier and then reports an unknown column. Therefore the
		// character stays in the query, and the parser rejects the query in the
		// same way that MySQL does. Vitess must not remove the character and then
		// run a statement that MySQL refuses.
		input:               "select 1 /*rule-tag*/\u00a0",
		outSQL:              "select 1 /*rule-tag*/\u00a0",
		outLeadingComments:  "",
		outTrailingComments: "",
	}}
	for _, testCase := range testCases {
		t.Run(testCase.input, func(t *testing.T) {
			gotSQL, gotComments := SplitMarginComments(testCase.input)
			gotLeadingComments, gotTrailingComments := gotComments.Leading, gotComments.Trailing

			assert.Equal(t, testCase.outSQL, gotSQL, "SQL mismatch")
			assert.Equal(t, testCase.outLeadingComments, gotLeadingComments, "LeadingComments mismatch")
			assert.Equal(t, testCase.outTrailingComments, gotTrailingComments, "TrailingCommints mismatch")
		})
	}
}

// TestMarginCommentRulesMatchTokenizer checks the space rules in this file
// against the tokenizer for every byte value.
//
// SplitMarginComments trims spaces off the query, and then the parser reads only
// the part that stays in the query. The two must agree about which bytes are
// spaces. If they do not agree, the split can put SQL where nothing parses it.
//
// The tokenizer cannot call IsSQLSpace. It reads each byte as a uint16 value, so
// that eofChar, which is 0x100, stays different from all 256 byte values. A byte
// with the value 0 is legal in a query, so the tokenizer needs a value outside
// the range of a byte to mark the end of the text. This test fails if somebody
// changes one of the two definitions and not the other.
func TestMarginCommentRulesMatchTokenizer(t *testing.T) {
	parser := NewTestParser()

	for b := range 256 {
		c := byte(b)

		// skipBlank must step over the byte only if IsSQLSpace accepts it.
		blankTkn := parser.NewStringTokenizer(string(c) + "x")
		blankTkn.skipBlank()
		assert.Equal(t, IsSQLSpace(c), blankTkn.Pos == 1,
			"skipBlank and IsSQLSpace disagree about byte 0x%02x", b)

		// A '--' opens a comment only if a space character comes after it.
		lineTkn := parser.NewStringTokenizer("--" + string(c))
		lineTkn.AllowComments = true
		typ, _ := lineTkn.Scan()
		assert.Equal(t, IsSQLSpace(c), typ == COMMENT,
			"the '--' rule and IsSQLSpace disagree about byte 0x%02x", b)
	}
}

// TestSQLSpaceCharsMatchMySQL checks that the parser accepts the characters MySQL
// treats as a space between two tokens, and no others.
//
// MySQL accepts six characters as a space. It does not accept a no-break space or
// the other characters that unicode.IsSpace accepts. A no-break space can be part
// of an unquoted identifier in MySQL, so MySQL reads it as a name and reports an
// unknown column.
//
// The accepted set comes from IsSQLSpace rather than from a list written here, so
// this test cannot hold a copy of the set that drifts from the one the code uses.
//
// Both directions matter. If the parser rejects a character that MySQL accepts,
// Vitess refuses a query that works. If the parser accepts a character that
// MySQL rejects, Vitess runs a query that MySQL refuses.
func TestSQLSpaceCharsMatchMySQL(t *testing.T) {
	parser := NewTestParser()

	spaces := 0
	for b := range 256 {
		c := byte(b)
		if !IsSQLSpace(c) {
			continue
		}
		spaces++
		sql := "select" + string(c) + "1 from" + string(c) + "t"
		_, err := parser.Parse(sql)
		// assert, not require: the point is to report every character the parser
		// disagrees about, not to stop at the first one.
		//nolint:testifylint // require-error would end the loop early
		assert.NoError(t, err, "the parser must accept %q as a space", c)
	}
	// MySQL accepts exactly six characters as a space. A seventh would mean the
	// set grew past what MySQL accepts.
	assert.Equal(t, 6, spaces, "IsSQLSpace must accept exactly six characters")

	for _, c := range []string{"\u0085", "\u00a0", "\u1680", "\u2000", "\u2028", "\u3000"} {
		_, err := parser.Parse("select" + c + "1 from t")
		assert.Error(t, err, "the parser must not accept %q as a space", c)
	}
}

func TestStripLeadingComments(t *testing.T) {
	testCases := []struct {
		input, outSQL string
	}{{
		input:  "/",
		outSQL: "/",
	}, {
		input:  "*/",
		outSQL: "*/",
	}, {
		input:  "/*/",
		outSQL: "/*/",
	}, {
		input:  "/*a",
		outSQL: "/*a",
	}, {
		input:  "/*a*",
		outSQL: "/*a*",
	}, {
		input:  "/*a**",
		outSQL: "/*a**",
	}, {
		input:  "/*b**a*/",
		outSQL: "",
	}, {
		input:  "/*a*/",
		outSQL: "",
	}, {
		input:  "/**/",
		outSQL: "",
	}, {
		input:  "/*!*/",
		outSQL: "/*!*/",
	}, {
		input:  "/*!a*/",
		outSQL: "/*!a*/",
	}, {
		input:  "/*b*/ /*a*/",
		outSQL: "",
	}, {
		input: `/*b*/ --foo
bar`,
		outSQL: "bar",
	}, {
		input:  "foo /* bar */",
		outSQL: "foo /* bar */",
	}, {
		input:  "/* foo */ bar",
		outSQL: "bar",
	}, {
		input:  "-- /* foo */ bar",
		outSQL: "",
	}, {
		input:  "foo -- bar */",
		outSQL: "foo -- bar */",
	}, {
		input: `/*
foo */ bar`,
		outSQL: "bar",
	}, {
		input: `-- foo bar
a`,
		outSQL: "a",
	}, {
		input:  `-- foo bar`,
		outSQL: "",
	}, {
		// The trim uses the SQL space set, so all six of those characters go.
		input:  "\v\f select 1 \r\n",
		outSQL: "select 1",
	}, {
		// A no-break space is not one of them. MySQL reads it as part of an
		// unquoted identifier, so removing it here would report on a statement
		// that is not the one the client sent.
		input:  " select 1",
		outSQL: " select 1",
	}}
	for _, testCase := range testCases {
		gotSQL := StripLeadingComments(testCase.input)
		assert.Equal(t, testCase.outSQL, gotSQL)
	}
}

func TestRewriteDoubleSlashComments(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{{
		input:  "select 1 from t",
		output: "select 1 from t",
	}, {
		input:  "select 1 from t // x",
		output: "select 1 from t #/ x",
	}, {
		input:  "select 1 from t //x\n",
		output: "select 1 from t #/x\n",
	}, {
		input:  "select 1 from t //",
		output: "select 1 from t #/",
	}, {
		input:  "// x\nselect 1 from t",
		output: "#/ x\nselect 1 from t",
	}, {
		// MySQL reads this as 10 / 2; Vitess reads it as 10.
		input:  "select 10 //* x */ 2",
		output: "select 10 #/* x */ 2",
	}, {
		// The ; is inside the comment, so it does not end the statement.
		input:  "select 1 // ; drop table t\n, 2",
		output: "select 1 #/ ; drop table t\n, 2",
	}, {
		input:  "select 1 // a\r\n, 2 /// b\n, 3",
		output: "select 1 #/ a\r\n, 2 #// b\n, 3",
	}, {
		input:  "select 'http://x', `a//b`, \"//\" from t /* // */ -- //\n",
		output: "select 'http://x', `a//b`, \"//\" from t /* // */ -- //\n",
	}, {
		// Inside a versioned comment that Vitess executes, / is division.
		input:  "select /*!50000 4 //*x*/ 2 */",
		output: "select /*!50000 4 //*x*/ 2 */",
	}, {
		// A versioned comment that Vitess skips is a comment as a whole.
		input:  "select 1 /*!99999 // x */",
		output: "select 1 /*!99999 // x */",
	}, {
		// An unterminated string runs to the end of the text.
		input:  "select 1 // a\n, 'b // c",
		output: "select 1 #/ a\n, 'b // c",
	}, {
		input:  "select 'a // b",
		output: "select 'a // b",
	}, {
		input:  "select @'ab' // x\n",
		output: "select @'ab' #/ x\n",
	}, {
		// Vitess cannot read this quoted user variable name whole, so it is
		// a lexing error. The tokenizer then reads the quoted name as a
		// string, as MySQL does, and the comment after it is rewritten.
		input:  "do @'a//b' := 1 // x\n",
		output: "do @'a//b' := 1 #/ x\n",
	}, {
		input:  "select 1; select @'a b', 1 // ; select 2\n, 3",
		output: "select 1; select @'a b', 1 #/ ; select 2\n, 3",
	}}
	parser := NewTestParser()
	for _, tcase := range testCases {
		t.Run(tcase.input, func(t *testing.T) {
			out, rewritten := parser.RewriteDoubleSlashComments(tcase.input)
			assert.Equal(t, tcase.output, out)
			assert.Equal(t, tcase.output != tcase.input, rewritten)
		})
	}
}

// TestRewriteDoubleSlashCommentsKeepsStatement checks that the rewrite does
// not change what Vitess parses.
func TestRewriteDoubleSlashCommentsKeepsStatement(t *testing.T) {
	parser := NewTestParser()
	inputs := []string{
		"select 1e//+ 2\n from t",
		"select 1 from t // x",
		"select 10 //* x */ 2 from t // y\n, 3",
		"select 1 // ; drop table t\n, 2",
		"// x\nselect 1 from t",
		"select 1 from t // a\r\nwhere a = 1 /// b\n",
	}
	for _, tcase := range validSQL {
		inputs = append(inputs, tcase.input)
	}
	for _, input := range inputs {
		want, err := parser.Parse(input)
		if err != nil {
			continue
		}
		rewritten, _ := parser.RewriteDoubleSlashComments(input)
		got, err := parser.Parse(rewritten)
		require.NoError(t, err, rewritten)
		assert.Equal(t, String(want), String(got), input)
	}
}

// TestRewriteDoubleSlashCommentsLeavesNoComment checks that, after the rewrite,
// the tokenizer reads no "//" comment and splits the text into the same
// statements, also in text that does not lex or parse.
func TestRewriteDoubleSlashCommentsLeavesNoComment(t *testing.T) {
	parser := NewTestParser()
	inputs := []string{
		"SELECT @'a b', 10 //*x*/ 2",
		"select 1; select @'a b', 1 // ; select 2\n, 3",
		"select @'a//b' // x\n; select 1 // y",
		"select :1x // a\n; select 2 // b",
	}
	for _, tcase := range validSQL {
		inputs = append(inputs, tcase.input)
	}
	for _, tcase := range invalidSQL {
		inputs = append(inputs, tcase.input)
	}
	for _, input := range inputs {
		rewritten, _ := parser.RewriteDoubleSlashComments(input)
		tokenizer := parser.NewStringTokenizer(rewritten)
		for {
			typ, val := tokenizer.Scan()
			if typ == 0 {
				break
			}
			if typ == COMMENT {
				assert.False(t, strings.HasPrefix(val, "//"), "%q left %q", input, val)
			}
		}
		want, wantErr := parser.SplitStatementToPieces(input)
		got, gotErr := parser.SplitStatementToPieces(rewritten)
		assert.Equal(t, wantErr, gotErr, input)
		assert.Len(t, got, len(want), input)
	}
}

func TestExtractCommentDirectives(t *testing.T) {
	testCases := []struct {
		input string
		vals  map[string]string
	}{{
		input: "",
		vals:  nil,
	}, {
		input: "/* not a vt comment */",
		vals:  map[string]string{},
	}, {
		input: "/*vt+ */",
		vals:  map[string]string{},
	}, {
		input: "/*vt+ SINGLE_OPTION */",
		vals: map[string]string{
			"single_option": "true",
		},
	}, {
		input: "/*vt+ ONE_OPT TWO_OPT */",
		vals: map[string]string{
			"one_opt": "true",
			"two_opt": "true",
		},
	}, {
		input: "/*vt+ ONE_OPT */ /* other comment */ /*vt+ TWO_OPT */",
		vals: map[string]string{
			"one_opt": "true",
			"two_opt": "true",
		},
	}, {
		input: "/*vt+ ONE_OPT=abc TWO_OPT=def */",
		vals: map[string]string{
			"one_opt": "abc",
			"two_opt": "def",
		},
	}, {
		input: "/*vt+ ONE_OPT=true TWO_OPT=false */",
		vals: map[string]string{
			"one_opt": "true",
			"two_opt": "false",
		},
	}, {
		input: "/*vt+ ONE_OPT=true TWO_OPT=\"false\" */",
		vals: map[string]string{
			"one_opt": "true",
			"two_opt": "\"false\"",
		},
	}, {
		input: "/*vt+ RANGE_OPT=[a:b] ANOTHER ANOTHER_WITH_VALEQ=val= AND_ONE_WITH_EQ== */",
		vals: map[string]string{
			"range_opt":          "[a:b]",
			"another":            "true",
			"another_with_valeq": "val=",
			"and_one_with_eq":    "=",
		},
	}}

	parser := NewTestParser()
	for _, testCase := range testCases {
		t.Run(testCase.input, func(t *testing.T) {
			sqls := []string{
				"select " + testCase.input + " 1 from dual",
				"update " + testCase.input + " t set i=i+1",
				"delete " + testCase.input + " from t where id>1",
				"drop " + testCase.input + " table t",
				"create " + testCase.input + " table if not exists t (id int primary key)",
				"alter " + testCase.input + " table t add column c int not null",
				"create " + testCase.input + " view v as select * from t",
				"create " + testCase.input + " or replace view v as select * from t",
				"alter " + testCase.input + " view v as select * from t",
				"drop " + testCase.input + " view v",
			}
			for _, sql := range sqls {
				t.Run(sql, func(t *testing.T) {
					var comments *ParsedComments
					stmt, _ := parser.Parse(sql)
					switch s := stmt.(type) {
					case *Select:
						comments = s.Comments
					case *Update:
						comments = s.Comments
					case *Delete:
						comments = s.Comments
					case *DropTable:
						comments = s.Comments
					case *AlterTable:
						comments = s.Comments
					case *CreateTable:
						comments = s.Comments
					case *CreateView:
						comments = s.Comments
					case *AlterView:
						comments = s.Comments
					case *DropView:
						comments = s.Comments
					default:
						assert.Failf(t, "unexpected statement type", "Unexpected statement type %+v", s)
					}

					vals := comments.Directives()
					if vals == nil {
						require.Nil(t, vals)
						return
					}

					assert.Equal(t, testCase.vals, vals.m)
				})
			}
		})
	}

	d := &CommentDirectives{m: map[string]string{
		"one_opt": "true",
		"two_opt": "false",
		"three":   "1",
		"four":    "2",
		"five":    "0",
		"six":     "true",
	}}

	assert.True(t, d.IsSet("ONE_OPT"), "d.IsSet(ONE_OPT)")
	assert.False(t, d.IsSet("TWO_OPT"), "d.IsSet(TWO_OPT)")
	assert.True(t, d.IsSet("three"), "d.IsSet(three)")
	assert.False(t, d.IsSet("four"), "d.IsSet(four)")
	assert.False(t, d.IsSet("five"), "d.IsSet(five)")
	assert.True(t, d.IsSet("six"), "d.IsSet(six)")
}

func TestSkipQueryPlanCacheDirective(t *testing.T) {
	parser := NewTestParser()
	stmt, _ := parser.Parse("insert /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ into user(id) values (1), (2)")
	assert.False(t, CachePlan(stmt))

	stmt, _ = parser.Parse("insert into user(id) values (1), (2)")
	assert.True(t, CachePlan(stmt))

	stmt, _ = parser.Parse("update /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ users set name=1")
	assert.False(t, CachePlan(stmt))

	stmt, _ = parser.Parse("select /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ * from users")
	assert.False(t, CachePlan(stmt))

	stmt, _ = parser.Parse("delete /*vt+ SKIP_QUERY_PLAN_CACHE=1 */ from users")
	assert.False(t, CachePlan(stmt))
}

func TestIgnoreMaxPayloadSizeDirective(t *testing.T) {
	testCases := []struct {
		query    string
		expected bool
	}{
		{"insert /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ into user(id) values (1), (2)", true},
		{"insert into user(id) values (1), (2)", false},
		{"update /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ users set name=1", true},
		{"update users set name=1", false},
		{"select /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ * from users", true},
		{"select * from users", false},
		{"delete /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ from users", true},
		{"delete from users", false},
		{"show /*vt+ IGNORE_MAX_PAYLOAD_SIZE=1 */ create table users", false},
		{"show create table users", false},
	}

	parser := NewTestParser()
	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := parser.Parse(test.query)
			got := IgnoreMaxPayloadSizeDirective(stmt)
			assert.Equalf(t, test.expected, got, "IgnoreMaxPayloadSizeDirective(stmt) returned %v but expected %v", got, test.expected)
		})
	}
}

func TestIgnoreMaxMaxMemoryRowsDirective(t *testing.T) {
	testCases := []struct {
		query    string
		expected bool
	}{
		{"insert /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ into user(id) values (1), (2)", true},
		{"insert into user(id) values (1), (2)", false},
		{"update /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ users set name=1", true},
		{"update users set name=1", false},
		{"select /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ * from users", true},
		{"select * from users", false},
		{"delete /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ from users", true},
		{"delete from users", false},
		{"show /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ create table users", false},
		{"show create table users", false},
	}

	parser := NewTestParser()
	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := parser.Parse(test.query)
			got := IgnoreMaxMaxMemoryRowsDirective(stmt)
			assert.Equalf(t, test.expected, got, "IgnoreMaxPayloadSizeDirective(stmt) returned %v but expected %v", got, test.expected)
		})
	}
}

func TestConsolidator(t *testing.T) {
	testCases := []struct {
		query    string
		expected querypb.ExecuteOptions_Consolidator
	}{
		{"insert /*vt+ CONSOLIDATOR=enabled */ into user(id) values (1), (2)", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"update /*vt+ CONSOLIDATOR=enabled */ users set name=1", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"delete /*vt+ CONSOLIDATOR=enabled */ from users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"show /*vt+ CONSOLIDATOR=enabled */ create table users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"select * from users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"select /*vt+ CONSOLIDATOR=invalid_value */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"select /*vt+ IGNORE_MAX_MEMORY_ROWS=1 */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED},
		{"select /*vt+ CONSOLIDATOR=disabled */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
		{"select /*vt+ CONSOLIDATOR=enabled */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_ENABLED},
		{"select /*vt+ CONSOLIDATOR=enabled_replicas */ * from users", querypb.ExecuteOptions_CONSOLIDATOR_ENABLED_REPLICAS},
	}

	parser := NewTestParser()
	for _, test := range testCases {
		t.Run(test.query, func(t *testing.T) {
			stmt, _ := parser.Parse(test.query)
			qh, err := BuildQueryHints(stmt)
			require.NoError(t, err)
			assert.Equalf(t, test.expected, qh.Consolidator,
				"Consolidator(stmt) returned %v but expected %v", qh.Consolidator, test.expected)
		})
	}
}

func TestGetPriorityFromStatement(t *testing.T) {
	testCases := []struct {
		query            string
		expectedPriority string
		expectedError    error
	}{
		{
			query:            "select * from a_table",
			expectedPriority: "",
			expectedError:    nil,
		},
		{
			query:            "select /*vt+ ANOTHER_DIRECTIVE=324 */ * from another_table",
			expectedPriority: "",
			expectedError:    nil,
		},
		{
			query:            "select /*vt+ PRIORITY=33 */ * from another_table",
			expectedPriority: "33",
			expectedError:    nil,
		},
		{
			query:            "select /*vt+ PRIORITY=200 */ * from another_table",
			expectedPriority: "",
			expectedError:    ErrInvalidPriority,
		},
		{
			query:            "select /*vt+ PRIORITY=-1 */ * from another_table",
			expectedPriority: "",
			expectedError:    ErrInvalidPriority,
		},
		{
			query:            "select /*vt+ PRIORITY=some_text */ * from another_table",
			expectedPriority: "",
			expectedError:    ErrInvalidPriority,
		},
		{
			query:            "select /*vt+ PRIORITY=0 */ * from another_table",
			expectedPriority: "0",
			expectedError:    nil,
		},
		{
			query:            "select /*vt+ PRIORITY=100 */ * from another_table",
			expectedPriority: "100",
			expectedError:    nil,
		},
	}

	parser := NewTestParser()
	for _, testCase := range testCases {
		t.Run(testCase.query, func(t *testing.T) {
			t.Parallel()
			stmt, err := parser.Parse(testCase.query)
			require.NoError(t, err)
			qh, err := BuildQueryHints(stmt)
			if testCase.expectedError != nil {
				assert.ErrorIs(t, err, testCase.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, testCase.expectedPriority, qh.Priority)
			}
		})
	}
}

// TestGetMySQLSetVarValue tests the functionality of GetMySQLSetVarValue
func TestGetMySQLSetVarValue(t *testing.T) {
	tests := []struct {
		name      string
		comments  []string
		valToFind string
		want      string
	}{
		{
			name:      "SET_VAR clause in the middle",
			comments:  []string{"/*+ NO_RANGE_OPTIMIZATION(t3 PRIMARY, f2_idx) SET_VAR(foreign_key_checks=OFF) NO_ICP(t1, t2) */"},
			valToFind: sysvars.ForeignKeyChecks,
			want:      "OFF",
		},
		{
			name:      "Single SET_VAR clause",
			comments:  []string{"/*+ SET_VAR(sort_buffer_size = 16M) */"},
			valToFind: "sort_buffer_size",
			want:      "16M",
		},
		{
			name:      "No comments",
			comments:  nil,
			valToFind: "sort_buffer_size",
			want:      "",
		},
		{
			name:      "Multiple SET_VAR clauses",
			comments:  []string{"/*+ SET_VAR(sort_buffer_size = 16M) */", "/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') */", "/*+ SET_VAR( foReiGn_key_checks = On) */"},
			valToFind: sysvars.ForeignKeyChecks,
			want:      "",
		},
		{
			name:      "Verify casing",
			comments:  []string{"/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') SET_VAR( foReiGn_key_checks = On) */"},
			valToFind: sysvars.ForeignKeyChecks,
			want:      "On",
		},
		{
			name:      "Leading comment is a normal comment",
			comments:  []string{"/* This is a normal comment */", "/*+ MAX_EXECUTION_TIME(1000) SET_VAR( foreign_key_checks = 1) */"},
			valToFind: sysvars.ForeignKeyChecks,
			want:      "1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ParsedComments{
				comments: tt.comments,
			}
			assert.Equal(t, tt.want, c.GetMySQLSetVarValue(tt.valToFind))
		})
	}
}

func TestGetMySQLSetVarNames(t *testing.T) {
	tests := []struct {
		name     string
		comments []string
		want     []string
	}{{
		name:     "SET_VAR clause in the middle",
		comments: []string{"/*+ NO_RANGE_OPTIMIZATION(t3 PRIMARY, f2_idx) SET_VAR(foreign_key_checks=OFF) NO_ICP(t1, t2) */"},
		want:     []string{"foreign_key_checks"},
	}, {
		name:     "Single SET_VAR clause",
		comments: []string{"/*+ SET_VAR(sort_buffer_size = 16M) */"},
		want:     []string{"sort_buffer_size"},
	}, {
		name:     "No comments",
		comments: nil,
		want:     nil,
	}, {
		name:     "Multiple SET_VAR clauses in first optimizer hint comment",
		comments: []string{"/*+ SET_VAR(sort_buffer_size = 16M) SET_VAR( foReiGn_key_checks = On) */"},
		want:     []string{"sort_buffer_size", "foReiGn_key_checks"},
	}, {
		name:     "Only first optimizer hint comment is parsed",
		comments: []string{"/*+ SET_VAR(sort_buffer_size = 16M) */", "/*+ SET_VAR(foreign_key_checks = On) */"},
		want:     []string{"sort_buffer_size"},
	}, {
		name:     "Leading comment is a normal comment",
		comments: []string{"/* This is a normal comment */", "/*+ MAX_EXECUTION_TIME(1000) SET_VAR( foreign_key_checks = 1) */"},
		want:     []string{"foreign_key_checks"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ParsedComments{
				comments: tt.comments,
			}
			assert.Equal(t, tt.want, c.GetMySQLSetVarNames())
		})
	}
}

func TestSetMySQLSetVarValue(t *testing.T) {
	tests := []struct {
		name           string
		comments       []string
		key            string
		value          string
		commentsWanted Comments
	}{
		{
			name:           "SET_VAR clause in the middle",
			comments:       []string{"/*+ NO_RANGE_OPTIMIZATION(t3 PRIMARY, f2_idx) SET_VAR(foreign_key_checks=OFF) NO_ICP(t1, t2) */"},
			key:            sysvars.ForeignKeyChecks,
			value:          "On",
			commentsWanted: []string{"/*+ NO_RANGE_OPTIMIZATION(t3 PRIMARY, f2_idx) SET_VAR(foreign_key_checks=On) NO_ICP(t1, t2) */"},
		},
		{
			name:           "Single SET_VAR clause",
			comments:       []string{"/*+ SET_VAR(sort_buffer_size = 16M) */"},
			key:            "sort_buffer_size",
			value:          "1Mb",
			commentsWanted: []string{"/*+ SET_VAR(sort_buffer_size=1Mb) */"},
		},
		{
			name:           "No comments",
			comments:       nil,
			key:            "sort_buffer_size",
			value:          "13M",
			commentsWanted: []string{"/*+ SET_VAR(sort_buffer_size=13M) */"},
		},
		{
			name:           "Multiple SET_VAR clauses",
			comments:       []string{"/*+ SET_VAR(sort_buffer_size = 16M) */", "/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') */", "/*+ SET_VAR( foReiGn_key_checks = On) */"},
			key:            sysvars.ForeignKeyChecks,
			value:          "1",
			commentsWanted: []string{"/*+ SET_VAR(sort_buffer_size = 16M) SET_VAR(foreign_key_checks=1) */", "/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') */", "/*+ SET_VAR( foReiGn_key_checks = On) */"},
		},
		{
			name:           "Verify casing",
			comments:       []string{"/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') SET_VAR( foReiGn_key_checks = On) */"},
			key:            sysvars.ForeignKeyChecks,
			value:          "off",
			commentsWanted: []string{"/*+ SET_VAR(optimizer_switch = 'mrr_cost_b(ased=of\"f') SET_VAR(foReiGn_key_checks=off) */"},
		},
		{
			name:           "Leading comment is a normal comment",
			comments:       []string{"/* This is a normal comment */", "/*+ MAX_EXECUTION_TIME(1000) SET_VAR( foreign_key_checks = 1) */"},
			key:            sysvars.ForeignKeyChecks,
			value:          "Off",
			commentsWanted: []string{"/* This is a normal comment */", "/*+ MAX_EXECUTION_TIME(1000) SET_VAR(foreign_key_checks=Off) */"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ParsedComments{
				comments: tt.comments,
			}
			newComments := c.SetMySQLSetVarValue(tt.key, tt.value)
			require.Equal(t, tt.commentsWanted, newComments)
		})
	}
}

// TestQueryTimeout tests the extraction of Query_Timeout_MS from the comments.
func TestQueryTimeout(t *testing.T) {
	testCases := []struct {
		query      string
		expTimeout int
		noTimeout  bool
	}{{
		query:     "select * from a_table",
		noTimeout: true,
	}, {
		query:      "select /*vt+ QUERY_TIMEOUT_MS=21 */ * from another_table",
		expTimeout: 21,
	}, {
		query:      "select /*vt+ QUERY_TIMEOUT_MS=0 */ * from another_table",
		expTimeout: 0,
	}, {
		query:     "select /*vt+ PRIORITY=-42 */ * from another_table",
		noTimeout: true,
	}}

	parser := NewTestParser()
	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			stmt, err := parser.Parse(tc.query)
			require.NoError(t, err)
			qh, _ := BuildQueryHints(stmt)
			if tc.noTimeout {
				assert.Nil(t, qh.Timeout)
			} else {
				assert.Equal(t, tc.expTimeout, *qh.Timeout)
			}
		})
	}
}
