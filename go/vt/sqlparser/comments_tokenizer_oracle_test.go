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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// tokenizerMarginBounds is the reference for SplitMarginComments: it finds the
// statement in sql with the tokenizer the parser uses. It returns the part
// [start, end) of sql that is not a margin comment, and ok is false when the
// tokenizer rejects sql.
//
// Only block comment tokens and the spaces around them are margin. Everything
// else the tokenizer reads is statement: every other token, a line comment up
// to its newline, and the bytes of a /*!...*/ comment that Scan steps over
// without returning a token, which show up between two tokens.
func tokenizerMarginBounds(parser *Parser, sql string) (start, end int, ok bool) {
	tkn := parser.NewStringTokenizer(sql)
	start = -1
	keep := func(from, to int) {
		if start < 0 {
			start = from
		}
		end = to
	}
	var lastTyp int
	for {
		before := tkn.Pos
		typ, val := tkn.Scan()
		if typ == LEX_ERROR {
			return 0, len(sql), false
		}
		if gap := sql[before:tkn.currStart]; strings.Trim(gap, sqlSpaceChars) != "" {
			keep(before+len(gap)-len(strings.TrimLeft(gap, sqlSpaceChars)),
				before+len(strings.TrimRight(gap, sqlSpaceChars)))
		}
		switch {
		case typ == 0:
			if start < 0 {
				return 0, 0, true
			}
			// "select 0--/**/": cutting off the comment would leave a '--' that
			// the end of the text turns into a comment. Without a trailing
			// comment nothing is cut there, and the '--' reads the same.
			if lastTyp == '-' && strings.HasSuffix(sql[:end], "--") && strings.Trim(sql[end:], sqlSpaceChars) != "" {
				// The leading comments are at the start of the text, so cutting
				// them off changes nothing after them.
				return start, len(sql), true
			}
			return start, end, true
		case typ == COMMENT && strings.HasPrefix(val, "/*"):
		case typ == COMMENT:
			keep(tkn.currStart, tkn.currStart+len(strings.TrimSuffix(val, "\n")))
			lastTyp = typ
		default:
			keep(tkn.currStart, tkn.Pos)
			lastTyp = typ
		}
	}
}

// tokenizerSplit is SplitMarginComments computed from tokenizerMarginBounds.
func tokenizerSplit(parser *Parser, sql string) (query string, comments MarginComments, ok bool) {
	start, end, ok := tokenizerMarginBounds(parser, sql)
	comments = MarginComments{
		Leading:  strings.TrimLeft(sql[:start], sqlSpaceChars),
		Trailing: strings.TrimRight(sql[end:], sqlSpaceChars),
	}
	return strings.Trim(sql[start:end], sqlSpaceChars+";"), comments, ok
}

// FuzzSplitMarginCommentsMatchesTokenizer checks that SplitMarginComments splits
// every input the tokenizer accepts exactly where the tokenizer says the
// statement starts and ends.
func FuzzSplitMarginCommentsMatchesTokenizer(f *testing.F) {
	seeds := []string{
		"/* a */ select 1 /* b */",
		"select 1 -- x\n/*b*/",
		"select 1 /*!80000 union select 2 */ /*b*/",
		"select 1 /*!99999 union select 2 */ /*b*/",
		"select 1 /*!99999 'x */ select 'a' /* t */",
		"select 1 /*!80000 2 // 3 */ /* t */",
		"select 1 /*!99999 /* n */ x */ /* t */",
		"select 0--/**/",
		"select 'a /*' , 1 /* t */",
		"select `a */` /* t */",
		"select n'x' /* t */",
		"select x'0a' /* t */",
		"select 1 /*! 2 */ /* t */",
		"select 1;/* t */",
		"/**/#0",
		// Found by fuzzing. The tokenizer takes the byte after '@' into the
		// variable name, and a number takes a sign after its exponent.
		"@ /**/",
		"0e--\n/**/",
		"/**/0e--",
		"/**/--/**/",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	parser := NewTestParser()
	f.Fuzz(func(t *testing.T, sql string) {
		wantQuery, wantComments, ok := tokenizerSplit(parser, sql)
		if !ok {
			return
		}
		query, comments := SplitMarginComments(sql)
		require.Equal(t, wantQuery, query, "query differs from the tokenizer's (from %q)", sql)
		require.Equal(t, wantComments, comments, "margins differ from the tokenizer's (from %q)", sql)
	})
}
