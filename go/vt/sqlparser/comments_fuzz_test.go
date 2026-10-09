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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// FuzzSplitMarginComments tests the two rules that the callers of
// SplitMarginComments need.
//
// vtgate and vttablet make a plan for only the query that SplitMarginComments
// returns, and they authorize only that query. Then they add the margin comments
// to the query again. Nothing parses the margin comments. Therefore the margin
// comments must not contain SQL. The two rules are:
//
//  1. Trailing contains only spaces and complete comments.
//  2. When we add the margin comments to the query again, the statement stays
//     the same. Leading+query+Trailing must read as the same tokens as query
//     alone.
//
// Rule 2 compares tokens, not ASTs. vtgate sends some statements, such as
// OPTIMIZE TABLE, as written, and the AST of such a statement does not hold its
// text, so two different texts give the same AST. A wrong comment boundary
// breaks the second rule. The test uses the real tokenizer for this check. It
// does not use the rules in comments.go.
func FuzzSplitMarginComments(f *testing.F) {
	seeds := []string{
		"select id from t where id=1 /*x*/ union select authentication_string from mysql.user -- */",
		"select id from t where id=1 /*x*/ union select 1 # */",
		"insert into t(a) values (1) /*x*/ , (2) -- */",
		"select 1 /*a*/ union select 2 --*/",
		"select 'a /*' , 1 -- */",
		"select `a /*` , 1 -- */",
		"select 1 -- x\n/*b*/",
		"select 1 /*!80000 union select 2 */ /*b*/",
		"/* before */ select 1 /* after */",
		"select 1 from t where col = '*//*'",
		"select 1 /* unterminated",
		"/*! select 1 */",
		"/*b*/ /*a*/",
		"select 1;",
		"select 'a\\' /*x*/ union select 2 -- */",
		"select \"a *\" /*x*/, 1 -- */",
		"select 1 // x\n/*b*/",
		"/**/#0",
		"optimize table t1 --; /* x\n, t2 /* */",
		"select 1 --; /* x\nunion select 2 /* */",
		"optimize table t1 -- x\n; /* c\n, t2 /* */",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	parser := NewTestParser()

	f.Fuzz(func(t *testing.T, sql string) {
		query, comments := SplitMarginComments(sql)

		// Rule 1: Trailing contains only comments and spaces.
		require.True(t, isCommentsAndWhitespaceOnly(parser, comments.Trailing),
			"Trailing carries non-comment text: %q (from %q)", comments.Trailing, sql)

		// Rule 2: when we add the comments again, the statement stays the same.
		// This rule applies only if the parser accepts the query. Only then do
		// the callers make a plan and authorize the query.
		//
		// It applies whether or not the parser accepts the whole input. The
		// callers never parse the whole input, only the query, so an input the
		// parser rejects can still hide text from them.
		stmt, err, panicked := parseNoPanic(parser, query)
		if err != nil || panicked {
			return
		}
		// The AST of a CommentOnly statement is the text of the comments.
		// Such a statement has no SQL to hide, and rule 2 does not apply to it.
		if _, ok := stmt.(*CommentOnly); ok {
			return
		}
		want, ok := statementTokens(parser, query)
		require.True(t, ok, "the tokenizer rejects a query the parser accepts: %q (from %q)", query, sql)
		recombined := comments.Leading + query + comments.Trailing
		got, ok := statementTokens(parser, recombined)
		require.True(t, ok, "re-attaching margin comments made the text unreadable: %q (from %q)", recombined, sql)
		require.Equal(t, want, got,
			"re-attaching margin comments changed the statement: %q (from %q)", recombined, sql)
	})
}

// parseNoPanic parses sql. It also reports whether the parser had a panic.
//
// The parser has a panic for some inputs that have no relation to the margin
// comments. One example is "set ````", which has an empty quoted identifier.
// This input goes to createIdentifierCI, and that function reads str[0] without
// a length check. Such an input stops the fuzzer before it can test the margin
// comments. Therefore this test ignores such an input. No other code ignores
// these panics.
func parseNoPanic(parser *Parser, sql string) (stmt Statement, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	stmt, err = parser.Parse(sql)
	return stmt, err, false
}

// statementTokens returns the tokens the tokenizer reads in sql, without its
// comments. ok is false when the tokenizer rejects sql.
func statementTokens(parser *Parser, sql string) (tokens []string, ok bool) {
	tkn := parser.NewStringTokenizer(sql)
	for {
		typ, val := tkn.Scan()
		switch typ {
		case 0:
			return tokens, true
		case LEX_ERROR:
			return nil, false
		case COMMENT:
		default:
			tokens = append(tokens, fmt.Sprintf("%d:%s", typ, val))
		}
	}
}

// isCommentsAndWhitespaceOnly reports whether text contains only spaces and
// complete comments. It uses the tokenizer, and it does not use the code that
// splits the comments. Therefore the two cannot have the same defect.
func isCommentsAndWhitespaceOnly(parser *Parser, text string) bool {
	if strings.TrimSpace(text) == "" {
		return true
	}
	tkn := parser.NewStringTokenizer(text)
	// Leave SkipSpecialComments at its default. With it set, a /*!...*/ comment
	// comes back as one COMMENT token, so a versioned comment in Trailing would
	// satisfy this rule -- and that is the one thing which must never land there,
	// since MySQL executes what is inside it. At the default the tokenizer reads
	// the SQL inside such a comment, so this rule fails on it directly instead of
	// leaning on rule 2 to catch it.
	for {
		typ, _ := tkn.Scan()
		switch typ {
		case 0:
			return true
		case COMMENT:
			continue
		default:
			return false
		}
	}
}
