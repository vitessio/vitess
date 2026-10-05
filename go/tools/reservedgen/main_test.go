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

package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGeneratedFileIsUpToDate guards the generated set against drifting from
// sql.y. Moving a keyword into or out of non_reserved_keyword changes whether the
// formatter writes that name bare, so the two must not diverge silently: run
// `make sqlparser` if this fails.
func TestGeneratedFileIsUpToDate(t *testing.T) {
	grammar, err := os.ReadFile("../../vt/sqlparser/sql.y")
	require.NoError(t, err)
	committed, err := os.ReadFile("../../vt/sqlparser/non_reserved_keywords.go")
	require.NoError(t, err)

	tokens, err := NonReservedKeywords(string(grammar))
	require.NoError(t, err)
	fresh, err := generate(tokens)
	require.NoError(t, err)

	assert.Equal(t, string(fresh), string(committed))
}

// TestNonReservedKeywordsSkipsBlankAndCommentLines covers a rule with a blank
// line and comments in the middle of it, all of which yacc accepts. Each used to
// end the extraction early, so the generator wrote a short list and exited
// successfully.
func TestNonReservedKeywordsSkipsBlankAndCommentLines(t *testing.T) {
	const n = 400
	var rule strings.Builder
	var want []string
	for i := range n {
		tok := fmt.Sprintf("TOK%03d", i)
		want = append(want, tok)
		switch {
		case i == 0:
			rule.WriteString("  " + tok + "\n")
		case i == 1:
			rule.WriteString("| " + tok + " %prec FUNCTION_CALL_NON_KEYWORD\n")
		default:
			rule.WriteString("| " + tok + "\n")
		}
		switch i {
		case 320:
			rule.WriteString("  // a comment-only line\n")
		case 340:
			rule.WriteString("\n")
		case 360:
			rule.WriteString("  /* a block comment\n     over two lines */\n")
		case 380:
			rule.WriteString("| /* inline */ TOK380 // trailing\n")
		}
	}
	grammar := "%%\n\nnon_reserved_keyword:\n" + rule.String() + "\nopenb:\n  '('\n  {\n  }\n"

	got, err := NonReservedKeywords(grammar)
	require.NoError(t, err)
	require.Len(t, got, n, "the rule was cut short")
	assert.Equal(t, want, got)
}

func TestRuleTokens(t *testing.T) {
	testcases := []struct {
		name    string
		grammar string
		want    []string
		err     string
	}{{
		name:    "ends at the next rule",
		grammar: "\nkw:\n  B\n| A\n\n// next\nother:\n  C\n",
		want:    []string{"A", "B"},
	}, {
		name:    "ends at a semicolon",
		grammar: "\nkw:\n  B\n| A\n;\nother:\n  C\n",
		want:    []string{"A", "B"},
	}, {
		name:    "ends at a trailing semicolon",
		grammar: "\nkw:\n  B\n| A ;\n| C\n",
		want:    []string{"A", "B"},
	}, {
		name:    "ends at the end of the rules section",
		grammar: "\nkw:\n  B\n| A\n%%\n",
		want:    []string{"A", "B"},
	}, {
		name:    "duplicates are dropped",
		grammar: "\nkw:\n  A\n| A\n;\n",
		want:    []string{"A"},
	}, {
		name:    "no rule",
		grammar: "\nother:\n  A\n;\n",
		err:     "no kw rule found",
	}, {
		name:    "no end",
		grammar: "\nkw:\n  A\n| B\n",
		err:     "end of kw rule not found",
	}, {
		name:    "unterminated block comment",
		grammar: "\nkw:\n  A\n/* | B\n;\n",
		err:     "end of kw rule not found",
	}, {
		name:    "empty",
		grammar: "\nkw:\n;\n",
		err:     "kw rule is empty",
	}, {
		name:    "second alternative without a bar",
		grammar: "\nkw:\n  A\n  B\n;\n",
		err:     "unexpected line",
	}, {
		name:    "two tokens in one alternative",
		grammar: "\nkw:\n  A\n| B C\n;\n",
		err:     "unexpected line",
	}, {
		name:    "action block",
		grammar: "\nkw:\n  A\n  {\n  }\n;\n",
		err:     "unexpected line",
	}, {
		name:    "literal",
		grammar: "\nkw:\n  A\n| '('\n;\n",
		err:     "unexpected line",
	}}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ruleTokens(tc.grammar, "kw")
			if tc.err != "" {
				assert.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestNonReservedKeywordsRejectsShortList pins the canary: a rule that parses
// cleanly but is far shorter than sql.y's has always been is an error.
func TestNonReservedKeywordsRejectsShortList(t *testing.T) {
	_, err := NonReservedKeywords("\nnon_reserved_keyword:\n  A\n| B\n;\n")
	assert.ErrorContains(t, err, "only found 2 non-reserved keywords")
}
