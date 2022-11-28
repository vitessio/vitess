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
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseNextValid concatenates all the valid SQL test cases and check it can read
// them as one long string.
func TestParseNextValid(t *testing.T) {
	var sql bytes.Buffer
	for _, tcase := range validSQL {
		sql.WriteString(strings.TrimSuffix(tcase.input, ";"))
		sql.WriteRune(';')
	}

	tokens := NewStringTokenizer(sql.String())
	for i, tcase := range validSQL {
		input := tcase.input + ";"
		want := tcase.output
		if want == "" {
			want = tcase.input
		}

		tree, err := ParseNext(tokens)
		require.NoError(t, err)

		if got := String(tree); got != want {
			t.Fatalf("[%d] ParseNext(%q) = %q, want %q", i, input, got, want)
		}
	}

	// Read once more and it should be EOF.
	if tree, err := ParseNext(tokens); err != io.EOF {
		t.Errorf("ParseNext(tokens) = (%q, %v) want io.EOF", String(tree), err)
	}
}

func TestIgnoreSpecialComments(t *testing.T) {
	input := `SELECT 1;/*! ALTER TABLE foo DISABLE KEYS */;SELECT 2;`

	tokenizer := NewStringTokenizer(input)
	tokenizer.SkipSpecialComments = true
	one, err := ParseNextStrictDDL(tokenizer)
	require.NoError(t, err)
	require.Equal(t, "select 1 from dual", String(one))
	two, err := ParseNextStrictDDL(tokenizer)
	require.NoError(t, err)
	require.Equal(t, "select 2 from dual", String(two))
}

// TestParseNextErrors tests all the error cases, and ensures a valid
// SQL statement can be passed afterwards.
func TestParseNextErrors(t *testing.T) {
	for _, tcase := range invalidSQL {
		if tcase.excludeMulti {
			// Skip tests which leave unclosed strings, or comments.
			continue
		}
		t.Run(tcase.input, func(t *testing.T) {
			sql := tcase.input + "; select 1 from t"
			tokens := NewStringTokenizer(sql)

			// The first statement should be an error
			_, err := ParseNextStrictDDL(tokens)
			require.EqualError(t, err, tcase.output)

			// The second should be valid
			tree, err := ParseNextStrictDDL(tokens)
			require.NoError(t, err)

			want := "select 1 from t"
			assert.Equal(t, want, String(tree))

			// Read once more and it should be EOF.
			_, err = ParseNextStrictDDL(tokens)
			require.Same(t, io.EOF, err)
		})
	}
}

// TestParseNextEdgeCases tests various ParseNext edge cases.
func TestParseNextEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{{
		name:  "Trailing ;",
		input: "select 1 from a; update a set b = 2;",
		want:  []string{"select 1 from a", "update a set b = 2"},
	}, {
		name:  "No trailing ;",
		input: "select 1 from a; update a set b = 2",
		want:  []string{"select 1 from a", "update a set b = 2"},
	}, {
		name:  "Trailing whitespace",
		input: "select 1 from a; update a set b = 2    ",
		want:  []string{"select 1 from a", "update a set b = 2"},
	}, {
		name:  "Trailing whitespace and ;",
		input: "select 1 from a; update a set b = 2   ;   ",
		want:  []string{"select 1 from a", "update a set b = 2"},
	}, {
		name:  "Handle SkipToEnd statements",
		input: "set character set utf8; select 1 from a",
		want:  []string{"set charset 'utf8'", "select 1 from a"},
	}, {
		name:  "Semicolin inside a string",
		input: "set character set ';'; select 1 from a",
		want:  []string{"set charset ';'", "select 1 from a"},
	}, {
		name:  "Partial DDL",
		input: "create table a; select 1 from a",
		want:  []string{"create table a", "select 1 from a"},
	}, {
		name:  "Partial DDL",
		input: "create table a ignore me this is garbage; select 1 from a",
		want:  []string{"create table a", "select 1 from a"},
	}}

	for _, test := range tests {
		tokens := NewStringTokenizer(test.input)

		for i, want := range test.want {
			tree, err := ParseNext(tokens)
			require.NoError(t, err)

			if got := String(tree); got != want {
				t.Fatalf("[%d] ParseNext(%q) = %q, want %q", i, test.input, got, want)
			}
		}

		// Read once more and it should be EOF.
		if tree, err := ParseNext(tokens); err != io.EOF {
			t.Errorf("ParseNext(%q) = (%q, %v) want io.EOF", test.input, String(tree), err)
		}

		// And again, once more should be EOF.
		if tree, err := ParseNext(tokens); err != io.EOF {
			t.Errorf("ParseNext(%q) = (%q, %v) want io.EOF", test.input, String(tree), err)
		}
	}
}

// TestParseNextEdgeCases tests various ParseNext edge cases.
func TestParseNextStrictNonStrict(t *testing.T) {
	// This is one of the edge cases above.
	input := "create table a ignore me this is garbage; select 1 from a"
	want := []string{"create table a", "select 1 from a"}

	// First go through as expected with non-strict DDL parsing.
	tokens := NewStringTokenizer(input)
	for i, want := range want {
		tree, err := ParseNext(tokens)
		if err != nil {
			t.Fatalf("[%d] ParseNext(%q) err = %q, want nil", i, input, err)
		}
		if got := String(tree); got != want {
			t.Fatalf("[%d] ParseNext(%q) = %q, want %q", i, input, got, want)
		}
	}

	// Now try again with strict parsing and observe the expected error.
	tokens = NewStringTokenizer(input)
	_, err := ParseNextStrictDDL(tokens)
	if err == nil || !strings.Contains(err.Error(), "ignore") {
		t.Fatalf("ParseNext(%q) err = %q, want ignore", input, err)
	}
	tree, err := ParseNextStrictDDL(tokens)
	if err != nil {
		t.Fatalf("ParseNext(%q) err = %q, want nil", input, err)
	}
	if got := String(tree); got != want[1] {
		t.Fatalf("ParseNext(%q) = %q, want %q", input, got, want)
	}
}
