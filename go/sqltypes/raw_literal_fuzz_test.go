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

package sqltypes

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// rawLiteralGrammar is an independent statement of the grammar isRawSQLLiteral
// implements by hand, so the two check each other.
var rawLiteralGrammar = map[querypb.Type]*regexp.Regexp{
	Int64:   regexp.MustCompile(`^[ \t]*-?[0-9]+[ \t]*$`),
	Uint64:  regexp.MustCompile(`^[ \t]*[0-9]+[ \t]*$`),
	Float64: regexp.MustCompile(`^[ \t]*[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?[ \t]*$`),
	Decimal: regexp.MustCompile(`^[ \t]*[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)[ \t]*$`),
	HexNum:  regexp.MustCompile(`^0x[0-9a-fA-F]+$`),
	HexVal:  regexp.MustCompile(`^[xX]'([0-9a-fA-F]{2})*'$`),
	BitNum:  regexp.MustCompile(`^0b[01]+$`),
}

var rawLiteralFuzzTypes = []querypb.Type{Int64, Uint64, Float64, Decimal, HexNum, HexVal, BitNum}

// FuzzIsRawSQLLiteral checks the hand-written byte scan against the regexp
// grammar for every raw-written type. The seed corpus runs under plain
// `go test` as a regression test.
func FuzzIsRawSQLLiteral(f *testing.F) {
	for _, seed := range []string{
		"", "0", "-1", "+1", "-", " 42\t", "1.5", ".5", "1.", "1e5", "1E+5", "1e", "NaN", "Inf", "Infinity",
		"0x", "0xAB", "0xab", "0X1", "0xG", "x''", "x'''", "x'41'", "X'aB'", "x'4'", "x'41", "'41'",
		"0b", "0b101", "0B1", "0b12", "1+1", "1; drop table x #", "(select user())", "x'41' or 'a'='a",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, val string) {
		for _, typ := range rawLiteralFuzzTypes {
			want := rawLiteralGrammar[typ].MatchString(val)
			got := isRawSQLLiteral(typ, []byte(val))
			require.Equalf(t, want, got, "type %s payload %q", typ, val)
		}
	})
}
