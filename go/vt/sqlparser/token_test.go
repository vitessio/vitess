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
	"fmt"
	"strconv"
	"strings"
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

// scanStringBenchCase holds a literal input; benchmarks start at `Pos=1`,
// after its opening quote.
type scanStringBenchCase struct {
	name  string
	delim byte
	sql   string
	token int
}

// scanStringBenchCases covers clean, escaped and unterminated literals around
// the scalar and window handoffs.
func scanStringBenchCases() []scanStringBenchCase {
	const text = "The quick brown fox jumps over the lazy dog, then it does so again. "
	var cases []scanStringBenchCase
	for _, delim := range []struct {
		name string
		ch   byte
	}{{"squote", '\''}, {"dquote", '"'}} {
		addCase := func(name, sql string, token int) {
			cases = append(cases, scanStringBenchCase{
				name:  fmt.Sprintf("%s/%s", delim.name, name),
				delim: delim.ch,
				sql:   sql,
				token: token,
			})
		}
		addTerminated := func(name, literal string) {
			addCase(name, string(delim.ch)+literal+string(delim.ch), STRING)
		}
		addUnterminated := func(name, literal string) {
			addCase(name, string(delim.ch)+literal, LEX_ERROR)
		}

		for _, size := range []int{16, 64, 256, 512, 4096, 65536} {
			body := strings.Repeat(text, size/len(text)+1)[:size]
			shapes := []struct {
				name    string
				literal string
			}{
				{"clean", body},
				{"escape", body[:size/2] + `\n` + body[size/2+2:]},
				{"single-escape-long-tail", body[:2] + `\n` + body[4:]},
				{"two-escape-long-tail", body[:2] + `\n` + body[4:6] + `\n` + body[8:]},
				{"three-escape-long-tail", body[:2] + `\n` + body[4:6] + `\n` + body[8:10] + `\n` + body[12:]},
			}
			denseHead := strings.Repeat(`aaaa\n`, 10)
			if size >= len(denseHead) {
				shapes = append(shapes, struct {
					name    string
					literal string
				}{"escape-dense-head", denseHead + body[len(denseHead):]})
			}
			for _, shape := range shapes {
				addTerminated(fmt.Sprintf("%d/%s", size, shape.name), shape.literal)
			}
			runs := []int{2, 4, 6, 16, 31, 32, 48, 64}
			if size == 65536 {
				runs = []int{2, 16, 31, 32, 48, 64}
			}
			for _, run := range runs {
				unit := strings.Repeat("a", run) + `\n`
				if len(unit) > size {
					continue
				}
				literal := strings.Repeat(unit, size/len(unit))
				literal += strings.Repeat("a", size-len(literal))
				addTerminated(fmt.Sprintf("%d/escape-run-%d", size, run), literal)
			}
		}

		const unterminatedSize = 4096
		unterminated := strings.Repeat(text, unterminatedSize/len(text)+1)[:unterminatedSize]
		addUnterminated("4096/unterminated-clean", unterminated)
		addUnterminated("4096/unterminated-escape", unterminated[:2]+`\n`+unterminated[4:])

		for _, n := range []int{scanStringScalarPrefix - 1, scanStringScalarPrefix, scanStringScalarPrefix + 1} {
			addTerminated(fmt.Sprintf("handoff/scalar-prefix-%d", n), strings.Repeat("a", n))
		}
		for _, n := range []int{scanStringSlowScalarPrefix - 1, scanStringSlowScalarPrefix, scanStringSlowScalarPrefix + 1} {
			addTerminated(fmt.Sprintf("handoff/slow-prefix-%d", n), `\n`+strings.Repeat("a", n))
		}
	}
	return cases
}

// BenchmarkTokenizerScanString compares the byte loops from `main` with the
// scanner `vtgate` runs before normalization.
func BenchmarkTokenizerScanString(b *testing.B) {
	for _, tc := range scanStringBenchCases() {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("impl=reference", func(b *testing.B) {
				tkn := NewTestParser().NewStringTokenizer(tc.sql)
				b.ReportAllocs()
				b.SetBytes(int64(len(tc.sql) - 1))
				for b.Loop() {
					tkn.Pos = 1
					if id, _ := scanStringReference(tkn, uint16(tc.delim), STRING); id != tc.token {
						b.Fatalf("scanStringReference returned token %d, want %d", id, tc.token)
					}
				}
			})
			b.Run("impl=branch", func(b *testing.B) {
				tkn := NewTestParser().NewStringTokenizer(tc.sql)
				b.ReportAllocs()
				b.SetBytes(int64(len(tc.sql) - 1))
				for b.Loop() {
					// `Scan` enters `scanString` just past the opening quote.
					tkn.Pos = 1
					if id, _ := tkn.scanString(uint16(tc.delim), STRING); id != tc.token {
						b.Fatalf("scanString returned token %d, want %d", id, tc.token)
					}
				}
			})
		})
	}
}
