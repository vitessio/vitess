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
	"maps"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/bytes2"
)

// This file checks our SQL literal encoding against a model of how MySQL's
// lexer reads one back, for every connection charset a client can select.
//
// The encoder cannot see the connection charset, so it has to produce a
// literal that is read identically under all of them. The interesting ones are
// the multi-byte charsets where `0x5c` is a valid trail byte -- `sjis`,
// `cp932`, `gbk`, `big5` and `gb18030` -- because there a `\` placed right
// after a lead byte is swallowed into a two-byte character instead of escaping
// what follows. That is the classic `SET NAMES gbk` injection, and it is why
// `--db-charset` is part of the threat model rather than a display setting.

func between(c, lo, hi byte) bool { return c >= lo && c <= hi }

// mysqlCharset models one charset's my_ismbchar(), which returns the length of
// the multi-byte character at the head of p, or 0 when there is none. A nil
// ismbchar is a single-byte charset, where MySQL's use_mb() is false and the
// lexer never looks ahead. Ranges are transcribed from the Percona Server
// strings/ctype-*.cc at ref 7d94bafcc37 (8.4.10); they have been stable since
// 5.7 and are shared with Oracle MySQL.
type mysqlCharset struct {
	name     string
	ismbchar func(p []byte) int
}

// mysqlCharsets covers every multi-byte charset MySQL accepts as a client
// charset, plus one single-byte charset as a control. `ucs2`, `utf16`,
// `utf16le` and `utf32` are deliberately absent: check_cs_client() in
// sql/sys_vars.cc:1916 rejects any charset whose mbminlen is above 1, so a
// client cannot select one. That matters, because in those every byte pair is
// a character and a closing quote could be swallowed.
var mysqlCharsets = []mysqlCharset{
	// strings/ctype-bin.cc:482 leaves ismbchar nullptr; latin1 and tis620 do
	// the same, so the lexer never looks ahead in them.
	{name: "latin1"},
	{name: "utf8mb4", ismbchar: utf8Like},
	// utf8mb3 differs from utf8mb4 only by rejecting 4-byte forms, which are
	// a strict subset of what utf8Like already accepts.
	{name: "utf8mb3", ismbchar: func(p []byte) int {
		if l := utf8Like(p); l > 1 && l < 4 {
			return l
		}
		return 0
	}},
	// sjis and cp932 share their head/tail ranges.
	{name: "sjis", ismbchar: sjisLike},
	{name: "cp932", ismbchar: sjisLike},
	{name: "gbk", ismbchar: func(p []byte) int {
		if len(p) > 1 && between(p[0], 0x81, 0xfe) &&
			(between(p[1], 0x40, 0x7e) || between(p[1], 0x80, 0xfe)) {
			return 2
		}
		return 0
	}},
	{name: "big5", ismbchar: func(p []byte) int {
		if len(p) > 1 && between(p[0], 0xa1, 0xf9) &&
			(between(p[1], 0x40, 0x7e) || between(p[1], 0xa1, 0xfe)) {
			return 2
		}
		return 0
	}},
	{name: "gb2312", ismbchar: func(p []byte) int {
		if len(p) > 1 && between(p[0], 0xa1, 0xf7) && between(p[1], 0xa1, 0xfe) {
			return 2
		}
		return 0
	}},
	{name: "euckr", ismbchar: func(p []byte) int {
		if len(p) == 0 || p[0] < 0x80 {
			return 0
		}
		tail := func(c byte) bool {
			return between(c, 0x41, 0x5a) || between(c, 0x61, 0x7a) || between(c, 0x81, 0xfe)
		}
		if len(p) > 1 && between(p[0], 0x81, 0xfe) && tail(p[1]) {
			return 2
		}
		return 0
	}},
	// ujis and eucjpms share their ranges and their SS2/SS3 structure.
	{name: "ujis", ismbchar: eucJPLike},
	{name: "eucjpms", ismbchar: eucJPLike},
	{name: "gb18030", ismbchar: func(p []byte) int {
		odd := func(c byte) bool { return between(c, 0x81, 0xfe) }
		even2 := func(c byte) bool { return between(c, 0x40, 0x7e) || between(c, 0x80, 0xfe) }
		// A four-byte gb18030 character takes ASCII digits in its even
		// positions, so it can reach across bytes that look harmless.
		even4 := func(c byte) bool { return between(c, 0x30, 0x39) }
		if len(p) <= 1 || !odd(p[0]) {
			return 0
		}
		switch {
		case even2(p[1]):
			return 2
		case len(p) > 3 && even4(p[1]) && odd(p[2]) && even4(p[3]):
			return 4
		}
		return 0
	}},
}

func sjisLike(p []byte) int {
	head := func(c byte) bool { return between(c, 0x81, 0x9f) || between(c, 0xe0, 0xfc) }
	tail := func(c byte) bool { return between(c, 0x40, 0x7e) || between(c, 0x80, 0xfc) }
	if len(p) > 1 && head(p[0]) && tail(p[1]) {
		return 2
	}
	return 0
}

// utf8Like models my_ismbchar_utf8mb4, which goes through
// my_valid_mbcharlen_utf8mb4 with RANGE_CHECK -- that rejects overlongs and
// surrogates exactly as Go's decoder does.
func utf8Like(p []byte) int {
	r, size := utf8.DecodeRune(p)
	if r == utf8.RuneError && size <= 1 {
		return 0
	}
	if size > 1 {
		return size
	}
	return 0
}

// eucJPLike models ismbchar_ujis / ismbchar_eucjpms, which are identical.
func eucJPLike(p []byte) int {
	if len(p) == 0 || p[0] < 0x80 {
		return 0
	}
	code := func(c byte) bool { return between(c, 0xa1, 0xfe) }
	kata := func(c byte) bool { return between(c, 0xa1, 0xdf) }
	switch {
	case code(p[0]) && len(p) > 1 && code(p[1]):
		return 2
	case p[0] == 0x8e && len(p) > 1 && kata(p[1]):
		return 2
	case p[0] == 0x8f && len(p) > 2 && code(p[1]) && code(p[2]):
		return 3
	}
	return 0
}

// charsetByName keeps the guard tests from indexing mysqlCharsets, so adding
// an entry cannot silently point them at the wrong charset.
func charsetByName(t *testing.T, name string) mysqlCharset {
	t.Helper()
	for _, cs := range mysqlCharsets {
		if cs.name == name {
			return cs
		}
	}
	require.FailNow(t, "unknown charset "+name)
	return mysqlCharset{}
}

// sqlEncoders covers the literal and expression entry points. There are two
// hand-maintained copies of the escaping logic behind them, not three:
// EncodeSQL goes through encodeBytesSQL, which is a thin wrapper around the
// same encodeBytesSQLBytes2 that EncodeSQLBytes2 uses. It is still listed so
// that the delegation itself stays covered -- if it ever grew its own logic,
// the sweep would be watching.
var sqlEncoders = []struct {
	name       string
	encode     func(v Value) string
	expression bool
}{
	{name: "EncodeSQL", encode: func(v Value) string {
		var sb strings.Builder // strings.Builder satisfies BinWriter
		v.EncodeSQL(&sb)
		return sb.String()
	}},
	{name: "EncodeSQLStringBuilder", encode: func(v Value) string {
		var sb strings.Builder
		v.EncodeSQLStringBuilder(&sb)
		return sb.String()
	}},
	{name: "EncodeSQLBytes2", encode: func(v Value) string {
		var buf bytes2.Buffer
		v.EncodeSQLBytes2(&buf)
		return buf.String()
	}},
	{name: "EncodeSQLExprBytes2", expression: true, encode: func(v Value) string {
		var buf bytes2.Buffer
		v.EncodeSQLExprBytes2(&buf)
		return buf.String()
	}},
	{name: "EncodeSQLExprStringBuilder", expression: true, encode: func(v Value) string {
		var sb strings.Builder
		v.EncodeSQLExprStringBuilder(&sb)
		return sb.String()
	}},
}

// findTerminator models the first pass of get_text() in sql/sql_lex.cc, the
// one that decides where the literal ends. Note the ordering: my_ismbchar() is
// tested before the `\` check, and it is handed the end of the whole query
// rather than the end of the literal, so a lead byte can reach past the
// closing quote. It returns the index of the terminating quote, or -1 if the
// literal never closes.
func (cs mysqlCharset) findTerminator(query []byte, start int) int {
	for i := start; i < len(query); {
		if cs.ismbchar != nil {
			if l := cs.ismbchar(query[i:]); l > 0 {
				i += l
				continue
			}
		}
		switch query[i] {
		case '\\':
			if i+1 >= len(query) {
				return -1 // string terminates mid escape character
			}
			i += 2
		case '\'':
			if i+1 < len(query) && query[i+1] == '\'' {
				i += 2 // doubled quote, not the end
				continue
			}
			return i
		default:
			i++
		}
	}
	return -1
}

// unescape models the second pass of get_text(), which rebuilds the value from
// the literal's content. Same my_ismbchar()-before-`\` ordering, but bounded by
// the literal rather than the query.
func (cs mysqlCharset) unescape(content []byte) []byte {
	out := make([]byte, 0, len(content))
	for i := 0; i < len(content); {
		if cs.ismbchar != nil {
			if l := cs.ismbchar(content[i:]); l > 0 {
				out = append(out, content[i:i+l]...)
				i += l
				continue
			}
		}
		if content[i] == '\\' && i+1 < len(content) {
			i++
			switch content[i] {
			case 'n':
				out = append(out, '\n')
			case 't':
				out = append(out, '\t')
			case 'r':
				out = append(out, '\r')
			case 'b':
				out = append(out, '\b')
			case '0':
				out = append(out, 0)
			case 'Z':
				out = append(out, 26)
			case '_', '%':
				out = append(out, '\\', content[i]) // wildcard escape is kept
			default:
				out = append(out, content[i])
			}
			i++
			continue
		}
		if content[i] == '\'' {
			out = append(out, '\'')
			i += 2
			continue
		}
		out = append(out, content[i])
		i++
	}
	return out
}

// checkLiteral encodes val with the given encoder, embeds it in a statement,
// and asserts MySQL would both close the literal exactly where we closed it
// and read back the bytes we started with. A terminator landing early is a SQL
// injection; a mismatched value is silent corruption.
func checkLiteral(t *testing.T, cs mysqlCharset, enc string, literal string, typ Type, val []byte) {
	t.Helper()

	if idx := strings.IndexByte(literal, '\''); idx > 0 {
		literal = literal[idx:] // drop the _binary introducer
	}

	const prefix = "select * from t where c = "
	const suffix = " and d = 1"
	query := []byte(prefix + literal + suffix)

	start := len(prefix) + 1
	wantEnd := len(prefix) + len(literal) - 1
	decoded := make([]byte, 0, len(val))
	for {
		end := cs.findTerminator(query, start)
		if !assert.True(t, end >= start && end <= wantEnd,
			"%s/%s/%s: literal %q closes at %d, want at most %d", cs.name, enc, typ, literal, end, wantEnd) {
			return
		}
		decoded = append(decoded, cs.unescape(query[start:end])...)
		if end == wantEnd {
			break
		}
		if !assert.True(t, end+3 <= wantEnd && string(query[end+1:end+3]) == " '",
			"%s/%s/%s: unexpected text after a literal in %q", cs.name, enc, typ, literal) {
			return
		}
		start = end + 3
	}
	assert.Equal(t, val, decoded,
		"%s/%s/%s: literal %q does not read back as %x", cs.name, enc, typ, literal, val)
}

// lexerSequences builds the byte sequences the sweep runs. Depth 1-3 over the
// bytes that matter covers the common shapes; depth 4 is added over a narrower
// alphabet so that gb18030's four-byte form and the 3-byte ujis/eucjpms forms
// are also followed by a hazard byte, which shorter sequences cannot reach.
func lexerSequences() [][]byte {
	var sequences [][]byte
	build := func(alphabet []byte, depth int) {
		var rec func(prefix []byte, left int)
		rec = func(prefix []byte, left int) {
			if left == 0 {
				sequences = append(sequences, append([]byte(nil), prefix...))
				return
			}
			for _, b := range alphabet {
				rec(append(prefix, b), left-1)
			}
		}
		rec(nil, depth)
	}

	// quote, backslash, NUL, the LIKE wildcards, ASCII, an ASCII digit for
	// gb18030's four-byte form, and high bytes spanning the lead/trail ranges.
	broad := []byte{0x00, '\'', '\\', '%', '_', 'a', '0', 0x7f, 0x80, 0x81, 0xa1, 0xe0, 0xfe}
	for depth := 1; depth <= 3; depth++ {
		build(broad, depth)
	}
	// 0x8e and 0x8f are the ujis/eucjpms SS2/SS3 leads; 0x30 is a gb18030
	// four-byte even position.
	build([]byte{0x00, '\'', '\\', '0', 0x30, 0x81, 0x8e, 0x8f, 0xa1}, 4)
	return sequences
}

// TestEncodeSQLAgainstMySQLLexer checks every sequence against every charset's
// lexer, through every encoder entry point.
func TestEncodeSQLAgainstMySQLLexer(t *testing.T) {
	sequences := lexerSequences()
	require.NotEmpty(t, sequences)

	for _, cs := range mysqlCharsets {
		t.Run(cs.name, func(t *testing.T) {
			for _, enc := range sqlEncoders {
				for _, typ := range []Type{VarBinary, VarChar} {
					for _, seq := range sequences {
						checkLiteral(t, cs, enc.name, enc.encode(MakeTrusted(typ, seq)), typ, seq)
					}
				}
			}
		})
	}
}

// TestEncodeSQLEncodersAgree pins the single-token entry points to byte-identical
// output. Expression encoding can split the value into adjacent literals.
func TestEncodeSQLEncodersAgree(t *testing.T) {
	sequences := lexerSequences()
	for _, typ := range []Type{VarBinary, VarChar} {
		for _, seq := range sequences {
			v := MakeTrusted(typ, seq)
			want := sqlEncoders[0].encode(v)
			for _, enc := range sqlEncoders[1:] {
				if enc.expression {
					continue
				}
				if got := enc.encode(v); got != want {
					t.Fatalf("%s/%v: %s gave %q, %s gave %q for %x",
						typ, seq, sqlEncoders[0].name, want, enc.name, got, seq)
				}
			}
		}
	}
}

// TestEncodeSQLValueSurvivesEveryEscape pins the property that actually
// matters: a run of bytes above ASCII followed by any escape has to read back
// byte for byte, in every charset and through every entry point. The encoder
// escapes the run ahead of a `\` and doubles a quote instead of escaping it,
// so both shapes are exercised here.
//
// This checks lexer boundaries and bytes, not expression metadata. The live
// TestTextBindConnectionCharset also checks conversion and coercibility.
func TestEncodeSQLValueSurvivesEveryEscape(t *testing.T) {
	// Taken from encodeRef rather than written out, so a new escape cannot slip
	// past this test. Sorted only to keep a failure reproducible.
	escapes := slices.Sorted(maps.Keys(encodeRef))
	require.NotEmpty(t, escapes)

	// A bare lead byte, a two-byte utf8 character, and a longer run.
	runs := [][]byte{{0x81}, {0xc3, 0xa9}, {0x81, 0x82, 0x83}}

	for _, cs := range mysqlCharsets {
		t.Run(cs.name, func(t *testing.T) {
			for _, enc := range sqlEncoders {
				for _, run := range runs {
					for _, esc := range escapes {
						// Leading "x" so the run is never at index 0, and a
						// trailing byte so the escape is never last either.
						val := append(append([]byte("x"), run...), esc, 'y')
						for _, typ := range []Type{VarChar, VarBinary} {
							checkLiteral(t, cs, enc.name, enc.encode(MakeTrusted(typ, val)), typ, val)
						}
					}
				}
			}
		})
	}
}

// TestEncodeSQLLexerModelCatchesHazards guards the model itself: it has to
// catch a literal that leaves a lead byte raw ahead of a `\`. If these stop
// failing, TestEncodeSQLAgainstMySQLLexer has stopped proving anything.
func TestEncodeSQLLexerModelCatchesHazards(t *testing.T) {
	sjis := charsetByName(t, "sjis")
	utf8mb4 := charsetByName(t, "utf8mb4")

	for _, tc := range []struct {
		name  string
		query string
	}{
		// What a naive encoder emits for []byte{'m', 'a', 0x81, '\''}: the
		// 0x81 swallows the backslash under sjis, so the quote terminates.
		{name: "lead byte before escaped quote", query: "select 'ma\x81\\'' and d = 1"},
		// And for []byte{0x81, '\\'}, where the run is left raw.
		{name: "lead byte before escaped backslash", query: "select '\x81\\\\' and d = 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := []byte(tc.query)
			wantEnd := len(query) - len(" and d = 1") - 1
			assert.Equal(t, wantEnd, utf8mb4.findTerminator(query, len("select '")),
				"utf8mb4 should read this literal correctly")
			assert.NotEqual(t, wantEnd, sjis.findTerminator(query, len("select '")),
				"sjis should run past the intended terminator")
		})
	}
}
