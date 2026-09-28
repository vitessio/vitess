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

	"vitess.io/vitess/go/sqltypes"
)

// scanStringReference is the byte-at-a-time loop scanString replaced, run
// against a fresh tokenizer over the same input. It is the definition of
// correct output: token id, string and final position must all match. It
// hands off to scanStringSlowReference, the old slow path, so the whole
// reference chain is the code that shipped before the rewrite.
func scanStringReference(tkn *Tokenizer, delim uint16, typ int) (int, string) {
	start := tkn.Pos

	for {
		switch tkn.cur() {
		case delim:
			if tkn.peek(1) != delim {
				tkn.skip(1)
				return typ, tkn.buf[start : tkn.Pos-1]
			}
			fallthrough

		case '\\':
			var buffer strings.Builder
			buffer.WriteString(tkn.buf[start:tkn.Pos])
			return scanStringSlowReference(tkn, &buffer, delim, typ)

		case eofChar:
			return LEX_ERROR, tkn.buf[start:tkn.Pos]
		}

		tkn.skip(1)
	}
}

// scanStringSlowReference is the old scanStringSlow, verbatim -- and still
// identical to it, because the rewrite deliberately left its inner
// byte-at-a-time hunt alone (see scanStringSlow for why). It is a copy so
// the reference chain stays independent of the code it checks.
func scanStringSlowReference(tkn *Tokenizer, buffer *strings.Builder, delim uint16, typ int) (int, string) {
	for {
		ch := tkn.cur()
		if ch == eofChar {
			// Unterminated string.
			return LEX_ERROR, buffer.String()
		}

		if ch != delim && ch != '\\' {
			// Scan ahead to the next interesting character.
			start := tkn.Pos
			for ; tkn.Pos < len(tkn.buf); tkn.Pos++ {
				ch = uint16(tkn.buf[tkn.Pos])
				if ch == delim || ch == '\\' {
					break
				}
			}

			buffer.WriteString(tkn.buf[start:tkn.Pos])
			if tkn.Pos >= len(tkn.buf) {
				// Reached the end of the buffer without finding a delim or
				// escape character.
				tkn.skip(1)
				continue
			}
		}
		tkn.skip(1) // Read one past the delim or escape character.

		if ch == '\\' {
			if tkn.cur() == eofChar {
				// String terminates mid escape character.
				return LEX_ERROR, buffer.String()
			}
			// Preserve escaping of % and _
			if tkn.cur() == '%' || tkn.cur() == '_' {
				buffer.WriteByte('\\')
				ch = tkn.cur()
			} else if decodedChar := sqltypes.SQLDecodeMap[byte(tkn.cur())]; decodedChar == sqltypes.DontEscape {
				ch = tkn.cur()
			} else {
				ch = uint16(decodedChar)
			}
		} else if ch == delim && tkn.cur() != delim {
			// Correctly terminated string, which is not a double delim.
			break
		}

		buffer.WriteByte(byte(ch))
		tkn.skip(1)
	}

	return typ, buffer.String()
}

// requireScansLikeReference scans sql, which starts just past the opening
// delimiter, with both implementations and requires the same result.
func requireScansLikeReference(t *testing.T, name string, sql string, delim byte) {
	t.Helper()
	parser := NewTestParser()

	ref := parser.NewStringTokenizer(sql)
	wantID, wantStr := scanStringReference(ref, uint16(delim), STRING)

	got := parser.NewStringTokenizer(sql)
	gotID, gotStr := got.scanString(uint16(delim), STRING)

	require.Equal(t, wantID, gotID, "%s: token id", name)
	require.Equal(t, wantStr, gotStr, "%s: string", name)
	require.Equal(t, ref.Pos, got.Pos, "%s: position", name)
}

func TestScanStringBoundaries(t *testing.T) {
	const text = "The quick brown fox jumps over the lazy dog, then it does so again. "
	body := func(n int) string {
		return strings.Repeat(text, n/len(text)+1)[:n]
	}

	for _, delim := range []byte{'\'', '"'} {
		d := string(delim)
		// A clean literal of every length around the scanStringScalarPrefix
		// handoff, IndexByte's block edges and IndexAny2's first window, with
		// the closing delimiter at each.
		for _, n := range []int{0, 1, 7, 8, 9, 14, 15, 16, 17, 30, 31, 32, 33, 62, 63, 64, 65, 100, 4096} {
			requireScansLikeReference(t, fmt.Sprintf("%c clean %d", delim, n), body(n)+d+" and more", delim)
			// Unterminated: the delimiter never comes.
			requireScansLikeReference(t, fmt.Sprintf("%c unterminated %d", delim, n), body(n), delim)
		}
		// A backslash escape at the handoff and the block edges, before the
		// closing delimiter further on.
		for _, pos := range []int{0, 7, 8, 9, 15, 16, 31, 32, 63, 64} {
			b := []byte(body(80))
			b[pos] = '\\'
			requireScansLikeReference(t, fmt.Sprintf("%c backslash at %d", delim, pos), string(b)+d+" x", delim)
		}
		// A doubled delimiter straddling the handoff or a block edge, and at
		// the end.
		for _, pos := range []int{7, 8, 15, 31, 63} {
			b := []byte(body(80))
			b[pos], b[pos+1] = delim, delim
			requireScansLikeReference(t, fmt.Sprintf("%c doubled at %d", delim, pos), string(b)+d+" x", delim)
		}
		requireScansLikeReference(t, fmt.Sprintf("%c doubled then end", delim), body(20)+d+d+d, delim)
		// A backslash as the very last byte, with nothing to escape.
		requireScansLikeReference(t, fmt.Sprintf("%c trailing backslash", delim), body(33)+`\`, delim)
		// The other delimiter inside the literal is just a byte.
		other := byte('"')
		if delim == '"' {
			other = '\''
		}
		requireScansLikeReference(t, fmt.Sprintf("%c contains other quote", delim), body(20)+string(other)+body(20)+d, delim)
		// Empty literal.
		requireScansLikeReference(t, fmt.Sprintf("%c empty", delim), d, delim)
	}
}

func FuzzScanString(f *testing.F) {
	f.Add("hello' world", true)
	f.Add(`it\'s a \\ test' rest`, true)
	f.Add(`it''s doubled' rest`, true)
	f.Add(strings.Repeat("a", 31)+`"`, false)
	f.Add(strings.Repeat("a", 64), true)
	f.Fuzz(func(t *testing.T, sql string, single bool) {
		delim := byte('"')
		if single {
			delim = '\''
		}
		parser := NewTestParser()
		ref := parser.NewStringTokenizer(sql)
		wantID, wantStr := scanStringReference(ref, uint16(delim), STRING)
		got := parser.NewStringTokenizer(sql)
		gotID, gotStr := got.scanString(uint16(delim), STRING)
		if wantID != gotID || wantStr != gotStr || ref.Pos != got.Pos {
			t.Fatalf("scanString(%q, %c) = (%d, %q, pos %d), want (%d, %q, pos %d)",
				sql, delim, gotID, gotStr, got.Pos, wantID, wantStr, ref.Pos)
		}
	})
}

// BenchmarkTokenizerScanStringReference measures the byte-at-a-time loop
// scanString replaced, over the same inputs as BenchmarkTokenizerScanString,
// so one test binary reports today's code next to the new scalar and SIMD
// paths.
func BenchmarkTokenizerScanStringReference(b *testing.B) {
	for _, tc := range scanStringBenchCases() {
		tkn := NewTestParser().NewStringTokenizer(tc.sql)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(tc.size))
			for b.Loop() {
				tkn.Pos = 1
				if id, _ := scanStringReference(tkn, uint16(tc.delim), STRING); id != STRING {
					b.Fatalf("scanStringReference returned token %d, want STRING", id)
				}
			}
		})
	}
}
