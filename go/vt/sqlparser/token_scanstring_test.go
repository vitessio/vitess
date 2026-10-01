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

// These ends mirror `indexAny2`'s 4 x window growth.
const (
	indexAny2SecondWindowEnd = indexAny2Window * (1 + 4)
	indexAny2ThirdWindowEnd  = indexAny2Window * (1 + 4 + 16)
	indexAny2FourthWindowEnd = indexAny2Window * (1 + 4 + 16 + 64)
	indexAny2FifthWindowEnd  = indexAny2Window * (1 + 4 + 16 + 64 + 256)
	indexAny2SixthWindowEnd  = indexAny2Window * (1 + 4 + 16 + 64 + 256 + 1024)
)

var indexAny2BoundarySizes = []int{
	0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 128, 129,
	indexAny2Window - 1, indexAny2Window, indexAny2Window + 1,
	indexAny2SecondWindowEnd - 1, indexAny2SecondWindowEnd, indexAny2SecondWindowEnd + 1,
	indexAny2ThirdWindowEnd - 1, indexAny2ThirdWindowEnd, indexAny2ThirdWindowEnd + 1,
}

func indexAny2Clean(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(i%26)
	}
	return b
}

func indexAny2Reference(b []byte, first, second byte) int {
	for i, v := range b {
		if v == first || v == second {
			return i
		}
	}
	return -1
}

func TestIndexAny2(t *testing.T) {
	check := func(t *testing.T, name string, in []byte, first, second byte) {
		t.Helper()
		require.Equal(t, indexAny2Reference(in, first, second), indexAny2(string(in), first, second), name)
	}

	for _, n := range indexAny2BoundarySizes {
		t.Run(fmt.Sprintf("size=%d", n), func(t *testing.T) {
			check(t, "clean", indexAny2Clean(n), '\'', '\\')
			for pos := range n {
				in := indexAny2Clean(n)
				in[pos] = '\''
				check(t, fmt.Sprintf("quote at %d", pos), in, '\'', '\\')
				in = indexAny2Clean(n)
				in[pos] = '\\'
				check(t, fmt.Sprintf("backslash at %d", pos), in, '\'', '\\')
			}
			if n >= 2 {
				in := indexAny2Clean(n)
				in[n-1] = '\''
				in[0] = '\\'
				check(t, "second needle first", in, '\'', '\\')
				in = indexAny2Clean(n)
				in[0] = '\''
				in[n-1] = '\\'
				check(t, "first needle first", in, '\'', '\\')
			}
		})
	}

	t.Run("zero as a needle", func(t *testing.T) {
		for _, n := range []int{17, 25, 33, 63} {
			in := indexAny2Clean(n)
			require.Equal(t, -1, indexAny2(string(in), 0, '"'), "size %d", n)
			in[n-1] = 0
			require.Equal(t, n-1, indexAny2(string(in), 0, '"'), "size %d", n)
		}
	})

	t.Run("same byte twice", func(t *testing.T) {
		in := indexAny2Clean(40)
		in[33] = '\''
		require.Equal(t, 33, indexAny2(string(in), '\'', '\''))
	})

	// A later hit cannot hide an earlier hit for the other needle.
	t.Run("windows", func(t *testing.T) {
		n := indexAny2SixthWindowEnd + 1000
		last := n - 1
		for _, pos := range []int{
			0,
			indexAny2Window - 1, indexAny2Window, indexAny2Window + 1,
			indexAny2SecondWindowEnd - 1, indexAny2SecondWindowEnd, indexAny2SecondWindowEnd + 1,
			indexAny2ThirdWindowEnd - 1, indexAny2ThirdWindowEnd, indexAny2ThirdWindowEnd + 1,
			indexAny2FourthWindowEnd - 1, indexAny2FourthWindowEnd, indexAny2FourthWindowEnd + 1,
			indexAny2FifthWindowEnd - 1, indexAny2FifthWindowEnd, indexAny2FifthWindowEnd + 1,
			indexAny2SixthWindowEnd - 1, indexAny2SixthWindowEnd, indexAny2SixthWindowEnd + 1,
		} {
			in := indexAny2Clean(n)
			in[pos] = '\\'
			require.Equal(t, pos, indexAny2(string(in), '\'', '\\'), "lone second needle at %d", pos)
			in = indexAny2Clean(n)
			in[pos] = '\''
			in[last] = '\\'
			require.Equal(t, pos, indexAny2(string(in), '\'', '\\'), "first needle at %d", pos)
			in = indexAny2Clean(n)
			in[pos] = '\\'
			in[last] = '\''
			require.Equal(t, pos, indexAny2(string(in), '\'', '\\'), "second needle at %d", pos)
		}
	})
}

func FuzzIndexAny2(f *testing.F) {
	f.Add([]byte("hello"), byte('\''), byte('\\'))
	f.Add(indexAny2Clean(64), byte('"'), byte('\\'))
	f.Add(append(indexAny2Clean(31), 0), byte(0), byte('x'))
	f.Fuzz(func(t *testing.T, in []byte, first, second byte) {
		want := indexAny2Reference(in, first, second)
		require.Equal(t, want, indexAny2(string(in), first, second),
			"indexAny2(%q, %#x, %#x)", in, first, second)
	})
}

func BenchmarkIndexAny2(b *testing.B) {
	for _, size := range []int{16, 64, 256, 4096} {
		in := string(indexAny2Clean(size))
		b.Run(fmt.Sprintf("%d/no-hit", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if indexAny2(in, '\'', '\\') != -1 {
					b.Fatal("unexpected hit")
				}
			}
		})
	}

	const size = 64 * 1024
	for _, tc := range []struct {
		name        string
		quoteAt     int
		backslashAt int
	}{
		{"backslash-at-9-quote-at-end", size - 1, 9},
		{"quote-at-9-backslash-at-end", 9, size - 1},
	} {
		in := indexAny2Clean(size)
		in[tc.quoteAt] = '\''
		in[tc.backslashAt] = '\\'
		sql := string(in)
		b.Run(fmt.Sprintf("%d/%s", size, tc.name), func(b *testing.B) {
			b.ReportAllocs()
			// `SetBytes` would count the unread tail as work.
			for b.Loop() {
				if got := indexAny2(sql, '\'', '\\'); got != 9 {
					b.Fatalf("indexAny2 returned %d, want 9", got)
				}
			}
		})
	}
}

// scanStringReference keeps the byte loops from `main` as an independent
// oracle. Tests compare token, value and final position.
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

func scanStringSlowReference(tkn *Tokenizer, buffer *strings.Builder, delim uint16, typ int) (int, string) {
	for {
		ch := tkn.cur()
		if ch == eofChar {
			return LEX_ERROR, buffer.String()
		}

		if ch != delim && ch != '\\' {
			start := tkn.Pos
			for ; tkn.Pos < len(tkn.buf); tkn.Pos++ {
				ch = uint16(tkn.buf[tkn.Pos])
				if ch == delim || ch == '\\' {
					break
				}
			}
			buffer.WriteString(tkn.buf[start:tkn.Pos])
			if tkn.Pos >= len(tkn.buf) {
				tkn.skip(1)
				continue
			}
		}
		tkn.skip(1)

		if ch == '\\' {
			if tkn.cur() == eofChar {
				return LEX_ERROR, buffer.String()
			}
			if tkn.cur() == '%' || tkn.cur() == '_' {
				buffer.WriteByte('\\')
				ch = tkn.cur()
			} else if decodedChar := sqltypes.SQLDecodeMap[byte(tkn.cur())]; decodedChar == sqltypes.DontEscape {
				ch = tkn.cur()
			} else {
				ch = uint16(decodedChar)
			}
		} else if ch == delim && tkn.cur() != delim {
			break
		}

		buffer.WriteByte(byte(ch))
		tkn.skip(1)
	}

	return typ, buffer.String()
}

// requireScansLikeReference compares token, value and final position.
func requireScansLikeReference(t *testing.T, sql string, delim byte) {
	t.Helper()
	parser := NewTestParser()

	ref := parser.NewStringTokenizer(sql)
	wantID, wantStr := scanStringReference(ref, uint16(delim), STRING)

	got := parser.NewStringTokenizer(sql)
	gotID, gotStr := got.scanString(uint16(delim), STRING)

	require.Equal(t, wantID, gotID, "token id")
	require.Equal(t, wantStr, gotStr, "string")
	require.Equal(t, ref.Pos, got.Pos, "position")
}

func TestScanStringBoundaries(t *testing.T) {
	const text = "The quick brown fox jumps over the lazy dog, then it does so again. "
	body := func(n int) string {
		return strings.Repeat(text, n/len(text)+1)[:n]
	}

	var windowPositions []int
	for _, windowEnd := range []int{
		indexAny2Window,
		indexAny2SecondWindowEnd,
		indexAny2ThirdWindowEnd,
		indexAny2FourthWindowEnd,
	} {
		pos := scanStringScalarPrefix + windowEnd
		windowPositions = append(windowPositions, pos-1, pos, pos+1)
	}
	longBodyLen := windowPositions[len(windowPositions)-1] + 64

	for _, delim := range []byte{'\'', '"'} {
		t.Run(fmt.Sprintf("delimiter=%q", delim), func(t *testing.T) {
			d := string(delim)
			lengths := append([]int{
				0, 1,
				scanStringScalarPrefix - 1, scanStringScalarPrefix, scanStringScalarPrefix + 1,
				16, 100, 4096,
			}, windowPositions...)
			for _, n := range lengths {
				t.Run(fmt.Sprintf("clean/%d", n), func(t *testing.T) {
					requireScansLikeReference(t, body(n)+d+" and more", delim)
				})
				t.Run(fmt.Sprintf("unterminated/%d", n), func(t *testing.T) {
					requireScansLikeReference(t, body(n), delim)
				})
			}

			escapePositions := append([]int{
				0,
				scanStringScalarPrefix - 1, scanStringScalarPrefix, scanStringScalarPrefix + 1,
				15, 16, 31, 32, 63, 64,
			}, windowPositions...)
			for _, pos := range escapePositions {
				t.Run(fmt.Sprintf("backslash/%d", pos), func(t *testing.T) {
					b := []byte(body(longBodyLen))
					b[pos] = '\\'
					requireScansLikeReference(t, string(b)+d+" x", delim)
				})
			}

			doubledPositions := append([]int{
				scanStringScalarPrefix - 1, scanStringScalarPrefix,
				15, 31, 63,
			}, windowPositions...)
			for _, pos := range doubledPositions {
				t.Run(fmt.Sprintf("doubled/%d", pos), func(t *testing.T) {
					b := []byte(body(longBodyLen))
					b[pos], b[pos+1] = delim, delim
					requireScansLikeReference(t, string(b)+d+" x", delim)
				})
			}

			for _, run := range []int{
				0,
				scanStringSlowScalarPrefix - 1, scanStringSlowScalarPrefix, scanStringSlowScalarPrefix + 1,
				scanStringSlowScalarPrefix + indexAny2Window - 1,
				scanStringSlowScalarPrefix + indexAny2Window,
				scanStringSlowScalarPrefix + indexAny2Window + 1,
				scanStringSlowScalarPrefix + indexAny2SecondWindowEnd - 1,
				scanStringSlowScalarPrefix + indexAny2SecondWindowEnd,
				scanStringSlowScalarPrefix + indexAny2SecondWindowEnd + 1,
				scanStringSlowScalarPrefix + indexAny2ThirdWindowEnd - 1,
				scanStringSlowScalarPrefix + indexAny2ThirdWindowEnd,
				scanStringSlowScalarPrefix + indexAny2ThirdWindowEnd + 1,
				scanStringSlowScalarPrefix + indexAny2FourthWindowEnd - 1,
				scanStringSlowScalarPrefix + indexAny2FourthWindowEnd,
				scanStringSlowScalarPrefix + indexAny2FourthWindowEnd + 1,
			} {
				t.Run(fmt.Sprintf("slow-run/%d", run), func(t *testing.T) {
					tail := body(run)
					sql := `\n` + tail + d + body(100)
					requireScansLikeReference(t, sql, delim)

					tkn := NewTestParser().NewStringTokenizer(sql)
					id, got := tkn.scanString(uint16(delim), STRING)
					require.Equal(t, STRING, id)
					require.Equal(t, "\n"+tail, got)
					require.Equal(t, len(tail)+3, tkn.Pos)
				})

				t.Run(fmt.Sprintf("escaped-head/%d", run), func(t *testing.T) {
					encodedHead := strings.Repeat(`aa\n`, 10)
					decodedHead := strings.Repeat("aa\n", 10)
					tail := body(run)
					sql := encodedHead + tail + d + body(100)
					requireScansLikeReference(t, sql, delim)

					tkn := NewTestParser().NewStringTokenizer(sql)
					id, got := tkn.scanString(uint16(delim), STRING)
					require.Equal(t, STRING, id)
					require.Equal(t, decodedHead+tail, got)
					require.Equal(t, len(encodedHead)+len(tail)+1, tkn.Pos)
				})
			}

			t.Run("doubled then end", func(t *testing.T) {
				requireScansLikeReference(t, body(20)+d+d+d, delim)
			})
			t.Run("trailing backslash", func(t *testing.T) {
				requireScansLikeReference(t, body(33)+`\`, delim)
			})
			t.Run("contains other quote", func(t *testing.T) {
				other := byte('"')
				if delim == '"' {
					other = '\''
				}
				requireScansLikeReference(t, body(20)+string(other)+body(20)+d, delim)
			})
			t.Run("empty", func(t *testing.T) {
				requireScansLikeReference(t, d, delim)
			})
		})
	}
}

func TestScanStringSlow(t *testing.T) {
	for _, delim := range []byte{'\'', '"'} {
		t.Run(fmt.Sprintf("delimiter=%q", delim), func(t *testing.T) {
			d := string(delim)
			for _, tc := range []struct {
				name       string
				tail       string
				wantID     int
				want       string
				terminated bool
			}{
				{"percent", `\%`, STRING, "\na\\%", true},
				{"underscore", `\_`, STRING, "\na\\_", true},
				{"unknown escape", `\q`, STRING, "\naq", true},
				{"backslash", `\\`, STRING, "\na\\", true},
				{"escape at EOF", `\`, LEX_ERROR, "\na", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					body := `\na` + tc.tail
					sql := body
					wantPos := len(body)
					if tc.terminated {
						sql += d + " rest"
						wantPos++
					}

					tkn := NewTestParser().NewStringTokenizer(sql)
					id, got := tkn.scanString(uint16(delim), STRING)
					require.Equal(t, tc.wantID, id)
					require.Equal(t, tc.want, got)
					require.Equal(t, wantPos, tkn.Pos)
				})
			}

			t.Run("doubled delimiter", func(t *testing.T) {
				body := `\na` + d + d + d
				tkn := NewTestParser().NewStringTokenizer(body)
				id, got := tkn.scanString(uint16(delim), STRING)
				require.Equal(t, STRING, id)
				require.Equal(t, "\na"+d, got)
				require.Equal(t, len(body), tkn.Pos)
			})

			encodedHead := strings.Repeat(`aaaa\n`, 10)
			decodedHead := strings.Repeat("aaaa\n", 10)
			t.Run("escaped-head EOF", func(t *testing.T) {
				tail := strings.Repeat("x", 100)
				sql := encodedHead + tail
				tkn := NewTestParser().NewStringTokenizer(sql)
				id, got := tkn.scanString(uint16(delim), STRING)
				require.Equal(t, LEX_ERROR, id)
				require.Equal(t, decodedHead+tail, got)
				require.Equal(t, len(sql)+1, tkn.Pos)
			})

			t.Run("single-escape EOF", func(t *testing.T) {
				tail := strings.Repeat("x", scanStringSlowScalarPrefix+indexAny2Window)
				sql := `\n` + tail
				tkn := NewTestParser().NewStringTokenizer(sql)
				id, got := tkn.scanString(uint16(delim), STRING)
				require.Equal(t, LEX_ERROR, id)
				require.Equal(t, "\n"+tail, got)
				require.Equal(t, len(sql)+1, tkn.Pos)
			})

			t.Run("escaped-head long-tail", func(t *testing.T) {
				tail := strings.Repeat("x", indexAny2Window+100)
				sql := encodedHead + tail + d + " rest"
				tkn := NewTestParser().NewStringTokenizer(sql)
				id, got := tkn.scanString(uint16(delim), STRING)
				require.Equal(t, STRING, id)
				require.Equal(t, decodedHead+tail, got)
				require.Equal(t, len(encodedHead)+len(tail)+1, tkn.Pos)
			})
		})
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
		require.Equal(t, wantID, gotID, "scanString(%q, %c): token id", sql, delim)
		require.Equal(t, wantStr, gotStr, "scanString(%q, %c): string", sql, delim)
		require.Equal(t, ref.Pos, got.Pos, "scanString(%q, %c): position", sql, delim)
	})
}
