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

package charset_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
)

// mysql8ConversionGolden holds the number of lines and the SHA-256 of two
// text dumps for every character set of MySQL 8, and for every collation
// whose conversion differs from the default collation of its character set.
// MySQL 8.0.46 and 8.4.13 gave identical dumps.
//
// The decode dump has one line "HEX(seq)<TAB>HEX(utf8mb4)" for every byte
// sequence of decodeCandidates that MySQL converts as one character:
//
//	SELECT HEX(seq), HEX(CONVERT(CONVERT(seq USING cs) USING utf8mb4))
//	-- kept when CHAR_LENGTH(CONVERT(seq USING cs)) = 1 and the result is
//	-- one character
//
// A single byte that MySQL does not convert to one character is in the
// decode dump with "?" (3F), except for the Unicode character sets; see
// decodeDump. The lines are in the order of decodeCandidates: by length,
// then by byte value.
//
// The encode dump has one line "codepoint<TAB>HEX(bytes)" for every code
// point U+0000..U+10FFFF without the surrogates, in code point order:
//
//	SELECT cp, HEX(CONVERT(CONVERT(c USING utf8mb4) USING cs))
//
// For a collation, the dumps come from
// CONVERT((CONVERT(seq USING cs) COLLATE coll) USING utf8mb4), and from an
// INSERT of every code point into a VARCHAR(1) column of the collation with
// an empty sql_mode. For the default collations, that INSERT gives the same
// encode dumps as CONVERT.
const mysql8ConversionGolden = "testdata/mysql8_conversion.txt"

// mysql8Charsets are the character sets of SHOW CHARACTER SET on MySQL 8.0
// and 8.4, without binary.
var mysql8Charsets = []string{
	"armscii8", "ascii", "big5", "cp1250", "cp1251", "cp1256", "cp1257",
	"cp850", "cp852", "cp866", "cp932", "dec8", "eucjpms", "euckr",
	"gb18030", "gb2312", "gbk", "geostd8", "greek", "hebrew", "hp8",
	"keybcs2", "koi8r", "koi8u", "latin1", "latin2", "latin5", "latin7",
	"macce", "macroman", "sjis", "swe7", "tis620", "ucs2", "ujis", "utf16",
	"utf16le", "utf32", "utf8mb3", "utf8mb4",
}

func charsetForMySQL8(t *testing.T, name string) charset.Charset {
	id := collations.MySQL8().DefaultCollationForCharset(name)
	coll := colldata.Lookup(id)
	require.NotNil(t, coll, "no collation for %s", name)
	require.Equal(t, name, coll.Charset().Name())
	return coll.Charset()
}

// decodeCandidates calls yield with every byte sequence that the decode dump
// of the character set looks at: every sequence of one and two bytes, and
// every sequence of the three- and four-byte forms of the character set, by
// length and then by byte value. For ucs2, utf16, utf16le and utf32 it skips
// the sequences that are not a whole number of code units, because
// CONVERT(seq USING ucs2) pads those with zero bytes on the left: 0x01
// becomes U+0001.
func decodeCandidates(name string, yield func([]byte)) {
	var b [4]byte
	one := func() {
		for x := range 256 {
			b[0] = byte(x)
			yield(b[:1])
		}
	}
	two := func() {
		for x := range 1 << 16 {
			b[0], b[1] = byte(x>>8), byte(x)
			yield(b[:2])
		}
	}
	three := func(lo0, hi0, lo1, hi1, lo2, hi2 int) {
		for x := lo0; x <= hi0; x++ {
			for y := lo1; y <= hi1; y++ {
				for z := lo2; z <= hi2; z++ {
					b[0], b[1], b[2] = byte(x), byte(y), byte(z)
					yield(b[:3])
				}
			}
		}
	}
	four := func(lo0, hi0, lo1, hi1, lo2, hi2, lo3, hi3 int) {
		for w := lo0; w <= hi0; w++ {
			for x := lo1; x <= hi1; x++ {
				for y := lo2; y <= hi2; y++ {
					for z := lo3; z <= hi3; z++ {
						b[0], b[1], b[2], b[3] = byte(w), byte(x), byte(y), byte(z)
						yield(b[:4])
					}
				}
			}
		}
	}
	switch name {
	case "ujis", "eucjpms":
		one()
		two()
		three(0x8F, 0x8F, 0x00, 0xFF, 0x00, 0xFF)
	case "utf8mb3":
		one()
		two()
		three(0xE0, 0xEF, 0x80, 0xBF, 0x80, 0xBF)
	case "utf8mb4":
		one()
		two()
		three(0xE0, 0xEF, 0x80, 0xBF, 0x80, 0xBF)
		four(0xF0, 0xF4, 0x80, 0xBF, 0x80, 0xBF, 0x80, 0xBF)
	case "gb18030":
		one()
		two()
		four(0x81, 0xFE, 0x30, 0x39, 0x81, 0xFE, 0x30, 0x39)
	case "ucs2":
		two()
	case "utf16":
		two()
		// Every high surrogate followed by every low surrogate.
		four(0xD8, 0xDB, 0x00, 0xFF, 0xDC, 0xDF, 0x00, 0xFF)
	case "utf16le":
		two()
		four(0x00, 0xFF, 0xD8, 0xDB, 0x00, 0xFF, 0xDC, 0xDF)
	case "utf32":
		// U+0000..U+11FFFF: the surrogates and 64K values above U+10FFFF
		// are rejected by MySQL and by Vitess.
		four(0x00, 0x00, 0x00, 0x11, 0x00, 0xFF, 0x00, 0xFF)
	default:
		one()
		two()
	}
}

// conversionDump hashes the lines of a dump as they are added. With a
// writer, it also writes them, to compare them with the dumps of MySQL.
type conversionDump struct {
	hash  hash.Hash
	count int
	line  []byte
	w     *bufio.Writer
}

func newConversionDump(t *testing.T, path string) *conversionDump {
	d := &conversionDump{hash: sha256.New()}
	if path != "" {
		f, err := os.Create(path)
		require.NoError(t, err)
		d.w = bufio.NewWriter(f)
		t.Cleanup(func() {
			assert.NoError(t, d.w.Flush())
			assert.NoError(t, f.Close())
		})
	}
	return d
}

func appendUpperHex(dst, src []byte) []byte {
	const digits = "0123456789ABCDEF"
	for _, b := range src {
		dst = append(dst, digits[b>>4], digits[b&0xF])
	}
	return dst
}

func (d *conversionDump) add(line []byte) {
	d.line = append(line, '\n')
	d.hash.Write(d.line)
	d.count++
	if d.w != nil {
		d.w.Write(d.line)
	}
}

func (d *conversionDump) addDecode(seq, out []byte) {
	l := appendUpperHex(d.line[:0], seq)
	l = append(l, '\t')
	d.add(appendUpperHex(l, out))
}

func (d *conversionDump) addEncode(cp rune, out []byte) {
	l := strconv.AppendInt(d.line[:0], int64(cp), 10)
	l = append(l, '\t')
	d.add(appendUpperHex(l, out))
}

func (d *conversionDump) digest() (int, string) {
	return d.count, hex.EncodeToString(d.hash.Sum(nil))
}

// decodeDump builds the decode dump of cs from DecodeRune and Convert to
// utf8mb4, and calls each with the sequences of the dump. A sequence is in
// the dump when DecodeRune reads all of it as one character, and Convert must
// then write that character. When DecodeRune reads all of it as an invalid
// character, Convert must write "?" for it, as MySQL does for a code without
// a Unicode character, like A2 A0 in gbk. Two cases are handled apart:
//
//   - In ucs2, utf16, utf16le, utf32, utf8mb3 and utf8mb4 every valid code has
//     a character, so an invalid one is a code that MySQL rejects, like the
//     lone surrogate D8 00 in utf16. Those are not in the dump.
//   - For a single byte that MySQL rejects, like 81 in gbk or 80 in ascii, the
//     MySQL dump has "?", because Vitess cannot tell a rejected byte from a
//     byte without a character, like 81 in cp1250, and converts both to one
//     "?".
func decodeDump(t *testing.T, cs charset.Charset, path string, each func(seq, out []byte)) *conversionDump {
	d := newConversionDump(t, path)
	utf8mb4 := charset.Charset_utf8mb4{}
	unicode := charset.IsUnicode(cs)
	buf := make([]byte, 0, 16)
	decodeCandidates(cs.Name(), func(seq []byte) {
		r, width, ok := cs.DecodeRune(seq)
		if width != len(seq) || (!ok && unicode) {
			return
		}
		out, _ := charset.Convert(buf[:0], utf8mb4, seq, cs)
		var want []byte
		if ok {
			want = utf8.AppendRune(nil, r)
		} else {
			want = []byte{'?'}
		}
		if !bytes.Equal(want, out) {
			require.Failf(t, "DecodeRune and Convert disagree", "%s %X: DecodeRune %U %v, Convert %X", cs.Name(), seq, r, ok, out)
		}
		d.addDecode(seq, out)
		if each != nil {
			each(seq, out)
		}
	})
	return d
}

// encodeDump builds the encode dump of cs from Convert from utf8mb4, and
// calls each with the UTF-8 of every code point and the bytes of cs.
func encodeDump(t *testing.T, cs charset.Charset, path string, each func(src, out []byte)) *conversionDump {
	d := newConversionDump(t, path)
	utf8mb4 := charset.Charset_utf8mb4{}
	buf := make([]byte, 0, 16)
	var src [4]byte
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if r >= 0xD800 && r <= 0xDFFF {
			continue
		}
		n := utf8.EncodeRune(src[:], r)
		out, _ := charset.Convert(buf[:0], cs, src[:n], utf8mb4)
		d.addEncode(r, out)
		if each != nil {
			each(src[:n], out)
		}
	}
	return d
}

type goldenEntry struct {
	count  int
	digest string
}

func readConversionGolden(t *testing.T) map[string]goldenEntry {
	f, err := os.Open(mysql8ConversionGolden)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	golden := make(map[string]goldenEntry)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 4, "golden line %q", line)
		count, err := strconv.Atoi(fields[2])
		require.NoError(t, err)
		golden[fields[0]+" "+fields[1]] = goldenEntry{count: count, digest: fields[3]}
	}
	require.NoError(t, sc.Err())
	return golden
}

// checkConversionDumps compares the dumps of cs with the golden entries of
// name. It also converts the string of all the sequences of the decode dump,
// and the string of all the code points, and checks that Convert gives the
// concatenation of the characters, once into a nil buffer and once after a
// prefix.
func checkConversionDumps(t *testing.T, golden map[string]goldenEntry, name string, cs charset.Charset) {
	utf8mb4 := charset.Charset_utf8mb4{}
	dumpDir := os.Getenv("CHARSET_DUMP_DIR")
	for _, dir := range []string{"decode", "encode"} {
		want, ok := golden[name+" "+dir]
		require.True(t, ok, "no golden %s dump for %s", dir, name)

		var path string
		if dumpDir != "" {
			path = filepath.Join(dumpDir, name+"."+dir+".txt")
		}
		var src, out []byte
		var d *conversionDump
		if dir == "decode" {
			d = decodeDump(t, cs, path, func(seq, conv []byte) {
				// A single byte that is rejected, like 81 in gbk, would
				// start a character with the next byte, and a string of
				// the character set cannot have it.
				if len(seq) == 1 && cs.MaxWidth() > 1 {
					if _, _, ok := cs.DecodeRune(seq); !ok {
						return
					}
				}
				src = append(src, seq...)
				out = append(out, conv...)
			})
		} else {
			d = encodeDump(t, cs, path, func(s, conv []byte) {
				src = append(src, s...)
				out = append(out, conv...)
			})
		}
		count, digest := d.digest()
		assert.Equal(t, want.count, count, "%s %s dump: number of lines", name, dir)
		assert.Equal(t, want.digest, digest, "%s %s dump: SHA-256", name, dir)

		from, to := cs, charset.Charset(utf8mb4)
		if dir == "encode" {
			from, to = to, from
		}
		got, _ := charset.Convert(nil, to, src, from)
		assert.True(t, bytes.Equal(out, got), "%s %s: Convert of the %d bytes of all characters differs from the concatenation", name, dir, len(src))
		got, _ = charset.Convert([]byte("prefix"), to, src, from)
		assert.True(t, bytes.Equal(append([]byte("prefix"), out...), got), "%s %s: Convert after a prefix differs from the concatenation", name, dir)
	}
}

// TestMySQL8Conversion checks, for every character set of MySQL 8, that
// Vitess converts every character to and from utf8mb4 as MySQL 8.0.46 and
// 8.4.13 do, one character at a time and in one string. The string goes
// through every code path of Convert: from UTF-8, through DecodeRune and
// EncodeRune, and the copy when one character set IsSuperset of the other.
// Set CHARSET_DUMP_DIR to write the dumps of Vitess to that
// directory, to compare them line by line with the dumps of MySQL.
func TestMySQL8Conversion(t *testing.T) {
	golden := readConversionGolden(t)
	for _, name := range mysql8Charsets {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			checkConversionDumps(t, golden, name, charsetForMySQL8(t, name))
		})
	}
}

// TestMySQL8CollationConversion checks the character set of every collation
// of MySQL 8. MySQL converts the text of a collation with the tables of that
// collation, and so does Vitess. They are the tables of the default
// collation, except for latin2_czech_cs, which has no character at 80..9F
// and 7F: CONVERT((CONVERT(0x80 USING latin2) COLLATE latin2_czech_cs) USING
// utf8mb4) is "?", and with latin2_general_ci it is U+0080.
func TestMySQL8CollationConversion(t *testing.T) {
	golden := readConversionGolden(t)
	env := collations.MySQL8()
	for _, id := range env.AllCollationIDs() {
		coll := colldata.Lookup(id)
		if coll == nil {
			continue
		}
		name := coll.Name()
		cs := coll.Charset()
		if _, ok := golden[name+" decode"]; ok {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				checkConversionDumps(t, golden, name, cs)
			})
			continue
		}
		if cs.Name() == "binary" {
			continue
		}
		def := charsetForMySQL8(t, cs.Name())
		assert.True(t, reflect.DeepEqual(def, cs), "%s: the character set differs from the one of %s", name, env.LookupName(env.DefaultCollationForCharset(cs.Name())))
	}
}

// TestMySQL8IsSuperset checks that every IsSuperset among the character sets
// of MySQL 8 is right: when dst.IsSuperset(src), Convert copies the bytes,
// so every character of src must be the same bytes in dst. The character sets
// of two collations of one character set are left out: MySQL does not
// convert between them either, and COLLATE keeps the bytes.
func TestMySQL8IsSuperset(t *testing.T) {
	var all []charset.Charset
	for _, name := range mysql8Charsets {
		all = append(all, charsetForMySQL8(t, name))
	}
	pairs := 0
	for _, dst := range all {
		for _, src := range all {
			if dst.Name() == src.Name() || !dst.IsSuperset(src) {
				continue
			}
			pairs++
			decodeDump(t, src, "", func(seq, out []byte) {
				want, _, ok := src.DecodeRune(seq)
				if !ok {
					return
				}
				r, width, ok := dst.DecodeRune(seq)
				if !ok || width != len(seq) || r != want {
					require.Failf(t, "IsSuperset is wrong", "%s.IsSuperset(%s): %X is %U in %s and %U (%v) in %s", dst.Name(), src.Name(), seq, want, src.Name(), r, ok, dst.Name())
				}
			})
		}
	}
	// utf8mb4 of utf8mb3, utf16 of ucs2.
	assert.Equal(t, 2, pairs)
}
