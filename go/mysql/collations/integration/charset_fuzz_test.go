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

package integration

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
)

// The charset fuzz tests feed random byte strings, biased towards the
// structural boundaries of each encoding, through both Vitess's charset
// implementations and a live MySQL server, and require that they agree.
//
// MySQL applies three different notions of validity, and each Vitess API is
// compared against its own counterpart:
//
//   - CONVERT(binary → cs) relabels the bytes after a per-lead-byte length
//     check, zero-padding the fixed-width Unicode charsets. This is
//     ConvertFromBinary. Vitess is allowed to reject strings MySQL relabels
//     (its validity also rejects well-formed but unassigned characters, which
//     MySQL only flags once something converts them), but whenever MySQL
//     flags the input, Vitess must too, and whenever both sides accept,
//     the bytes must match.
//
//   - CONVERT(cs → utf8mb4) substitutes '?' silently or truncates with a
//     warning, depending on the charset. This is Convert: when MySQL converts
//     without complaint the output bytes must match ours, and when MySQL
//     complains we must report an error.
//
//   - String walking (CHAR_LENGTH and friends) advances per ismbchar. This is
//     DecodeRune's width, compared through Length over a connection whose
//     client charset is the one under test, which is the only way to hand
//     MySQL malformed labeled bytes.
//
// The random source is seeded deterministically so failures reproduce; set
// COLLATIONS_FUZZ_SEED and COLLATIONS_FUZZ_ITERATIONS to explore further.

// Byte values that sit on the structural edges of the encodings under test:
// digit and letter range limits, lead and trail range limits, and the
// surrogate halves.
var fuzzBoundaryBytes = []byte{
	0x00, 0x20, 0x30, 0x39, 0x3A, 0x40, 0x41, 0x5A, 0x5B, 0x61, 0x7A, 0x7E,
	0x7F, 0x80, 0x81, 0x8E, 0x8F, 0x9F, 0xA0, 0xA1, 0xC2, 0xC7, 0xD8, 0xDC,
	0xDF, 0xE0, 0xED, 0xF0, 0xF7, 0xF8, 0xFC, 0xFD, 0xFE, 0xFF,
}

var fuzzRuneRanges = [][2]rune{
	{0x20, 0x7E},
	{0xA0, 0x2FF},
	{0x3000, 0x30FF},
	{0x4E00, 0x9FFF},
	{0xAC00, 0xD7A3},
	{0xFF61, 0xFF9F},
	{0x1F300, 0x1F64F},
}

func fuzzConfig(t *testing.T) (seed int64, iterations int) {
	iterations = 400
	if s := os.Getenv("COLLATIONS_FUZZ_ITERATIONS"); s != "" {
		var err error
		iterations, err = strconv.Atoi(s)
		require.NoError(t, err)
	}
	seed = 1
	if s := os.Getenv("COLLATIONS_FUZZ_SEED"); s != "" {
		var err error
		seed, err = strconv.ParseInt(s, 10, 64)
		require.NoError(t, err)
	}
	t.Logf("fuzzing with seed %d, %d iterations per charset", seed, iterations)
	return seed, iterations
}

func fuzzGenerate(rng *rand.Rand, cs charset.Charset) []byte {
	var out []byte
	switch rng.IntN(3) {
	case 0: // random bytes
		out = make([]byte, 1+rng.IntN(12))
		for i := range out {
			out[i] = byte(rng.IntN(256))
		}
	case 1: // boundary-byte soup
		out = make([]byte, 1+rng.IntN(12))
		for i := range out {
			out[i] = fuzzBoundaryBytes[rng.IntN(len(fuzzBoundaryBytes))]
		}
	default: // a valid string with a few mutated bytes
		var enc [8]byte
		for range 1 + rng.IntN(4) {
			rr := fuzzRuneRanges[rng.IntN(len(fuzzRuneRanges))]
			r := rr[0] + rune(rng.Int64N(int64(rr[1]-rr[0]+1)))
			if width := cs.EncodeRune(enc[:], r); width > 0 {
				out = append(out, enc[:width]...)
			}
		}
		for range rng.IntN(3) {
			if len(out) == 0 {
				break
			}
			out[rng.IntN(len(out))] = fuzzBoundaryBytes[rng.IntN(len(fuzzBoundaryBytes))]
		}
	}
	return out
}

func fuzzEightbit(collation string) charset.Charset {
	return colldata.Lookup(collations.MySQL8().LookupByName(collation)).Charset()
}

// queryHex runs a query whose single column is a HEX() expression and returns
// the decoded bytes (nil for NULL) along with whether MySQL raised an
// "Invalid ... character string" (1300) or "Cannot convert string" (3854 /
// 1977) style warning for it.
func queryHex(t *testing.T, conn *mysql.Conn, query string) (result []byte, flagged bool) {
	res, err := conn.ExecuteFetch(query, 1, false)
	require.NoError(t, err, "query: %s", query)
	require.Len(t, res.Rows, 1)

	warnings, err := conn.ExecuteFetch("SHOW WARNINGS", -1, false)
	require.NoError(t, err)
	for _, row := range warnings.Rows {
		switch row[1].ToString() {
		case "1287", "3719", "3778": // deprecated charset / collation notes
		default:
			flagged = true
		}
	}

	if res.Rows[0][0].IsNull() {
		return nil, flagged
	}
	result, err = hex.DecodeString(res.Rows[0][0].ToString())
	require.NoError(t, err)
	return result, flagged
}

func TestCharsetFuzzConversions(t *testing.T) {
	seed, iterations := fuzzConfig(t)

	charsets := []charset.Charset{
		fuzzEightbit("ascii_general_ci"),
		fuzzEightbit("greek_general_ci"),
		fuzzEightbit("latin2_general_ci"),
		charset.Charset_latin1{},
		charset.Charset_sjis{},
		charset.Charset_cp932{},
		charset.Charset_ujis{},
		charset.Charset_eucjpms{},
		charset.Charset_euckr{},
		charset.Charset_gb2312{},
		charset.Charset_gb18030{},
		charset.Charset_utf16{},
		charset.Charset_utf16le{},
		charset.Charset_ucs2{},
		charset.Charset_utf32{},
		charset.Charset_utf8mb4{},
	}

	conn := mysqlconn(t)
	defer conn.Close()
	_, err := conn.ExecuteFetch("SET sql_mode = ''", 0, false)
	require.NoError(t, err)

	for _, cs := range charsets {
		t.Run(cs.Name(), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 0))
			for range iterations {
				input := fuzzGenerate(rng, cs)
				compareConversions(t, conn, cs, input)
				if t.Failed() {
					return
				}
			}
		})
	}
}

func compareConversions(t *testing.T, conn *mysql.Conn, cs charset.Charset, input []byte) {
	relabeled, flagged := queryHex(t, conn, fmt.Sprintf(
		"SELECT HEX(CONVERT(X'%x' USING %s))", input, cs.Name()))
	vitessRelabeled, vitessErr := charset.ConvertFromBinary(nil, cs, input)

	if relabeled == nil || flagged {
		if vitessErr == nil {
			t.Errorf("%s: ConvertFromBinary(%X) = %X with no error, but MySQL flags the input (result %X)",
				cs.Name(), input, vitessRelabeled, relabeled)
		}
		return
	}
	if vitessErr == nil && !bytes.Equal(vitessRelabeled, relabeled) {
		t.Errorf("%s: ConvertFromBinary(%X) = %X, but MySQL relabels it as %X",
			cs.Name(), input, vitessRelabeled, relabeled)
		return
	}

	converted, flagged := queryHex(t, conn, fmt.Sprintf(
		"SELECT HEX(CONVERT(CONVERT(X'%x' USING %s) USING utf8mb4))", relabeled, cs.Name()))
	vitessConverted, vitessErr := charset.Convert(nil, charset.Charset_utf8mb4{}, relabeled, cs)

	if converted == nil || flagged {
		if vitessErr == nil {
			t.Errorf("%s: Convert(%X) to utf8mb4 = %X with no error, but MySQL flags the conversion (result %X)",
				cs.Name(), relabeled, vitessConverted, converted)
		}
		return
	}
	if !bytes.Equal(vitessConverted, converted) {
		// MySQL's eucjpms converter silently drops an incomplete multibyte
		// character at the end of the input, without the warning its sjis and
		// ujis counterparts raise; Vitess substitutes '?' and reports an
		// error, like it does for the other charsets.
		if cs.Name() == "eucjpms" && vitessErr != nil && bytes.HasPrefix(vitessConverted, converted) &&
			len(bytes.Trim(vitessConverted[len(converted):], "?")) == 0 {
			return
		}
		t.Errorf("%s: Convert(%X) to utf8mb4 = %X, but MySQL converts it to %X",
			cs.Name(), relabeled, vitessConverted, converted)
	}
}

func TestCharsetFuzzWalking(t *testing.T) {
	seed, iterations := fuzzConfig(t)

	// Only charsets that MySQL accepts as a client character set can be
	// walked over malformed input: raw bytes inside a string literal are the
	// only way to bypass the CONVERT() validity checks.
	charsets := []charset.Charset{
		fuzzEightbit("ascii_general_ci"),
		fuzzEightbit("greek_general_ci"),
		fuzzEightbit("latin2_general_ci"),
		charset.Charset_latin1{},
		charset.Charset_sjis{},
		charset.Charset_cp932{},
		charset.Charset_ujis{},
		charset.Charset_eucjpms{},
		charset.Charset_euckr{},
		charset.Charset_gb2312{},
		charset.Charset_gb18030{},
		charset.Charset_utf8mb4{},
	}

	for _, cs := range charsets {
		t.Run(cs.Name(), func(t *testing.T) {
			conn := mysqlconn(t)
			defer conn.Close()
			if _, err := conn.ExecuteFetch(fmt.Sprintf("SET NAMES '%s'", cs.Name()), 0, false); err != nil {
				t.Skipf("cannot use %s as the client charset: %v", cs.Name(), err)
			}
			_, err := conn.ExecuteFetch("SET sql_mode = 'NO_BACKSLASH_ESCAPES'", 0, false)
			require.NoError(t, err)

			rng := rand.New(rand.NewPCG(uint64(seed), 0))
			for range iterations {
				input := fuzzGenerate(rng, cs)
				// The quote and NUL bytes would end or break the literal; no
				// multibyte character in these charsets contains them, so
				// skipping them does not lose structural coverage.
				if len(input) == 0 || bytes.ContainsAny(input, "'\x00") {
					continue
				}
				res, err := conn.ExecuteFetch(fmt.Sprintf("SELECT CHAR_LENGTH('%s')", input), 1, false)
				require.NoError(t, err)
				mysqlLength, err := res.Rows[0][0].ToInt()
				require.NoError(t, err)
				if vitessLength := charset.Length(cs, input); vitessLength != mysqlLength {
					t.Errorf("%s: Length(%X) = %d, but MySQL counts %d characters",
						cs.Name(), input, vitessLength, mysqlLength)
					return
				}
			}
		})
	}
}
