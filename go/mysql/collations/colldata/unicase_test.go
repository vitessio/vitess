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

package colldata

import (
	"bufio"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// goldenRows returns the tab-separated rows of a file in testdata. It skips
// the comment lines, which start with "#".
func goldenRows(t *testing.T, path string, columns int) [][]string {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	var rows [][]string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		row := strings.Split(line, "\t")
		require.Len(t, row, columns, "malformed row in %s: %q", path, line)
		rows = append(rows, row)
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, rows)
	return rows
}

// TestUnicaseTablesMatchMySQL checks the upper-case and lower-case form of
// every code point in every case table. The expected values were measured on
// MySQL: testdata/mysql_unicase_default.txt has a row for U+00FF, "ÿ", with
// the upper case U+0178, and no row for U+00DF, "ß", which MySQL does not
// change.
func TestUnicaseTablesMatchMySQL(t *testing.T) {
	tables := []struct {
		name string
		info *UnicaseInfo
	}{
		{"default", unicaseInfo_default},
		{"turkish", unicaseInfo_turkish},
		{"unicode520", unicaseInfo_unicode520},
	}

	for _, table := range tables {
		t.Run(table.name, func(t *testing.T) {
			type mapping struct{ upper, lower rune }
			expected := make(map[rune]mapping)
			for _, row := range goldenRows(t, "testdata/mysql_unicase_"+table.name+".txt", 3) {
				var codepoints [3]rune
				for i, column := range row {
					codepoint, err := strconv.ParseUint(column, 16, 32)
					require.NoError(t, err)
					codepoints[i] = rune(codepoint)
				}
				expected[codepoints[0]] = mapping{upper: codepoints[1], lower: codepoints[2]}
			}

			var mismatches int
			for codepoint := rune(0); codepoint <= 0x10FFFF; codepoint++ {
				want, ok := expected[codepoint]
				if !ok {
					want = mapping{upper: codepoint, lower: codepoint}
				}
				got := mapping{upper: table.info.toUpper(codepoint), lower: table.info.toLower(codepoint)}
				if got != want {
					mismatches++
					if mismatches <= 10 {
						assert.Equal(t, want, got, "U+%04X", codepoint)
					}
				}
			}
			assert.Zero(t, mismatches, "code points that differ from MySQL")
		})
	}
}

// TestCaseConversionMatchesMySQL converts strings to upper and lower case in
// every collation that testdata/mysql_case_conversion.txt lists, and compares
// the result with what MySQL returned. The upper case of "aɐb" (61 C9 90 62)
// is "AɐB" (41 C9 90 42) in utf8mb4_bin and "AⱯ" (41 E2 B1 AF) in
// utf8mb4_unicode_520_ci, where the three bytes of "Ɐ" overwrite the "b".
func TestCaseConversionMatchesMySQL(t *testing.T) {
	decode := func(column string) []byte {
		if column == "-" {
			return []byte{}
		}
		raw, err := hex.DecodeString(column)
		require.NoError(t, err)
		return raw
	}

	for _, row := range goldenRows(t, "testdata/mysql_case_conversion.txt", 4) {
		coll, ok := testcollation(t, row[0]).(CaseAwareCollation)
		require.True(t, ok, "%s has no case conversion", row[0])

		input := decode(row[1])
		assert.Equal(t, decode(row[2]), coll.ToUpper([]byte{}, input), "UPPER(X'%s') in %s", row[1], row[0])
		assert.Equal(t, decode(row[3]), coll.ToLower([]byte{}, input), "LOWER(X'%s') in %s", row[1], row[0])
	}
}

// TestCaseConversionMalformedInput converts strings that are not valid in the
// character set of the collation. MySQL rejects such a string in a query, so
// the expected values come from the MySQL source (my_caseup_utf8mb4,
// my_caseup_utf16 and my_caseup_mb in strings/), not from a measurement.
func TestCaseConversionMalformedInput(t *testing.T) {
	testcases := []struct {
		collation string
		input     string
		upper     string
	}{
		// The result ends before the incomplete character C3.
		{"utf8mb4_bin", "ab\xC3", "AB"},
		// A utf8mb3 string cannot have a four-byte character.
		{"utf8mb3_general_ci", "ab\xF0\x90\x90\xA8c", "AB"},
		// The high surrogate D800 has no low surrogate. The conversion
		// stops there and the rest of the input is unchanged.
		{"utf16_bin", "\x00a\xD8\x00\x00b", "\x00A\xD8\x00\x00b"},
		// The last character is two bytes short.
		{"utf32_general_ci", "\x00\x00\x00a\x00\x00", "\x00\x00\x00A\x00\x00"},
		// The Turkish collations convert to a second buffer. The result
		// also ends before the incomplete character.
		{"utf8mb4_turkish_ci", "i\xC3", "\xC4\xB0"},
		// The lead byte 82 has no second byte. It is a single byte, and
		// a single byte above 7F does not change.
		{"sjis_japanese_ci", "a\x82", "A\x82"},
	}

	for _, tc := range testcases {
		coll, ok := testcollation(t, tc.collation).(CaseAwareCollation)
		require.True(t, ok, "%s has no case conversion", tc.collation)

		assert.Equal(t, []byte(tc.upper), coll.ToUpper(nil, []byte(tc.input)), "UPPER(%q) in %s", tc.input, tc.collation)
	}
}
