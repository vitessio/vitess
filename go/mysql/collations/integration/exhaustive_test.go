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
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
)

// exhaustiveBatch is the number of strings whose weight strings one query
// asks MySQL for.
const exhaustiveBatch = 1000

// TestExhaustiveWeightStrings compares the weight strings of the collations
// whose tables come from MySQL's strings/ctype-*.cc with WEIGHT_STRING() on
// MySQL, for every character the tables have: every valid two-byte character
// of gbk, of big5 and of the two-byte range of gb18030, a sample of the
// four-byte characters of gb18030, and every string of one and of two bytes
// of the 8-bit collations. The weight strings are those of the SQL function,
// see sqlWeightString: in big5_chinese_ci, the ones that VTGate sorts by
// follow its comparison instead, see TestBig5ChineseCIIndexOrder.
func TestExhaustiveWeightStrings(t *testing.T) {
	conn := mysqlconn(t)
	defer conn.Close()

	testcases := []struct {
		collation string
		inputs    func(coll colldata.Collation) [][]byte
	}{
		{"gbk_chinese_ci", gbkCharacters},
		{"big5_chinese_ci", big5Characters},
		{"gbk_bin", gbkCharacters},
		{"big5_bin", big5Characters},
		{"gb18030_chinese_ci", gb18030Characters},
		{"latin1_german2_ci", oneAndTwoBytes},
		{"tis620_thai_ci", oneAndTwoBytes},
		{"tis620_bin", oneAndTwoBytes},
		{"latin2_czech_cs", oneAndTwoBytes},
		{"cp1250_czech_cs", oneAndTwoBytes},
	}
	for _, tc := range testcases {
		t.Run(tc.collation, func(t *testing.T) {
			coll := lookupCollation(t, tc.collation)
			inputs := tc.inputs(coll)
			for batch := range slices.Chunk(inputs, exhaustiveBatch) {
				remote := remoteWeightStrings(t, conn, coll, batch)
				for i, input := range batch {
					local := strings.ToUpper(hex.EncodeToString(sqlWeightString(coll, input)))
					require.Equal(t, remote[i], local, "WEIGHT_STRING(x'%X' COLLATE %s)", input, tc.collation)
				}
			}
			t.Logf("%s: %d strings", tc.collation, len(inputs))
		})
	}
}

// TestBig5ChineseCIIndexOrder checks that big5_chinese_ci orders every
// two-byte big5 character as an index of MySQL does, which is the order of
// its comparison. MySQL's WEIGHT_STRING() and a filesort order them by
// strokes instead (https://bugs.mysql.com/bug.php?id=65899).
func TestBig5ChineseCIIndexOrder(t *testing.T) {
	conn := mysqlconn(t)
	defer conn.Close()

	coll := lookupCollation(t, "big5_chinese_ci")
	inputs := big5Characters(coll)

	exec(t, conn, "CREATE TEMPORARY TABLE big5_order (s VARCHAR(2) CHARACTER SET big5 COLLATE big5_chinese_ci NOT NULL, KEY (s))")
	defer exec(t, conn, "DROP TEMPORARY TABLE big5_order")
	for batch := range slices.Chunk(inputs, exhaustiveBatch) {
		values := make([]string, len(batch))
		for i, input := range batch {
			values[i] = fmt.Sprintf("(_big5 x'%X')", input)
		}
		exec(t, conn, "INSERT INTO big5_order VALUES "+strings.Join(values, ","))
	}

	res := exec(t, conn, "SELECT HEX(s) FROM big5_order FORCE INDEX (s) ORDER BY s")
	require.Len(t, res.Rows, len(inputs))
	remote := make([]string, len(res.Rows))
	for i, row := range res.Rows {
		remote[i] = row[0].ToString()
	}

	local := slices.Clone(inputs)
	slices.SortFunc(local, func(a, b []byte) int { return coll.Collate(a, b, false) })
	for i, input := range local {
		assert.Equal(t, remote[i], fmt.Sprintf("%X", input), "position %d", i)
	}
}

func lookupCollation(t *testing.T, name string) colldata.Collation {
	env := collations.MySQL8()
	coll := colldata.Lookup(env.LookupByName(name))
	require.NotNil(t, coll, "collation %s", name)
	return coll
}

// remoteWeightStrings returns WEIGHT_STRING() of each input in hex, from one
// query.
func remoteWeightStrings(t *testing.T, conn *mysql.Conn, coll colldata.Collation, inputs [][]byte) []string {
	exprs := make([]string, len(inputs))
	for i, input := range inputs {
		exprs[i] = fmt.Sprintf("HEX(WEIGHT_STRING(_%s x'%X' COLLATE %s))", coll.Charset().Name(), input, coll.Name())
	}
	res := exec(t, conn, "SELECT "+strings.Join(exprs, ","))
	require.Len(t, res.Rows, 1)
	remote := make([]string, len(inputs))
	for i, value := range res.Rows[0] {
		remote[i] = value.ToString()
	}
	return remote
}

// validCharacters returns the strings of candidates that are one valid
// character of the character set of coll.
func validCharacters(coll colldata.Collation, candidates [][]byte) [][]byte {
	cs := coll.Charset()
	var valid [][]byte
	for _, candidate := range candidates {
		if _, width, ok := cs.DecodeRune(candidate); ok && width == len(candidate) {
			valid = append(valid, candidate)
		}
	}
	return valid
}

// twoByteCharacters returns the two-byte strings with a first byte from
// first to last and a second byte in one of the ranges of seconds.
func twoByteCharacters(first, last byte, seconds ...[2]byte) [][]byte {
	var out [][]byte
	for lead := int(first); lead <= int(last); lead++ {
		for _, r := range seconds {
			for trail := int(r[0]); trail <= int(r[1]); trail++ {
				out = append(out, []byte{byte(lead), byte(trail)})
			}
		}
	}
	return out
}

func gbkCharacters(coll colldata.Collation) [][]byte {
	return validCharacters(coll, twoByteCharacters(0x81, 0xFE, [2]byte{0x40, 0x7E}, [2]byte{0x80, 0xFE}))
}

func big5Characters(coll colldata.Collation) [][]byte {
	return validCharacters(coll, twoByteCharacters(0xA1, 0xF9, [2]byte{0x40, 0x7E}, [2]byte{0xA1, 0xFE}))
}

// gb18030Characters returns every two-byte character of gb18030 and one in
// 101 of its four-byte characters, which are 81 30 81 30 to FE 39 FE 39.
func gb18030Characters(coll colldata.Collation) [][]byte {
	candidates := twoByteCharacters(0x81, 0xFE, [2]byte{0x40, 0x7E}, [2]byte{0x80, 0xFE})
	n := 0
	for b1 := 0x81; b1 <= 0xFE; b1++ {
		for b2 := 0x30; b2 <= 0x39; b2++ {
			for b3 := 0x81; b3 <= 0xFE; b3++ {
				for b4 := 0x30; b4 <= 0x39; b4++ {
					if n%101 == 0 {
						candidates = append(candidates, []byte{byte(b1), byte(b2), byte(b3), byte(b4)})
					}
					n++
				}
			}
		}
	}
	return validCharacters(coll, candidates)
}

func oneAndTwoBytes(colldata.Collation) [][]byte {
	var out [][]byte
	for b := range 256 {
		out = append(out, []byte{byte(b)})
	}
	for b := range 1 << 16 {
		out = append(out, []byte{byte(b >> 8), byte(b)})
	}
	return out
}
