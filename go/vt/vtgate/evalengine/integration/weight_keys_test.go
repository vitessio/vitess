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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// A backend oracle checks comparison equivalence across the supported
// catalogue, including language contractions, expansions and ignorable weights.
func TestServerWeightKeyComparison(t *testing.T) {
	conn := mysqlconn(t)
	t.Cleanup(conn.Close)
	env := collations.NewEnvironment(conn.ServerVersion)
	corpus := []string{"", " ", "  ", "a", "a ", "A", "a\x00", "a\x00 ", "a\x01", "a\x1f", "a\u0301", "á", "ß", "ss", "a\u200d", "a\u200b", "ab", "ch", "cH", "ll", "lL", "dz", "dž", "æ", "œ", "İ", "ı", "ก", "เก"}
	catalogue, err := conn.ExecuteFetch("select collation_name,id from information_schema.collations order by id", 1000, true)
	require.NoError(t, err)
	for _, entry := range catalogue.Rows {
		id, _ := env.LookupID(entry[0].ToString())
		if id == collations.Unknown {
			continue
		}
		name, cs := env.LookupKnownCollation(id)
		t.Run(name, func(t *testing.T) {
			switch name {
			case "tis620_thai_ci", "cp1250_czech_cs", "gb18030_chinese_ci":
				t.Skip("MySQL weight-key order differs from strcmp; UNION ordering is covered directly")
			}
			properties, err := conn.ExecuteFetch("select pad_attribute from information_schema.collations where id="+entry[1].ToString(), 1, true)
			require.NoError(t, err)
			require.Len(t, properties.Rows, 1)
			pad, known := env.PadSpace(id)
			require.True(t, known)
			require.Equal(t, properties.Rows[0][0].ToString() == "PAD SPACE", pad)
			expr := func(value string) string {
				if id == collations.CollationBinaryID {
					return fmt.Sprintf("cast(_utf8mb4 X'%x' as binary)", []byte(value))
				}
				return fmt.Sprintf("convert(_utf8mb4 X'%x' using %s) collate %s", []byte(value), cs, name)
			}
			cmp := evalengine.OrderByParams{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID)}
			var space sqltypes.Value
			if pad {
				server, err := conn.ExecuteFetch("select weight_string("+expr(" ")+")", 1, true)
				require.NoError(t, err)
				space = server.Rows[0][0]
				if coll := colldata.Lookup(id); coll != nil {
					var encoded [4]byte
					n := coll.Charset().EncodeRune(encoded[:], ' ')
					require.Positive(t, n)
					cmp.PadSpaceWeight = coll.WeightString(nil, encoded[:n], 0)
					require.Equal(t, space.Raw(), cmp.PadSpaceWeight)
				} else {
					cmp.PadSpaceWeightCol = 1
				}
			}

			for _, left := range corpus {
				for _, right := range corpus {
					result, err := conn.ExecuteFetch("select strcmp("+expr(left)+","+expr(right)+"),weight_string("+expr(left)+"),weight_string("+expr(right)+")", 1, true)
					require.NoError(t, err)
					row := result.Rows[0]
					want, err := row[0].ToInt64()
					require.NoError(t, err)
					require.Equal(t, int(want), cmp.Compare(sqltypes.Row{row[1], space}, sqltypes.Row{row[2], space}), "left=%q right=%q weights=%x/%x space=%x", left, right, row[1].Raw(), row[2].Raw(), space.Raw())
				}
			}
		})
	}
}

// These values compare differently under SQL's collation order and strcmp.
// strcmp is therefore not a valid oracle for checking UNION weight-key sort
// behavior; the UNION ORDER BY result must be tested directly.
func TestServerWeightKeyOrderingDiffersFromStrcmp(t *testing.T) {
	conn := mysqlconn(t)
	t.Cleanup(conn.Close)
	for _, tc := range []struct{ collation, charset, left, right string }{
		{"tis620_thai_ci", "tis620", "a", "a\x00"},
		{"cp1250_czech_cs", "cp1250", "", "  "},
		{"gb18030_chinese_ci", "gb18030", "ss", "ı"},
	} {
		t.Run(tc.collation, func(t *testing.T) {
			expr := func(value string) string {
				return fmt.Sprintf("convert(_utf8mb4 X'%x' using %s) collate %s", value, tc.charset, tc.collation)
			}
			result, err := conn.ExecuteFetch("select strcmp("+expr(tc.left)+","+expr(tc.right)+"),weight_string("+expr(tc.left)+"),weight_string("+expr(tc.right)+"),weight_string("+expr(" ")+")", 1, true)
			require.NoError(t, err)
			row := result.Rows[0]
			want, err := row[0].ToInt64()
			require.NoError(t, err)
			cmp := evalengine.OrderByParams{Col: 0, WeightStringCol: -1, Type: evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID), PadSpaceWeightCol: 1}
			require.NotEqual(t, int(want), cmp.Compare(sqltypes.Row{row[1], row[3]}, sqltypes.Row{row[2], row[3]}))
		})
	}
}
