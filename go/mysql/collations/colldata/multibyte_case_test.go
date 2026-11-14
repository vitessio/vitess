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
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultibyteCaseTablesMatchMySQL checks the case table of each multi-byte
// character set against the characters that UPPER() or LOWER() changes in
// MySQL. testdata/mysql_multibyte_case_sjis.txt has a row for the full-width
// "ａ" (82 81) with the upper case "Ａ" (82 60).
//
// The test converts every character in the file, and checks that the table
// has no multi-byte character that is not in the file.
func TestMultibyteCaseTablesMatchMySQL(t *testing.T) {
	tables := []struct {
		charset    string
		collations []string
		caseinfo   *multibyteCase
	}{
		{"sjis", []string{"sjis_bin", "sjis_japanese_ci"}, multibyteCase_sjis},
		{"cp932", []string{"cp932_bin", "cp932_japanese_ci"}, multibyteCase_cp932},
		{"ujis", []string{"ujis_bin", "ujis_japanese_ci"}, multibyteCase_ujis},
		{"eucjpms", []string{"eucjpms_bin", "eucjpms_japanese_ci"}, multibyteCase_eucjpms},
		{"euckr", []string{"euckr_bin", "euckr_korean_ci"}, multibyteCase_euckr},
		{"gb2312", []string{"gb2312_bin", "gb2312_chinese_ci"}, multibyteCase_gb2312},
		{"gbk", []string{"gbk_bin", "gbk_chinese_ci"}, multibyteCase_gbk},
		{"big5", []string{"big5_bin", "big5_chinese_ci"}, multibyteCase_big5},
	}

	for _, table := range tables {
		t.Run(table.charset, func(t *testing.T) {
			rows := goldenRows(t, "testdata/mysql_multibyte_case_"+table.charset+".txt", 3)

			measured := make(map[uint32]bool)
			for _, row := range rows {
				var char, upper, lower []byte
				for i, dst := range []*[]byte{&char, &upper, &lower} {
					raw, err := hex.DecodeString(row[i])
					require.NoError(t, err)
					*dst = raw
				}
				if len(char) > 1 {
					code, err := strconv.ParseUint(row[0], 16, 32)
					require.NoError(t, err)
					measured[uint32(code)] = true
				}

				for _, name := range table.collations {
					coll, ok := testcollation(t, name).(CaseAwareCollation)
					require.True(t, ok, "%s has no case conversion", name)

					assert.Equal(t, upper, coll.ToUpper(nil, char), "UPPER(X'%s') in %s", row[0], name)
					assert.Equal(t, lower, coll.ToLower(nil, char), "LOWER(X'%s') in %s", row[0], name)
				}
			}

			for _, char := range table.caseinfo.chars {
				assert.True(t, measured[char.code], "MySQL does not change X'%X'", char.code)
			}
		})
	}
}
