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
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeywordTable(t *testing.T) {
	for _, kw := range keywords {
		lookup, ok := keywordLookupTable.LookupString(kw.name)
		require.Truef(t, ok, "keyword %q failed to match", kw.name)
		require.Equalf(t, lookup, kw.id, "keyword %q matched to %d (expected %d)", kw.name, lookup, kw.id)
	}
}

// vitessReserved are words that MySQL does not reserve, but that the grammar
// cannot take as names.
var vitessReserved = map[string]bool{
	"ESCAPE":       true,
	"NEXT":         true,
	"OFF":          true,
	"SAVEPOINT":    true,
	"SQL_NO_CACHE": true,
}

// keywordNameForms are statements that take a word as a name in different
// places.
var keywordNameForms = []string{
	"create table %s (c1 int)",
	"create table t (%s int)",
	"create table t (c1 int, constraint %s check (c1 > 0))",
	"select 1 as %s from t",
	"select c1 from %s",
}

// TestCompatibility tests that the parser takes a word as a name where MySQL
// does, and rejects it where MySQL does, by the list of keywords of MySQL. A
// reserved word is a name when it is quoted, and after a dot.
func TestCompatibility(t *testing.T) {
	file, err := os.Open(path.Join("testdata", "mysql_keywords.txt"))
	require.NoError(t, err)
	defer file.Close()

	parser := NewTestParser()
	scanner := bufio.NewScanner(file)
	skipStep := 4
	for scanner.Scan() {
		if skipStep != 0 {
			skipStep--
			continue
		}

		afterSplit := strings.SplitN(scanner.Text(), "\t", 2)
		word, reserved := afterSplit[0], afterSplit[1] == "1"
		for _, form := range keywordNameForms {
			sql := fmt.Sprintf(form, word)
			_, err := parser.Parse(sql)
			switch {
			case word == "DUAL" && strings.HasPrefix(form, "select c1 from"):
				// FROM DUAL names no table.
			case reserved:
				assert.Errorf(t, err, "%s: MySQL reserves %s", sql, word)
			case !vitessReserved[word]:
				assert.NoErrorf(t, err, "%s: MySQL does not reserve %s", sql, word)
			}
			sql = fmt.Sprintf(form, "`"+word+"`")
			_, err = parser.Parse(sql)
			assert.NoErrorf(t, err, "%s", sql)
		}
		sql := fmt.Sprintf("select t.%s from t", word)
		_, err := parser.Parse(sql)
		assert.NoErrorf(t, err, "%s", sql)
	}
	require.NoError(t, scanner.Err())
}
