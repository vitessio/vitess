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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
)

// TestSetExprsRejectUnsafeCharsets checks that a connection cannot be switched to
// a character set Vitess cannot parse safely through any of the paths that apply
// a SET to it. SET statements accept a safe character set. Connection settings,
// whether built into a settings query or applied directly on a reserved
// connection, refuse every character set change: their reset would restore the
// server's global character set, which need not be safe.
func TestSetExprsRejectUnsafeCharsets(t *testing.T) {
	parser := vtenv.NewTestEnv().Parser()

	accepted := []string{
		"set character_set_client = 'utf8mb4'",
		"set @@session.character_set_connection = latin1",
		"set character_set_results = null",
		"set collation_connection = 'utf8mb4_0900_ai_ci'",
		"set names utf8mb4",
		// the global scope is the operator's domain
		"set @@global.character_set_client = 'gbk'",
	}
	rejected := []string{
		"set character_set_client = 'gbk'",
		"set @@character_set_client = sjis",
		"set character_set_connection = 'cp932'",
		"set character_set_results = 'big5'",
		"set collation_connection = 'gb18030_unicode_520_ci'",
		"set names 'sjis'",
		"set sql_safe_updates = 1, character_set_client = 'gbk'",
		// these resolve to a character set that cannot be judged upfront
		"set character_set_client = default",
		"set names default",
		"set character_set_client = @charset",
		"set character_set_client = null",
	}

	// SET CHARACTER SET sets character_set_connection to the database's default
	// character set, whatever value it is given, so it is refused outright.
	characterSet := []string{
		"set character set 'latin1'",
		"set character set utf8mb4",
		"set charset cp932",
		"set character set utf16",
	}

	// user-defined variables named like the connection character set variables
	// change no connection setting, so a reserved connection's pre-queries may
	// assign them
	userVariables := []string{
		"set @charset = 'latin1'",
		"set @character_set_client = 'gbk'",
		"set @names = 'sjis'",
		"set @collation_connection = 'cp932_japanese_ci'",
	}

	const settingsErr = "the connection character set cannot be changed through connection settings"
	for _, setting := range userVariables {
		t.Run(setting, func(t *testing.T) {
			require.NoError(t, ValidateSettingsSQLMode([]string{setting}, parser, true))
			stmt, err := parser.Parse(setting)
			require.NoError(t, err)
			_, err = analyzeSet(stmt.(*sqlparser.Set))
			require.NoError(t, err)
		})
	}
	for _, setting := range accepted {
		t.Run(setting, func(t *testing.T) {
			stmt, err := parser.Parse(setting)
			require.NoError(t, err)
			_, err = analyzeSet(stmt.(*sqlparser.Set))
			require.NoError(t, err)
			_, _, err = BuildSettingQuery([]string{setting}, parser, true)
			require.ErrorContains(t, err, settingsErr)
			require.ErrorContains(t, ValidateSettingsSQLMode([]string{setting}, parser, true), settingsErr)
		})
	}
	for _, setting := range rejected {
		t.Run(setting, func(t *testing.T) {
			_, _, err := BuildSettingQuery([]string{setting}, parser, true)
			require.ErrorContains(t, err, settingsErr)
			require.ErrorContains(t, ValidateSettingsSQLMode([]string{setting}, parser, true), settingsErr)
			stmt, err := parser.Parse(setting)
			require.NoError(t, err)
			_, err = analyzeSet(stmt.(*sqlparser.Set))
			require.ErrorContains(t, err, "unsupported connection character set")
		})
	}
	for _, setting := range characterSet {
		t.Run(setting, func(t *testing.T) {
			_, _, err := BuildSettingQuery([]string{setting}, parser, true)
			require.ErrorContains(t, err, settingsErr)
			stmt, err := parser.Parse(setting)
			require.NoError(t, err)
			_, err = analyzeSet(stmt.(*sqlparser.Set))
			require.ErrorContains(t, err, "SET CHARACTER SET is not supported")
		})
	}
}
