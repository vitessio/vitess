/*
Copyright 2025 The Vitess Authors.

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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtorc/config"
)

func TestGetSqlitePragmaSQLs(t *testing.T) {
	origSqliteDataPath := config.GetSQLiteDataFile()
	defer config.SetSQLiteDataFile(origSqliteDataPath)

	// no shared-cache mode
	config.SetSQLiteDataFile("file::memory:?mode=memory")
	require.Equal(t, []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	}, getSqlitePragmaSQLs())

	// shared-cache mode
	config.SetSQLiteDataFile("file::memory:?mode=memory&cache=shared")
	require.Equal(t, []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA read_uncommitted = 1",
	}, getSqlitePragmaSQLs())
}
