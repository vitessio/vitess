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

package fakesqldb

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log/logtest"
)

func TestUnknownQueryIsLogged(t *testing.T) {
	db := New(t)
	defer db.Close()
	conn, err := mysql.Connect(t.Context(), db.ConnParams())
	require.NoError(t, err)
	defer conn.Close()

	logs := logtest.Capture(t, slog.LevelInfo)
	_, err = conn.ExecuteFetch("select unknown_query", 1, false)
	require.Error(t, err)

	record := logs.Record(t, "Query not found")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "select unknown_query", record["query"])
}
