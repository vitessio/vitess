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

package mysqlctl

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
)

// TestExecuteFetchContextTimeoutRedactsQuery verifies that when a query is
// killed because its context expired, the query logged on the kill path has
// its password redacted, like the exec log on the happy path.
func TestExecuteFetchContextTimeoutRedactsQuery(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	// Capture the structured log output.
	var logBuf bytes.Buffer
	oldLogger := log.SwapLogger(slog.New(slog.NewTextHandler(&logBuf, nil)))
	defer log.SwapLogger(oldLogger)

	query := `START xxx USER = 'vt_repl', PASSWORD = 'secret'`
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(query, &sqltypes.Result{})
	// Block the query until the timeout path kills the connection, so the
	// kill branch is taken deterministically.
	unblock := make(chan struct{})
	db.SetBeforeFunc(query, func() {
		<-unblock
	})
	db.AddQueryPatternWithCallback(`kill \d+`, &sqltypes.Result{}, func(string) {
		close(unblock)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := testMysqld.ExecuteSuperQueryList(ctx, []string{query})
	// The fake returns the blocked query's result successfully once the kill
	// releases it, so this lands in the "ExecuteFetch() may have succeeded
	// before we tried to kill it" branch. The query can only complete after
	// the kill callback fires, so reaching here proves the kill path ran.
	require.NoError(t, err)

	logs := logBuf.String()
	assert.Contains(t, logs, "killing connID")
	assert.Contains(t, logs, `PASSWORD = '****'`)
	assert.NotContains(t, logs, "secret")
}
