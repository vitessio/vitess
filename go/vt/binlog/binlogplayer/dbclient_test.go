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

package binlogplayer

import (
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/log/logtest"
)

// TestSetMultiStatementsOnAClosedConnection covers what a dead connection
// remembers. Closing it leaves the capability it negotiated on record, so a
// client that answered from that would tell its caller the capability was
// granted on a connection that cannot carry it, and the caller would go on to
// build a batch for it.
func TestSetMultiStatementsOnAClosedConnection(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { server.Close() })

	conn := mysql.NewConnForTest(client)
	conn.Capabilities = mysql.CapabilityClientMultiStatements
	conn.Close()

	dc := &dbClientImpl{dbConn: conn}
	require.Error(t, dc.SetMultiStatements(true), "a closed connection cannot grant the capability, whatever it last negotiated")
}

// TestTransactionErrorLogs checks that BEGIN, COMMIT and ROLLBACK errors are
// logged under error, truncated like the vreplication message.
func TestTransactionErrorLogs(t *testing.T) {
	const tail = "zq7errortail"
	longError := strings.Repeat("x", 2000) + tail

	db := fakesqldb.New(t)
	defer db.Close()
	for _, query := range []string{"begin", "commit", "rollback"} {
		db.AddRejectedQuery(query, errors.New(longError))
	}

	logs := logtest.Capture(t, slog.LevelInfo)
	for _, call := range []func(*dbClientImpl) error{(*dbClientImpl).Begin, (*dbClientImpl).Commit, (*dbClientImpl).Rollback} {
		conn, err := mysql.Connect(t.Context(), db.ConnParams())
		require.NoError(t, err)
		dc := &dbClientImpl{dbConn: conn}
		require.ErrorContains(t, call(dc), tail)
		dc.Close()
	}

	for _, msg := range []string{"BEGIN failed w/ error", "COMMIT failed w/ error", "ROLLBACK failed w/ error"} {
		record := logs.Record(t, msg)
		assert.Equal(t, "ERROR", record["level"])
		require.IsType(t, "", record["error"])
		assert.LessOrEqual(t, len(record["error"].(string)), 950)
		assert.Contains(t, record["error"], TruncationIndicator)
	}
}
