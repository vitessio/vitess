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

package unsharded

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// streamingConn opens a vtgate connection pinned to the OLAP workload so that
// queries are routed through vtgate's StreamExecute path instead of the buffered
// Execute path.
func streamingConn(t *testing.T) *mysql.Conn {
	t.Helper()
	vtParams := mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		Flags:  mysql.CapabilityClientMultiResults,
		DbName: "@primary",
	}
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	utils.Exec(t, conn, "set workload = olap")
	return conn
}

// TestStreamingCallProcedureRowsAffected verifies that a CALL of a procedure that
// performs DML reports the affected-row count to the client over the streaming
// (OLAP) path, matching the buffered Execute path that TestCallProcedure pins for
// the OLTP path.
func TestStreamingCallProcedureRowsAffected(t *testing.T) {
	conn := streamingConn(t)

	// sp_insert performs `insert into allDefaults () values ()`, returning an OK
	// packet that carries the affected-row count.
	qr := utils.Exec(t, conn, `CALL sp_insert()`)
	assert.EqualValues(t, 1, qr.RowsAffected, "streamed CALL must report affected rows to the client")
}
