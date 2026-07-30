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

package transaction

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestStreamedSavepointRollbackPrunesRedoLog verifies that savepoint and
// rollback-to-savepoint statements executed over the streaming path maintain
// the transaction's redo log like the non-streaming path: DML rolled back to
// a savepoint must not be recorded for 2PC recovery replay. Streamed
// savepoints that bypass the transaction bookkeeping would leave rolled-back
// DML in the redo log, and crash recovery would re-apply it.
func TestStreamedSavepointRollbackPrunesRedoLog(t *testing.T) {
	defer cleanup(t)

	shard := clusterInstance.Keyspaces[0].Shards[0]
	primary := shard.FindPrimaryTablet()
	tablet := getTablet(primary.GrpcPort)

	ctx := t.Context()
	conn, err := tabletconn.GetDialer()(ctx, tablet, grpcclient.FailFast(false))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.WithoutCancel(ctx)) })

	target := &querypb.Target{Keyspace: keyspaceName, Shard: shard.Name, TabletType: topodatapb.TabletType_PRIMARY}

	state, err := conn.Begin(ctx, nil, target, nil)
	require.NoError(t, err)

	streamExec := func(sql string) error {
		return conn.StreamExecute(ctx, nil, target, sql, nil, state.TransactionID, 0, nil, func(*sqltypes.Result) error { return nil })
	}
	require.NoError(t, streamExec("insert into twopc_user(id, name) values (7, 'kept')"))
	require.NoError(t, streamExec("savepoint sp"))
	require.NoError(t, streamExec("insert into twopc_user(id, name) values (8, 'discarded')"))
	require.NoError(t, streamExec("rollback to sp"))

	dtid := "streamed-savepoint-test-dtid"
	require.NoError(t, conn.Prepare(ctx, target, state.TransactionID, dtid))
	t.Cleanup(func() {
		require.NoError(t, conn.RollbackPrepared(context.WithoutCancel(ctx), target, dtid, 0))
	})

	qr, err := primary.VttabletProcess.QueryTabletWithDB("select statement from redo_statement order by id", sidecarDBName)
	require.NoError(t, err)
	var statements []string
	for _, row := range qr.Rows {
		statements = append(statements, row[0].ToString())
	}
	assert.True(t, slices.ContainsFunc(statements, func(stmt string) bool { return strings.Contains(stmt, "'kept'") }),
		"the redo log must record the transaction's kept DML")
	for _, stmt := range statements {
		assert.NotContains(t, stmt, "'discarded'",
			"DML rolled back to a savepoint must not be recorded in the redo log for 2PC recovery replay")
	}
}
