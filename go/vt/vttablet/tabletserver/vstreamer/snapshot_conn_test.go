/*
Copyright 2020 The Vitess Authors.

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

package vstreamer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func TestStartSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, 'aaa')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})

	ctx := context.Background()
	conn, err := snapshotConnect(ctx, env.TabletEnv.Config().DB.AppWithDB())
	require.NoError(t, err)
	defer conn.Close()

	conn.startSnapshot(ctx, "t1")

	// This second row should not be in the result.
	execStatement(t, "insert into t1 values(2, 'bbb')")

	wantqr := &sqltypes.Result{
		Rows: [][]sqltypes.Value{
			{sqltypes.NewInt32(1), sqltypes.NewVarBinary("aaa")},
		},
		StatusFlags: sqltypes.ServerStatusNoIndexUsed | sqltypes.ServerStatusAutocommit | sqltypes.ServerStatusInTrans,
	}
	qr, err := conn.ExecuteFetch("select * from t1", 10, false)
	require.NoError(t, err)
	assert.Equal(t, wantqr, qr)
}
