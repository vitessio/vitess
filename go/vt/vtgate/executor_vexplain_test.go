/*
Copyright 2024 The Vitess Authors.

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

package vtgate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func TestSimpleVexplainTrace(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	u := createSandbox(KsTestUnsharded)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	u.VSchema = unshardedVSchema
	serv := newSandboxForCells(ctx, []string{cell})
	resolver := newTestResolver(ctx, hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
				{Name: "col2", Type: sqltypes.Int32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG)},
				{Name: "weight_string(col2)", Type: sqltypes.VarBinary, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_BINARY_FLAG)},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				// i%4 ensures that there are duplicates across shards.
				// This will allow us to test that cross-shard ordering
				// still works correctly.
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NULL,
			}, {
				sqltypes.NewInt32(1),
				// i%4 ensures that there are duplicates across shards.
				// This will allow us to test that cross-shard ordering
				// still works correctly.
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NULL,
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(ctx, serv, cell, resolver)
	defer executor.Close()

	query := "vexplain trace select col1, col2 from music order by col2 desc"
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	gotResult, err := executorExec(ctx, executor, session, query, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select col1, col2, weight_string(col2) from music order by music.col2 desc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	expectedRowString := `{
	"OperatorType": "Route",
	"Variant": "Scatter",
	"Keyspace": {
		"Name": "TestExecutor",
		"Sharded": true
	},
	"NoOfCalls": 1,
	"Rows": [
		16
	],
	"FieldQuery": "select col1, col2, weight_string(col2) from music where 1 != 1",
	"OrderBy": "(1|2) DESC",
	"Query": "select col1, col2, weight_string(col2) from music order by music.col2 desc",
	"ResultColumns": 2,
	"Table": "music"
}`

	gotRowString := gotResult.Rows[0][0].ToString()
	require.Equal(t, expectedRowString, gotRowString)
}
