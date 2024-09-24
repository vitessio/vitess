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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

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
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
				{Name: "weight_string(col2)"},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt32(1), sqltypes.NewInt32(int32(i % 4)), sqltypes.NULL},
				{sqltypes.NewInt32(2), sqltypes.NewInt32(int32(i % 4)), sqltypes.NULL},
			},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(ctx, serv, cell, resolver)
	defer executor.Close()

	query := "vexplain trace select count(*), col2 from music group by col2"
	session := &vtgatepb.Session{
		TargetString: "@primary",
	}
	gotResult, err := executorExec(ctx, executor, session, query, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select count(*), col2, weight_string(col2) from music group by col2, weight_string(col2) order by col2 asc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	expectedRowString := `{
	"OperatorType": "Aggregate",
	"Variant": "Ordered",
	"NoOfCalls": 1,
	"AvgNumberOfRows": 4,
	"MedianNumberOfRows": 4,
	"Aggregates": "sum_count_star(0) AS count(*)",
	"GroupBy": "(1|2)",
	"ResultColumns": 2,
	"Inputs": [
		{
			"OperatorType": "Route",
			"Variant": "Scatter",
			"Keyspace": {
				"Name": "TestExecutor",
				"Sharded": true
			},
			"NoOfCalls": 1,
			"AvgNumberOfRows": 16,
			"MedianNumberOfRows": 16,
			"ShardsQueried": 8,
			"FieldQuery": "select count(*), col2, weight_string(col2) from music where 1 != 1 group by col2, weight_string(col2)",
			"OrderBy": "(1|2) ASC",
			"Query": "select count(*), col2, weight_string(col2) from music group by col2, weight_string(col2) order by col2 asc",
			"Table": "music"
		}
	]
}`

	gotRowString := gotResult.Rows[0][0].ToString()
	require.Equal(t, expectedRowString, gotRowString)
}

func TestVExplainKeys(t *testing.T) {
	tests := []struct {
		query             string
		expectedRowString string
	}{
		{
			query: "select count(*), col2 from music group by col2",
			expectedRowString: `{
	"groupingColumns": [
		"music.col2"
	],
	"statementType": "SELECT"
}`,
		}, {
			query: "select * from user u join user_extra ue on u.id = ue.user_id where u.col1 > 100 and ue.noLimit = 'foo'",
			expectedRowString: `{
	"joinColumns": [
		"user.id",
		"user_extra.user_id"
	],
	"filterColumns": [
		"user.col1",
		"user_extra.noLimit"
	],
	"statementType": "SELECT"
}`,
		}, {
			// same as above, but written differently
			query: "select * from user_extra ue, user u where ue.noLimit = 'foo' and u.col1 > 100 and ue.user_id = u.id",
			expectedRowString: `{
	"joinColumns": [
		"user.id",
		"user_extra.user_id"
	],
	"filterColumns": [
		"user.col1",
		"user_extra.noLimit"
	],
	"statementType": "SELECT"
}`,
		},
		{
			query: "select u.foo, ue.bar, count(*) from user u join user_extra ue on u.id = ue.user_id where u.name = 'John Doe' group by 1, 2",
			expectedRowString: `{
	"groupingColumns": [
		"user.foo",
		"user_extra.bar"
	],
	"joinColumns": [
		"user.id",
		"user_extra.user_id"
	],
	"filterColumns": [
		"user.name"
	],
	"statementType": "SELECT"
}`,
		},
		{
			query: "select * from (select * from user) as derived where derived.amount > 1000",
			expectedRowString: `{
	"statementType": "SELECT"
}`,
		},
		{
			query: "select name, sum(amount) from user group by name",
			expectedRowString: `{
	"groupingColumns": [
		"user.name"
	],
	"statementType": "SELECT"
}`,
		},
		{
			query: "select name from user where age > 30",
			expectedRowString: `{
	"filterColumns": [
		"user.age"
	],
	"statementType": "SELECT"
}`,
		},
		{
			query: "select * from user where name = 'apa' union select * from user_extra where name = 'monkey'",
			expectedRowString: `{
	"filterColumns": [
		"user.name",
		"user_extra.name"
	],
	"statementType": "SELECT"
}`,
		},
		{
			query: "update user set name = 'Jane Doe' where id = 1",
			expectedRowString: `{
	"filterColumns": [
		"user.id"
	],
	"statementType": "UPDATE"
}`,
		},
		{
			query: "delete from user where order_date < '2023-01-01'",
			expectedRowString: `{
	"filterColumns": [
		"user.order_date"
	],
	"statementType": "DELETE"
}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			executor, _, _, _, _ := createExecutorEnv(t)
			session := NewSafeSession(&vtgatepb.Session{TargetString: "@primary"})
			gotResult, err := executor.Execute(context.Background(), nil, "Execute", session, "vexplain keys "+tt.query, nil)
			require.NoError(t, err)

			gotRowString := gotResult.Rows[0][0].ToString()
			assert.Equal(t, tt.expectedRowString, gotRowString)
			if t.Failed() {
				fmt.Println(gotRowString)
			}
		})
	}
}
