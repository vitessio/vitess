/*
Copyright 2019 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	getSchemaT1Results8030 = "CREATE TABLE `t1` (\n  `id` bigint NOT NULL,\n  `value` varchar(16) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3"
	getSchemaT1Results80   = "CREATE TABLE `t1` (\n  `id` bigint NOT NULL,\n  `value` varchar(16) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	getSchemaT1Results57   = "CREATE TABLE `t1` (\n  `id` bigint(20) NOT NULL,\n  `value` varchar(16) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8"
	getSchemaV1Results     = fmt.Sprintf("CREATE ALGORITHM=UNDEFINED DEFINER=`%s`@`%s` SQL SECURITY DEFINER VIEW {{.DatabaseName}}.`v1` AS select {{.DatabaseName}}.`t1`.`id` AS `id`,{{.DatabaseName}}.`t1`.`value` AS `value` from {{.DatabaseName}}.`t1`", username, hostname)
)

// TabletCommands tests the basic tablet commands
func TestTabletCommands(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.Nil(t, err)
	defer conn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	require.Nil(t, err)
	defer replicaConn.Close()

	// Sanity Check
	utils.Exec(t, conn, "delete from t1")
	utils.Exec(t, conn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	t.Run("ExecuteFetchAsDBA", func(t *testing.T) {
		// make sure direct dba queries work
		sql := "select * from t1"
		result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDBA", "--json", primaryTablet.Alias, sql)
		require.Nil(t, err)
		assertExecuteFetch(t, result)
	})

	t.Run("ExecuteMultiFetchAsDBA", func(t *testing.T) {
		// make sure direct dba queries work
		sql := "select * from t1; select * from t1 limit 100"
		result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("ExecuteMultiFetchAsDBA", "--json", primaryTablet.Alias, sql)
		require.Nil(t, err)
		assertExecuteMultiFetch(t, result)
	})
	// check Ping / RefreshState / RefreshStateByShard
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("PingTablet", primaryTablet.Alias)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("RefreshState", primaryTablet.Alias)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("RefreshStateByShard", keyspaceShard)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("RefreshStateByShard", "--cells", cell, keyspaceShard)
	require.Nil(t, err, "error should be Nil")

	// Check basic actions.
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("SetWritable", primaryTablet.Alias, "false")
	require.Nil(t, err, "error should be Nil")
	qr := utils.Exec(t, conn, "show variables like 'read_only'")
	got := fmt.Sprintf("%v", qr.Rows)
	want := "[[VARCHAR(\"read_only\") VARCHAR(\"ON\")]]"
	assert.Equal(t, want, got)

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("SetWritable", primaryTablet.Alias, "true")
	require.Nil(t, err, "error should be Nil")
	qr = utils.Exec(t, conn, "show variables like 'read_only'")
	got = fmt.Sprintf("%v", qr.Rows)
	want = "[[VARCHAR(\"read_only\") VARCHAR(\"OFF\")]]"
	assert.Equal(t, want, got)

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Validate")
	require.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("Validate", "--ping-tablets")
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ValidateKeyspace", keyspaceName)
	require.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ValidateKeyspace", "--ping-tablets", keyspaceName)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ValidateShard", "--ping-tablets", keyspaceShard)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ValidateShard", "--ping-tablets", keyspaceShard)
	require.Nil(t, err, "error should be Nil")

}

func assertExcludeFields(t *testing.T, qr string) {
	resultMap := make(map[string]any)
	err := json.Unmarshal([]byte(qr), &resultMap)
	require.Nil(t, err)

	rows := resultMap["rows"].([]any)
	assert.Equal(t, 2, len(rows))

	fields := resultMap["fields"]
	assert.NotContainsf(t, fields, "name", "name should not be in field list")
}

func assertExecuteFetch(t *testing.T, qr string) {
	resultMap := make(map[string]any)
	err := json.Unmarshal([]byte(qr), &resultMap)
	require.Nil(t, err)

	rows := reflect.ValueOf(resultMap["rows"])
	got := rows.Len()
	want := int(2)
	assert.Equal(t, want, got)

	fields := reflect.ValueOf(resultMap["fields"])
	got = fields.Len()
	want = int(2)
	assert.Equal(t, want, got)
}
func assertExecuteMultiFetch(t *testing.T, qr string) {
	resultMap := make([]map[string]any, 0)
	err := json.Unmarshal([]byte(qr), &resultMap)
	require.Nil(t, err)
	require.NotEmpty(t, resultMap)

	rows := reflect.ValueOf(resultMap[0]["rows"])
	got := rows.Len()
	want := int(2)
	assert.Equal(t, want, got)

	fields := reflect.ValueOf(resultMap[0]["fields"])
	got = fields.Len()
	want = int(2)
	assert.Equal(t, want, got)
}

func TestHook(t *testing.T) {
	// test a regular program works
	defer cluster.PanicHandler(t)
	runHookAndAssert(t, []string{
		"ExecuteHook", primaryTablet.Alias, "test.sh", "--", "--flag1", "--param1=hello"}, 0, false, "")

	// test stderr output
	runHookAndAssert(t, []string{
		"ExecuteHook", primaryTablet.Alias, "test.sh", "--", "--to-stderr"}, 0, false, "ERR: --to-stderr\n")

	// test commands that fail
	runHookAndAssert(t, []string{
		"ExecuteHook", primaryTablet.Alias, "test.sh", "--", "--exit-error"}, 1, false, "ERROR: exit status 1\n")

	// test hook that is not present
	runHookAndAssert(t, []string{
		"ExecuteHook", primaryTablet.Alias, "not_here.sh", "--", "--exit-error"}, -1, false, "missing hook")

	// test hook with invalid name

	runHookAndAssert(t, []string{
		"ExecuteHook", primaryTablet.Alias, "/bin/ls"}, -1, true, "hook name cannot have")
}

func runHookAndAssert(t *testing.T, params []string, expectedStatus int64, expectedError bool, expectedStderr string) {
	hr, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(params...)
	if expectedError {
		assert.Error(t, err, "Expected error")
	} else {
		require.Nil(t, err)

		var resp vtctldatapb.ExecuteHookResponse
		err = json2.Unmarshal([]byte(hr), &resp)
		require.Nil(t, err)

		assert.Equal(t, expectedStatus, resp.HookResult.ExitStatus)
		assert.Contains(t, resp.HookResult.Stderr, expectedStderr)
	}

}

func TestShardReplicationFix(t *testing.T) {
	// make sure the replica is in the replication graph, 2 nodes: 1 primary, 1 replica
	defer cluster.PanicHandler(t)
	result, err := clusterInstance.VtctldClientProcess.GetShardReplication(keyspaceName, shardName, cell)
	require.Nil(t, err, "error should be Nil")
	require.NotNil(t, result[cell], "result should not be Nil")
	assert.Len(t, result[cell].Nodes, 3)

	// Manually add a bogus entry to the replication graph, and check it is removed by ShardReplicationFix
	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ShardReplicationAdd", keyspaceShard, fmt.Sprintf("%s-9000", cell))
	require.Nil(t, err, "error should be Nil")

	result, err = clusterInstance.VtctldClientProcess.GetShardReplication(keyspaceName, shardName, cell)
	require.Nil(t, err, "error should be Nil")
	require.NotNil(t, result[cell], "result should not be Nil")
	assert.Len(t, result[cell].Nodes, 4)

	err = clusterInstance.VtctldClientProcess.ExecuteCommand("ShardReplicationFix", cell, keyspaceShard)
	require.Nil(t, err, "error should be Nil")
	result, err = clusterInstance.VtctldClientProcess.GetShardReplication(keyspaceName, shardName, cell)
	require.Nil(t, err, "error should be Nil")
	require.NotNil(t, result[cell], "result should not be Nil")
	assert.Len(t, result[cell].Nodes, 3)
}

func TestGetSchema(t *testing.T) {
	defer cluster.PanicHandler(t)

	res, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetSchema",
		"--include-views", "--tables", "t1,v1",
		fmt.Sprintf("%s-%d", clusterInstance.Cell, primaryTablet.TabletUID))
	require.Nil(t, err)

	t1Create := gjson.Get(res, "table_definitions.#(name==\"t1\").schema")
	assert.Contains(t, []string{getSchemaT1Results8030, getSchemaT1Results80, getSchemaT1Results57}, t1Create.String())
	v1Create := gjson.Get(res, "table_definitions.#(name==\"v1\").schema")
	assert.Equal(t, getSchemaV1Results, v1Create.String())
}
