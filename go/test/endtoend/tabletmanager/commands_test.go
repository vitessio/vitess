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
	"time"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
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

	// make sure direct dba queries work
	sql := "select * from t1"
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDba", "--", "--json", primaryTablet.Alias, sql)
	require.Nil(t, err)
	assertExecuteFetch(t, result)

	// check Ping / RefreshState / RefreshStateByShard
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Ping", primaryTablet.Alias)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshState", primaryTablet.Alias)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshStateByShard", keyspaceShard)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshStateByShard", "--", "--cells="+cell, keyspaceShard)
	require.Nil(t, err, "error should be Nil")

	// Check basic actions.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadOnly", primaryTablet.Alias)
	require.Nil(t, err, "error should be Nil")
	qr := utils.Exec(t, conn, "show variables like 'read_only'")
	got := fmt.Sprintf("%v", qr.Rows)
	want := "[[VARCHAR(\"read_only\") VARCHAR(\"ON\")]]"
	assert.Equal(t, want, got)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadWrite", primaryTablet.Alias)
	require.Nil(t, err, "error should be Nil")
	qr = utils.Exec(t, conn, "show variables like 'read_only'")
	got = fmt.Sprintf("%v", qr.Rows)
	want = "[[VARCHAR(\"read_only\") VARCHAR(\"OFF\")]]"
	assert.Equal(t, want, got)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	require.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate", "--", "--ping-tablets=true")
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateKeyspace", keyspaceName)
	require.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateKeyspace", "--", "--ping-tablets=true", keyspaceName)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateShard", "--", "--ping-tablets=false", keyspaceShard)
	require.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateShard", "--", "--ping-tablets=true", keyspaceShard)
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

// ActionAndTimeout test
func TestActionAndTimeout(t *testing.T) {

	defer cluster.PanicHandler(t)
	err := clusterInstance.VtctlclientProcess.ExecuteCommand("Sleep", primaryTablet.Alias, "5s")
	require.Nil(t, err)
	time.Sleep(1 * time.Second)

	// try a frontend RefreshState that should timeout as the tablet is busy running the other one
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshState", "--", primaryTablet.Alias, "--wait-time", "2s")
	assert.Error(t, err, "timeout as tablet is in Sleep")
}

func TestHook(t *testing.T) {
	// test a regular program works
	defer cluster.PanicHandler(t)
	runHookAndAssert(t, []string{
		"ExecuteHook", "--", primaryTablet.Alias, "test.sh", "--flag1", "--param1=hello"}, "0", false, "")

	// test stderr output
	runHookAndAssert(t, []string{
		"ExecuteHook", "--", primaryTablet.Alias, "test.sh", "--to-stderr"}, "0", false, "ERR: --to-stderr\n")

	// test commands that fail
	runHookAndAssert(t, []string{
		"ExecuteHook", "--", primaryTablet.Alias, "test.sh", "--exit-error"}, "1", false, "ERROR: exit status 1\n")

	// test hook that is not present
	runHookAndAssert(t, []string{
		"ExecuteHook", "--", primaryTablet.Alias, "not_here.sh", "--exit-error"}, "-1", false, "missing hook")

	// test hook with invalid name

	runHookAndAssert(t, []string{
		"ExecuteHook", "--", primaryTablet.Alias, "/bin/ls"}, "-1", true, "hook name cannot have")
}

func runHookAndAssert(t *testing.T, params []string, expectedStatus string, expectedError bool, expectedStderr string) {

	hr, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(params...)
	if expectedError {
		assert.Error(t, err, "Expected error")
	} else {
		require.Nil(t, err)

		resultMap := make(map[string]any)
		err = json.Unmarshal([]byte(hr), &resultMap)
		require.Nil(t, err)

		exitStatus := reflect.ValueOf(resultMap["ExitStatus"]).Float()
		status := fmt.Sprintf("%.0f", exitStatus)
		assert.Equal(t, expectedStatus, status)

		stderr := reflect.ValueOf(resultMap["Stderr"]).String()
		assert.Contains(t, stderr, expectedStderr)
	}

}

func TestShardReplicationFix(t *testing.T) {
	// make sure the replica is in the replication graph, 2 nodes: 1 primary, 1 replica
	defer cluster.PanicHandler(t)
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	require.Nil(t, err, "error should be Nil")
	assertNodeCount(t, result, int(3))

	// Manually add a bogus entry to the replication graph, and check it is removed by ShardReplicationFix
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ShardReplicationAdd", keyspaceShard, fmt.Sprintf("%s-9000", cell))
	require.Nil(t, err, "error should be Nil")

	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	require.Nil(t, err, "error should be Nil")
	assertNodeCount(t, result, int(4))

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ShardReplicationFix", cell, keyspaceShard)
	require.Nil(t, err, "error should be Nil")
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	require.Nil(t, err, "error should be Nil")
	assertNodeCount(t, result, int(3))
}

func TestGetSchema(t *testing.T) {
	defer cluster.PanicHandler(t)

	res, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetSchema", "--",
		"--include-views", "--tables", "t1,v1",
		fmt.Sprintf("%s-%d", clusterInstance.Cell, primaryTablet.TabletUID))
	require.Nil(t, err)

	t1Create := gjson.Get(res, "table_definitions.#(name==\"t1\").schema")
	assert.Contains(t, []string{getSchemaT1Results8030, getSchemaT1Results80, getSchemaT1Results57}, t1Create.String())
	v1Create := gjson.Get(res, "table_definitions.#(name==\"v1\").schema")
	assert.Equal(t, getSchemaV1Results, v1Create.String())
}

func assertNodeCount(t *testing.T, result string, want int) {
	resultMap := make(map[string]any)
	err := json.Unmarshal([]byte(result), &resultMap)
	require.Nil(t, err)

	nodes := reflect.ValueOf(resultMap["nodes"])
	got := nodes.Len()
	assert.Equal(t, want, got)
}
