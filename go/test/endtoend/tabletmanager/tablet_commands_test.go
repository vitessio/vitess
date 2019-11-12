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

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/mysql"
)

// TabletCommands tests the basic tablet commands
func TestTabletCommands(t *testing.T) {
	ctx := context.Background()

	masterConn, err := mysql.Connect(ctx, &masterTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &replicaTabletParams)
	if err != nil {
		t.Fatal(err)
	}
	defer replicaConn.Close()

	// Sanity Check
	exec(t, masterConn, "delete from t1")
	exec(t, masterConn, "insert into t1(id, value) values(1,'a'), (2,'b')")
	checkDataOnReplica(t, replicaConn, `[[VARCHAR("a")] [VARCHAR("b")]]`)

	// test exclude_field_names to vttablet works as expected
	sql := "select id, value from t1"
	args := []string{
		"VtTabletExecute",
		"-options", "included_fields:TYPE_ONLY",
		"-json",
		masterTablet.Alias,
		sql,
	}
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
	assertExcludeFields(t, result)

	// make sure direct dba queries work
	sql = "select * from t1"
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDba", "-json", masterTablet.Alias, sql)
	assertExecuteFetch(t, result)

	// check Ping / RefreshState / RefreshStateByShard
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Ping", masterTablet.Alias)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshState", masterTablet.Alias)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshStateByShard", keyspaceShard)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshStateByShard", "--cells="+cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")

	// Check basic actions.
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadOnly", masterTablet.Alias)
	assert.Nil(t, err, "error should be Nil")
	qr := exec(t, masterConn, "show variables like 'read_only'")
	got := fmt.Sprintf("%v", qr.Rows)
	want := "[[VARCHAR(\"read_only\") VARCHAR(\"ON\")]]"
	assert.Equal(t, want, got)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("SetReadWrite", masterTablet.Alias)
	assert.Nil(t, err, "error should be Nil")
	qr = exec(t, masterConn, "show variables like 'read_only'")
	got = fmt.Sprintf("%v", qr.Rows)
	want = "[[VARCHAR(\"read_only\") VARCHAR(\"OFF\")]]"
	assert.Equal(t, want, got)

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate")
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("Validate", "-ping-tablets=true")
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateKeyspace", keyspaceName)
	assert.Nil(t, err, "error should be Nil")
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateKeyspace", "-ping-tablets=true", keyspaceName)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateShard", "-ping-tablets=false", keyspaceShard)
	assert.Nil(t, err, "error should be Nil")

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ValidateShard", "-ping-tablets=true", keyspaceShard)
	assert.Nil(t, err, "error should be Nil")

}

func assertExcludeFields(t *testing.T, qr string) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(qr), &resultMap)
	if err != nil {
		t.Fatal(err)
	}

	rowsAffected := resultMap["rows_affected"]
	want := float64(2)
	assert.Equal(t, want, rowsAffected)

	fields := resultMap["fields"]
	assert.NotContainsf(t, fields, "name", "name should not be in field list")
}

func assertExecuteFetch(t *testing.T, qr string) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(qr), &resultMap)
	if err != nil {
		t.Fatal(err)
	}

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

	err := clusterInstance.VtctlclientProcess.ExecuteCommand("Sleep", masterTablet.Alias, "5s")
	time.Sleep(1 * time.Second)

	// try a frontend RefreshState that should timeout as the tablet is busy running the other one
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("RefreshState", masterTablet.Alias, "-wait-time", "2s")
	assert.Error(t, err, "timeout as tablet is in Sleep")
}

func TestHook(t *testing.T) {
	// test a regular program works
	runHookAndAssert(t, []string{
		"ExecuteHook", masterTablet.Alias, "test.sh", "--flag1", "--param1=hello"}, "0", false, "")

	// test stderr output
	runHookAndAssert(t, []string{
		"ExecuteHook", masterTablet.Alias, "test.sh", "--to-stderr"}, "0", false, "ERR: --to-stderr\n")

	// test commands that fail
	runHookAndAssert(t, []string{
		"ExecuteHook", masterTablet.Alias, "test.sh", "--exit-error"}, "1", false, "ERROR: exit status 1\n")

	// test hook that is not present
	runHookAndAssert(t, []string{
		"ExecuteHook", masterTablet.Alias, "not_here.sh", "--exit-error"}, "-1", false, "missing hook")

	// test hook with invalid name

	runHookAndAssert(t, []string{
		"ExecuteHook", masterTablet.Alias, "/bin/ls"}, "-1", true, "hook name cannot have")
}

func runHookAndAssert(t *testing.T, params []string, expectedStatus string, expectedError bool, expectedStderr string) {

	hr, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(params...)
	if expectedError {
		assert.Error(t, err, "Expected error")
	} else {
		if err != nil {
			t.Fatal(err)
		}

		resultMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(hr), &resultMap)
		if err != nil {
			t.Fatal(err)
		}

		exitStatus := reflect.ValueOf(resultMap["ExitStatus"]).Float()
		status := fmt.Sprintf("%.0f", exitStatus)
		assert.Equal(t, expectedStatus, status)

		stderr := reflect.ValueOf(resultMap["Stderr"]).String()
		assert.Contains(t, stderr, expectedStderr)
	}

}

func TestShardReplicationFix(t *testing.T) {
	// make sure the replica is in the replication graph, 2 nodes: 1 master, 1 replica
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")
	assertNodeCount(t, result, int(3))

	// Manually add a bogus entry to the replication graph, and check it is removed by ShardReplicationFix
	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ShardReplicationAdd", keyspaceShard, fmt.Sprintf("%s-9000", cell))
	assert.Nil(t, err, "error should be Nil")

	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")
	assertNodeCount(t, result, int(4))

	err = clusterInstance.VtctlclientProcess.ExecuteCommand("ShardReplicationFix", cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("GetShardReplication", cell, keyspaceShard)
	assert.Nil(t, err, "error should be Nil")
	assertNodeCount(t, result, int(3))
}

func assertNodeCount(t *testing.T, result string, want int) {
	resultMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(result), &resultMap)
	if err != nil {
		t.Fatal(err)
	}

	nodes := reflect.ValueOf(resultMap["nodes"])
	got := nodes.Len()
	assert.Equal(t, want, got)
}
