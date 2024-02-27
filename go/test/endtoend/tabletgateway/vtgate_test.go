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

This tests select/insert using the unshared keyspace added in main_test
*/
package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	vtorcutils "vitess.io/vitess/go/test/endtoend/vtorc/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
)

func TestVtgateHealthCheck(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	verifyVtgateVariables(t, clusterInstance.VtgateProcess.VerifyURL)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := utils.Exec(t, conn, "show vitess_tablets")
	assert.Equal(t, 3, len(qr.Rows), "wrong number of results from show")
}

func TestVtgateReplicationStatusCheck(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	verifyVtgateVariables(t, clusterInstance.VtgateProcess.VerifyURL)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams) // VTGate
	require.NoError(t, err)
	defer conn.Close()

	// Only returns rows for REPLICA and RDONLY tablets -- so should be 2 of them
	qr := utils.Exec(t, conn, "show vitess_replication_status like '%'")
	expectNumRows := 2
	numRows := len(qr.Rows)
	assert.Equal(t, expectNumRows, numRows, fmt.Sprintf("wrong number of results from show vitess_replication_status. Expected %d, got %d", expectNumRows, numRows))

	// Disable VTOrc(s) recoveries so that it doesn't immediately repair/restart replication.
	for _, vtorcProcess := range clusterInstance.VTOrcProcesses {
		vtorcutils.DisableGlobalRecoveries(t, vtorcProcess)
	}
	// Re-enable recoveries afterward as the cluster is re-used.
	defer func() {
		for _, vtorcProcess := range clusterInstance.VTOrcProcesses {
			vtorcutils.EnableGlobalRecoveries(t, vtorcProcess)
		}
	}()
	// Stop replication on the non-PRIMARY tablets.
	_, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDBA", clusterInstance.Keyspaces[0].Shards[0].Replica().Alias, "stop slave")
	require.NoError(t, err)
	_, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDBA", clusterInstance.Keyspaces[0].Shards[0].Rdonly().Alias, "stop slave")
	require.NoError(t, err)
	// Restart replication afterward as the cluster is re-used.
	defer func() {
		_, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDBA", clusterInstance.Keyspaces[0].Shards[0].Replica().Alias, "start slave")
		require.NoError(t, err)
		_, err = clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDBA", clusterInstance.Keyspaces[0].Shards[0].Rdonly().Alias, "start slave")
		require.NoError(t, err)
	}()
	time.Sleep(2 * time.Second) // Build up some replication lag
	res, err := conn.ExecuteFetch("show vitess_replication_status", 2, false)
	require.NoError(t, err)
	expectNumRows = 2
	numRows = len(qr.Rows)
	assert.Equal(t, expectNumRows, numRows, fmt.Sprintf("wrong number of results from show vitess_replication_status, expected %d, got %d", expectNumRows, numRows))
	rawLag := res.Named().Rows[0]["ReplicationLag"] // Let's just look at the first row
	lagInt, _ := rawLag.ToInt64()                   // Don't check the error as the value could be "NULL"
	assert.True(t, rawLag.IsNull() || lagInt > 0, "replication lag should be NULL or greater than 0 but was: %s", rawLag.ToString())
}

func TestVtgateReplicationStatusCheckWithTabletTypeChange(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	verifyVtgateVariables(t, clusterInstance.VtgateProcess.VerifyURL)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Only returns rows for REPLICA and RDONLY tablets -- so should be 2 of them
	qr := utils.Exec(t, conn, "show vitess_replication_status like '%'")
	expectNumRows := 2
	numRows := len(qr.Rows)
	assert.Equal(t, expectNumRows, numRows, fmt.Sprintf("wrong number of results from show vitess_replication_status. Expected %d, got %d", expectNumRows, numRows))

	// change the RDONLY tablet to SPARE
	rdOnlyTablet := clusterInstance.Keyspaces[0].Shards[0].Rdonly()
	err = clusterInstance.VtctlclientChangeTabletType(rdOnlyTablet, topodata.TabletType_SPARE)
	require.NoError(t, err)
	// Change it back to RDONLY afterward as the cluster is re-used.
	defer func() {
		err = clusterInstance.VtctlclientChangeTabletType(rdOnlyTablet, topodata.TabletType_RDONLY)
		require.NoError(t, err)
	}()

	// Only returns rows for REPLICA and RDONLY tablets -- so should be 1 of them since we updated 1 to spare
	qr = utils.Exec(t, conn, "show vitess_replication_status like '%'")
	expectNumRows = 1
	numRows = len(qr.Rows)
	assert.Equal(t, expectNumRows, numRows, fmt.Sprintf("wrong number of results from show vitess_replication_status. Expected %d, got %d", expectNumRows, numRows))
}

func verifyVtgateVariables(t *testing.T, url string) {
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "Vtgate api url response not found")

	resultMap := make(map[string]any)
	respByte, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(respByte, &resultMap)
	require.NoError(t, err)
	assert.Contains(t, resultMap, "VtgateVSchemaCounts", "Vschema count should be present in variables")

	vschemaCountMap := getMapFromJSON(resultMap, "VtgateVSchemaCounts")
	assert.Contains(t, vschemaCountMap, "Reload", "Reload count should be present in vschemacount")

	object := reflect.ValueOf(vschemaCountMap["Reload"])
	assert.Greater(t, object.NumField(), 0, "Reload count should be greater than 0")
	assert.NotContains(t, vschemaCountMap, "WatchError", "There should not be any WatchError in VschemaCount")
	assert.NotContains(t, vschemaCountMap, "Parsing", "There should not be any Parsing in VschemaCount")
	assert.Contains(t, resultMap, "HealthcheckConnections", "HealthcheckConnections count should be present in variables")

	healthCheckConnection := getMapFromJSON(resultMap, "HealthcheckConnections")
	assert.NotEmpty(t, healthCheckConnection, "Atleast one healthy tablet needs to be present")
	assert.True(t, isPrimaryTabletPresent(healthCheckConnection), "Atleast one primary tablet needs to be present")
}

func retryNTimes(t *testing.T, maxRetries int, f func() bool) {
	i := 0
	for {
		res := f()
		if res {
			return
		}
		if i > maxRetries {
			t.Fatalf("retried %d times and failed", maxRetries)
			return
		}
		i++
	}
}

func TestReplicaTransactions(t *testing.T) {
	// TODO(deepthi): this test seems to depend on previous test. Fix tearDown so that tests are independent
	defer cluster.PanicHandler(t)
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	ctx := context.Background()
	writeConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer writeConn.Close()

	readConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer readConn.Close()

	readConn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer readConn2.Close()

	fetchAllCustomers := "select id, email from customer"
	checkCustomerRows := func(expectedRows int) func() bool {
		return func() bool {
			result := utils.Exec(t, readConn2, fetchAllCustomers)
			return len(result.Rows) == expectedRows
		}
	}

	// point the replica connections to the replica target
	utils.Exec(t, readConn, "use @replica")
	utils.Exec(t, readConn2, "use @replica")

	// insert a row using primary
	utils.Exec(t, writeConn, "insert into customer(id, email) values(1,'email1')")

	// we'll run this query a number of times, and then give up if the row count never reaches this value
	retryNTimes(t, 10 /*maxRetries*/, checkCustomerRows(1))

	// after a short pause, SELECT the data inside a tx on a replica
	// begin transaction on replica
	utils.Exec(t, readConn, "begin")
	qr := utils.Exec(t, readConn, fetchAllCustomers)
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")]]`, fmt.Sprintf("%v", qr.Rows), "select returned wrong result")

	// insert more data on primary using a transaction
	utils.Exec(t, writeConn, "begin")
	utils.Exec(t, writeConn, "insert into customer(id, email) values(2,'email2')")
	utils.Exec(t, writeConn, "commit")

	retryNTimes(t, 10 /*maxRetries*/, checkCustomerRows(2))

	// replica doesn't see new row because it is in a transaction
	qr2 := utils.Exec(t, readConn, fetchAllCustomers)
	assert.Equal(t, qr.Rows, qr2.Rows)

	// replica should see new row after closing the transaction
	utils.Exec(t, readConn, "commit")

	qr3 := utils.Exec(t, readConn, fetchAllCustomers)
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")] [INT64(2) VARCHAR("email2")]]`, fmt.Sprintf("%v", qr3.Rows), "we are not seeing the updates after closing the replica transaction")

	// begin transaction on replica
	utils.Exec(t, readConn, "begin")
	// try to delete a row, should fail
	utils.AssertContainsError(t, readConn, "delete from customer where id=1", "supported only for primary tablet type, current type: replica")
	utils.Exec(t, readConn, "commit")

	// begin transaction on replica
	utils.Exec(t, readConn, "begin")
	// try to update a row, should fail
	utils.AssertContainsError(t, readConn, "update customer set email='emailn' where id=1", "supported only for primary tablet type, current type: replica")
	utils.Exec(t, readConn, "commit")

	// begin transaction on replica
	utils.Exec(t, readConn, "begin")
	// try to insert a row, should fail
	utils.AssertContainsError(t, readConn, "insert into customer(id, email) values(1,'email1')", "supported only for primary tablet type, current type: replica")
	// call rollback just for fun
	utils.Exec(t, readConn, "rollback")

	// start another transaction
	utils.Exec(t, readConn, "begin")
	utils.Exec(t, readConn, fetchAllCustomers)
	// bring down the tablet and try to select again
	replicaTablet := clusterInstance.Keyspaces[0].Shards[0].Replica()
	// this gives us a "signal: killed" error, ignore it
	_ = replicaTablet.VttabletProcess.TearDown()
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	utils.AssertContainsError(t, readConn, fetchAllCustomers, "is either down or nonexistent")

	// bring up the tablet again
	// trying to use the same session/transaction should fail as the vtgate has
	// been restarted and the session lost
	replicaTablet.VttabletProcess.ServingStatus = "SERVING"
	err = replicaTablet.VttabletProcess.Setup()
	require.NoError(t, err)
	serving := replicaTablet.VttabletProcess.WaitForStatus("SERVING", 60*time.Second)
	assert.Equal(t, serving, true, "Tablet did not become ready within a reasonable time")
	utils.AssertContainsError(t, readConn, fetchAllCustomers, "not found")

	// create a new connection, should be able to query again
	readConn, err = mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	utils.Exec(t, readConn, "begin")
	qr4 := utils.Exec(t, readConn, fetchAllCustomers)
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")] [INT64(2) VARCHAR("email2")]]`, fmt.Sprintf("%v", qr4.Rows), "we are not able to reconnect after restart")
}

// TestStreamingRPCStuck tests that StreamExecute calls don't get stuck on the vttablets if a client stop reading from a stream.
func TestStreamingRPCStuck(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer vtConn.Close()

	// We want the table to have enough rows such that a streaming call returns multiple packets.
	// Therefore, we insert one row and keep doubling it.
	utils.Exec(t, vtConn, "insert into customer(email) values('testemail')")
	for i := 0; i < 15; i++ {
		// Double the number of rows in customer table.
		utils.Exec(t, vtConn, "insert into customer (email) select email from customer")
	}

	// Connect to vtgate and run a streaming query.
	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "test_user", "")
	require.NoError(t, err)
	stream, err := vtgateConn.Session("", &querypb.ExecuteOptions{}).StreamExecute(ctx, "select * from customer", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	// We read packets until we see the first set of results. This ensures that the stream is working.
	for {
		res, err := stream.Recv()
		require.NoError(t, err)
		if res != nil && len(res.Rows) > 0 {
			// breaking here stops reading from the stream.
			break
		}
	}

	// We simulate a misbehaving client that doesn't read from the stream anymore.
	// This however shouldn't block PlannedReparentShard calls.
	err = clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, "0", clusterInstance.Keyspaces[0].Shards[0].Vttablets[1].Alias)
	require.NoError(t, err)
}

func getMapFromJSON(JSON map[string]any, key string) map[string]any {
	result := make(map[string]any)
	object := reflect.ValueOf(JSON[key])
	if object.Kind() == reflect.Map {
		for _, key := range object.MapKeys() {
			value := object.MapIndex(key)
			result[key.String()] = value
		}
	}
	return result
}

func isPrimaryTabletPresent(tablets map[string]any) bool {
	for key := range tablets {
		if strings.Contains(key, "primary") {
			return true
		}
	}
	return false
}
