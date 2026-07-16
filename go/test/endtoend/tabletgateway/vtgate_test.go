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
	"vitess.io/vitess/go/vitesst"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestVtgateHealthCheck(t *testing.T) {
	setup(t)

	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	verifyVtgateVariables(t, vtgateVarsURL(t))
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := vitesst.Exec(t, conn, "show vitess_tablets")
	assert.Equal(t, 3, len(qr.Rows), "wrong number of results from show")
}

func TestVtgateReplicationStatusCheck(t *testing.T) {
	setup(t)

	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	verifyVtgateVariables(t, vtgateVarsURL(t))
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams) // VTGate
	require.NoError(t, err)
	defer conn.Close()

	// Only returns rows for REPLICA and RDONLY tablets -- so should be 2 of them
	qr := vitesst.Exec(t, conn, "show vitess_replication_status like '%'")
	expectNumRows := 2
	numRows := len(qr.Rows)
	assert.Equal(t, expectNumRows, numRows, fmt.Sprintf("wrong number of results from show vitess_replication_status. Expected %d, got %d", expectNumRows, numRows))

	// Disable VTOrc(s) recoveries so that it doesn't immediately repair/restart replication.
	disableGlobalRecoveries(t, clusterInstance.VTOrc())
	// Re-enable recoveries afterward as the cluster is re-used.
	defer enableGlobalRecoveries(t, clusterInstance.VTOrc())

	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	// Stop replication on the non-PRIMARY tablets.
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "ExecuteFetchAsDBA", shard.Replicas()[0].Alias(), "stop replica")
	require.NoError(t, err)
	_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "ExecuteMultiFetchAsDBA", shard.RDOnly()[0].Alias(), "stop replica")
	require.NoError(t, err)
	// Restart replication afterward as the cluster is re-used.
	defer func() {
		_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "ExecuteFetchAsDBA", shard.Replicas()[0].Alias(), "start replica")
		require.NoError(t, err)
		// Testing ExecuteMultiFetchAsDBA by running multiple commands in a single call:
		_, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "ExecuteMultiFetchAsDBA", shard.RDOnly()[0].Alias(), "start replica sql_thread; start replica io_thread;")
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
	setup(t)

	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	verifyVtgateVariables(t, vtgateVarsURL(t))
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Only returns rows for REPLICA and RDONLY tablets -- so should be 2 of them
	qr := vitesst.Exec(t, conn, "show vitess_replication_status like '%'")
	expectNumRows := 2
	numRows := len(qr.Rows)
	assert.Equal(t, expectNumRows, numRows, fmt.Sprintf("wrong number of results from show vitess_replication_status. Expected %d, got %d", expectNumRows, numRows))

	// change the RDONLY tablet to SPARE
	rdOnlyTablet := clusterInstance.Keyspace(keyspaceName).Shards()[0].RDOnly()[0]
	err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdOnlyTablet.Alias(), "spare")
	require.NoError(t, err)
	// Change it back to RDONLY afterward as the cluster is re-used.
	defer func() {
		err = clusterInstance.Vtctld().ExecuteCommand(ctx, "ChangeTabletType", rdOnlyTablet.Alias(), "rdonly")
		require.NoError(t, err)
	}()

	// Only returns rows for REPLICA and RDONLY tablets -- so should be 1 of them since we updated 1 to spare
	qr = vitesst.Exec(t, conn, "show vitess_replication_status like '%'")
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

// vtgateVarsURL returns the vtgate's host-reachable /debug/vars URL.
func vtgateVarsURL(t *testing.T) string {
	addr, err := clusterInstance.VTGate().HTTPAddr(t.Context())
	require.NoError(t, err)
	return "http://" + addr + "/debug/vars"
}

func disableGlobalRecoveries(t *testing.T, vtorc *vitesst.VTOrc) {
	status, resp, err := vtorc.MakeAPICall(t.Context(), "/api/disable-global-recoveries")
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.Equal(t, "Global recoveries disabled\n", resp)
}

func enableGlobalRecoveries(t *testing.T, vtorc *vitesst.VTOrc) {
	status, resp, err := vtorc.MakeAPICall(t.Context(), "/api/enable-global-recoveries")
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.Equal(t, "Global recoveries enabled\n", resp)
}

func retryNTimes(t *testing.T, maxRetries int, f func() bool) {
	i := 0
	for {
		res := f()
		if res {
			return
		}
		if i > maxRetries {
			require.Failf(t, "retry exhausted", "retried %d times and failed", maxRetries)
			return
		}
		i++
	}
}

func TestReplicaTransactions(t *testing.T) {
	setup(t)

	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	ctx := t.Context()
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
			result := vitesst.Exec(t, readConn2, fetchAllCustomers)
			return len(result.Rows) == expectedRows
		}
	}

	// point the replica connections to the replica target
	vitesst.Exec(t, readConn, "use @replica")
	vitesst.Exec(t, readConn2, "use @replica")

	vitesst.Exec(t, writeConn, "insert into customer(id, email) values(1,'email1')")

	// we'll run this query a number of times, and then give up if the row count never reaches this value
	retryNTimes(t, 10 /*maxRetries*/, checkCustomerRows(1))

	// after a short pause, SELECT the data inside a tx on a replica
	// begin transaction on replica
	vitesst.Exec(t, readConn, "begin")
	qr := vitesst.Exec(t, readConn, fetchAllCustomers)
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")]]`, fmt.Sprintf("%v", qr.Rows), "select returned wrong result")

	// insert more data on primary using a transaction
	vitesst.Exec(t, writeConn, "begin")
	vitesst.Exec(t, writeConn, "insert into customer(id, email) values(2,'email2')")
	vitesst.Exec(t, writeConn, "commit")

	retryNTimes(t, 10 /*maxRetries*/, checkCustomerRows(2))

	// replica doesn't see new row because it is in a transaction
	qr2 := vitesst.Exec(t, readConn, fetchAllCustomers)
	assert.Equal(t, qr.Rows, qr2.Rows)

	// replica should see new row after closing the transaction
	vitesst.Exec(t, readConn, "commit")

	qr3 := vitesst.Exec(t, readConn, fetchAllCustomers)
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")] [INT64(2) VARCHAR("email2")]]`, fmt.Sprintf("%v", qr3.Rows), "we are not seeing the updates after closing the replica transaction")

	// begin transaction on replica
	vitesst.Exec(t, readConn, "begin")
	// try to delete a row, should fail
	vitesst.AssertContainsError(t, readConn, "delete from customer where id=1", "supported only for primary tablet type, current type: replica")
	vitesst.Exec(t, readConn, "commit")

	// begin transaction on replica
	vitesst.Exec(t, readConn, "begin")
	// try to update a row, should fail
	vitesst.AssertContainsError(t, readConn, "update customer set email='emailn' where id=1", "supported only for primary tablet type, current type: replica")
	vitesst.Exec(t, readConn, "commit")

	// begin transaction on replica
	vitesst.Exec(t, readConn, "begin")
	// try to insert a row, should fail
	vitesst.AssertContainsError(t, readConn, "insert into customer(id, email) values(1,'email1')", "supported only for primary tablet type, current type: replica")
	// call rollback just for fun
	vitesst.Exec(t, readConn, "rollback")

	// start another transaction
	vitesst.Exec(t, readConn, "begin")
	vitesst.Exec(t, readConn, fetchAllCustomers)
	// bring down the tablet and try to select again
	replicaTablet := clusterInstance.Keyspace(keyspaceName).Shards()[0].Replicas()[0]
	// this gives us a "signal: killed" error, ignore it
	_ = replicaTablet.StopVttablet(ctx)
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	vitesst.AssertContainsMultipleErrors(t, readConn, fetchAllCustomers, "VT15001", "is either down or nonexistent")

	// bring up the tablet again
	// trying to use the same session/transaction should fail as the vtgate has
	// been restarted and the session lost
	err = replicaTablet.StartVttablet(ctx)
	require.NoError(t, err)
	serving := replicaTablet.WaitForTabletStatus(ctx, 60*time.Second, "SERVING") == nil
	assert.Equal(t, serving, true, "Tablet did not become ready within a reasonable time")
	vitesst.AssertContainsError(t, readConn, fetchAllCustomers, "VT09032")
	vitesst.Exec(t, readConn, "rollback")

	// create a new connection, should be able to query again
	readConn, err = mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	vitesst.Exec(t, readConn, "begin")
	qr4 := vitesst.Exec(t, readConn, fetchAllCustomers)
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")] [INT64(2) VARCHAR("email2")]]`, fmt.Sprintf("%v", qr4.Rows), "we are not able to reconnect after restart")
}

// TestStreamingRPCStuck tests that StreamExecute calls don't get stuck on the vttablets if a client stop reading from a stream.
func TestStreamingRPCStuck(t *testing.T) {
	setup(t)

	ctx := t.Context()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer vtConn.Close()

	// We want the table to have enough rows such that a streaming call returns multiple packets.
	// Therefore, we insert one row and keep doubling it.
	vitesst.Exec(t, vtConn, "insert into customer(email) values('testemail')")
	for range 15 {
		// Double the number of rows in customer table.
		vitesst.Exec(t, vtConn, "insert into customer (email) select email from customer")
	}

	// Connect to vtgate and run a streaming query.
	vtgateConn, err := clusterInstance.VTGate().DialVTGate(ctx)
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
	shard := clusterInstance.Keyspace(keyspaceName).Shards()[0]
	err = clusterInstance.Vtctld().ExecuteCommand(
		ctx,
		"PlannedReparentShard",
		shard.Ref(),
		"--new-primary", shard.Replicas()[0].Alias(),
		"--wait-replicas-timeout", "30s",
	)
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
