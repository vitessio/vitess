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
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestVtgateHealthCheck(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	verifyVtgateVariables(t, clusterInstance.VtgateProcess.VerifyURL)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr := exec(t, conn, "show vitess_tablets", "")
	assert.Equal(t, 3, len(qr.Rows), "wrong number of results from show")
}

func verifyVtgateVariables(t *testing.T, url string) {
	resp, err := http.Get(url)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "Vtgate api url response not found")

	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
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
	assert.True(t, isMasterTabletPresent(healthCheckConnection), "Atleast one master tablet needs to be present")
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
	masterConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer masterConn.Close()

	replicaConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer replicaConn.Close()

	replicaConn2, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer replicaConn2.Close()

	fetchAllCustomers := "select id, email from customer"
	checkCustomerRows := func(expectedRows int) func() bool {
		return func() bool {
			result := exec(t, replicaConn2, fetchAllCustomers, "")
			return len(result.Rows) == expectedRows
		}
	}

	// point the replica connections to the replica target
	exec(t, replicaConn, "use @replica", "")
	exec(t, replicaConn2, "use @replica", "")

	// insert a row using master
	exec(t, masterConn, "insert into customer(id, email) values(1,'email1')", "")

	// we'll run this query a number of times, and then give up if the row count never reaches this value
	retryNTimes(t, 10 /*maxRetries*/, checkCustomerRows(1))

	// after a short pause, SELECT the data inside a tx on a replica
	// begin transaction on replica
	exec(t, replicaConn, "begin", "")
	qr := exec(t, replicaConn, fetchAllCustomers, "")
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")]]`, fmt.Sprintf("%v", qr.Rows), "select returned wrong result")

	// insert more data on master using a transaction
	exec(t, masterConn, "begin", "")
	exec(t, masterConn, "insert into customer(id, email) values(2,'email2')", "")
	exec(t, masterConn, "commit", "")

	retryNTimes(t, 10 /*maxRetries*/, checkCustomerRows(2))

	// replica doesn't see new row because it is in a transaction
	qr2 := exec(t, replicaConn, fetchAllCustomers, "")
	assert.Equal(t, qr.Rows, qr2.Rows)

	// replica should see new row after closing the transaction
	exec(t, replicaConn, "commit", "")

	qr3 := exec(t, replicaConn, fetchAllCustomers, "")
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")] [INT64(2) VARCHAR("email2")]]`, fmt.Sprintf("%v", qr3.Rows), "we are not seeing the updates after closing the replica transaction")

	// begin transaction on replica
	exec(t, replicaConn, "begin", "")
	// try to delete a row, should fail
	exec(t, replicaConn, "delete from customer where id=1", "supported only for master tablet type, current type: replica")
	exec(t, replicaConn, "commit", "")

	// begin transaction on replica
	exec(t, replicaConn, "begin", "")
	// try to update a row, should fail
	exec(t, replicaConn, "update customer set email='emailn' where id=1", "supported only for master tablet type, current type: replica")
	exec(t, replicaConn, "commit", "")

	// begin transaction on replica
	exec(t, replicaConn, "begin", "")
	// try to insert a row, should fail
	exec(t, replicaConn, "insert into customer(id, email) values(1,'email1')", "supported only for master tablet type, current type: replica")
	// call rollback just for fun
	exec(t, replicaConn, "rollback", "")

	// start another transaction
	exec(t, replicaConn, "begin", "")
	exec(t, replicaConn, fetchAllCustomers, "")
	// bring down the tablet and try to select again
	replicaTablet := clusterInstance.Keyspaces[0].Shards[0].Replica()
	// this gives us a "signal: killed" error, ignore it
	_ = replicaTablet.VttabletProcess.TearDown()
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	exec(t, replicaConn, fetchAllCustomers, "is either down or nonexistent")

	// bring up tablet again
	// query using same transaction will fail
	_ = replicaTablet.VttabletProcess.Setup()
	exec(t, replicaConn, fetchAllCustomers, "not found")
	exec(t, replicaConn, "commit", "")
	// create a new connection, should be able to query again
	replicaConn, err = mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	exec(t, replicaConn, "begin", "")
	qr4 := exec(t, replicaConn, fetchAllCustomers, "")
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")] [INT64(2) VARCHAR("email2")]]`, fmt.Sprintf("%v", qr4.Rows), "we are not able to reconnect after restart")
}

func getMapFromJSON(JSON map[string]interface{}, key string) map[string]interface{} {
	result := make(map[string]interface{})
	object := reflect.ValueOf(JSON[key])
	if object.Kind() == reflect.Map {
		for _, key := range object.MapKeys() {
			value := object.MapIndex(key)
			result[key.String()] = value
		}
	}
	return result
}

func isMasterTabletPresent(tablets map[string]interface{}) bool {
	for key := range tablets {
		if strings.Contains(key, "master") {
			return true
		}
	}
	return false
}

func exec(t *testing.T, conn *mysql.Conn, query string, expectError string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}
