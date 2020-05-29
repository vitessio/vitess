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

/*
-begin on replica should explicitly say read only
-tabletserver planner should stop dml (if easy and reasonable)
-vtgate planbuilder should not send dml to replicas
*/

func TestReplicaTransactions(t *testing.T) {
	// TODO(deepthi): this test seems to depend on previous test. Fix tearDown so that tests are independent
	defer cluster.PanicHandler(t)
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	ctx := context.Background()
	masterConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	replicaConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer masterConn.Close()
	defer replicaConn.Close()

	// insert a row using master
	exec(t, masterConn, "insert into customer(id, email) values(1,'email1')", "")
	time.Sleep(1 * time.Second) // we sleep for a bit to make sure that the replication catches up

	// after a short pause, SELECT the data inside a tx on a replica
	exec(t, replicaConn, "use @replica", "")
	// begin transaction on replica
	exec(t, replicaConn, "begin", "")
	qr := exec(t, replicaConn, "select id, email from customer", "")
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")]]`, fmt.Sprintf("%v", qr.Rows), "select returned wrong result")

	// insert more data on master using a transaction
	exec(t, masterConn, "begin", "")
	exec(t, masterConn, "insert into customer(id, email) values(2,'email2')", "")
	exec(t, masterConn, "commit", "")
	time.Sleep(1 * time.Second)

	// replica doesn't see new row because it is in a transaction
	qr2 := exec(t, replicaConn, "select id, email from customer", "")
	assert.Equal(t, qr.Rows, qr2.Rows)

	// replica should see new row after closing the transaction
	exec(t, replicaConn, "commit", "")

	qr3 := exec(t, replicaConn, "select id, email from customer", "")
	assert.Equal(t, `[[INT64(1) VARCHAR("email1")] [INT64(2) VARCHAR("email2")]]`, fmt.Sprintf("%v", qr3.Rows), "we are not seeing the updates after closing the replica transaction")
	// since we can't do INSERT/DELETE/UPDATE, commit and rollback both just close the transaction
	exec(t, replicaConn, "rollback", "")

	// bring down the tablet and try to select again
	// Error: ERROR 1105 (HY000): vtgate: http://localhost:15001/: vttablet: rpc error: code = Unavailable desc = all SubConns are in TransientFailure,
	// latest connection error: connection error: desc = "transport: Error while dialing dial tcp 127.0.1.1:16101: connect: connection refused"

	//mysql> select id from customer;
	//ERROR 1105 (HY000): vtgate: http://deepthi-ThinkPad:15001/: vttablet: rpc error: code = Unavailable desc = all SubConns are in TransientFailure, latest connection error: connection error: desc = "transport: Error while dialing dial tcp 127.0.1.1:16101: connect: connection refused"
	//mysql> commit;
	//ERROR 1105 (HY000): vtgate: http://deepthi-ThinkPad:15001/: target: commerce.0.replica: vttablet: Connection Closed
	//mysql> select id from customer;
	//ERROR 1105 (HY000): vtgate: http://deepthi-ThinkPad:15001/: target: commerce.0.replica: vttablet: Connection Closed
	//mysql> use @replica;
	//Database changed
	//mysql> select id from customer;
	//ERROR 1105 (HY000): vtgate: http://deepthi-ThinkPad:15001/: target: commerce.0.replica: vttablet: Connection Closed

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
