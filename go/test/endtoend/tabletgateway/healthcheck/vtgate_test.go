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

func TestVtgateProcess(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Healthcheck interval on tablet is set to 1s, so sleep for 2s
	time.Sleep(2 * time.Second)
	verifyVtgateVariables(t, clusterInstance.VtgateProcess.VerifyURL)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "insert into customer(id, email) values(1,'email1')")

	qr := exec(t, conn, "select id, email from customer")
	assert.Equal(t, fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("email1")]]`, "select returned wrong result")
	qr = exec(t, conn, "show vitess_tablets")
	assert.Equal(t, len(qr.Rows), 3, "wrong number of results from show")
}

func verifyVtgateVariables(t *testing.T, url string) {
	resp, _ := http.Get(url)
	require.True(t, resp != nil && resp.StatusCode == 200, "Vtgate api url response not found")
	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err := json.Unmarshal(respByte, &resultMap)
	require.Nil(t, err)
	assert.True(t, resultMap["VtgateVSchemaCounts"] != nil, "Vschema count should be present in variables")
	vschemaCountMap := getMapFromJSON(resultMap, "VtgateVSchemaCounts")
	_, present := vschemaCountMap["Reload"]
	assert.True(t, present, "Reload count should be present in vschemacount")
	object := reflect.ValueOf(vschemaCountMap["Reload"])
	assert.True(t, object.NumField() > 0, "Reload count should be greater than 0")
	_, present = vschemaCountMap["WatchError"]
	assert.False(t, present, "There should not be any WatchError in VschemaCount")
	_, present = vschemaCountMap["Parsing"]
	assert.False(t, present, "There should not be any Parsing in VschemaCount")

	assert.True(t, resultMap["HealthcheckConnections"] != nil, "HealthcheckConnections count should be present in variables")
	healthCheckConnection := getMapFromJSON(resultMap, "HealthcheckConnections")
	assert.True(t, len(healthCheckConnection) > 0, "Atleast one healthy tablet needs to be present")
	assert.True(t, isMasterTabletPresent(healthCheckConnection), "Atleast one master tablet needs to be present")
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

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.Nil(t, err)
	return qr
}
