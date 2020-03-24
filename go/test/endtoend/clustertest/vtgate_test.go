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

This tests select/insert using the unshared keyspace added in main_test
*/
package clustertest

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestVtgateProcess(t *testing.T) {
	defer cluster.PanicHandler(t)
	verifyVtgateVariables(t, clusterInstance.VtgateProcess.VerifyURL)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "insert into customer(id, email) values(1,'email1')")

	qr := exec(t, conn, "select id, email from customer")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("email1")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func verifyVtgateVariables(t *testing.T, url string) {
	resp, _ := http.Get(url)
	if resp != nil && resp.StatusCode == 200 {
		resultMap := make(map[string]interface{})
		respByte, _ := ioutil.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		require.Nil(t, err)
		if resultMap["VtgateVSchemaCounts"] == nil {
			t.Error("Vschema count should be present in variables")
		}
		vschemaCountMap := getMapFromJSON(resultMap, "VtgateVSchemaCounts")
		if _, present := vschemaCountMap["Reload"]; !present {
			t.Error("Reload count should be present in vschemacount")
		} else if object := reflect.ValueOf(vschemaCountMap["Reload"]); object.NumField() <= 0 {
			t.Error("Reload count should be greater than 0")
		}
		if _, present := vschemaCountMap["WatchError"]; present {
			t.Error("There should not be any WatchError in VschemaCount")
		}
		if _, present := vschemaCountMap["Parsing"]; present {
			t.Error("There should not be any Parsing in VschemaCount")
		}

		if resultMap["HealthcheckConnections"] == nil {
			t.Error("HealthcheckConnections count should be present in variables")
		}

		healthCheckConnection := getMapFromJSON(resultMap, "HealthcheckConnections")
		if len(healthCheckConnection) <= 0 {
			t.Error("Atleast one healthy tablet needs to be present")
		}
		if !isMasterTabletPresent(healthCheckConnection) {
			t.Error("Atleast one master tablet needs to be present")
		}
	} else {
		t.Error("Vtgate api url response not found")
	}
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
