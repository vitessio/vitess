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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestVtgateProcess(t *testing.T) {
	verifyVtgateVariables(t, clusterInstance.VtgateProcess.VerifyURL)
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "insert into customer(id, email) values(1,'email1')")
	_ = utils.Exec(t, conn, "begin")
	qr := utils.Exec(t, conn, "select id, email from customer")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("email1")]]`; got != want {
		assert.Equalf(t, want, got, "select:\n%v want\n%v", got, want)
	}
}

func verifyVtgateVariables(t *testing.T, url string) {
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	resultMap := make(map[string]any)
	respByte, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(respByte, &resultMap)
	require.NoError(t, err)
	assert.NotNil(t, resultMap["VtgateVSchemaCounts"], "Vschema count should be present in variables")
	vschemaCountMap := getMapFromJSON(resultMap, "VtgateVSchemaCounts")
	if _, present := vschemaCountMap["Reload"]; !present {
		assert.Fail(t, "Reload count should be present in vschemacount")
	} else if object := reflect.ValueOf(vschemaCountMap["Reload"]); object.NumField() <= 0 {
		assert.Fail(t, "Reload count should be greater than 0")
	}
	_, watchErrorPresent := vschemaCountMap["WatchError"]
	assert.False(t, watchErrorPresent, "There should not be any WatchError in VschemaCount")
	_, parsingPresent := vschemaCountMap["Parsing"]
	assert.False(t, parsingPresent, "There should not be any Parsing in VschemaCount")

	assert.NotNil(t, resultMap["HealthcheckConnections"], "HealthcheckConnections count should be present in variables")

	healthCheckConnection := getMapFromJSON(resultMap, "HealthcheckConnections")
	assert.NotEmpty(t, healthCheckConnection, "Atleast one healthy tablet needs to be present")
	assert.True(t, isPrimaryTabletPresent(healthCheckConnection), "Atleast one PRIMARY tablet needs to be present")
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
