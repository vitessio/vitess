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
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestVtgateProcess(t *testing.T) {
	defer cluster.PanicHandler(t)
	verifyVtgateVariables(t, clusterInstance.VtgateProcess.VerifyURL)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "insert into customer(id, email) values(1,'email1')")
	_ = utils.Exec(t, conn, "begin")
	qr := utils.Exec(t, conn, "select id, email from customer")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("email1")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
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
	if !isPrimaryTabletPresent(healthCheckConnection) {
		t.Error("Atleast one PRIMARY tablet needs to be present")
	}
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
