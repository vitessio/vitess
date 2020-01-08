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

package clustertest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	oneTableOutput = `+---+
| a |
+---+
| 1 |
+---+
`
)

func TestVtctldProcess(t *testing.T) {
	url := fmt.Sprintf("http://%s:%d/api/keyspaces/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, url, "keyspace url")

	healthCheckURL := fmt.Sprintf("http://%s:%d/debug/health/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, healthCheckURL, "vtctld health check url")

	url = fmt.Sprintf("http://%s:%d/api/topodata/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)

	t.Run("Test topo data API", func(t *testing.T) {
		testTopoDataAPI(t, url)
	})
	t.Run("Test list all tablets cmd", func(t *testing.T) {
		testListAllTablets(t)
	})
	t.Run("Test tablet status", func(t *testing.T) {
		testTabletStatus(t)
	})
	t.Run("Test ExecuteAsDba", func(t *testing.T) {
		testExecuteAsDba(t)
	})
	t.Run("Test ExecuteAsApp", func(t *testing.T) {
		testExecuteAsApp(t)
	})
}

func testTopoDataAPI(t *testing.T, url string) {
	resp, err := http.Get(url)
	assert.Nil(t, err)
	assert.Equal(t, resp.StatusCode, 200)

	resultMap := make(map[string]interface{})
	respByte, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(respByte, &resultMap)
	assert.Nil(t, err)

	errorValue := reflect.ValueOf(resultMap["Error"])
	assert.Empty(t, errorValue.String())

	assert.Contains(t, resultMap, "Children")
	children := reflect.ValueOf(resultMap["Children"])
	childrenGot := fmt.Sprintf("%s", children)
	assert.Contains(t, childrenGot, "global")
	assert.Contains(t, childrenGot, clusterInstance.Cell)
}

func testListAllTablets(t *testing.T) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets", clusterInstance.Cell)
	assert.Nil(t, err)

	tablets := getAllTablets()

	tabletsFromCMD := strings.Split(result, "\n")
	tabletCountFromCMD := 0

	for _, line := range tabletsFromCMD {
		if len(line) > 0 {
			tabletCountFromCMD = tabletCountFromCMD + 1
			assert.Contains(t, tablets, strings.Split(line, " ")[0])
		}
	}
	assert.Equal(t, tabletCountFromCMD, len(tablets))
}

func testTabletStatus(t *testing.T) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d", clusterInstance.Hostname, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].HTTPPort))
	assert.Nil(t, err)
	respByte, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	result := string(respByte)
	println(result)
	println(strings.Contains(result, "Polling health information from."))
	matched, err := regexp.Match(`Polling health information from.+MySQLReplicationLag`, []byte(result))
	assert.Nil(t, err)
	assert.True(t, matched)
	assert.True(t, strings.Contains(result, `Alias: <a href="http://localhost:`))
	assert.True(t, strings.Contains(result, `</html>`))
}

func testExecuteAsDba(t *testing.T) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDba", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].Alias, `SELECT 1 AS a`)
	assert.Nil(t, err)
	assert.Equal(t, result, oneTableOutput)
}

func testExecuteAsApp(t *testing.T) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsApp", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].Alias, `SELECT 1 AS a`)
	assert.Nil(t, err)
	assert.Equal(t, result, oneTableOutput)
}

func getAllTablets() []string {
	tablets := make([]string, 0)
	for _, ks := range clusterInstance.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Vttablets {
				tablets = append(tablets, tablet.Alias)
			}
		}
	}
	return tablets
}
