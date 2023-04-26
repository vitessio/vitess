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
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
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
	defer cluster.PanicHandler(t)
	url := fmt.Sprintf("http://%s:%d/api/keyspaces/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, url, "keyspace url")

	healthCheckURL := fmt.Sprintf("http://%s:%d/debug/health/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, healthCheckURL, "vtctld health check url")

	url = fmt.Sprintf("http://%s:%d/api/topodata/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)

	testTopoDataAPI(t, url)
	testListAllTablets(t)
	testTabletStatus(t)
	testExecuteAsDba(t)
	testExecuteAsApp(t)
}

func testTopoDataAPI(t *testing.T, url string) {
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, resp.StatusCode, 200)

	resultMap := make(map[string]any)
	respByte, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(respByte, &resultMap)
	require.NoError(t, err)

	errorValue := reflect.ValueOf(resultMap["Error"])
	assert.Empty(t, errorValue.String())

	assert.Contains(t, resultMap, "Children")
	children := reflect.ValueOf(resultMap["Children"])
	childrenGot := fmt.Sprintf("%s", children)
	assert.Contains(t, childrenGot, "global")
	assert.Contains(t, childrenGot, clusterInstance.Cell)
}

func testListAllTablets(t *testing.T) {
	// first w/o any filters, aside from cell
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets", clusterInstance.Cell)
	require.NoError(t, err)

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

	// now filtering with the first keyspace and tablet type of primary, in
	// addition to the cell
	result, err = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(
		"ListAllTablets", "--", "--keyspace", clusterInstance.Keyspaces[0].Name,
		"--tablet_type", "primary",
		clusterInstance.Cell)
	require.NoError(t, err)

	// We should only return a single primary tablet per shard in the first keyspace
	tabletsFromCMD = strings.Split(result, "\n")
	// We don't count the final newline with nothing after it (it becomes an empty
	// line at the end of the slice)
	assert.Equal(t, len(clusterInstance.Keyspaces[0].Shards), len(tabletsFromCMD)-1)
}

func testTabletStatus(t *testing.T) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d", clusterInstance.Hostname, clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].HTTPPort))
	require.NoError(t, err)
	defer resp.Body.Close()
	respByte, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	result := string(respByte)
	log.Infof("Tablet status response: %v", result)
	assert.True(t, strings.Contains(result, `Alias: <a href="http://localhost:`))
	assert.True(t, strings.Contains(result, `</html>`))
}

func testExecuteAsDba(t *testing.T) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsDba", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].Alias, `SELECT 1 AS a`)
	require.NoError(t, err)
	assert.Equal(t, result, oneTableOutput)
}

func testExecuteAsApp(t *testing.T) {
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ExecuteFetchAsApp", clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].Alias, `SELECT 1 AS a`)
	require.NoError(t, err)
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
