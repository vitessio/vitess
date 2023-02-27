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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestVttabletProcess(t *testing.T) {
	defer cluster.PanicHandler(t)
	firstTabletPort := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0].HTTPPort
	testURL(t, fmt.Sprintf("http://localhost:%d/debug/vars/", firstTabletPort), "tablet debug var url")
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/debug/vars", firstTabletPort))
	require.NoError(t, err)
	defer resp.Body.Close()

	resultMap := make(map[string]any)
	respByte, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(respByte, &resultMap)
	require.NoError(t, err)
	if got, want := resultMap["TabletKeyspace"], "commerce"; got != want {
		t.Errorf("select:\n%v want\n%v for %s", got, want, "Keyspace of tablet should match")
	}
}

func TestDeleteTablet(t *testing.T) {
	defer cluster.PanicHandler(t)
	primary := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	require.NotNil(t, primary)
	_, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("DeleteTablet", "--", "--allow_primary", primary.Alias)
	require.NoError(t, err)
}
