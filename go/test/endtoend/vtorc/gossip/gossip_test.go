/*
Copyright 2026 The Vitess Authors.

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

package gossip

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

type debugGossipState struct {
	NodeID string                      `json:"node_id"`
	States map[string]debugGossipEntry `json:"states"`
}

type debugGossipEntry struct {
	Status string `json:"status"`
}

func fetchDebugGossip(url string) *debugGossipState {
	resp, err := http.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}

	var state debugGossipState
	if err := json.Unmarshal(body, &state); err != nil {
		return nil
	}

	return &state
}

func alivePeerCount(state *debugGossipState) int {
	if state == nil {
		return 0
	}

	alive := 0
	for id, entry := range state.States {
		if id == state.NodeID {
			continue
		}
		if entry.Status == "alive" {
			alive++
		}
	}
	return alive
}

func enableVttabletGossipService(t *testing.T) {
	t.Helper()

	originalServiceMaps := map[*cluster.VttabletProcess]string{}
	for _, cellInfo := range clusterInfo.CellInfos {
		for _, tablet := range append(cellInfo.ReplicaTablets, cellInfo.RdonlyTablets...) {
			process := tablet.VttabletProcess
			originalServiceMaps[process] = process.ServiceMap
			if !strings.Contains(","+process.ServiceMap+",", ",grpc-gossip,") {
				process.ServiceMap += ",grpc-gossip"
			}
		}
	}

	t.Cleanup(func() {
		for process, serviceMap := range originalServiceMaps {
			process.ServiceMap = serviceMap
		}
	})
}

func TestVTOrcGossipFlags(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)

	enableVttabletGossipService(t)

	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 0, []string{
		"--gossip-listen-addr", "localhost:16110",
	}, cluster.VTOrcConfiguration{}, cluster.DefaultVtorcsByCell, "")

	// Ensure VTOrc starts and serves debug vars.
	assert.Eventually(t, func() bool {
		vars := clusterInfo.ClusterInstance.VTOrcProcesses[0].GetVars()
		if vars == nil {
			return false
		}
		_, ok := vars["DiscoveriesAttempt"]
		return ok
	}, 10*time.Second, 500*time.Millisecond)

	out, err := clusterInfo.ClusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(
		"UpdateGossipConfig",
		"--enable",
		"--phi-threshold=4",
		"--ping-interval=100ms",
		"--max-update-age=1s",
		"ks",
	)
	require.NoError(t, err, out)

	vtorcDebugURL := fmt.Sprintf("http://localhost:%d/debug/gossip", clusterInfo.ClusterInstance.VTOrcProcesses[0].Port)
	tablets := clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets
	require.Len(t, tablets, 2)

	assert.Eventually(t, func() bool {
		vtorcState := fetchDebugGossip(vtorcDebugURL)
		if alivePeerCount(vtorcState) < 2 {
			return false
		}

		for _, tablet := range tablets {
			tabletDebugURL := fmt.Sprintf("http://localhost:%d/debug/gossip", tablet.HTTPPort)
			if alivePeerCount(fetchDebugGossip(tabletDebugURL)) < 1 {
				return false
			}
		}

		return true
	}, 15*time.Second, 200*time.Millisecond)
}
