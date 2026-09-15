/*
Copyright 2025 The Vitess Authors.

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

package general

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// TestConsistentHashRing brings up a VTOrc configured with a consistent hash
// ring (--vtorc-ring-size / --vtorc-ring-index) and verifies the instance
// starts, becomes healthy, and publishes its ring configuration via exported
// vars. Ring size 4 is the smallest size at which partitioning takes effect;
// sizes <= 3 are a deliberate no-op.
func TestConsistentHashRing(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)

	orcExtraArgs := []string{
		"--vtorc-ring-size=4",
		"--vtorc-ring-index=0",
	}
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, orcExtraArgs, cluster.VTOrcConfiguration{
		PreventCrossCellFailover: true,
	}, map[string]int{cluster.DefaultCell: 1}, "")

	vtorc := clusterInfo.ClusterInstance.VTOrcProcesses[0]

	// The instance must come up healthy even though ring gating may leave it
	// watching only a subset of shards.
	status, _ := utils.MakeAPICallRetry(t, vtorc, "/debug/health", func(code int, _ string) bool {
		return code != 200
	})
	require.Equal(t, 200, status)

	// The ring configuration is published via exported vars.
	utils.CheckVarExists(t, vtorc, "VtorcRingSize")
	utils.CheckVarExists(t, vtorc, "VtorcRingIndex")
	utils.CheckVarExists(t, vtorc, "VtorcRingBuckets")

	vars := vtorc.GetVars()
	require.EqualValues(t, 4, utils.GetIntFromValue(vars["VtorcRingSize"]))
	require.EqualValues(t, 0, utils.GetIntFromValue(vars["VtorcRingIndex"]))
	// No assignments file was provided, so pure hash mode (0 buckets loaded).
	require.EqualValues(t, 0, utils.GetIntFromValue(vars["VtorcRingBuckets"]))
}
