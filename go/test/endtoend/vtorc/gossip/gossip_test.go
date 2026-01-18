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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

func TestVTOrcGossipFlags(t *testing.T) {
	defer utils.PrintVTOrcLogsOnFailure(t, clusterInfo.ClusterInstance)

	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 0, []string{
		"--gossip-enabled",
		"--gossip-seed-addrs", "localhost:16100",
		"--gossip-listen-addr", "localhost:16101",
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
}
