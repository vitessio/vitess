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
