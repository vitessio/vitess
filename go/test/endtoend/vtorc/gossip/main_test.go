package gossip

import (
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

var clusterInfo *utils.VTOrcClusterInfo

func TestMain(m *testing.M) {
	cellInfos := []*utils.CellInfo{
		{
			CellName:    utils.Cell1,
			NumReplicas: 2,
			NumRdonly:   0,
			UIDBase:     300,
		},
	}

	exitCode, err := func() (int, error) {
		var err error
		clusterInfo, err = utils.CreateClusterAndStartTopo(cellInfos)
		if err != nil {
			return 1, err
		}

		return m.Run(), nil
	}()

	if clusterInfo != nil {
		for _, vtorcProcess := range clusterInfo.ClusterInstance.VTOrcProcesses {
			_ = vtorcProcess.TearDown()
		}

		for _, cellInfo := range clusterInfo.CellInfos {
			utils.KillTablets(cellInfo.ReplicaTablets)
			utils.KillTablets(cellInfo.RdonlyTablets)
		}
		clusterInfo.ClusterInstance.Keyspaces[0].Shards[0].Vttablets = nil
		clusterInfo.ClusterInstance.Teardown()
	}

	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	os.Exit(exitCode)
}
