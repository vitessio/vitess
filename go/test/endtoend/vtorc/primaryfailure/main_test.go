package primaryfailure

import (
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/vtorc/utils"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var clusterInfo *utils.VtOrcClusterInfo

func TestMain(m *testing.M) {
	// setup cellInfos before creating the cluster
	var cellInfos []*utils.CellInfo
	cellInfos = append(cellInfos, &utils.CellInfo{
		CellName:    utils.Cell1,
		NumReplicas: 12,
		NumRdonly:   2,
		UIDBase:     100,
	})
	cellInfos = append(cellInfos, &utils.CellInfo{
		CellName:    utils.Cell2,
		NumReplicas: 2,
		NumRdonly:   0,
		UIDBase:     200,
	})

	exitcode, err := func() (int, error) {
		var err error
		clusterInfo, err = utils.CreateClusterAndStartTopo(cellInfos)
		if err != nil {
			return 1, err
		}

		return m.Run(), nil
	}()

	cluster.PanicHandler(nil)

	if clusterInfo != nil {
		// stop vtorc first otherwise its logs get polluted
		// with instances being unreachable triggering unnecessary operations
		if clusterInfo.ClusterInstance.VtorcProcess != nil {
			_ = clusterInfo.ClusterInstance.VtorcProcess.TearDown()
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
	} else {
		os.Exit(exitcode)
	}
}
