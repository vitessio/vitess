/*
Copyright 2021 The Vitess Authors.

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

package api

import (
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

var clusterInfo *utils.VTOrcClusterInfo

func TestMain(m *testing.M) {
	// setup cellInfos before creating the cluster
	var cellInfos []*utils.CellInfo
	cellInfos = append(cellInfos, &utils.CellInfo{
		CellName:    utils.Cell1,
		NumReplicas: 2,
		NumRdonly:   1,
		UIDBase:     100,
	})
	cellInfos = append(cellInfos, &utils.CellInfo{
		CellName:    utils.Cell2,
		NumReplicas: 0,
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
	} else {
		os.Exit(exitcode)
	}
}
