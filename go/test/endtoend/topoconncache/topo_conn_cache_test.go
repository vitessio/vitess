/*
Copyright 2022 The Vitess Authors.

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

package topoconncache

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

/*
1. In steady state check the numbers of tablets through 'ListAllTablets'.
2. Now delete 'zone2' and 'ListAllTablets' should return only primary tablets
3. Add the cell back with same name but at different location in topo.
4. 'ListAllTablets' should return all the new tablets.
*/
func TestVtctldListAllTablets(t *testing.T) {
	defer cluster.PanicHandler(t)
	url := fmt.Sprintf("http://%s:%d/api/keyspaces/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, url, "keyspace url")

	healthCheckURL := fmt.Sprintf("http://%s:%d/debug/health/", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
	testURL(t, healthCheckURL, "vtctld health check url")

	testListAllTablets(t)
	deleteCell(t)
	addCellback(t)
}

func testListAllTablets(t *testing.T) {
	// first w/o any filters, aside from cell
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets")
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
	assert.Equal(t, len(tablets), tabletCountFromCMD)
}

func deleteCell(t *testing.T) {
	// delete all tablets for cell2
	deleteTablet(t, shard1Replica)
	deleteTablet(t, shard2Replica)
	deleteTablet(t, shard1Rdonly)
	deleteTablet(t, shard2Rdonly)

	// Delete cell2 info from topo
	res, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("DeleteCellInfo", "--", "--force", cell2)
	t.Log(res)
	require.NoError(t, err)

	// update clusterInstance to remaining vttablets and shards
	shard1.Vttablets = []*cluster.Vttablet{shard1Primary}
	shard2.Vttablets = []*cluster.Vttablet{shard2Primary}
	clusterInstance.Keyspaces[0].Shards = []cluster.Shard{shard1, shard2}

	// Now list all tablets
	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets")
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
	assert.Equal(t, len(tablets), tabletCountFromCMD)
}

func deleteTablet(t *testing.T, tablet *cluster.Vttablet) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func(tablet *cluster.Vttablet) {
		defer wg.Done()
		_ = tablet.VttabletProcess.TearDown()
		_ = tablet.MysqlctlProcess.Stop()
		tablet.MysqlctlProcess.CleanupFiles(tablet.TabletUID)
	}(tablet)
	wg.Wait()

	err := clusterInstance.VtctlclientProcess.ExecuteCommand("DeleteTablet", tablet.Alias)
	require.NoError(t, err)
}

func addCellback(t *testing.T) {
	// creating new cell , with same name as previously deleted one but at a different root path.
	clusterInstance.VtctlProcess.TopoRootPath = "/org1/obj1/"

	err := clusterInstance.VtctlProcess.AddCellInfo(cell2)
	require.NoError(t, err)

	// create new vttablets
	shard1Replica = clusterInstance.NewVttabletInstance("replica", 0, cell2)
	shard1Rdonly = clusterInstance.NewVttabletInstance("rdonly", 0, cell2)
	shard2Replica = clusterInstance.NewVttabletInstance("replica", 0, cell2)
	shard2Rdonly = clusterInstance.NewVttabletInstance("rdonly", 0, cell2)

	// update clusterInstance to new vttablets and shards
	shard1.Vttablets = []*cluster.Vttablet{shard1Primary, shard1Replica, shard1Rdonly}
	shard2.Vttablets = []*cluster.Vttablet{shard2Primary, shard2Replica, shard2Rdonly}
	clusterInstance.Keyspaces[0].Shards = []cluster.Shard{shard1, shard2}

	// create sql process for vttablets
	var mysqlProcs []*exec.Cmd
	for _, tablet := range []*cluster.Vttablet{shard1Replica, shard1Rdonly, shard2Replica, shard2Rdonly} {
		tablet.MysqlctlProcess = *cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
		tablet.VttabletProcess = cluster.VttabletProcessInstance(tablet.HTTPPort,
			tablet.GrpcPort,
			tablet.TabletUID,
			tablet.Cell,
			"",
			keyspaceName,
			clusterInstance.VtctldProcess.Port,
			tablet.Type,
			clusterInstance.TopoPort,
			hostname,
			clusterInstance.TmpDirectory,
			commonTabletArg,
			true,
			clusterInstance.DefaultCharset,
		)
		tablet.VttabletProcess.SupportsBackup = true
		proc, err := tablet.MysqlctlProcess.StartProcess()
		require.NoError(t, err)
		mysqlProcs = append(mysqlProcs, proc)
	}
	for _, proc := range mysqlProcs {
		err := proc.Wait()
		require.NoError(t, err)
	}

	for _, tablet := range []*cluster.Vttablet{shard1Replica, shard1Rdonly} {
		tablet.VttabletProcess.Shard = shard1.Name
		// The tablet should come up as serving since the primary for the shard already exists
		tablet.VttabletProcess.ServingStatus = "SERVING"
		err := tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	for _, tablet := range []*cluster.Vttablet{shard2Replica, shard2Rdonly} {
		tablet.VttabletProcess.Shard = shard2.Name
		// The tablet should come up as serving since the primary for the shard already exists
		tablet.VttabletProcess.ServingStatus = "SERVING"
		err := tablet.VttabletProcess.Setup()
		require.NoError(t, err)
	}

	shard1.Vttablets = append(shard1.Vttablets, shard1Replica)
	shard1.Vttablets = append(shard1.Vttablets, shard1Rdonly)
	shard2.Vttablets = append(shard2.Vttablets, shard2Replica)
	shard2.Vttablets = append(shard2.Vttablets, shard1Rdonly)

	result, err := clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput("ListAllTablets")
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
	assert.Equal(t, len(tablets), tabletCountFromCMD)
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
