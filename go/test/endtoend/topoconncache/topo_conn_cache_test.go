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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/throttler"
)

/*
 1. In steady state check the numbers of tablets through 'ListAllTablets'.
 2. Now delete 'zone2' and 'ListAllTablets' should return only primary tablets
 3. Add the cell back with same name but at different location in topo.
 4. 'ListAllTablets' should return all the new tablets.
 5. The tablet throttler on shard1's primary, whose topo reads reuse the cell's CellInfo for
    --topo-cell-info-cache-ttl, finds the replica in the re-added 'zone2' and sees its lag.
*/
func TestVtctldListAllTablets(t *testing.T) {
	testURL(t, vtctldHealthCheckURL(), "vtctld health check url")

	testListAllTablets(t)
	testFailedRepoint(t)
	deleteCell(t)
	addCellback(t)
	testThrottlerFollowsReAddedCell(t)
}

func vtctldHealthCheckURL() string {
	return fmt.Sprintf("http://%s:%d/debug/health", clusterInstance.Hostname, clusterInstance.VtctldHTTPPort)
}

// testFailedRepoint points cell2 at an address that refuses connections, as a typo in UpdateCellInfo would, and
// checks that vtctld's reads of the cell fail rather than crash it, and that once the cell is pointed back,
// vtctld serves it again. Before topo.Server.ConnForCell kept the cached connection when connecting to the new
// address failed, the second read closed an already closed connection and vtctld panicked.
func testFailedRepoint(t *testing.T) {
	vtctld := clusterInstance.VtctldClientProcess
	// Nothing listens on port 1, so the connection is refused, and etcd2topo's Create reports that.
	err := vtctld.ExecuteCommand("UpdateCellInfo", "--server-address", "127.0.0.1:1", cell2)
	require.NoError(t, err)

	for range 2 {
		_, err = vtctld.ExecuteCommandWithOutput("GetTablets", "--cell", cell2)
		require.Error(t, err, "GetTablets found tablets in a cell whose topo server is unreachable")
	}
	// Stop here if vtctld died, rather than retry the commands below against it until the test times out.
	require.Equal(t, 200, getStatusForURL(vtctldHealthCheckURL()), "vtctld did not answer its health check after the failed re-point")

	err = vtctld.ExecuteCommand("UpdateCellInfo", "--server-address", vtctld.TopoServerAddress, cell2)
	require.NoError(t, err)
	testListAllTablets(t)
}

func testListAllTablets(t *testing.T) {
	// first w/o any filters, aside from cell
	result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetTablets")
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
	res, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("DeleteCellInfo", "--force", cell2)
	t.Log(res)
	require.NoError(t, err)

	// update clusterInstance to remaining vttablets and shards
	shard1.Vttablets = []*cluster.Vttablet{shard1Primary}
	shard2.Vttablets = []*cluster.Vttablet{shard2Primary}
	clusterInstance.Keyspaces[0].Shards = []cluster.Shard{shard1, shard2}

	// Now list all tablets
	result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetTablets")
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

	err := clusterInstance.VtctldClientProcess.ExecuteCommand("DeleteTablets", tablet.Alias)
	require.NoError(t, err)
}

func addCellback(t *testing.T) {
	// creating new cell , with same name as previously deleted one but at a different root path.
	clusterInstance.VtctldClientProcess.TopoRootPath = "/org1/obj1/"

	err := clusterInstance.VtctldClientProcess.AddCellInfo(cell2)
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
		mysqlctlProcess, err := cluster.MysqlCtlProcessInstance(tablet.TabletUID, tablet.MySQLPort, clusterInstance.TmpDirectory)
		require.NoError(t, err)
		tablet.MysqlctlProcess = *mysqlctlProcess
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
			clusterInstance.DefaultCharset,
		)
		tablet.VttabletProcess.SupportsBackup = true
		tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, cellInfoCacheTTLArgs(tablet)...)
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
	shard2.Vttablets = append(shard2.Vttablets, shard2Rdonly)

	result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("GetTablets")
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

// testThrottlerFollowsReAddedCell enables the tablet throttler and stops replication on shard1's replica in
// the re-added zone2. Shard1's primary reads tablet records through the CellInfo cache, so until the TTL passes
// it may look for the shard's tablets at zone2's old root, where there are none; within the TTL and an
// inventory refresh it must find the replica at the new root and see its lag. Meanwhile it reads CellInfo from
// the global topo less often than shard2's primary, which reads it on every call.
func testThrottlerFollowsReAddedCell(t *testing.T) {
	vtctld := &clusterInstance.VtctldClientProcess
	throttler.EnableLagThrottlerAndWaitForStatus(t, clusterInstance)

	cachedBefore := globalTopoGets(t, shard1Primary)
	uncachedBefore := globalTopoGets(t, shard2Primary)
	countFrom := time.Now()

	// Keep VTOrc from starting replication again.
	vtorc := clusterInstance.VTOrcProcesses[0]
	vtorc.DisableGlobalRecoveries(t)
	require.NoError(t, vtctld.ExecuteCommand("StopReplication", shard1Replica.Alias))
	t.Cleanup(func() {
		_ = vtctld.ExecuteCommand("StartReplication", shard1Replica.Alias)
		vtorc.EnableGlobalRecoveries(t)
	})
	// The TTL, an inventory refresh, the replica's lag growing past the threshold, and time for the probes.
	timeout := cellInfoCacheTTL + 10*time.Second + 5*time.Second + 20*time.Second
	stopped := time.Now()
	require.Eventually(t, func() bool {
		status, err := throttler.GetThrottlerStatus(vtctld, shard1Primary)
		if err != nil {
			return false
		}
		lag, ok := status.AggregatedMetrics["shard/lag"]
		return ok && lag.Value > status.DefaultThreshold
	}, timeout, time.Second, "shard1's primary did not see the lag of the replica in the re-added cell")
	t.Logf("shard1's primary saw the replica's lag %s after replication stopped", time.Since(stopped).Round(time.Second))

	// Compare the primaries' global topo reads over a few inventory refreshes.
	time.Sleep(time.Until(countFrom.Add(35 * time.Second)))
	cached := globalTopoGets(t, shard1Primary) - cachedBefore
	uncached := globalTopoGets(t, shard2Primary) - uncachedBefore
	t.Logf("global topo Gets with the throttler on: %d with the CellInfo cache, %d without", cached, uncached)
	assert.Less(t, cached, uncached, "the primary with the CellInfo cache did not read the global topo less than the one without")
}

// globalTopoGets returns how many Get calls the tablet has made to the global topo.
func globalTopoGets(t *testing.T, tablet *cluster.Vttablet) int64 {
	vars := tablet.VttabletProcess.GetVars()
	require.NotNil(t, vars, "no /debug/vars from %s", tablet.Alias)
	ops, _ := vars["TopologyConnOperations"].(map[string]any)
	histograms, _ := ops["Histograms"].(map[string]any)
	require.Contains(t, histograms, "Get.global", "%s reports no global topo Gets", tablet.Alias)
	get, _ := histograms["Get.global"].(map[string]any)
	count, _ := get["Count"].(float64)
	return int64(count)
}
