package vreplication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/datapump"
)

func TestVreplicationStress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defaultCellName := "zone1"
	allCells := []string{"zone1"}
	allCellNames = "zone1"
	mainClusterConfig.packetSize = 1
	vc = NewVitessCluster(t, "TestVreplicationStress", allCells, mainClusterConfig)
	require.NotNil(t, vc)

	//defer vc.TearDown()

	sourceKs := "stress"
	targetKs := "stress2"
	defaultCell = vc.Cells[defaultCellName]
	vc.AddKeyspace(t, []*Cell{defaultCell}, sourceKs, "0", "", "", 0, 0, 100)
	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", sourceKs, "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	vc.AddKeyspace(t, []*Cell{defaultCell}, targetKs, "0", "", "", 0, 0, 200)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.master", targetKs, "0"), 1)

	tdp, err := datapump.NewTestDataPump(t, sourceKs, vtgateConn, 1)
	if output, err := vc.VtctlClient.ExecuteCommandWithOutput("ReloadSchemaKeyspace", "-include_master=true", sourceKs); err != nil {
		t.Fatalf("ReloadSchemaKeyspace command failed with %s, %+v\n", output, err)
	}

	tdp.Start(ctx)
	require.NoError(t, err)
	require.NotNil(t, tdp)
	// wait for some data for copy phase
	time.Sleep(1 * time.Second)

	workflow := "stresswf"
	ksWorkflow := "stress2.stresswf"
	moveTables(t, defaultCellName, workflow, sourceKs, targetKs, "stress_test_0")
	stressKs := vc.Cells[defaultCell.Name].Keyspaces["stress"]
	stressTab := stressKs.Shards["0"].Tablets["zone1-100"].Vttablet

	// wait for some data for replicate phase
	time.Sleep(3 * time.Second)
	cancel()

	// let movetables catchup
	time.Sleep(1 * time.Second)
	catchup(t, stressTab, workflow, "MoveTables")

	vdiff(t, ksWorkflow, "")

}
