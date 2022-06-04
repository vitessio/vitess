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

package vreplication

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVDiff2(t *testing.T) {
	allCellNames = "zone1"
	defaultCellName := "zone1"
	workflow := "wf1"
	sourceKs := "product"
	sourceShards := []string{"0"}
	targetKs := "customer"
	targetShards := []string{"-80", "80-"}
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)
	extraVTTabletArgs = []string{"--vstream_packet_size=1"}

	vc = NewVitessCluster(t, "TestVDiff2", []string{allCellNames}, mainClusterConfig)
	require.NotNil(t, vc)
	defaultCell = vc.Cells[defaultCellName]
	cells := []*Cell{defaultCell}

	defer vc.TearDown(t)

	vc.AddKeyspace(t, cells, sourceKs, strings.Join(sourceShards, ","), initialProductVSchema, initialProductSchema, 0, 0, 100, sourceKsOpts)

	vtgate = defaultCell.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", sourceKs, sourceShards[0]), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	insertInitialData(t)

	// Insert null and empty enum values for testing vdiff1 comparisons for those values.
	// If we add this to the initial data list, the counts in several other tests will need to change
	query := `insert into customer(cid, name, typ, sport) values(1001, null, 'soho','')`
	execVtgateQuery(t, vtgateConn, fmt.Sprintf("%s:%s", sourceKs, sourceShards[0]), query)

	_, err := vc.AddKeyspace(t, cells, targetKs, strings.Join(targetShards, ","), customerVSchema, customerSchema, 0, 0, 200, targetKsOpts)
	require.NoError(t, err)
	err = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, targetShards[0]), 1)
	require.NoError(t, err)
	err = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, targetShards[1]), 1)
	require.NoError(t, err)

	custKs := vc.Cells[defaultCell.Name].Keyspaces[targetKs]
	customerTab1 := custKs.Shards[targetShards[0]].Tablets[fmt.Sprintf("%s-200", defaultCell.Name)].Vttablet
	customerTab2 := custKs.Shards[targetShards[1]].Tablets[fmt.Sprintf("%s-300", defaultCell.Name)].Vttablet
	tables := "customer"
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "--", "--source", sourceKs, "--tables",
		tables, "Create", ksWorkflow)
	require.NoError(t, err, output)
	require.NotEmpty(t, output, "No output returned from MoveTables")

	catchup(t, customerTab1, workflow, "MoveTables")
	catchup(t, customerTab2, workflow, "MoveTables")

	vdiff(t, targetKs, workflowName, "", true, true, nil)

	// now test 2 => 3 shards split/merge
	require.NoError(t, vc.AddShards(t, cells, custKs, "-40,40-a0,a0-", 0, 0, 400))
	err = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, "-40"), 1)
	require.NoError(t, err)
	err = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, "40-a0"), 1)
	require.NoError(t, err)
	err = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, "a0-"), 1)
	require.NoError(t, err)

	ksWorkflow = "customer.c2c3"
	workflowName := "c2c3"
	if err := vc.VtctlClient.ExecuteCommand("Reshard", "--", "--source_shards", "-80,80-", "--target_shards", "-40,40-a0,a0-", "Create", ksWorkflow); err != nil {
		t.Fatalf("Reshard create failed with %+v\n", err)
	}
	require.NoError(t, err, output)
	require.NotEmpty(t, output, "No output returned from Reshard")

	customerTab3 := custKs.Shards["-40"].Tablets[fmt.Sprintf("%s-400", defaultCell.Name)].Vttablet
	catchup(t, customerTab3, workflowName, "Reshard")
	customerTab4 := custKs.Shards["40-a0"].Tablets[fmt.Sprintf("%s-500", defaultCell.Name)].Vttablet
	catchup(t, customerTab4, workflowName, "Reshard")
	customerTab5 := custKs.Shards["a0-"].Tablets[fmt.Sprintf("%s-600", defaultCell.Name)].Vttablet
	catchup(t, customerTab5, workflowName, "Reshard")

	vdiff(t, targetKs, workflowName, "", true, true, nil)
	if err := vc.VtctlClient.ExecuteCommand("Reshard", "--", "SwitchTraffic", ksWorkflow); err != nil {
		t.Fatalf("Reshard SwitchTraffic failed with %+v\n", err)
	}
	require.NoError(t, err, output)

	if err := vc.VtctlClient.ExecuteCommand("Reshard", "--", "Complete", ksWorkflow); err != nil {
		t.Fatalf("Reshard Complete failed with %+v\n", err)
	}
	require.NoError(t, err, output)

	// now test 3=>1 merge

	require.NoError(t, vc.AddShards(t, cells, custKs, "0", 0, 0, 700))
	err = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, "0"), 1)
	require.NoError(t, err)

	ksWorkflow = "customer.c3c1"
	workflowName = "c3c1"
	if err := vc.VtctlClient.ExecuteCommand("Reshard", "--", "--source_shards", "-40,40-a0,a0-", "--target_shards", "0", "Create", ksWorkflow); err != nil {
		t.Fatalf("Reshard create failed with %+v\n", err)
	}
	require.NoError(t, err, output)
	require.NotEmpty(t, output, "No output returned from Reshard")

	customerTab7 := custKs.Shards["0"].Tablets[fmt.Sprintf("%s-700", defaultCell.Name)].Vttablet
	catchup(t, customerTab7, workflowName, "Reshard")

	vdiff(t, targetKs, workflowName, "", true, true, nil)
	if err := vc.VtctlClient.ExecuteCommand("Reshard", "--", "SwitchTraffic", ksWorkflow); err != nil {
		t.Fatalf("Reshard SwitchTraffic failed with %+v\n", err)
	}
	require.NoError(t, err, output)

}
