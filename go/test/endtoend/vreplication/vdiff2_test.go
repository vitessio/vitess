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

type testCase struct {
	name, typ, sourceKs, targetKs string
	sourceShards, targetShards    string
	tables                        string
	workflow                      string
	tabletBaseID                  int
}

var testCases = []*testCase{
	{
		name:         "MoveTables unsharded to two shards",
		workflow:     "p1c2",
		typ:          "MoveTables",
		sourceKs:     "product",
		targetKs:     "customer",
		sourceShards: "0",
		targetShards: "-80,80-",
		tabletBaseID: 200,
		tables:       "customer,Lead,Lead-1",
	},
	{
		name:         "Reshard Split/Merge 2 to 3",
		workflow:     "c2c3",
		typ:          "Reshard",
		sourceKs:     "customer",
		targetKs:     "customer",
		sourceShards: "-80,80-",
		targetShards: "-40,40-a0,a0-",
		tabletBaseID: 400,
	},
	{
		name:         "Reshard Merge 3 to 1",
		workflow:     "c3c1",
		typ:          "Reshard",
		sourceKs:     "customer",
		targetKs:     "customer",
		sourceShards: "-40,40-a0,a0-",
		targetShards: "0",
		tabletBaseID: 700,
	},
}

func TestVDiff2(t *testing.T) {
	allCellNames = "zone1"
	defaultCellName := "zone1"
	sourceKs := "product"
	sourceShards := []string{"0"}
	targetKs := "customer"
	targetShards := []string{"-80", "80-"}
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

	// Insert null and empty enum values for testing vdiff comparisons for those values.
	// If we add this to the initial data list, the counts in several other tests will need to change
	query := `insert into customer(cid, name, typ, sport) values(1001, null, 'soho','')`
	execVtgateQuery(t, vtgateConn, fmt.Sprintf("%s:%s", sourceKs, sourceShards[0]), query)

	_, err := vc.AddKeyspace(t, cells, targetKs, strings.Join(targetShards, ","), customerVSchema, customerSchema, 0, 0, 200, targetKsOpts)
	require.NoError(t, err)
	err = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, targetShards[0]), 1)
	require.NoError(t, err)
	err = vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKs, targetShards[1]), 1)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testWorkflow(t, vc, tc, cells)
		})
	}
}

func testWorkflow(t *testing.T, vc *VitessCluster, tc *testCase, cells []*Cell) {
	arrTargetShards := strings.Split(tc.targetShards, ",")
	if tc.typ == "Reshard" {
		tks := vc.Cells[cells[0].Name].Keyspaces[tc.targetKs]
		require.NoError(t, vc.AddShards(t, cells, tks, tc.targetShards, 0, 0, tc.tabletBaseID))
	}
	ksWorkflow := fmt.Sprintf("%s.%s", tc.targetKs, tc.workflow)
	var args []string
	args = append(args, tc.typ, "--")
	args = append(args, "--source", tc.sourceKs)
	if tc.typ == "Reshard" {
		args = append(args, "--source_shards", tc.sourceShards, "--target_shards", tc.targetShards)
	}
	args = append(args, "--tables", tc.tables)
	args = append(args, "Create")
	args = append(args, ksWorkflow)
	err := vc.VtctlClient.ExecuteCommand(args...)
	require.NoError(t, err)

	for _, shard := range arrTargetShards {
		tab := vc.getPrimaryTablet(t, tc.targetKs, shard)
		catchup(t, tab, tc.workflow, tc.typ)
	}
	vdiff(t, tc.targetKs, tc.workflow, cells[0].Name, true, true, nil)

	err = vc.VtctlClient.ExecuteCommand(tc.typ, "--", "SwitchTraffic", ksWorkflow)
	require.NoError(t, err)
	err = vc.VtctlClient.ExecuteCommand(tc.typ, "--", "Complete", ksWorkflow)
	require.NoError(t, err)
}
