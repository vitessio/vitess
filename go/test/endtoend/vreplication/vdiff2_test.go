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
	"testing"
	"time"

	"github.com/buger/jsonparser"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestVDiff2(t *testing.T) {
	allCellNames = "zone1"
	defaultCellName := "zone1"
	workflow := "wf1"
	sourceKs := "product"
	targetKs := "customer"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKs, workflow)

	vc = NewVitessCluster(t, "TestVDiff2", []string{"zone1"}, mainClusterConfig)
	require.NotNil(t, vc)
	defaultCell = vc.Cells[defaultCellName]
	cells := []*Cell{defaultCell}

	defer vc.TearDown(t)

	cell1 := vc.Cells["zone1"]
	vc.AddKeyspace(t, []*Cell{cell1}, sourceKs, "0", initialProductVSchema, initialProductSchema, 0, 0, 100, sourceKsOpts)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "product", "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	productTab := vc.Cells[defaultCell.Name].Keyspaces[sourceKs].Shards["0"].Tablets["zone1-100"].Vttablet
	_ = productTab
	insertInitialData(t)

	// Insert null and empty enum values for testing vdiff comparisons for those values.
	// If we add this to the initial data list, the counts in several other tests will need to change
	query := "insert into customer(cid, name, typ, sport) values(1001, null, 'soho','');"
	execVtgateQuery(t, vtgateConn, "product:0", query)

	if _, err := vc.AddKeyspace(t, cells, targetKs, "-80,80-", customerVSchema, customerSchema, 0, 0, 200, targetKsOpts); err != nil {
		require.NoError(t, err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "customer", "-80"), 1); err != nil {
		require.NoError(t, err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", "customer", "80-"), 1); err != nil {
		require.NoError(t, err)
	}

	defaultCell := vc.Cells["zone1"]
	custKs := vc.Cells[defaultCell.Name].Keyspaces[targetKs]
	customerTab1 := custKs.Shards["-80"].Tablets["zone1-200"].Vttablet
	customerTab2 := custKs.Shards["80-"].Tablets["zone1-300"].Vttablet
	tables := "customer,customer2,Lead"
	output, err := vc.VtctlClient.ExecuteCommandWithOutput("MoveTables", "--", "--source", sourceKs, "--tables",
		tables, "Create", ksWorkflow)
	require.NoError(t, err, output)

	catchup(t, customerTab1, workflow, "MoveTables")
	catchup(t, customerTab2, workflow, "MoveTables")

	vdiff(t, ksWorkflow, "") // confirm vdiff gives same results as vdiff2

	uuid := vdiff2(t, ksWorkflow, "create", "")
	time.Sleep(5 * time.Second) // wait for vdiffs on tablets to start
	vdiff2(t, ksWorkflow, "show", uuid)
}

func vdiff2(t *testing.T, ksWorkflow, command, subCommand string) string {
	var uuid string
	t.Run(fmt.Sprintf("vdiff2.%s", command), func(t *testing.T) {
		output, err := vc.VtctlClient.ExecuteCommandWithOutput("VDiff", "--", "--v2", "-format", "json", ksWorkflow, command, subCommand)
		log.Infof("vdiff2 err: %+v, output: %+v", err, output)
		require.Nil(t, err)

		uuid, err = jsonparser.GetString([]byte(output), "UUID")
		require.NoError(t, err)
		require.NotEmpty(t, uuid)
	})
	return uuid
}
