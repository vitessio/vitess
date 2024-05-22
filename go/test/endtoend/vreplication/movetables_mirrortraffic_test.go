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

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

func testMoveTablesMirrorTraffic(t *testing.T, flavor workflowFlavor) {
	setSidecarDBName("_vt")
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	sourceKeyspace := "product"
	targetKeyspace := "customer"
	workflowName := "wf1"

	_ = setupMinimalCustomerKeyspace(t)

	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         "customer,loadtest,customer2",
		mirrorFlags:    []string{"--percent", "25"},
	}, flavor)

	mt.Create()
	confirmNoMirrorRules(t)

	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())

	mt.MirrorTraffic()
	confirmMirrorRulesExist(t)

	// Each table should have a mirror rule for each serving type.
	mirrorRules := getMirrorRules(t)
	require.Len(t, mirrorRules.Rules, 9)
	fromTableToRule := make(map[string]*vschemapb.MirrorRule)
	for _, rule := range mirrorRules.Rules {
		fromTableToRule[rule.FromTable] = rule
	}
	for _, table := range []string{"customer", "loadtest", "customer2"} {
		for _, tabletType := range []topodatapb.TabletType{
			topodatapb.TabletType_PRIMARY,
			topodatapb.TabletType_REPLICA,
			topodatapb.TabletType_RDONLY,
		} {
			fromTable := fmt.Sprintf("%s.%s", sourceKeyspace, table)
			if tabletType != topodatapb.TabletType_PRIMARY {
				fromTable = fmt.Sprintf("%s@%s", fromTable, topoproto.TabletTypeLString(tabletType))
			}
			require.Contains(t, fromTableToRule, fromTable)
			require.Equal(t, fmt.Sprintf("%s.%s", targetKeyspace, table), fromTableToRule[fromTable].ToTable)
			require.Equal(t, float32(25), fromTableToRule[fromTable].Percent)
		}
	}

	lg := newLoadGenerator(t, vc)
	go func() {
		lg.start()
	}()
	lg.waitForCount(1000)

	mt.SwitchReads()
	confirmMirrorRulesExist(t)

	// Each table should only have a mirror rule for primary serving type.
	mirrorRules = getMirrorRules(t)
	require.Len(t, mirrorRules.Rules, 3)
	fromTableToRule = make(map[string]*vschemapb.MirrorRule)
	for _, rule := range mirrorRules.Rules {
		fromTableToRule[rule.FromTable] = rule
	}
	for _, table := range []string{"customer", "loadtest", "customer2"} {
		fromTable := fmt.Sprintf("%s.%s", sourceKeyspace, table)
		require.Contains(t, fromTableToRule, fromTable)
		require.Equal(t, fmt.Sprintf("%s.%s", targetKeyspace, table), fromTableToRule[fromTable].ToTable)
		require.Equal(t, float32(25), fromTableToRule[fromTable].Percent)
	}

	mt.SwitchWrites()
	confirmNoMirrorRules(t)
}

func TestMoveTablesMirrorTraffic(t *testing.T) {
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	t.Run(workflowFlavorNames[workflowFlavorVtctld], func(t *testing.T) {
		testMoveTablesMirrorTraffic(t, workflowFlavorVtctld)
	})
}
