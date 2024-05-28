/*
Copyright 2024 The Vitess Authors.

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
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func testMoveTablesMirrorTraffic(t *testing.T, flavor workflowFlavor) {
	setSidecarDBName("_vt")
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	sourceKeyspace := "product"
	targetKeyspace := "customer"
	workflowName := "wf1"
	tables := []string{"customer", "loadtest", "customer2"}

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

	expectMirrorRules(t, sourceKeyspace, targetKeyspace, tables, []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}, 25)

	lg := newLoadGenerator(t, vc)
	go func() {
		lg.start()
	}()
	lg.waitForCount(1000)

	mt.SwitchReads()
	confirmMirrorRulesExist(t)

	expectMirrorRules(t, sourceKeyspace, targetKeyspace, tables, []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
	}, 25)

	mt.SwitchWrites()
	confirmNoMirrorRules(t)
}

func TestMoveTablesMirrorTraffic(t *testing.T) {
	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	t.Run(workflowFlavorNames[workflowFlavorVtctld], func(t *testing.T) {
		testMoveTablesMirrorTraffic(t, workflowFlavorVtctld)
	})
}
