/*
Copyright 2026 The Vitess Authors.

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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestMoveTablesBuffering(t *testing.T) {
	ogReplicas := defaultReplicas
	ogRdOnly := defaultRdonly
	defer func() {
		defaultReplicas = ogReplicas
		defaultRdonly = ogRdOnly
	}()
	defaultRdonly = 0
	defaultReplicas = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	setupMinimalTargetKeyspace(t)
	tables := "loadtest"
	err := tstWorkflowExec(t, defaultCellName, defaultWorkflowName, defaultSourceKs, defaultTargetKs,
		tables, workflowActionCreate, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)
	require.NoError(t, waitForWorkflowState(vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String()))

	lg := newLoadGenerator(t, vc)
	go func() {
		lg.start()
	}()
	lg.waitForCount(1000)

	catchup(t, targetTab1, defaultWorkflowName, "MoveTables")
	catchup(t, targetTab2, defaultWorkflowName, "MoveTables")
	vdiff(t, defaultTargetKs, defaultWorkflowName, "", nil)
	waitForLowLag(t, defaultTargetKs, defaultWorkflowName)
	for range 10 {
		tstWorkflowSwitchReadsAndWrites(t)
		lg.waitForTrafficToResume()
		tstWorkflowReverseReadsAndWrites(t)
		lg.waitForTrafficToResume()
	}
	log.Info("SwitchWrites done")
	lg.stop()

	log.Info("TestMoveTablesBuffering: done")
}
