package vreplication

import (
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestMoveTablesBuffering(t *testing.T) {
	defaultRdonly = 1
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	currentWorkflowType = binlogdatapb.VReplicationWorkflowType_MoveTables
	setupMinimalCustomerKeyspace(t)
	tables := "loadtest"
	err := tstWorkflowExec(t, defaultCellName, workflowName, sourceKs, targetKs,
		tables, workflowActionCreate, "", "", "", defaultWorkflowExecOptions)
	require.NoError(t, err)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())

	lg := newLoadGenerator(t, vc)
	go func() {
		lg.start()
	}()
	lg.waitForCount(1000)

	catchup(t, targetTab1, workflowName, "MoveTables")
	catchup(t, targetTab2, workflowName, "MoveTables")
	vdiffSideBySide(t, ksWorkflow, "")
	waitForLowLag(t, "customer", workflowName)
	tstWorkflowSwitchReads(t, "", "")
	tstWorkflowSwitchWrites(t)
	log.Infof("SwitchWrites done")
	lg.stop()

	log.Infof("TestMoveTablesBuffering: done")
}
