package vreplication

import (
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/wrangler"
)

func TestMoveTablesBuffering(t *testing.T) {
	defaultRdonly = 1
	vc = setupMinimalCluster(t)
	defer vtgateConn.Close()
	defer vc.TearDown(t)

	currentWorkflowType = wrangler.MoveTablesWorkflow
	setupMinimalCustomerKeyspace(t)
	tables := "loadtest"
	err := tstWorkflowExec(t, defaultCellName, workflowName, sourceKs, targetKs,
		tables, workflowActionCreate, "", "", "", false)
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
	log.Flush()
}
