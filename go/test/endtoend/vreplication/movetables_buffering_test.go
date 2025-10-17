package vreplication

import (
	"testing"
	"time"

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
	waitForWorkflowState(t, vc, defaultKsWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())

	lg := newLoadGenerator(t, vc)
	go func() {
		lg.start()
	}()
	lg.waitForCount(1000)

	catchup(t, targetTab1, defaultWorkflowName, "MoveTables")
	catchup(t, targetTab2, defaultWorkflowName, "MoveTables")
	vdiff(t, defaultTargetKs, defaultWorkflowName, "", nil)
	waitForLowLag(t, defaultTargetKs, defaultWorkflowName)
	for i := 0; i < 10; i++ {
		tstWorkflowSwitchReadsAndWrites(t)
		time.Sleep(loadTestBufferingWindowDuration + 1*time.Second)
		tstWorkflowReverseReadsAndWrites(t)
		time.Sleep(loadTestBufferingWindowDuration + 1*time.Second)
	}
	log.Infof("SwitchWrites done")
	lg.stop()

	log.Infof("TestMoveTablesBuffering: done")
}
