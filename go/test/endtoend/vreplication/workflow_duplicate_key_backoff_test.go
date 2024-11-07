package vreplication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

func TestWorkflowDuplicateKeyBackoff(t *testing.T) {
	t.Run("TestWorkflowDuplicateKeyBackoff with batching off", func(t *testing.T) {
		testWorkflowDuplicateKeyBackoff(t, false)
	})
	t.Run("TestWorkflowDuplicateKeyBackoff with batching on", func(t *testing.T) {
		testWorkflowDuplicateKeyBackoff(t, true)
	})
}

func testWorkflowDuplicateKeyBackoff(t *testing.T, setExperimentalFlags bool) {
	debugMode = false
	setSidecarDBName("_vt")
	origDefaultRdonly := defaultRdonly
	origDefailtReplica := defaultReplicas
	defer func() {
		defaultRdonly = origDefaultRdonly
		defaultReplicas = origDefailtReplica
	}()
	defaultRdonly = 0
	defaultReplicas = 0
	if setExperimentalFlags {
		setAllVTTabletExperimentalFlags()
	}

	setupMinimalCluster(t)
	vttablet.InitVReplicationConfigDefaults()
	defer vc.TearDown()

	sourceKeyspaceName := "product"
	targetKeyspaceName := "customer"
	workflowName := "wf1"
	targetTabs := setupMinimalCustomerKeyspace(t)
	_ = targetTabs
	tables := "customer,admins"

	req := &vtctldatapb.UpdateThrottlerConfigRequest{
		Enable: false,
	}
	res, err := throttler.UpdateThrottlerTopoConfigRaw(vc.VtctldClient, "customer", req, nil, nil)
	require.NoError(t, err, res)
	res, err = throttler.UpdateThrottlerTopoConfigRaw(vc.VtctldClient, "product", req, nil, nil)
	require.NoError(t, err, res)

	mt := createMoveTables(t, sourceKeyspaceName, targetKeyspaceName, workflowName, tables, nil, nil, nil)
	waitForWorkflowState(t, vc, "customer.wf1", binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.SwitchReadsAndWrites()
	vtgateConn, cancel := getVTGateConn()
	defer cancel()

	// team_id 1 => 80-, team_id 2 => -80
	queries := []string{
		"update admins set email = null, val = 'ibis-3' where team_id = 2",            // -80
		"update admins set email = 'b@example.com', val = 'ibis-4' where team_id = 1", // 80-
		"update admins set email = 'a@example.com', val = 'ibis-5' where team_id = 2", // -80
	}

	vc.VtctlClient.ExecuteCommandWithOutput("VReplicationExec", "zone1-100", "update _vt.vreplication set state = 'Stopped' where id = 1") //-80
	for _, query := range queries {
		execVtgateQuery(t, vtgateConn, targetKeyspaceName, query)
	}
	// Since -80 is stopped the "update admins set email = 'b@example.com' where team_id = 1" will fail with duplicate key
	// since it is already set for team_id = 2
	// The vplayer stream for -80 should backoff with the new logic and retry should be successful once the -80 stream is restarted
	time.Sleep(2 * time.Second) // fixme: add check that the table has the expected data after the inserts
	vc.VtctlClient.ExecuteCommandWithOutput("VReplicationExec", "zone1-100", "update _vt.vreplication set state = 'Running' where id = 1")
	//time.Sleep(5 * time.Second)
	productTab := vc.Cells["zone1"].Keyspaces[sourceKeyspaceName].Shards["0"].Tablets["zone1-100"].Vttablet
	waitForResult(t, productTab, "product", "select * from admins order by team_id",
		"[[INT32(1) VARCHAR(\"b@example.com\") VARCHAR(\"ibis-4\")] [INT32(2) VARCHAR(\"a@example.com\") VARCHAR(\"ibis-5\")]]", 30*time.Second)
	log.Infof("TestWorkflowDuplicateKeyBackoff passed")
}
