package vreplication

import (
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

func TestWorkflowDuplicateKeyBackoff(t *testing.T) {
	setSidecarDBName("_vt")
	origDefaultRdonly := defaultRdonly
	origDefailtReplica := defaultReplicas
	defer func() {
		defaultRdonly = origDefaultRdonly
		defaultReplicas = origDefailtReplica
	}()
	defaultRdonly = 0
	defaultReplicas = 0

	setupMinimalCluster(t)
	vttablet.InitVReplicationConfigDefaults()
	defer vc.TearDown()

	sourceKeyspaceName := "product"
	targetKeyspaceName := "customer"
	workflowName := "wf1"
	targetTabs := setupMinimalCustomerKeyspace(t)
	_ = targetTabs
	tables := "customer,admins"

	mt := createMoveTables(t, sourceKeyspaceName, targetKeyspaceName, workflowName, tables, nil, nil, nil)
	waitForWorkflowState(t, vc, "customer.wf1", binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.SwitchReadsAndWrites()
	vtgateConn, cancel := getVTGateConn()
	defer cancel()
	queries := []string{
		"update admins set email = null where team_id = 2",
		"update admins set email = 'b@example.com' where team_id = 1",
		"update admins set email = 'a@example.com' where team_id = 2",
	}

	vc.VtctlClient.ExecuteCommandWithOutput("VReplicationExec", "zone1-100", "update _vt.vreplication set state = 'Stopped' where id = 2")
	for _, query := range queries {
		execVtgateQuery(t, vtgateConn, targetKeyspaceName, query)
	}

}
