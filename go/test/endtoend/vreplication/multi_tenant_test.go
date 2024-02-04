package vreplication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type tenantMigrationStatus int

const (
	tenantMigrationStatusNotMigrated tenantMigrationStatus = iota
	tenantMigrationStatusMigrating
	tenantMigrationStatusMigrated

	numTenants = 3
)

type multiTenantMigration struct {
	t                     *testing.T
	tenantMigrationStatus map[int]tenantMigrationStatus
	activeMoveTables      map[int]*VtctldMoveTables

	sourceKeyspaceTemplate string
	targetKeyspace         string
	tables                 string
	tenantIdColumnName     string
}

const (
	targetKeyspaceName = "multi_tenant"
	mtSchema           = "create table t1(id int, tenant_id int, primary key(id, tenant_id)) Engine=InnoDB"
	mtVSchema          = `
{
  "tables": {
    "t1": {}
  }
}
`
)

func getSourceKeyspace(tenantId int) string {
	return fmt.Sprintf("tenant%d", tenantId)
}

func initTenantData(t *testing.T, tenantId int) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	for i := 1; i <= 10; i++ {
		execVtgateQuery(t, vtgateConn, fmt.Sprintf("%s:0", getSourceKeyspace(tenantId)), fmt.Sprintf("insert into t1(id, tenant_id) values(%d, %d)", i, tenantId))
	}
}

func newMultiTenantMigration(t *testing.T) *multiTenantMigration {
	_, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, targetKeyspaceName, "0", mtVSchema, mtSchema, 1, 0, 200, nil)
	require.NoError(t, err)
	mtm := &multiTenantMigration{
		t:                      t,
		tenantMigrationStatus:  make(map[int]tenantMigrationStatus),
		activeMoveTables:       make(map[int]*VtctldMoveTables),
		sourceKeyspaceTemplate: "tenant%d",
		targetKeyspace:         targetKeyspaceName,
		tables:                 "t1",
		tenantIdColumnName:     "tenant_id",
	}
	for i := 1; i <= numTenants; i++ {
		mtm.tenantMigrationStatus[i] = tenantMigrationStatusNotMigrated
	}

	return mtm
}

func (mtm *multiTenantMigration) create(tenantId int) {
	sourceKeyspace := fmt.Sprintf(mtm.sourceKeyspaceTemplate, tenantId)
	_, err := vc.AddKeyspace(mtm.t, []*Cell{vc.Cells["zone1"]}, sourceKeyspace, "0", mtVSchema, mtSchema, 1, 0, 1000+tenantId*100, nil)
	require.NoError(mtm.t, err)
	initTenantData(mtm.t, tenantId)
	mtm.tenantMigrationStatus[tenantId] = tenantMigrationStatusMigrating
	mt := newVtctldMoveTables(&moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   sourceKeyspace,
			targetKeyspace: mtm.targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         mtm.tables,
		createFlags:    []string{"--no-routing-rules", "--additional-filter", fmt.Sprintf("%s=%d", mtm.tenantIdColumnName, tenantId)},
	})
	mtm.activeMoveTables[tenantId] = mt
	mt.Create()
}

func TestMultiTenant(t *testing.T) {
	setSidecarDBName("_vt")
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultRdonly = origDefaultRdonly
	}()
	defaultRdonly = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	mtm := newMultiTenantMigration(t)
	numMigrated := 0
	for i := 1; i <= numTenants; i++ {
		migrateTenant(t, mtm, i)
		numMigrated++
	}
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	require.Equal(t, numMigrated*10, getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", mtm.targetKeyspace, "t1")))
}

func migrateTenant(t *testing.T, mtm *multiTenantMigration, tenantId int) {
	mtm.create(tenantId)
	mt := mtm.activeMoveTables[tenantId]
	ksWorkflow := fmt.Sprintf("%s.%s", mtm.targetKeyspace, mt.workflowName)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.SwitchReadsAndWrites()
	mt.Complete()
}
