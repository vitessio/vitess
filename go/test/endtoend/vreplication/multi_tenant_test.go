package vreplication

import (
	"encoding/json"
	"fmt"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vschema"

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type tenantMigrationStatus int

const (
	tenantMigrationStatusNotMigrated tenantMigrationStatus = iota
	tenantMigrationStatusMigrating
	tenantMigrationStatusMigrated

	numTenants             = 3
	sourceKeyspaceTemplate = "unmanaged_tenant%d"
	userKeyspaceTemplate   = "userks_tenant%d"
	targetKeyspaceName     = "multi_tenant"
)

type multiTenantMigration struct {
	t                     *testing.T
	tenantMigrationStatus map[int]tenantMigrationStatus
	activeMoveTables      map[int]*VtctldMoveTables

	targetKeyspace     string
	tables             string
	tenantIdColumnName string
}

var lastIDs = make(map[int]int)

const (
	mtSchema  = "create table t1(id int, tenant_id int, primary key(id, tenant_id)) Engine=InnoDB"
	mtVSchema = `
{
  "tables": {
    "t1": {}
  }
}
`
)

func getSourceKeyspace(tenantId int) string {
	return fmt.Sprintf(sourceKeyspaceTemplate, tenantId)
}

func getUserKeyspace(tenantId int) string {
	return fmt.Sprintf(userKeyspaceTemplate, tenantId)
}

func initTenantData(t *testing.T, tenantId int, userKeyspace string) {
	insertSomeData(t, tenantId, userKeyspace, 10)
}

func newMultiTenantMigration(t *testing.T) *multiTenantMigration {
	_, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, targetKeyspaceName, "0", mtVSchema, mtSchema, 1, 0, 200, nil)
	require.NoError(t, err)
	mtm := &multiTenantMigration{
		t:                     t,
		tenantMigrationStatus: make(map[int]tenantMigrationStatus),
		activeMoveTables:      make(map[int]*VtctldMoveTables),
		targetKeyspace:        targetKeyspaceName,
		tables:                "t1",
		tenantIdColumnName:    "tenant_id",
	}
	for i := 1; i <= numTenants; i++ {
		mtm.tenantMigrationStatus[i] = tenantMigrationStatusNotMigrated
	}

	return mtm
}

func printKeyspaceRoutingRules(t *testing.T, vc *VitessCluster, msg string) {
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("GetKeyspaceRoutingRules")
	require.NoError(t, err)
	log.Infof("%s: Keyspace routing rules are: %s", msg, output)
}

func updateKeyspaceRoutingRules(t *testing.T, vc *VitessCluster, fromKeyspace, toKeyspace string) {
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("GetKeyspaceRoutingRules")
	require.NoError(t, err)
	var rules vschema.KeyspaceRoutingRules
	err = protojson.Unmarshal([]byte(output), &rules)
	require.NoError(t, err)
	found := false
	for _, rule := range rules.Rules {
		if rule.FromKeyspace == fromKeyspace {
			rule.ToKeyspace = toKeyspace
			found = true
		}
	}
	if !found {
		rules.Rules = append(rules.Rules, &vschema.KeyspaceRoutingRule{
			FromKeyspace: fromKeyspace,
			ToKeyspace:   toKeyspace,
		})
	}
	newRulesJSON, err := json.Marshal(&rules)
	require.NoError(t, err)
	log.Infof("Applying new keyspace routing rules: %s", newRulesJSON)
	err = vc.VtctldClient.ExecuteCommand("ApplyKeyspaceRoutingRules", "--rules", string(newRulesJSON))
	require.NoError(t, err)
	printKeyspaceRoutingRules(t, vc, "After creating unmatched keyspace")
}

func (mtm *multiTenantMigration) create(tenantId int) {
	lastIDs[tenantId] = 0
	sourceKeyspace := getSourceKeyspace(tenantId)
	userKeyspace := getUserKeyspace(tenantId)
	_, err := vc.AddKeyspace(mtm.t, []*Cell{vc.Cells["zone1"]}, sourceKeyspace, "0", mtVSchema, mtSchema, 1, 0, 1000+tenantId*100, nil)
	require.NoError(mtm.t, err)
	updateKeyspaceRoutingRules(mtm.t, vc, userKeyspace, sourceKeyspace)
	log.Flush()
	initTenantData(mtm.t, tenantId, userKeyspace)
	mtm.tenantMigrationStatus[tenantId] = tenantMigrationStatusMigrating
	mt := newVtctldMoveTables(&moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   fmt.Sprintf("wf_tenant%d", tenantId),
			targetKeyspace: mtm.targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         mtm.tables,
		createFlags: []string{
			"--additional-filter", fmt.Sprintf("%s=%d", mtm.tenantIdColumnName, tenantId),
			"--use-keyspace-routing-rules",
			"--source-keyspace-alias", userKeyspace,
		},
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
	tenantsToMigrate := 1 //numTenants
	for i := 1; i <= tenantsToMigrate; i++ {
		migrateTenant(t, mtm, i)
		numMigrated++
	}
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	require.Equal(t, numMigrated*10*3, getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", mtm.targetKeyspace, "t1")))
}

func insertSomeData(t *testing.T, tenantId int, userKeyspace string, numRows int) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	for i := lastIDs[tenantId] + 1; i <= lastIDs[tenantId]+numRows; i++ {
		execVtgateQuery(t, vtgateConn, "",
			fmt.Sprintf("insert into %s.t1(id, tenant_id) values(%d, %d)", userKeyspace, i, tenantId))
	}
	lastIDs[tenantId] = lastIDs[tenantId] + numRows
}

func migrateTenant(t *testing.T, mtm *multiTenantMigration, tenantId int) {
	userKeyspace := getUserKeyspace(tenantId)
	mtm.create(tenantId)
	printKeyspaceRoutingRules(t, vc, "After MoveTables Create")
	mt := mtm.activeMoveTables[tenantId]
	ksWorkflow := fmt.Sprintf("%s.%s", mtm.targetKeyspace, mt.workflowName)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	insertSomeData(t, tenantId, userKeyspace, 10)
	mt.SwitchReadsAndWrites()
	printKeyspaceRoutingRules(t, vc, "After MoveTables SwitchTraffic")
	insertSomeData(t, tenantId, userKeyspace, 10)
	mt.Complete()
	printKeyspaceRoutingRules(t, vc, "After MoveTables Complete")
	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()
	waitForRowCount(t, vtgateConn, targetKeyspaceName, "t1", lastIDs[tenantId])
}
