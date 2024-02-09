package vreplication

import (
	"encoding/json"
	"fmt"
	"sync"
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
	mu                    sync.Mutex
	tenantMigrationStatus map[int]tenantMigrationStatus
	activeMoveTables      map[int]*VtctldMoveTables

	targetKeyspace     string
	tables             string
	tenantIdColumnName string

	lastIDs map[int]int
}

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
		lastIDs:               make(map[int]int),
	}
	for i := 1; i <= numTenants; i++ {
		mtm.setTenantMigrationStatus(i, tenantMigrationStatusNotMigrated)
	}
	return mtm
}

func (mtm *multiTenantMigration) getTenantMigrationStatus(tenantId int) tenantMigrationStatus {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	return mtm.tenantMigrationStatus[tenantId]
}

func (mtm *multiTenantMigration) setTenantMigrationStatus(tenantId int, status tenantMigrationStatus) {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	mtm.tenantMigrationStatus[tenantId] = status
}

func (mtm *multiTenantMigration) getActiveMoveTables(tenantId int) *VtctldMoveTables {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	return mtm.activeMoveTables[tenantId]
}

func (mtm *multiTenantMigration) setActiveMoveTables(tenantId int, mt *VtctldMoveTables) {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	mtm.activeMoveTables[tenantId] = mt
}

func (mtm *multiTenantMigration) setLastID(tenantId, lastID int) {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	mtm.lastIDs[tenantId] = lastID
}

func (mtm *multiTenantMigration) getLastID(tenantId int) int {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	return mtm.lastIDs[tenantId]
}

func (mtm *multiTenantMigration) initTenantData(t *testing.T, tenantId int, userKeyspace string) {
	mtm.insertSomeData(t, tenantId, userKeyspace, 10)
}

func (mtm *multiTenantMigration) create(tenantId int) {
	mtm.setLastID(tenantId, 0)
	sourceKeyspace := getSourceKeyspace(tenantId)
	userKeyspace := getUserKeyspace(tenantId)
	_, err := vc.AddKeyspace(mtm.t, []*Cell{vc.Cells["zone1"]}, sourceKeyspace, "0", mtVSchema, mtSchema, 1, 0, 1000+tenantId*100, nil)
	require.NoError(mtm.t, err)
	updateKeyspaceRoutingRules(mtm.t, vc, userKeyspace, sourceKeyspace)
	log.Flush()
	mtm.initTenantData(mtm.t, tenantId, userKeyspace)
	mtm.setTenantMigrationStatus(tenantId, tenantMigrationStatusMigrating)
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
	mtm.setActiveMoveTables(tenantId, mt)
	mt.Create()
}

func (mtm *multiTenantMigration) insertSomeData(t *testing.T, tenantId int, userKeyspace string, numRows int) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	idx := mtm.getLastID(tenantId)
	for i := idx + 1; i <= idx+numRows; i++ {
		execVtgateQuery(t, vtgateConn, "",
			fmt.Sprintf("insert into %s.t1(id, tenant_id) values(%d, %d)", userKeyspace, i, tenantId))
	}
	mtm.setLastID(tenantId, idx+numRows)
}

func (mtm *multiTenantMigration) startTenantMigration(t *testing.T, tenantId int) {
	userKeyspace := getUserKeyspace(tenantId)
	mtm.create(tenantId)
	printKeyspaceRoutingRules(t, vc, "After MoveTables Create")
	mt := mtm.activeMoveTables[tenantId]
	ksWorkflow := fmt.Sprintf("%s.%s", mtm.targetKeyspace, mt.workflowName)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mtm.insertSomeData(t, tenantId, userKeyspace, 10)
	mt.SwitchReadsAndWrites()
	printKeyspaceRoutingRules(t, vc, "After MoveTables SwitchTraffic")
	mtm.insertSomeData(t, tenantId, userKeyspace, 10)
	mt.Complete()
	printKeyspaceRoutingRules(t, vc, "After MoveTables Complete")
	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()
	waitForRowCount(t, vtgateConn, targetKeyspaceName, "t1", mtm.lastIDs[tenantId])
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
		mtm.startTenantMigration(t, i)
		numMigrated++
	}
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	require.Equal(t, numMigrated*10*3, getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", mtm.targetKeyspace, "t1")))
}
