package vreplication

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

	sourceKeyspaceTemplate      = "unmanaged_tenant%d"
	sourceAliasKeyspaceTemplate = "sourceAlias_tenant%d"
	targetKeyspaceName          = "multi_tenant"

	numTenants                 = 50
	numInitialRowsPerTenant    = 100
	numAdditionalRowsPerTenant = 100
	baseInitialTabletId        = 1000
	tabletIdStep               = 100
	maxRandomDelaySeconds      = 5
	waitTimeout                = 10 * time.Minute
)

var (
	// channels to coordinate the migration workflow
	chNotSetup, chNotCreated, chInProgress, chSwitched, chCompleted chan int64
	// counters to keep track of the number of tenants in each state
	numSetup, numInProgress, numSwitched, numCompleted atomic.Int64
)

// multiTenantMigration manages the migration of multiple tenants to a single target keyspace.
// A singleton object of this type is created for the test case.
type multiTenantMigration struct {
	t                     *testing.T
	mu                    sync.Mutex
	tenantMigrationStatus map[int64]tenantMigrationStatus // current migration status for each tenant
	activeMoveTables      map[int64]*VtctldMoveTables     // the internal MoveTables object for each tenant

	targetKeyspace     string
	tables             string
	tenantIdColumnName string // the name of the column in each table that holds the tenant ID

	lastIDs map[int64]int64 // the last primary key inserted for each tenant
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

func getSourceKeyspace(tenantId int64) string {
	return fmt.Sprintf(sourceKeyspaceTemplate, tenantId)
}

func getSourceAliasKeyspace(tenantId int64) string {
	return fmt.Sprintf(sourceAliasKeyspaceTemplate, tenantId)
}

// printKeyspaceRoutingRules is used for debugging purposes.
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
	err = vc.VtctldClient.ExecuteCommand("ApplyKeyspaceRoutingRules", "--rules", string(newRulesJSON))
	require.NoError(t, err)
}

func newMultiTenantMigration(t *testing.T) *multiTenantMigration {
	_, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, targetKeyspaceName, "0", mtVSchema, mtSchema, 1, 0, 200, nil)
	require.NoError(t, err)
	mtm := &multiTenantMigration{
		t:                     t,
		tenantMigrationStatus: make(map[int64]tenantMigrationStatus),
		activeMoveTables:      make(map[int64]*VtctldMoveTables),
		targetKeyspace:        targetKeyspaceName,
		tables:                "t1",
		tenantIdColumnName:    "tenant_id",
		lastIDs:               make(map[int64]int64),
	}
	for i := 1; i <= numTenants; i++ {
		mtm.setTenantMigrationStatus(int64(i), tenantMigrationStatusNotMigrated)
	}
	channelSize := numTenants + 1 // +1 to make sure the channels never block
	for _, ch := range []*chan int64{&chNotSetup, &chNotCreated, &chInProgress, &chSwitched, &chCompleted} {
		*ch = make(chan int64, channelSize)
	}
	return mtm
}

func (mtm *multiTenantMigration) getTenantMigrationStatus(tenantId int64) tenantMigrationStatus {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	return mtm.tenantMigrationStatus[tenantId]
}

func (mtm *multiTenantMigration) setTenantMigrationStatus(tenantId int64, status tenantMigrationStatus) {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	mtm.tenantMigrationStatus[tenantId] = status
}

func (mtm *multiTenantMigration) getActiveMoveTables(tenantId int64) *VtctldMoveTables {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	return mtm.activeMoveTables[tenantId]
}

func (mtm *multiTenantMigration) setActiveMoveTables(tenantId int64, mt *VtctldMoveTables) {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	mtm.activeMoveTables[tenantId] = mt
}

func (mtm *multiTenantMigration) setLastID(tenantId, lastID int64) {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	mtm.lastIDs[tenantId] = lastID
}

func (mtm *multiTenantMigration) getLastID(tenantId int64) int64 {
	mtm.mu.Lock()
	defer mtm.mu.Unlock()
	return mtm.lastIDs[tenantId]
}

func (mtm *multiTenantMigration) initTenantData(t *testing.T, tenantId int64, sourceAliasKeyspace string) {
	mtm.insertSomeData(t, tenantId, sourceAliasKeyspace, numInitialRowsPerTenant)
}

func getInitialTabletIdForTenant(tenantId int64) int {
	return int(baseInitialTabletId + tenantId*tabletIdStep)
}

func (mtm *multiTenantMigration) setup(tenantId int64) {
	log.Infof("Creating MoveTables for tenant %d", tenantId)
	mtm.setLastID(tenantId, 0)
	sourceKeyspace := getSourceKeyspace(tenantId)
	sourceAliasKeyspace := getSourceAliasKeyspace(tenantId)
	_, err := vc.AddKeyspace(mtm.t, []*Cell{vc.Cells["zone1"]}, sourceKeyspace, "0", mtVSchema, mtSchema,
		1, 0, getInitialTabletIdForTenant(tenantId), nil)
	require.NoError(mtm.t, err)
	updateKeyspaceRoutingRules(mtm.t, vc, sourceAliasKeyspace, sourceKeyspace)
	mtm.initTenantData(mtm.t, tenantId, sourceAliasKeyspace)
}

func (mtm *multiTenantMigration) start(tenantId int64) {
	sourceKeyspace := getSourceKeyspace(tenantId)
	sourceAliasKeyspace := getSourceAliasKeyspace(tenantId)
	mtm.setTenantMigrationStatus(tenantId, tenantMigrationStatusMigrating)
	additionalFilterClause := fmt.Sprintf("%s=%d", mtm.tenantIdColumnName, tenantId)
	mt := newVtctldMoveTables(&moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   fmt.Sprintf("wf_tenant%d", tenantId),
			targetKeyspace: mtm.targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         mtm.tables,
		createFlags: []string{
			"--additional-filter", additionalFilterClause,
			"--use-keyspace-routing-rules",
			"--source-keyspace-alias", sourceAliasKeyspace,
		},
	})
	mtm.setActiveMoveTables(tenantId, mt)
	mt.Create()
}

func (mtm *multiTenantMigration) insertSomeData(t *testing.T, tenantId int64, sourceAliasKeyspace string, numRows int64) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	idx := mtm.getLastID(tenantId)
	for i := idx + 1; i <= idx+numRows; i++ {
		execVtgateQuery(t, vtgateConn, "",
			fmt.Sprintf("insert into %s.t1(id, tenant_id) values(%d, %d)", sourceAliasKeyspace, i, tenantId))
	}
	mtm.setLastID(tenantId, idx+numRows)
}

func (mtm *multiTenantMigration) switchTraffic(tenantId int64) {
	t := mtm.t
	sourceAliasKeyspace := getSourceAliasKeyspace(tenantId)
	mt := mtm.activeMoveTables[tenantId]
	ksWorkflow := fmt.Sprintf("%s.%s", mtm.targetKeyspace, mt.workflowName)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mtm.insertSomeData(t, tenantId, sourceAliasKeyspace, numAdditionalRowsPerTenant)
	mt.SwitchReadsAndWrites()
	mtm.insertSomeData(t, tenantId, sourceAliasKeyspace, numAdditionalRowsPerTenant)
}

func (mtm *multiTenantMigration) complete(tenantId int64) {
	mt := mtm.activeMoveTables[tenantId]
	mt.Complete()
	vtgateConn := vc.GetVTGateConn(mtm.t)
	defer vtgateConn.Close()
	waitForQueryResult(mtm.t, vtgateConn, "",
		fmt.Sprintf("select count(*) from multi_tenant.t1 where tenant_id=%d", tenantId),
		fmt.Sprintf("[[INT64(%d)]]", mtm.getLastID(tenantId)))
}

// TestMultiTenant tests a multi-tenant migration scenario where each tenant is in a separate database.
// It uses MoveTables to migrate all tenants to the same target keyspace. The test creates a separate source keyspace
// for each tenant. It then steps through the migration process for each tenant, and verifies that the data is migrated
// correctly. The migration steps are done concurrently and randomly to simulate an actual multi-tenant migration.
func TestMultiTenant(t *testing.T) {
	setSidecarDBName("_vt")
	// Don't create RDONLY tablets to reduce number of tablets created to reduce resource requirements for the test.
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultRdonly = origDefaultRdonly
	}()
	defaultRdonly = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	mtm := newMultiTenantMigration(t)
	numTenantsMigrated := 0
	mtm.run() // Start the migration process for all tenants.
	timer := time.NewTimer(waitTimeout)
	for numTenantsMigrated < numTenants {
		select {
		case tenantId := <-chCompleted:
			mtm.setTenantMigrationStatus(tenantId, tenantMigrationStatusMigrated)
			numTenantsMigrated++
			timer.Reset(waitTimeout)
		case <-timer.C:
			require.FailNow(t, "Timed out waiting for all tenants to complete")
		}
	}
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	t.Run("Verify all rows have been migrated", func(t *testing.T) {
		totalRowsInsertedPerTenant := numInitialRowsPerTenant + numAdditionalRowsPerTenant*2
		totalRowsInserted := totalRowsInsertedPerTenant * numTenants
		totalActualRowsInserted := getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", mtm.targetKeyspace, "t1"))
		require.Equal(t, totalRowsInserted, totalActualRowsInserted)
	})
}

func randomWait() {
	time.Sleep(time.Duration(rand.IntN(maxRandomDelaySeconds)) * time.Second)
}

func (mtm *multiTenantMigration) doStuff(name string, chIn, chOut chan int64, counter *atomic.Int64, f func(int64)) {
	timer := time.NewTimer(waitTimeout)
	for counter.Load() < numTenants {
		select {
		case tenantId := <-chIn:
			f(tenantId)
			counter.Add(1)
			chOut <- tenantId
			timer.Reset(waitTimeout)
		case <-timer.C:
			require.FailNowf(mtm.t, "Timed out: %s", name)
		}
		randomWait()
	}
}

// run starts the migration process for all tenants. It starts concurrent

func (mtm *multiTenantMigration) run() {
	go mtm.doStuff("Setup tenant keyspace/schemas", chNotSetup, chNotCreated, &numSetup, mtm.setup)
	for i := int64(1); i <= numTenants; i++ {
		chNotSetup <- i
	}
	// Wait for all tenants to be created before starting the workflows: 10 seconds per tenant to account for CI overhead.
	perTenantLoadTimeout := 10 * time.Second
	require.NoError(mtm.t, waitForCondition("All tenants created",
		func() bool {
			return numSetup.Load() == numTenants
		}, perTenantLoadTimeout*numTenants))

	go mtm.doStuff("Start Migrations", chNotCreated, chInProgress, &numInProgress, mtm.start)
	go mtm.doStuff("Switch Traffic", chInProgress, chSwitched, &numSwitched, mtm.switchTraffic)
	go mtm.doStuff("Mark Migrations Complete", chSwitched, chCompleted, &numCompleted, mtm.complete)
}
