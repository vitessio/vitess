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

	numTenants             = int64(30)
	sourceKeyspaceTemplate = "unmanaged_tenant%d"
	userKeyspaceTemplate   = "userks_tenant%d"
	targetKeyspaceName     = "multi_tenant"
)

var (
	chNotSetup, chNotCreated, chInProgress, chSwitched, chCompleted chan int64
	numSetup, numInProgress, numSwitched, numCompleted              atomic.Int64

	maxDelay = 5
)

type multiTenantMigration struct {
	t                     *testing.T
	mu                    sync.Mutex
	tenantMigrationStatus map[int64]tenantMigrationStatus
	activeMoveTables      map[int64]*VtctldMoveTables

	targetKeyspace     string
	tables             string
	tenantIdColumnName string

	lastIDs map[int64]int64
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

func getUserKeyspace(tenantId int64) string {
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
	//log.Infof("Applying new keyspace routing rules: %s", newRulesJSON)
	err = vc.VtctldClient.ExecuteCommand("ApplyKeyspaceRoutingRules", "--rules", string(newRulesJSON))
	require.NoError(t, err)
	//printKeyspaceRoutingRules(t, vc, "After creating unmatched keyspace")
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
	for i := int64(1); i <= numTenants; i++ {
		mtm.setTenantMigrationStatus(i, tenantMigrationStatusNotMigrated)
	}
	channelSize := numTenants + 1
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

func (mtm *multiTenantMigration) initTenantData(t *testing.T, tenantId int64, userKeyspace string) {
	mtm.insertSomeData(t, tenantId, userKeyspace, 10)
}

func (mtm *multiTenantMigration) setup(tenantId int64) {
	log.Infof("Creating MoveTables for tenant %d", tenantId)
	mtm.setLastID(tenantId, 0)
	sourceKeyspace := getSourceKeyspace(tenantId)
	userKeyspace := getUserKeyspace(tenantId)
	_, err := vc.AddKeyspace(mtm.t, []*Cell{vc.Cells["zone1"]}, sourceKeyspace, "0", mtVSchema, mtSchema, 1, 0, int(1000+tenantId*100), nil)
	require.NoError(mtm.t, err)
	updateKeyspaceRoutingRules(mtm.t, vc, userKeyspace, sourceKeyspace)
	mtm.initTenantData(mtm.t, tenantId, userKeyspace)
}

func (mtm *multiTenantMigration) start(tenantId int64) {
	sourceKeyspace := getSourceKeyspace(tenantId)
	userKeyspace := getUserKeyspace(tenantId)
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

func (mtm *multiTenantMigration) insertSomeData(t *testing.T, tenantId int64, userKeyspace string, numRows int64) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	idx := mtm.getLastID(tenantId)
	for i := idx + 1; i <= idx+numRows; i++ {
		execVtgateQuery(t, vtgateConn, "",
			fmt.Sprintf("insert into %s.t1(id, tenant_id) values(%d, %d)", userKeyspace, i, tenantId))
	}
	mtm.setLastID(tenantId, idx+numRows)
}

func (mtm *multiTenantMigration) switchTraffic(tenantId int64) {
	t := mtm.t
	userKeyspace := getUserKeyspace(tenantId)
	mt := mtm.activeMoveTables[tenantId]
	ksWorkflow := fmt.Sprintf("%s.%s", mtm.targetKeyspace, mt.workflowName)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mtm.insertSomeData(t, tenantId, userKeyspace, 10)
	mt.SwitchReadsAndWrites()
	//printKeyspaceRoutingRules(t, vc, "After MoveTables SwitchTraffic")
	mtm.insertSomeData(t, tenantId, userKeyspace, 10)
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

const waitTimeout = 10 * time.Minute

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
	numMigrated := int64(0)
	mtm.run()
	timer := time.NewTimer(waitTimeout)
	for numMigrated < numTenants {
		select {
		case tenantId := <-chCompleted:
			mtm.setTenantMigrationStatus(tenantId, tenantMigrationStatusMigrated)
			numMigrated++
			timer.Reset(waitTimeout)
		case <-timer.C:
			require.FailNow(t, "Timed out waiting for all tenants to complete")
		}
	}
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	require.Equal(t, numMigrated*10*3, int64(getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", mtm.targetKeyspace, "t1"))))
}

func randomWait() {
	time.Sleep(time.Duration(rand.IntN(maxDelay)) * time.Second)
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

func (mtm *multiTenantMigration) run() {
	go mtm.doStuff("Setup tenant keyspace/schemas", chNotSetup, chNotCreated, &numSetup, mtm.setup)
	for i := int64(1); i <= numTenants; i++ {
		chNotSetup <- i
	}
	require.NoError(mtm.t, waitForCondition("All tenants created", func() bool {
		return numSetup.Load() == numTenants
	}, time.Duration(numTenants*1)*time.Minute)) // give it 3 seconds per tenant

	go mtm.doStuff("Start Migrations", chNotCreated, chInProgress, &numInProgress, mtm.start)
	go mtm.doStuff("Switch Traffic", chInProgress, chSwitched, &numSwitched, mtm.switchTraffic)
	go mtm.doStuff("Mark Migrations Complete", chSwitched, chCompleted, &numCompleted, mtm.complete)
}
