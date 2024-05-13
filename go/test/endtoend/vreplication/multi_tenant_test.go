/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Consists of two tests. Both tests are for multi-tenant migration scenarios.

1. TestMultiTenantSimple: migrates a single tenant to a target keyspace.

2. TestMultiTenantComplex: migrates multiple tenants to a single target keyspace, with concurrent migrations.

The tests use the MoveTables workflow to migrate the tenants. They are designed to simulate a real-world multi-tenant
migration scenario, where each tenant is in a separate database.
*/

package vreplication

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtctldata"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

type tenantMigrationStatus int

const (
	tenantMigrationStatusNotMigrated tenantMigrationStatus = iota
	tenantMigrationStatusMigrating
	tenantMigrationStatusMigrated

	sourceKeyspaceTemplate = "s%d"
	targetKeyspaceName     = "mt"

	numTenants                 = 10
	numInitialRowsPerTenant    = 10
	numAdditionalRowsPerTenant = 10
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

	emptyKeyspaceRoutingRules = &vschemapb.KeyspaceRoutingRules{}
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
  "multi_tenant_spec": {
      "tenant_id_column_name": "tenant_id",
      "tenant_id_column_type": "INT64"
  },
  "tables": {
    "t1": {}
  }
}
`
	mtShardedVSchema = `
{
  "sharded": true,
  "multi_tenant_spec": {
      "tenant_id_column_name": "tenant_id",
      "tenant_id_column_type": "INT64"
  },
 "vindexes": {
    "reverse_bits": {
      "type": "reverse_bits"
    }
  },
  "tables": {
     "t1": { 
       "column_vindexes": [
         {
           "column": "tenant_id",
           "name": "reverse_bits"
         }
     ]
   }
  }
}
`
	stSchema  = mtSchema
	stVSchema = `
{
  "tables": {
    "t1": {}
  }
}
`
)

// TestMultiTenantSimple tests a single tenant migration. The aim here is to test all the steps of the migration process
// including keyspace routing rules, addition of tenant filters to the forward and reverse vreplication streams, and
// verifying that the data is migrated correctly.
func TestMultiTenantSimple(t *testing.T) {
	setSidecarDBName("_vt")
	// Don't create RDONLY tablets to reduce number of tablets created to reduce resource requirements for the test.
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultRdonly = origDefaultRdonly
	}()
	defaultRdonly = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	targetKeyspace := "mt"
	_, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, targetKeyspace, "0", mtVSchema, mtSchema, 1, 0, 200, nil)
	require.NoError(t, err)

	tenantId := int64(1)
	sourceKeyspace := getSourceKeyspace(tenantId)
	_, err = vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, sourceKeyspace, "0", stVSchema, stSchema, 1, 0, getInitialTabletIdForTenant(tenantId), nil)
	require.NoError(t, err)

	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	numRows := 10
	lastIndex := int64(0)
	insertRows := func(lastIndex int64, keyspace string) int64 {
		for i := 1; i <= numRows; i++ {
			execQueryWithRetry(t, vtgateConn,
				fmt.Sprintf("insert into %s.t1(id, tenant_id) values(%d, %d)", keyspace, int64(i)+lastIndex, tenantId), queryTimeout)
		}
		return int64(numRows) + lastIndex
	}
	lastIndex = insertRows(lastIndex, sourceKeyspace)

	mt := newVtctldMoveTables(&moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   fmt.Sprintf("wf%d", tenantId),
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		createFlags: []string{
			"--tenant-id", strconv.FormatInt(tenantId, 10),
		},
	})

	// Expected keyspace routing rules on creation of the workflow.
	initialRules := &vschemapb.KeyspaceRoutingRules{
		Rules: []*vschemapb.KeyspaceRoutingRule{
			{FromKeyspace: "s1", ToKeyspace: "s1"},
			{FromKeyspace: "s1@rdonly", ToKeyspace: "s1"},
			{FromKeyspace: "s1@replica", ToKeyspace: "s1"},
		},
	}

	require.Zero(t, len(getKeyspaceRoutingRules(t, vc).Rules))

	mt.Create()
	confirmKeyspacesRoutedTo(t, sourceKeyspace, "s1", "t1", nil)
	validateKeyspaceRoutingRules(t, vc, initialRules)

	lastIndex = insertRows(lastIndex, sourceKeyspace)
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, mt.workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())

	mt.SwitchReads()
	confirmOnlyReadsSwitched(t)

	mt.SwitchWrites()
	confirmBothReadsAndWritesSwitched(t)

	// Note: here we have already switched, and we can insert into the target keyspace, and it should get reverse
	// replicated to the source keyspace. The source keyspace is routed to the target keyspace at this point.
	lastIndex = insertRows(lastIndex, sourceKeyspace)
	sourceTablet := vc.getPrimaryTablet(t, sourceKeyspace, "0")
	require.NotNil(t, sourceTablet)
	// Wait for the rows to be reverse replicated to the source keyspace.
	waitForRowCountInTablet(t, sourceTablet, sourceKeyspace, "t1", int(lastIndex))

	mt.Complete()
	require.Zero(t, len(getKeyspaceRoutingRules(t, vc).Rules))
	// Targeting to target keyspace should start working now. Upto this point we had to target the source keyspace.
	lastIndex = insertRows(lastIndex, targetKeyspace)

	actualRowsInserted := getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", targetKeyspace, "t1"))
	log.Infof("Migration completed, total rows in target: %d", actualRowsInserted)
	require.Equal(t, lastIndex, int64(actualRowsInserted))

	t.Run("Test ApplyKeyspaceRoutingRules", func(t *testing.T) {
		// First set of rules
		applyKeyspaceRoutingRules(t, initialRules)

		updatedRules := &vschemapb.KeyspaceRoutingRules{
			Rules: []*vschemapb.KeyspaceRoutingRule{
				{FromKeyspace: "s1", ToKeyspace: "mt"},
				{FromKeyspace: "s1@rdonly", ToKeyspace: "mt"},
				{FromKeyspace: "s1@replica", ToKeyspace: "mt"},
			},
		}
		// Update the rules
		applyKeyspaceRoutingRules(t, updatedRules)
		// Update with the same rules
		applyKeyspaceRoutingRules(t, updatedRules)
		// Remove the rules
		applyKeyspaceRoutingRules(t, emptyKeyspaceRoutingRules)
		// Test setting empty rules again
		applyKeyspaceRoutingRules(t, emptyKeyspaceRoutingRules)
	})
}

func applyKeyspaceRoutingRules(t *testing.T, newRules *vschemapb.KeyspaceRoutingRules) {
	var rulesJSON []byte
	var err error
	require.NotNil(t, newRules)
	rulesJSON, err = json.Marshal(newRules)
	require.NoError(t, err)
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("ApplyKeyspaceRoutingRules", "--rules", string(rulesJSON))
	require.NoError(t, err)

	response := &vtctldata.ApplyKeyspaceRoutingRulesResponse{}
	err = json.Unmarshal([]byte(output), response)
	require.NoError(t, err)
	require.ElementsMatch(t, newRules.Rules, response.GetKeyspaceRoutingRules().Rules)
}

func confirmOnlyReadsSwitched(t *testing.T) {
	confirmKeyspacesRoutedTo(t, "s1", "mt", "t1", []string{"rdonly", "replica"})
	confirmKeyspacesRoutedTo(t, "s1", "s1", "t1", []string{"primary"})
	rules := &vschemapb.KeyspaceRoutingRules{
		Rules: []*vschemapb.KeyspaceRoutingRule{
			{FromKeyspace: "s1", ToKeyspace: "s1"},
			{FromKeyspace: "s1@rdonly", ToKeyspace: "mt"},
			{FromKeyspace: "s1@replica", ToKeyspace: "mt"},
		},
	}
	validateKeyspaceRoutingRules(t, vc, rules)
}

func confirmOnlyWritesSwitched(t *testing.T) {
	confirmKeyspacesRoutedTo(t, "s1", "s1", "t1", []string{"rdonly", "replica"})
	confirmKeyspacesRoutedTo(t, "s1", "mt", "t1", []string{"primary"})
	rules := &vschemapb.KeyspaceRoutingRules{
		Rules: []*vschemapb.KeyspaceRoutingRule{
			{FromKeyspace: "s1", ToKeyspace: "mt"},
			{FromKeyspace: "s1@rdonly", ToKeyspace: "s1"},
			{FromKeyspace: "s1@replica", ToKeyspace: "s1"},
		},
	}
	validateKeyspaceRoutingRules(t, vc, rules)
}

// TestMultiTenantSharded tests a single tenant migration to a sharded target. The aim is to test
// the specification of the target shards in all the MoveTables subcommands, including creating only one stream
// for a tenant on the shard to which this tenant id will be routed, using the specified Vindex.
func TestMultiTenantSharded(t *testing.T) {
	setSidecarDBName("_vt")
	// Don't create RDONLY tablets to reduce number of tablets created to reduce resource requirements for the test.
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultRdonly = origDefaultRdonly
	}()
	defaultRdonly = 0
	vc = setupMinimalCluster(t)
	defer vc.TearDown()

	targetKeyspace := "mt"
	_, err := vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, targetKeyspace, "-40,40-80,80-a0,a0-", mtShardedVSchema, mtSchema, 1, 0, 200, nil)
	require.NoError(t, err)

	tenantId := int64(1)
	tenantShard := "80-a0" // matches the vindex
	sourceKeyspace := getSourceKeyspace(tenantId)
	_, err = vc.AddKeyspace(t, []*Cell{vc.Cells["zone1"]}, sourceKeyspace, "0", stVSchema, stSchema, 1, 0, getInitialTabletIdForTenant(tenantId), nil)
	require.NoError(t, err)

	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	numRows := 10
	lastIndex := int64(0)
	insertRows := func(lastIndex int64, keyspace string) int64 {
		for i := 1; i <= numRows; i++ {
			execQueryWithRetry(t, vtgateConn,
				fmt.Sprintf("insert into %s.t1(id, tenant_id) values(%d, %d)", keyspace, int64(i)+lastIndex, tenantId), queryTimeout)
		}
		return int64(numRows) + lastIndex
	}
	lastIndex = insertRows(lastIndex, sourceKeyspace)

	mt := newVtctldMoveTables(&moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   fmt.Sprintf("wf%d", tenantId),
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		createFlags: []string{
			"--tenant-id", strconv.FormatInt(tenantId, 10),
			"--shards", tenantShard, // create the workflow for tenantid 1 in shard 80-a0: matches the vindex
		},
		switchFlags: []string{
			"--shards", tenantShard,
		},
		completeFlags: []string{
			"--shards", tenantShard,
		},
		showFlags: []string{
			"--shards", tenantShard,
		},
	})

	mt.Create()
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, mt.workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.Show()
	var workflowState vtctldata.GetWorkflowsResponse
	err = protojson.Unmarshal([]byte(mt.lastOutput), &workflowState)
	require.NoError(t, err)
	require.Equal(t, 1, len(workflowState.Workflows))
	wf := workflowState.Workflows[0]
	// Verifies that only one stream is created for the tenant on the shard to which this tenant id will be routed.
	require.Equal(t, 1, len(wf.ShardStreams))

	// Note: we cannot insert into the target keyspace since that is never routed to the source keyspace.
	lastIndex = insertRows(lastIndex, sourceKeyspace)
	waitForWorkflowState(t, vc, fmt.Sprintf("%s.%s", targetKeyspace, mt.workflowName), binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.SwitchReadsAndWrites()
	// Note: here we have already switched, and we can insert into the target keyspace, and it should get reverse
	// replicated to the source keyspace. The source keyspace is routed to the target keyspace at this point.
	lastIndex = insertRows(lastIndex, sourceKeyspace)
	mt.Complete()
	require.Zero(t, len(getKeyspaceRoutingRules(t, vc).Rules))
	actualRowsInserted := getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", targetKeyspace, "t1"))
	require.Equal(t, lastIndex, int64(actualRowsInserted))
	require.Equal(t, lastIndex, int64(getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", targetKeyspace, "t1"))))
	log.Infof("Migration completed, total rows in target: %d", actualRowsInserted)
}

func confirmBothReadsAndWritesSwitched(t *testing.T) {
	confirmKeyspacesRoutedTo(t, "s1", "mt", "t1", []string{"rdonly", "replica"})
	confirmKeyspacesRoutedTo(t, "s1", "mt", "t1", []string{"primary"})
	rules := &vschemapb.KeyspaceRoutingRules{
		Rules: []*vschemapb.KeyspaceRoutingRule{
			{FromKeyspace: "s1", ToKeyspace: "mt"},
			{FromKeyspace: "s1@rdonly", ToKeyspace: "mt"},
			{FromKeyspace: "s1@replica", ToKeyspace: "mt"},
		},
	}
	validateKeyspaceRoutingRules(t, vc, rules)
}

func validateKeyspaceRoutingRules(t *testing.T, vc *VitessCluster, expectedRules *vschemapb.KeyspaceRoutingRules) {
	currentRules := getKeyspaceRoutingRules(t, vc)
	require.ElementsMatch(t, expectedRules.Rules, currentRules.Rules)
}

func getSourceKeyspace(tenantId int64) string {
	return fmt.Sprintf(sourceKeyspaceTemplate, tenantId)
}

func (mtm *multiTenantMigration) insertSomeData(t *testing.T, tenantId int64, keyspace string, numRows int64) {
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	idx := mtm.getLastID(tenantId)
	for i := idx + 1; i <= idx+numRows; i++ {
		execQueryWithRetry(t, vtgateConn,
			fmt.Sprintf("insert into %s.t1(id, tenant_id) values(%d, %d)", keyspace, i, tenantId), queryTimeout)
	}
	mtm.setLastID(tenantId, idx+numRows)
}

func getKeyspaceRoutingRules(t *testing.T, vc *VitessCluster) *vschemapb.KeyspaceRoutingRules {
	output, err := vc.VtctldClient.ExecuteCommandWithOutput("GetKeyspaceRoutingRules")
	require.NoError(t, err)
	rules := &vschemapb.KeyspaceRoutingRules{}
	err = json.Unmarshal([]byte(output), rules)
	require.NoError(t, err)
	return rules
}

// TestMultiTenant tests a multi-tenant migration scenario where each tenant is in a separate database.
// It uses MoveTables to migrate all tenants to the same target keyspace. The test creates a separate source keyspace
// for each tenant. It then steps through the migration process for each tenant, and verifies that the data is migrated
// correctly. The migration steps are done concurrently and randomly to simulate an actual multi-tenant migration.
func TestMultiTenantComplex(t *testing.T) {
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
		numAdditionalInsertSets := 2 /* during the SwitchTraffic stop */ + 1 /* after Complete */
		totalRowsInsertedPerTenant := numInitialRowsPerTenant + numAdditionalRowsPerTenant*numAdditionalInsertSets
		totalRowsInserted := totalRowsInsertedPerTenant * numTenants
		totalActualRowsInserted := getRowCount(t, vtgateConn, fmt.Sprintf("%s.%s", mtm.targetKeyspace, "t1"))
		require.Equal(t, totalRowsInserted, totalActualRowsInserted)
		log.Infof("Migration completed, total rows inserted in target: %d", totalActualRowsInserted)
	})
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

func (mtm *multiTenantMigration) initTenantData(t *testing.T, tenantId int64) {
	mtm.insertSomeData(t, tenantId, getSourceKeyspace(tenantId), numInitialRowsPerTenant)
}

func getInitialTabletIdForTenant(tenantId int64) int {
	return int(baseInitialTabletId + tenantId*tabletIdStep)
}

func (mtm *multiTenantMigration) setup(tenantId int64) {
	log.Infof("Creating MoveTables for tenant %d", tenantId)
	mtm.setLastID(tenantId, 0)
	sourceKeyspace := getSourceKeyspace(tenantId)
	_, err := vc.AddKeyspace(mtm.t, []*Cell{vc.Cells["zone1"]}, sourceKeyspace, "0", stVSchema, stSchema,
		1, 0, getInitialTabletIdForTenant(tenantId), nil)
	require.NoError(mtm.t, err)
	mtm.initTenantData(mtm.t, tenantId)
}

func (mtm *multiTenantMigration) start(tenantId int64) {
	sourceKeyspace := getSourceKeyspace(tenantId)
	mtm.setTenantMigrationStatus(tenantId, tenantMigrationStatusMigrating)
	mt := newVtctldMoveTables(&moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   fmt.Sprintf("wf%d", tenantId),
			targetKeyspace: mtm.targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		tables:         mtm.tables,
		createFlags: []string{
			"--tenant-id", strconv.FormatInt(tenantId, 10),
		},
	})
	mtm.setActiveMoveTables(tenantId, mt)
	mt.Create()
}

func (mtm *multiTenantMigration) switchTraffic(tenantId int64) {
	t := mtm.t
	sourceKeyspaceName := getSourceKeyspace(tenantId)
	mt := mtm.getActiveMoveTables(tenantId)
	ksWorkflow := fmt.Sprintf("%s.%s", mtm.targetKeyspace, mt.workflowName)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mtm.insertSomeData(t, tenantId, sourceKeyspaceName, numAdditionalRowsPerTenant)
	mt.SwitchReadsAndWrites()
	mtm.insertSomeData(t, tenantId, sourceKeyspaceName, numAdditionalRowsPerTenant)
}

func (mtm *multiTenantMigration) complete(tenantId int64) {
	mt := mtm.getActiveMoveTables(tenantId)
	mt.Complete()
	mtm.insertSomeData(mtm.t, tenantId, mtm.targetKeyspace, numAdditionalRowsPerTenant)
	vtgateConn := vc.GetVTGateConn(mtm.t)
	defer vtgateConn.Close()
	waitForQueryResult(mtm.t, vtgateConn, "",
		fmt.Sprintf("select count(*) from %s.t1 where tenant_id=%d", mt.targetKeyspace, tenantId),
		fmt.Sprintf("[[INT64(%d)]]", mtm.getLastID(tenantId)))
}

func randomWait() {
	time.Sleep(time.Duration(rand.IntN(maxRandomDelaySeconds)) * time.Second)
}

func (mtm *multiTenantMigration) doThis(name string, chIn, chOut chan int64, counter *atomic.Int64, f func(int64)) {
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
	go mtm.doThis("Setup tenant keyspace/schemas", chNotSetup, chNotCreated, &numSetup, mtm.setup)
	for i := int64(1); i <= numTenants; i++ {
		chNotSetup <- i
	}
	// Wait for all tenants to be created before starting the workflows: 10 seconds per tenant to account for CI overhead.
	perTenantLoadTimeout := 1 * time.Minute
	require.NoError(mtm.t, waitForCondition("All tenants created",
		func() bool {
			return numSetup.Load() == numTenants
		}, perTenantLoadTimeout*numTenants))

	go mtm.doThis("Start Migrations", chNotCreated, chInProgress, &numInProgress, mtm.start)
	go mtm.doThis("Switch Traffic", chInProgress, chSwitched, &numSwitched, mtm.switchTraffic)
	go mtm.doThis("Mark Migrations Complete", chSwitched, chCompleted, &numCompleted, mtm.complete)
}
