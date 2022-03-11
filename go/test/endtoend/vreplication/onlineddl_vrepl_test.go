/*
Copyright 2022 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/wrangler"
)

// starts an Online DDL migration with postponed completion and waits for migration to start.
func createOnlineDDL(t *testing.T, keyspace, ddl string) string {
	execVtgateQuery(t, vtgateConn, keyspace, "SET @@ddl_strategy='online -allow-zero-in-date -postpone-completion'")
	qr := execVtgateQuery(t, vtgateConn, keyspace, ddl)
	require.NotNil(t, qr)
	require.Equal(t, len(qr.Rows), 1)
	uuid, _ := qr.Named().Row().ToString("uuid")
	require.NotEmpty(t, uuid)
	// wait for process to start, otherwise "alter vitess_migration" fails
	status := waitForMigrationStatus(t, vtgateConn, uuid, keyspace, 2, 20*time.Second, schema.OnlineDDLStatusReady, schema.OnlineDDLStatusRunning, schema.OnlineDDLStatusFailed)
	require.NotEqual(t, schema.OnlineDDLStatusFailed, status)
	return uuid
}

// "complete"s a migration started with createOnlineDDL(). We need to wait for the migration to catchup and complete.
func completeOnlineDDL(t *testing.T, keyspace, uuid string) {
	// need to osExec() here because execVtgateQuery() takes a different query serving route. Latter calls txConnExec() instead of Execute() in query_executor.go
	log.Infof("Completing Online DDL %s", uuid)
	query := fmt.Sprintf("use %s; alter vitess_migration '%s' complete;", keyspace, uuid)
	output, err := osExec(t, "mysql", []string{"-u", "vtdba", "-P", fmt.Sprintf("%d", vc.ClusterConfig.vtgateMySQLPort),
		"--host=127.0.0.1", "-e", query})
	if err != nil {
		require.FailNow(t, output)
	}

	status := waitForMigrationStatus(t, vtgateConn, uuid, keyspace, 2, 20*time.Second, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
	require.NotEqual(t, schema.OnlineDDLStatusFailed, status)
	log.Infof("Completed Online DDL %s", uuid)
}

const (
	// 	Case 1. Online DDL ALTER is started after Reshard and completed before cutover: table should have the new schema after cutover
	case1Ddl    = "ALTER TABLE customer ADD COLUMN description varchar(20) NOT NULL;"
	case1Update = "update customer set description = concat('cust-', cid)"
	enableCase1 = true

	//  Case 2: Online DDL ALTER is started before Reshard starts and completed after it starts, but before cutover:
	case2Ddl    = "ALTER TABLE `Lead` ADD COLUMN description varchar(64) NOT NULL;"
	case2Update = "update `Lead` set description = concat('Lead-id-', md5(`Lead-id`))"
	enableCase2 = true

	//  Case 3: Online DDL ALTER is started before Reshard starts and completed after it ends:
	// not supported, this should error out
	case3Ddl    = "ALTER TABLE `Lead-1` ADD COLUMN description varchar(64) NOT NULL;"
	case3Update = "update `Lead-1` set description = concat('Lead1-', md5(`Lead`))"
	enableCase3 = false
)

// TestOnlineDDLsDuringReshard validates that tables participating in online ddls do the right thing during reshard
// for the use cases defined above
func TestOnlineDDLsDuringReshard(t *testing.T) {
	cellName := "zone1"
	unshardedKeyspaceName := "product"
	shardedKeyspaceName := "customer"

	cells := []string{cellName}
	vc = NewVitessCluster(t, "TestOnlineDDLsDuringReshard", cells, mainClusterConfig)
	require.NotNil(t, vc)
	allCellNames = cellName
	cell1 := vc.Cells[cellName]
	defaultCell = cell1
	defaultReplicas = 0 // because of CI resource constraints we can only run this test with primary tablets
	defer func() { defaultReplicas = 1 }()

	defer vc.TearDown(t)

	vc.AddKeyspace(t, []*Cell{cell1}, unshardedKeyspaceName, "0", initialProductVSchema, initialProductSchema, 0, 0, 100)

	vtgate = cell1.Vtgates[0]
	require.NotNil(t, vtgate)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", unshardedKeyspaceName, "0"), 1)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)
	insertInitialData(t)
	shardCustomer(t, true, []*Cell{cell1}, cellName, false)

	var err error
	var ksWorkflow string
	workflowName := "rs1"

	currentWorkflowType = wrangler.ReshardWorkflow
	ks := vc.Cells[defaultCell.Name].Keyspaces[shardedKeyspaceName]
	require.NoError(t, vc.AddShards(t, []*Cell{cell1}, ks, "80-c0,c0-", 0, 0, 400))

	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", shardedKeyspaceName, "80-c0"), 1); err != nil {
		t.Fatal(err)
	}
	if err := vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", shardedKeyspaceName, "c0-"), 1); err != nil {
		t.Fatal(err)
	}
	ksWorkflow = fmt.Sprintf("%s.%s", shardedKeyspaceName, workflowName)

	var case1Uuid, case2Uuid, case3Uuid string
	if enableCase2 {
		case2Uuid = createOnlineDDL(t, shardedKeyspaceName, case2Ddl)
	}
	err = tstWorkflowExec(t, defaultCellName, workflowName, shardedKeyspaceName, shardedKeyspaceName, "", workflowActionCreate, "", "80-", "80-c0,c0-")
	require.NoError(t, err)

	targetTab1 = vc.getPrimaryTablet(t, shardedKeyspaceName, "80-c0")
	targetTab2 = vc.getPrimaryTablet(t, shardedKeyspaceName, "c0-")
	catchup(t, targetTab1, workflowName, "Reshard")
	catchup(t, targetTab2, workflowName, "Reshard")

	if enableCase2 {
		completeOnlineDDL(t, shardedKeyspaceName, case2Uuid)
		execVtgateQuery(t, vtgateConn, shardedKeyspaceName, case2Update)
	}
	if enableCase1 {
		case1Uuid = createOnlineDDL(t, shardedKeyspaceName, case1Ddl)
		completeOnlineDDL(t, shardedKeyspaceName, case1Uuid)
		execVtgateQuery(t, vtgateConn, shardedKeyspaceName, case1Update)
	}

	if enableCase3 {
		case3Uuid = createOnlineDDL(t, shardedKeyspaceName, case3Ddl)
	}

	waitForLowLag(t, shardedKeyspaceName, workflowName)
	vdiff(t, ksWorkflow, "")
	if vdiffError != nil {
		require.FailNowf(t, "vdiff failed", "", vdiffError)
	}
	require.Nil(t, vdiffError)
	// complete Reshard
	log.Infof("Switching traffic")
	err = tstWorkflowExec(t, defaultCellName, workflowName, shardedKeyspaceName, shardedKeyspaceName, "", workflowActionSwitchTraffic, "", "", "")
	require.NoError(t, err)
	log.Infof("Reshard completed")
	if enableCase3 {
		completeOnlineDDL(t, shardedKeyspaceName, case3Uuid)
		execVtgateQuery(t, vtgateConn, shardedKeyspaceName, case3Update)
	}
	if enableCase1 {
		//customerRS := execVtgateQuery(t, vtgateConn, shardedKeyspaceName, "select * from customer")
		//require.NotNil(t, customerRS)
		//log.Infof("customerRS: %+v", customerRS.Rows)
		customerRS := execVtgateQuery(t, vtgateConn, shardedKeyspaceName, "select count(*) cnt from customer")
		require.NotNil(t, customerRS)
		require.Equal(t, int64(4), customerRS.Named().Row().AsInt64("cnt", -1))

		customerRS = execVtgateQuery(t, vtgateConn, shardedKeyspaceName, "select count(*) cnt from customer where description = ''")
		if customerRS == nil {
			log.Infof("description not yet available")
			time.Sleep(8 * time.Minute)
			customerRS = execVtgateQuery(t, vtgateConn, shardedKeyspaceName, "select count(*) cnt from customer where description = ''")
		}
		require.NotNil(t, customerRS)
		require.Equal(t, int64(0), customerRS.Named().Row().AsInt64("cnt", -1))
	}
	if enableCase2 {
		customerLead := execVtgateQuery(t, vtgateConn, shardedKeyspaceName, "select count(*) cnt from `Lead`")
		require.NotNil(t, customerLead)
		require.Equal(t, int64(6), customerLead.Named().Row().AsInt64("cnt", -1))
		customerLead = execVtgateQuery(t, vtgateConn, shardedKeyspaceName, "select count(*) cnt from `Lead` where description = ''")
		require.NotNil(t, customerLead)
		require.Equal(t, int64(0), customerLead.Named().Row().AsInt64("cnt", -1))
	}

	if enableCase3 {
		customerLead1 := execVtgateQuery(t, vtgateConn, shardedKeyspaceName, "select count(*) cnt from `Lead-1`")
		require.NotNil(t, customerLead1)
		require.Equal(t, int64(6), customerLead1.Named().Row().AsInt64("cnt", -1))
		customerLead1 = execVtgateQuery(t, vtgateConn, shardedKeyspaceName, "select count(*) cnt from `Lead-1` where description = ''")
		require.NotNil(t, customerLead1)
		require.Equal(t, int64(0), customerLead1.Named().Row().AsInt64("cnt", -1))
	}
}

// waitForMigrationStatus waits for a migration to reach one of the provided statuses
func waitForMigrationStatus(t *testing.T, vtgateConn *mysql.Conn, uuid, keyspace string, numShards int,
	timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {

	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a", sqltypes.StringBindVariable(uuid))
	require.NoError(t, err)

	statusMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusMap[string(status)] = true
	}
	startTime := time.Now()
	lastKnownStatus := ""
	for time.Since(startTime) < timeout {
		countMatchedShards := 0
		qr, err := vtgateConn.ExecuteFetch(query, 1000, true)
		require.NoError(t, err)
		for _, row := range qr.Named().Rows {
			lastKnownStatus = row["migration_status"].ToString()
			if statusMap[lastKnownStatus] {
				countMatchedShards++
			}
			log.Infof("Uuid %s, Status %s, Shard %s", row["migration_context"], lastKnownStatus, row["shard"])
		}
		if countMatchedShards == numShards {
			return schema.OnlineDDLStatus(lastKnownStatus)
		}
		time.Sleep(3 * time.Second)
	}
	require.FailNowf(t, "WaitForMigrationStatus timed out", "status is %s", lastKnownStatus)
	return schema.OnlineDDLStatus(lastKnownStatus)
}
