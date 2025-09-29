package vreplication

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/utils"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
)

func TestVStreamOnlineDDL(t *testing.T) {
	origDefaultReplicas := defaultReplicas
	origDefaultRdonly := defaultRdonly
	defer func() {
		defaultReplicas = origDefaultReplicas
		defaultRdonly = origDefaultRdonly
	}()
	defaultReplicas = 0
	defaultRdonly = 0
	vc = setupMinimalCluster(t)
	vttablet.InitVReplicationConfigDefaults()
	defer vc.TearDown()

	sourceKeyspaceName := "product"
	targetKeyspaceName := "customer"
	var mt iMoveTables
	workflowName := "wf1"

	sourceTab = vc.Cells["zone1"].Keyspaces[sourceKeyspaceName].Shards["0"].Tablets["zone1-100"].Vttablet
	require.NotNil(t, sourceTab)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	populateCustomer2Init(t, 10000, sourceTab)

	targetTabs := setupMinimalCustomerKeyspace(t)
	targetTab1 = targetTabs["-80"]
	require.NotNil(t, targetTab1)
	targetTab2 = targetTabs["80-"]
	require.NotNil(t, targetTab2)
	tables := "customer2"

	mt = createMoveTables(t, sourceKeyspaceName, targetKeyspaceName, workflowName, tables, nil, nil, nil)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.SwitchReadsAndWrites()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Stopped.String())
	numOperations := 1000
	populateCustomer2Load(t, ctx)
	go runOnlineDDLOperations(t, ctx, targetKeyspaceName, numOperations)
	time.Sleep(600 * time.Second)
}

func populateCustomer2Init(t *testing.T, numRows int, sourceTab *cluster.VttabletProcess) {
	insertDirect := func(i int) {
		query := "insert into customer2(cid, name) values(" + fmt.Sprintf("%d", i) + ", 'name')"
		_, err := sourceTab.QueryTablet(query, "product", true)
		require.NoError(t, err)
	}
	for i := 1000; i < numRows; i++ {
		insertDirect(i)
	}
}

func populateCustomer2Load(t *testing.T, ctx context.Context) {
	insertVtgate := func(i int, conn *mysql.Conn) {
		query := "insert into customer2(cid, name) values(" + fmt.Sprintf("%d", i) + ", 'name')"
		execQuery(t, conn, query)
	}
	go func(ctx context.Context) {
		conn, cancel := getVTGateConn()
		defer cancel()
		require.NotNil(t, conn)

		startIdx := 10000
		for {
			select {
			case <-ctx.Done():
				log.Infof("Context canceled in load generator")
				return
			default:
				insertVtgate(startIdx, conn)
				startIdx++
				// time.Sleep(1 * time.Millisecond)
			}
		}
	}(ctx)
}

func runOnlineDDLOperations(t *testing.T, ctx context.Context, keyspace string, numOperations int) {
	newType := "bigint"
	for i := 0; i < numOperations; i++ {
		if ctx.Err() != nil {
			log.Infof("Context canceled in OnlineDDL Ops")
			return
		}
		// Generate random varchar length between 256 and 512
		// newLength := rand.IntN(257) + 256 // 256-512
		query := fmt.Sprintf("alter table customer2 modify cid %s", newType)
		switch newType {
		case "int":
			newType = "bigint"
		case "bigint":
			newType = "int"
		default:
			panic("unknown type: " + newType)
		}

		log.Infof("Starting OnlineDDL operation %d/%d: %s", i+1, numOperations, query)

		// Execute OnlineDDL
		output, err := vc.VtctldClient.ExecuteCommandWithOutput("ApplySchema", utils.GetFlagVariantForTests("--ddl-strategy"), "vitess", "--sql", query, keyspace)
		require.NoError(t, err, output)

		re := regexp.MustCompile(`([0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12})`)
		var uuid string
		match := re.FindStringSubmatch(output)
		if len(match) > 1 {
			uuid = match[1]
		}
		require.NotEmpty(t, uuid, "Failed to extract UUID from ApplySchema output: %s", output)

		// Wait for completion on all shards
		waitForOnlineDDLComplete(t, keyspace, uuid)

		log.Infof("OnlineDDL operation %d/%d completed successfully", i+1, numOperations)

		if i < numOperations-1 {
			// Sleep before next DDL
			time.Sleep(1 * time.Second)
		}
	}
}

func waitForOnlineDDLComplete(t *testing.T, keyspace, uuid string) {
	// For customer keyspace, check both -80 and 80- shards
	shards := []string{"-80", "80-"}

	// Check each shard individually
	for _, shard := range shards {
		// log.Infof("Waiting for OnlineDDL %s to complete on shard %s", uuid, shard)

		err := waitForCondition(fmt.Sprintf("OnlineDDL %s to complete on shard %s", uuid, shard), func() bool {
			response := onlineDDLShow(t, keyspace, uuid)
			if len(response.Migrations) == 0 {
				return false
			}

			// Find migration for this shard
			var migration *vtctldata.SchemaMigration
			for _, m := range response.Migrations {
				if m.Shard == shard {
					migration = m
					break
				}
			}

			if migration == nil {
				log.Infof("No migration found for shard %s yet", shard)
				return false
			}

			// log.Infof("OnlineDDL %s on shard %s status: %s", uuid, shard, migration.Status.String())

			if migration.Status == vtctldata.SchemaMigration_FAILED {
				t.Errorf("OnlineDDL %s failed on shard %s: %s", uuid, shard, migration.Message)
				return false
			}
			rowsCopied := migration.RowsCopied
			log.Infof("OnlineDDL %s on shard %s, rows_copied: %d", uuid, shard, rowsCopied)

			return migration.Status == vtctldata.SchemaMigration_COMPLETE
		}, defaultTimeout)

		require.NoError(t, err, "OnlineDDL %s did not complete on shard %s within timeout", uuid, shard)
	}

	log.Infof("OnlineDDL %s completed successfully on all shards", uuid)
}
