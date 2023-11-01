package vreplication

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestMultipleConcurrentVDiffs(t *testing.T) {
	cellName := "zone"
	cells := []string{cellName}
	vc = NewVitessCluster(t, t.Name(), cells, mainClusterConfig)

	require.NotNil(t, vc)
	allCellNames = cellName
	defaultCellName := cellName
	defaultCell = vc.Cells[defaultCellName]
	sourceKeyspace := "product"
	shardName := "0"

	defer vc.TearDown(t)

	cell := vc.Cells[cellName]
	vc.AddKeyspace(t, []*Cell{cell}, sourceKeyspace, shardName, initialProductVSchema, initialProductSchema, 0, 0, 100, sourceKsOpts)

	vtgate = cell.Vtgates[0]
	require.NotNil(t, vtgate)
	err := cluster.WaitForHealthyShard(vc.VtctldClient, sourceKeyspace, shardName)
	require.NoError(t, err)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", sourceKeyspace, shardName), 1, 30*time.Second)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	insertInitialData(t)
	targetTabletId := 200
	targetKeyspace := "customer"
	vc.AddKeyspace(t, []*Cell{cell}, targetKeyspace, shardName, initialProductVSchema, initialProductSchema, 0, 0, targetTabletId, sourceKsOpts)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKeyspace, shardName), 1, 30*time.Second)

	index := 1000
	var loadCtx context.Context
	var loadCancel context.CancelFunc
	loadCtx, loadCancel = context.WithCancel(context.Background())
	load := func(tableName string) {
		query := "insert into %s(cid, name) values(%d, 'customer-%d')"
		for {
			select {
			case <-loadCtx.Done():
				log.Infof("load cancelled")
				return
			default:
				index += 1
				vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
				q := fmt.Sprintf(query, tableName, index, index)
				_, err := vtgateConn.ExecuteFetch(q, 1000, false)
				if err != nil {
					t.Fatalf("failed to insert data: %s: %v", q, err)
				}
				//log.Infof("inserted data: %s", q)
				vtgateConn.Close()
			}
		}
	}
	time.Sleep(5 * time.Minute)
	workflowName1 := "wf1"
	ksWorkflow1 := fmt.Sprintf("%s.%s", targetKeyspace, workflowName1)
	mt1 := newMoveTables(vc, &moveTables{
		workflowName:   workflowName1,
		targetKeyspace: targetKeyspace,
		sourceKeyspace: sourceKeyspace,
		tables:         "customer",
	}, moveTablesFlavorVtctld)
	mt1.Create()
	waitForWorkflowState(t, vc, ksWorkflow1, binlogdatapb.VReplicationWorkflowState_Running.String())
	targetKs := vc.Cells[cellName].Keyspaces[targetKeyspace]
	targetTab := targetKs.Shards["0"].Tablets[fmt.Sprintf("%s-%d", cellName, targetTabletId)].Vttablet
	require.NotNil(t, targetTab)
	catchup(t, targetTab, workflowName1, "MoveTables")

	workflowName2 := "wf2"
	ksWorkflow2 := fmt.Sprintf("%s.%s", targetKeyspace, workflowName2)
	mt2 := newMoveTables(vc, &moveTables{
		workflowName:   workflowName2,
		targetKeyspace: targetKeyspace,
		sourceKeyspace: sourceKeyspace,
		tables:         "customer2",
	}, moveTablesFlavorVtctld)
	mt2.Create()
	waitForWorkflowState(t, vc, ksWorkflow2, binlogdatapb.VReplicationWorkflowState_Running.String())
	catchup(t, targetTab, workflowName2, "MoveTables")

	go load("customer")
	go load("customer2")

	var wg sync.WaitGroup
	wg.Add(2)

	doVdiff := func(workflowName, table string) {
		defer wg.Done()
		vdiff(t, targetKeyspace, workflowName, cellName, true, false, nil)
		vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
		defer vtgateConn.Close()
		qr, err := vtgateConn.ExecuteFetch(fmt.Sprintf("select count(*) from %s", table), 1000, false)
		if err != nil {
			t.Fatalf("failed to get count of table %s: %v", table, err)
		}
		log.Infof("count of table %s: %v", table, qr.Rows[0][0])
	}
	go doVdiff(workflowName1, "customer")
	go doVdiff(workflowName2, "customer2")
	wg.Wait()
	loadCancel()
}
