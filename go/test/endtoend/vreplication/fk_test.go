/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestFKWorkflow runs a MoveTables workflow with atomic copy for a db with foreign key constraints.
// It inserts initial data, then simulates load. We insert both child rows with foreign keys and those without,
// i.e. with foreign_key_checks=0.
func TestFKWorkflow(t *testing.T) {
	// ensure that there are multiple copy phase cycles per table
	extraVTTabletArgs = []string{"--vstream_packet_size=256"}
	defer func() { extraVTTabletArgs = nil }()

	cellName := "zone"
	cells := []string{cellName}
	vc = NewVitessCluster(t, "TestFKWorkflow", cells, mainClusterConfig)

	require.NotNil(t, vc)
	allCellNames = cellName
	defaultCellName := cellName
	defaultCell = vc.Cells[defaultCellName]
	sourceKeyspace := "fksource"
	shardName := "0"

	defer vc.TearDown(t)

	cell := vc.Cells[cellName]
	vc.AddKeyspace(t, []*Cell{cell}, sourceKeyspace, shardName, initialFKSourceVSchema, initialFKSchema, 0, 0, 100, sourceKsOpts)

	vtgate = cell.Vtgates[0]
	require.NotNil(t, vtgate)
	err := cluster.WaitForHealthyShard(vc.VtctldClient, sourceKeyspace, shardName)
	require.NoError(t, err)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", sourceKeyspace, shardName), 1, 30*time.Second)

	vtgateConn = getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	verifyClusterHealth(t, vc)

	insertInitialFKData(t)
	withLoad := true // Set it to false to skip load simulation, while debugging
	var cancel context.CancelFunc
	var ctx context.Context
	if withLoad {
		ctx, cancel = context.WithCancel(context.Background())
		defer func() {
			select {
			case <-ctx.Done():
			default:
				cancel()
			}
		}()
		go simulateLoad(t, ctx)
	}
	targetKeyspace := "fktarget"
	targetTabletId := 200
	vc.AddKeyspace(t, []*Cell{cell}, targetKeyspace, shardName, initialFKTargetVSchema, initialFKSchema, 0, 0, targetTabletId, sourceKsOpts)
	vtgate.WaitForStatusOfTabletInShard(fmt.Sprintf("%s.%s.primary", targetKeyspace, shardName), 1, 30*time.Second)

	workflowName := "fk"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKeyspace, workflowName)

	mt := newMoveTables(vc, &moveTables{
		workflowName:   workflowName,
		targetKeyspace: targetKeyspace,
		sourceKeyspace: sourceKeyspace,
		atomicCopy:     true,
	}, moveTablesFlavorRandom)
	mt.Create()

	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	targetKs := vc.Cells[cellName].Keyspaces[targetKeyspace]
	targetTab := targetKs.Shards["0"].Tablets[fmt.Sprintf("%s-%d", cellName, targetTabletId)].Vttablet
	require.NotNil(t, targetTab)
	catchup(t, targetTab, workflowName, "MoveTables")
	vdiff(t, targetKeyspace, workflowName, cellName, true, false, nil)
	waitForAdditionalRows(t, 200)
	vdiff(t, targetKeyspace, workflowName, cellName, true, false, nil)
	if withLoad {
		cancel()
		<-ch
	}
	mt.SwitchReadsAndWrites()

	log.Infof("Switch traffic done")

	if withLoad {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
		go simulateLoad(t, ctx)
	}
	waitForAdditionalRows(t, 200)
	if withLoad {
		cancel()
		<-ch
	}
}

func insertInitialFKData(t *testing.T) {
	t.Run("insertInitialFKData", func(t *testing.T) {
		sourceKeyspace := "fksource"
		shard := "0"
		db := fmt.Sprintf("%s:%s", sourceKeyspace, shard)
		log.Infof("Inserting initial FK data")
		execMultipleQueries(t, vtgateConn, db, initialFKData)
		log.Infof("Done inserting initial FK data")
		waitForRowCount(t, vtgateConn, db, "parent", 2)
		waitForRowCount(t, vtgateConn, db, "child", 3)
	})
}

var currentParentId int64
var currentChildId int64

func init() {
	currentParentId = 100
	currentChildId = 100
}

var ch = make(chan bool)

func simulateLoad(t *testing.T, ctx context.Context) {
	var err error
	for i := 0; ; i++ {
		if i%1000 == 0 {
			log.Infof("Load simulation iteration %d", i)
		}
		select {
		case <-ctx.Done():
			ch <- true
			return
		default:
		}
		// Decide operation based on random number
		op := rand.Intn(100)
		switch {
		case op < 50: // 50% chance to insert
			insert(t)
		case op < 80: // 30% chance to update
			update(t)
		default: // 20% chance to delete
			delete(t)
		}
		require.NoError(t, err)
		time.Sleep(1 * time.Millisecond)
	}
}

func getNumRowsParent(t *testing.T, vtgateConn *mysql.Conn) int {
	qr := execVtgateQuery(t, vtgateConn, "fksource", "SELECT COUNT(*) FROM parent")
	require.NotNil(t, qr)
	numRows, err := strconv.Atoi(qr.Rows[0][0].ToString())
	require.NoError(t, err)
	return numRows
}

func waitForAdditionalRows(t *testing.T, count int) {
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	numRowsStart := getNumRowsParent(t, vtgateConn)
	shortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		switch {
		case shortCtx.Err() != nil:
			t.Fatalf("Timed out waiting for additional rows")
		default:
			numRows := getNumRowsParent(t, vtgateConn)
			if numRows >= numRowsStart+count {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func exec2(t *testing.T, query string) *sqltypes.Result {
	qr := execVtgateQuery(t, vtgateConn, "fksource", query)
	require.NotNil(t, qr)
	return qr
}

func insert(t *testing.T) {
	currentParentId++
	insertQuery := fmt.Sprintf("INSERT INTO parent (id) VALUES (%d)", currentParentId)
	qr := exec2(t, insertQuery)
	require.NotNil(t, qr)
	// insert one or more children, some with valid foreign keys, some without.
	for i := 0; i < rand.Intn(4)+1; i++ {
		currentChildId++
		if i == 3 {
			insertQuery = fmt.Sprintf("INSERT /*+ SET_VAR(foreign_key_checks=0) */ INTO child (id, parent_id) VALUES (%d, %d)", currentChildId, currentParentId+1000000)
			exec2(t, insertQuery)
		} else {
			insertQuery = fmt.Sprintf("INSERT INTO child (id, parent_id) VALUES (%d, %d)", currentChildId, currentParentId)
			exec2(t, insertQuery)
		}
	}
}

func getRandomId(t *testing.T) int64 {
	selectQuery := "SELECT id FROM parent ORDER BY RAND() LIMIT 1"
	qr := exec2(t, selectQuery)
	require.NotNil(t, qr)
	if len(qr.Rows) == 0 {
		return 0
	}
	id, err := qr.Rows[0][0].ToInt64()
	require.NoError(t, err)
	return id
}

func update(t *testing.T) {
	updateQuery := fmt.Sprintf("UPDATE parent SET name = 'parent%d' WHERE id = %d", rand.Intn(1000)+1, getRandomId(t))
	exec2(t, updateQuery)
}

func delete(t *testing.T) {
	deleteQuery := fmt.Sprintf("DELETE FROM parent WHERE id = %d", getRandomId(t))
	exec2(t, deleteQuery)
}
