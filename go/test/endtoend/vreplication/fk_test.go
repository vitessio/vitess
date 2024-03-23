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
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

const testWorkflowFlavor = workflowFlavorRandom

// TestFKWorkflow runs a MoveTables workflow with atomic copy for a db with foreign key constraints.
// It inserts initial data, then simulates load. We insert both child rows with foreign keys and those without,
// i.e. with foreign_key_checks=0.
func TestFKWorkflow(t *testing.T) {
	extraVTTabletArgs = []string{
		// Ensure that there are multiple copy phase cycles per table.
		"--vstream_packet_size=256",
		// Test VPlayer batching mode.
		fmt.Sprintf("--vreplication_experimental_flags=%d",
			vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage|vttablet.VReplicationExperimentalFlagOptimizeInserts|vttablet.VReplicationExperimentalFlagVPlayerBatching),
	}
	defer func() { extraVTTabletArgs = nil }()

	cellName := "zone1"
	vc = NewVitessCluster(t, nil)

	sourceKeyspace := "fksource"
	shardName := "0"

	defer vc.TearDown()

	cell := vc.Cells[cellName]
	vc.AddKeyspace(t, []*Cell{cell}, sourceKeyspace, shardName, initialFKSourceVSchema, initialFKSchema, 0, 0, 100, sourceKsOpts)

	verifyClusterHealth(t, vc)
	insertInitialFKData(t)

	var ls *fkLoadSimulator
	withLoad := true // Set it to false to skip load simulation, while debugging
	var cancel context.CancelFunc
	var ctx context.Context
	if withLoad {
		ctx, cancel = context.WithCancel(context.Background())
		ls = newFKLoadSimulator(t, ctx)
		defer func() {
			select {
			case <-ctx.Done():
			default:
				cancel()
			}
		}()
		go ls.simulateLoad()
	}

	targetKeyspace := "fktarget"
	targetTabletId := 200
	vc.AddKeyspace(t, []*Cell{cell}, targetKeyspace, shardName, initialFKTargetVSchema, "", 0, 0, targetTabletId, sourceKsOpts)

	testFKCancel(t, vc)

	workflowName := "fk"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKeyspace, workflowName)

	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		atomicCopy:     true,
	}, testWorkflowFlavor)
	mt.Create()

	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	targetKs := vc.Cells[cellName].Keyspaces[targetKeyspace]
	targetTab := targetKs.Shards["0"].Tablets[fmt.Sprintf("%s-%d", cellName, targetTabletId)].Vttablet
	require.NotNil(t, targetTab)
	catchup(t, targetTab, workflowName, "MoveTables")
	vdiff(t, targetKeyspace, workflowName, cellName, true, false, nil)
	if withLoad {
		ls.waitForAdditionalRows(200)
	}
	vdiff(t, targetKeyspace, workflowName, cellName, true, false, nil)
	if withLoad {
		cancel()
		<-ch
	}
	mt.SwitchReadsAndWrites()

	log.Infof("Switch traffic done")

	if withLoad {
		ctx, cancel = context.WithCancel(context.Background())
		ls = newFKLoadSimulator(t, ctx)
		defer cancel()
		go ls.simulateLoad()
		ls.waitForAdditionalRows(200)
		cancel()
		<-ch
	}
	mt.Complete()
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()

	t11Count := getRowCount(t, vtgateConn, "t11")
	t12Count := getRowCount(t, vtgateConn, "t12")
	require.Greater(t, t11Count, 1)
	require.Greater(t, t12Count, 1)
	require.Equal(t, t11Count, t12Count)
}

func insertInitialFKData(t *testing.T) {
	t.Run("insertInitialFKData", func(t *testing.T) {
		vtgateConn, closeConn := getVTGateConn()
		defer closeConn()
		sourceKeyspace := "fksource"
		shard := "0"
		db := fmt.Sprintf("%s:%s", sourceKeyspace, shard)
		log.Infof("Inserting initial FK data")
		execMultipleQueries(t, vtgateConn, db, initialFKData)
		log.Infof("Done inserting initial FK data")

		type tableCounts struct {
			name  string
			count int
		}
		for _, table := range []tableCounts{
			{"parent", 2}, {"child", 3},
			{"t1", 2}, {"t2", 3},
			{"t11", 1}, {"t12", 1},
		} {
			waitForRowCount(t, vtgateConn, db, table.name, table.count)
		}
	})
}

var currentParentId int64
var currentChildId int64

func init() {
	currentParentId = 100
	currentChildId = 100
}

var ch = make(chan bool)

type fkLoadSimulator struct {
	t   *testing.T
	ctx context.Context
}

func newFKLoadSimulator(t *testing.T, ctx context.Context) *fkLoadSimulator {
	return &fkLoadSimulator{
		t:   t,
		ctx: ctx,
	}
}

var indexCounter int = 100 // used to insert into t11 and t12
func (ls *fkLoadSimulator) simulateLoad() {
	t := ls.t
	var err error
	for i := 0; ; i++ {
		if i%1000 == 0 {
			log.Infof("Load simulation iteration %d", i)
		}
		select {
		case <-ls.ctx.Done():
			ch <- true
			return
		default:
		}
		// Decide operation based on random number
		op := rand.IntN(100)
		switch {
		case op < 50: // 50% chance to insert
			ls.insert()
		case op < 80: // 30% chance to update
			ls.update()
		default: // 20% chance to delete
			ls.delete()
		}
		for _, table := range []string{"t11", "t12"} {
			query := fmt.Sprintf("insert /*+ SET_VAR(foreign_key_checks=0) */ into fksource.%s values(%d, %d)", table, indexCounter, indexCounter)
			ls.exec(query)
			indexCounter++
		}
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ls *fkLoadSimulator) getNumRowsParent(vtgateConn *mysql.Conn) int {
	t := ls.t
	qr := execVtgateQuery(t, vtgateConn, "fksource", "SELECT COUNT(*) FROM parent")
	require.NotNil(t, qr)
	numRows, err := strconv.Atoi(qr.Rows[0][0].ToString())
	require.NoError(t, err)
	return numRows
}

func (ls *fkLoadSimulator) waitForAdditionalRows(count int) {
	t := ls.t
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	numRowsStart := ls.getNumRowsParent(vtgateConn)
	shortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		switch {
		case shortCtx.Err() != nil:
			t.Fatalf("Timed out waiting for additional rows")
		default:
			numRows := ls.getNumRowsParent(vtgateConn)
			if numRows >= numRowsStart+count {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ls *fkLoadSimulator) insert() {
	t := ls.t
	currentParentId++
	insertQuery := fmt.Sprintf("INSERT INTO parent (id) VALUES (%d)", currentParentId)
	qr := ls.exec(insertQuery)
	require.NotNil(t, qr)
	// insert one or more children, some with valid foreign keys, some without.
	for i := 0; i < rand.IntN(4)+1; i++ {
		currentChildId++
		if i == 3 {
			insertQuery = fmt.Sprintf("INSERT /*+ SET_VAR(foreign_key_checks=0) */ INTO child (id, parent_id) VALUES (%d, %d)", currentChildId, currentParentId+1000000)
			ls.exec(insertQuery)
		} else {
			insertQuery = fmt.Sprintf("INSERT INTO child (id, parent_id) VALUES (%d, %d)", currentChildId, currentParentId)
			ls.exec(insertQuery)
		}
	}
}

func (ls *fkLoadSimulator) getRandomId() int64 {
	t := ls.t
	selectQuery := "SELECT id FROM parent ORDER BY RAND() LIMIT 1"
	qr := ls.exec(selectQuery)
	require.NotNil(t, qr)
	if len(qr.Rows) == 0 {
		return 0
	}
	id, err := qr.Rows[0][0].ToInt64()
	require.NoError(t, err)
	return id
}

func (ls *fkLoadSimulator) update() {
	updateQuery := fmt.Sprintf("UPDATE parent SET name = 'parent%d' WHERE id = %d", rand.IntN(1000)+1, ls.getRandomId())
	ls.exec(updateQuery)
}

func (ls *fkLoadSimulator) delete() {
	deleteQuery := fmt.Sprintf("DELETE FROM parent WHERE id = %d", ls.getRandomId())
	ls.exec(deleteQuery)
}

func (ls *fkLoadSimulator) exec(query string) *sqltypes.Result {
	t := ls.t
	vtgateConn, closeConn := getVTGateConn()
	defer closeConn()
	qr := execVtgateQuery(t, vtgateConn, "fksource", query)
	require.NotNil(t, qr)
	return qr
}

// testFKCancel confirms that a MoveTables workflow which includes tables with foreign key
// constraints, where the parent table is lexicographically sorted before the child table and
// thus may be dropped first, can be successfully cancelled.
func testFKCancel(t *testing.T, vc *VitessCluster) {
	targetKeyspace := "fktarget"
	sourceKeyspace := "fksource"
	workflowName := "wf2"
	ksWorkflow := fmt.Sprintf("%s.%s", targetKeyspace, workflowName)
	mt := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: targetKeyspace,
		},
		sourceKeyspace: sourceKeyspace,
		atomicCopy:     true,
	}, testWorkflowFlavor)
	mt.Create()
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())
	mt.Cancel()
}
