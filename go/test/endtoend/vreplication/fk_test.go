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

	var ls *fkLoadSimulator

	insertInitialFKData(t)
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
	ls.waitForAdditionalRows(200)
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
	}
	ls.waitForAdditionalRows(200)
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
		op := rand.Intn(100)
		switch {
		case op < 50: // 50% chance to insert
			ls.insert()
		case op < 80: // 30% chance to update
			ls.update()
		default: // 20% chance to delete
			ls.delete()
		}
		require.NoError(t, err)
		time.Sleep(1 * time.Millisecond)
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
	for i := 0; i < rand.Intn(4)+1; i++ {
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
	updateQuery := fmt.Sprintf("UPDATE parent SET name = 'parent%d' WHERE id = %d", rand.Intn(1000)+1, ls.getRandomId())
	ls.exec(updateQuery)
}

func (ls *fkLoadSimulator) delete() {
	deleteQuery := fmt.Sprintf("DELETE FROM parent WHERE id = %d", ls.getRandomId())
	ls.exec(deleteQuery)
}

func (ls *fkLoadSimulator) exec(query string) *sqltypes.Result {
	t := ls.t
	qr := execVtgateQuery(t, vtgateConn, "fksource", query)
	require.NotNil(t, qr)
	return qr
}
