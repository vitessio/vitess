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

package stress

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/syscallutil"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
)

var (
	// updateRowBaseVals is the base row values that we use to ensure 1 update on each shard with the same increment.
	updateRowBaseVals = [3]int{
		4, // 4 maps to 0x20 and ends up in the first shard (-40)
		6, // 6 maps to 0x60 and ends up in the second shard (40-80)
		9, // 9 maps to 0x90 and ends up in the third shard (80-)
		// We can increment all of these values by multiples of 16 and they'll always be in the same shard.
	}

	insertIntoFuzzUpdate   = "INSERT INTO twopc_fuzzer_update (id, col) VALUES (%d, %d)"
	updateFuzzUpdate       = "UPDATE twopc_fuzzer_update SET col = col + %d WHERE id = %d"
	insertIntoFuzzInsert   = "INSERT INTO twopc_fuzzer_insert (id, updateSet, threadId) VALUES (%d, %d, %d)"
	selectFromFuzzUpdate   = "SELECT col FROM twopc_fuzzer_update WHERE id = %d"
	selectIdFromFuzzInsert = "SELECT threadId FROM twopc_fuzzer_insert WHERE updateSet = %d AND id = %d ORDER BY col"
)

// TestTwoPCFuzzTest tests 2PC transactions in a fuzzer environment.
// The testing strategy involves running many transactions and checking that they all must be atomic.
// To this end, we have a very unique strategy. We have two sharded tables `twopc_fuzzer_update`, and `twopc_fuzzer_insert` with the following columns.
//   - id: This is the sharding column. We use reverse_bits as the sharding vindex because it is easy to reason about where a row will end up.
//   - col in `twopc_fuzzer_insert`: An auto-increment column.
//   - col in `twopc_fuzzer_update`: This is a bigint value that we will use to increment on updates.
//   - updateSet: This column will store which update set the inserts where done for.
//   - threadId: It stores the thread id of the fuzzer thread that inserted the row.
//
// The testing strategy is as follows -
// Every transaction will do 2 things -
//   - One, it will increment the `col` on 1 row in each of the shards of the `twopc_fuzzer_update` table.
//     To do this, we have sets of rows that each map to one shard. We prepopulate this before the test starts.
//     These sets are stored in the fuzzer in updateRowsVals.
//   - Two, it will insert one row in each of the shards of the `twopc_fuzzer_insert` table and it will also store the update set that it updated the rows off.
//
// We can check that a transaction was atomic by basically checking that the `col` value for all the rows that were updated together should match.
// If any transaction was partially successful, then it would have missed an increment on one of the rows.
// Moreover, the threadIDs of rows for a given update set in the 3 shards should be the same to ensure that conflicting transactions got committed in the same exact order.
func TestTwoPCFuzzTest(t *testing.T) {
	testcases := []struct {
		name                  string
		threads               int
		updateSets            int
		timeForTesting        time.Duration
		clusterDisruptions    []func(t *testing.T)
		disruptionProbability []int
	}{
		{
			name:           "Single Thread - Single Set",
			threads:        1,
			updateSets:     1,
			timeForTesting: 5 * time.Second,
		},
		{
			name:           "Multiple Threads - Single Set",
			threads:        2,
			updateSets:     1,
			timeForTesting: 5 * time.Second,
		},
		{
			name:           "Multiple Threads - Multiple Set",
			threads:        15,
			updateSets:     15,
			timeForTesting: 5 * time.Second,
		},
		{
			name:                  "Multiple Threads - Multiple Set - PRS, ERS, and MySQL & Vttablet restart, OnlineDDL disruptions",
			threads:               15,
			updateSets:            15,
			timeForTesting:        5 * time.Second,
			clusterDisruptions:    []func(t *testing.T){prs, ers, mysqlRestarts, vttabletRestarts, onlineDDLFuzzer},
			disruptionProbability: []int{5, 5, 5, 5, 5},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			conn, closer := start(t)
			defer closer()
			fz := newFuzzer(t, tt.threads, tt.updateSets, tt.clusterDisruptions, tt.disruptionProbability)

			fz.initialize(t, conn)
			conn.Close()
			// Start the fuzzer.
			fz.start(t)

			// Wait for the timeForTesting so that the threads continue to run.
			time.Sleep(tt.timeForTesting)

			// Signal the fuzzer to stop.
			fz.stop()

			// Wait for all transactions to be resolved.
			waitForResults(t, fmt.Sprintf(`show unresolved transactions for %v`, keyspaceName), "[]", 30*time.Second)
			// Verify that all the transactions run were actually atomic and no data issues have occurred.
			fz.verifyTransactionsWereAtomic(t)

			log.Errorf("Verification complete. All good!")
		})
	}
}

// verifyTransactionsWereAtomic verifies that the invariants of test are held.
// It checks the heuristics to ensure that the transactions run were atomic.
func (fz *fuzzer) verifyTransactionsWereAtomic(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	for updateSetIdx, updateSet := range fz.updateRowsVals {
		// All the three values of the update set must be equal.
		shard1Val := getColValueForIdFromFuzzUpdate(t, conn, updateSet[0])
		shard2Val := getColValueForIdFromFuzzUpdate(t, conn, updateSet[1])
		shard3Val := getColValueForIdFromFuzzUpdate(t, conn, updateSet[2])
		require.EqualValues(t, shard1Val, shard2Val)
		require.EqualValues(t, shard3Val, shard2Val)

		// Next we get the IDs from all the three shards for the given update set index.
		shard1IDs := getThreadIDsForUpdateSetFromFuzzInsert(t, conn, updateSetIdx, 1)
		shard2IDs := getThreadIDsForUpdateSetFromFuzzInsert(t, conn, updateSetIdx, 2)
		shard3IDs := getThreadIDsForUpdateSetFromFuzzInsert(t, conn, updateSetIdx, 3)
		require.EqualValues(t, shard1IDs, shard2IDs)
		require.EqualValues(t, shard3IDs, shard2IDs)
	}
}

// getColValueForIdFromFuzzUpdate gets the col column value for the given id in the twopc_fuzzer_update table.
func getColValueForIdFromFuzzUpdate(t *testing.T, conn *mysql.Conn, id int) uint64 {
	res, err := conn.ExecuteFetch(fmt.Sprintf(selectFromFuzzUpdate, id), 1, false)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	val, err := res.Rows[0][0].ToUint64()
	require.NoError(t, err)
	return val
}

// getThreadIDsForUpdateSetFromFuzzInsert gets the thread IDs for the given update set ordered by the col column from the twopc_fuzzer_insert table.
func getThreadIDsForUpdateSetFromFuzzInsert(t *testing.T, conn *mysql.Conn, updateSet int, shard int) []int {
	// We will select all the rows for the given update set for the given shard. To get all the rows for the given shard,
	// we can use the id column for filtering, since we know that the first shard will have all the values of id as 4, second shard as 6 and the last one as 9.
	res, err := conn.ExecuteFetch(fmt.Sprintf(selectIdFromFuzzInsert, updateSet, updateRowBaseVals[shard-1]), 10000, false)
	require.NoError(t, err)
	var ids []int
	for _, row := range res.Rows {
		require.Len(t, row, 1)
		threadId, err := row[0].ToInt()
		require.NoError(t, err)
		ids = append(ids, threadId)
	}
	return ids
}

// fuzzer runs threads that runs queries against the databases.
// It has parameters that define the way the queries are constructed.
type fuzzer struct {
	threads    int
	updateSets int
	t          *testing.T

	// shouldStop is an internal state variable, that tells the fuzzer
	// whether it should stop or not.
	shouldStop atomic.Bool
	// wg is an internal state variable, that used to know whether the fuzzer threads are running or not.
	wg sync.WaitGroup
	// updateRowVals are the rows that we use to ensure 1 update on each shard with the same increment.
	updateRowsVals [][]int
	// clusterDisruptions are the cluster level disruptions that can happen in a running cluster.
	clusterDisruptions []func(t *testing.T)
	// disruptionProbability is the chance for the disruption to happen. We check this every 100 milliseconds.
	disruptionProbability []int
}

// newFuzzer creates a new fuzzer struct.
func newFuzzer(t *testing.T, threads int, updateSets int, clusterDisruptions []func(t *testing.T), disruptionProbability []int) *fuzzer {
	fz := &fuzzer{
		t:                     t,
		threads:               threads,
		updateSets:            updateSets,
		wg:                    sync.WaitGroup{},
		clusterDisruptions:    clusterDisruptions,
		disruptionProbability: disruptionProbability,
	}
	// Initially the fuzzer thread is stopped.
	fz.shouldStop.Store(true)
	return fz
}

// stop stops the fuzzer and waits for it to finish execution.
func (fz *fuzzer) stop() {
	// Mark the thread to be stopped.
	fz.shouldStop.Store(true)
	// Wait for the fuzzer thread to stop.
	fz.wg.Wait()
}

// start starts running the fuzzer.
func (fz *fuzzer) start(t *testing.T) {
	// We mark the fuzzer thread to be running now.
	fz.shouldStop.Store(false)
	// fz.threads is the count of fuzzer threads, and one disruption thread.
	fz.wg.Add(fz.threads + 1)
	for i := 0; i < fz.threads; i++ {
		go func() {
			fz.runFuzzerThread(t, i)
		}()
	}
	go func() {
		fz.runClusterDisruptionThread(t)
	}()
}

// runFuzzerThread is used to run a thread of the fuzzer.
func (fz *fuzzer) runFuzzerThread(t *testing.T, threadId int) {
	// Whenever we finish running this thread, we should mark the thread has stopped.
	defer func() {
		fz.wg.Done()
	}()

	for {
		// If fuzzer thread is marked to be stopped, then we should exit this go routine.
		if fz.shouldStop.Load() == true {
			return
		}
		// Run an atomic transaction
		fz.generateAndExecuteTransaction(threadId)
	}

}

// initialize initializes all the variables that will be needed for running the fuzzer.
// It also creates the rows for the `twopc_fuzzer_update` table.
func (fz *fuzzer) initialize(t *testing.T, conn *mysql.Conn) {
	for i := 0; i < fz.updateSets; i++ {
		fz.updateRowsVals = append(fz.updateRowsVals, []int{
			updateRowBaseVals[0] + i*16,
			updateRowBaseVals[1] + i*16,
			updateRowBaseVals[2] + i*16,
		})
	}

	for _, updateSet := range fz.updateRowsVals {
		for _, id := range updateSet {
			_, err := conn.ExecuteFetch(fmt.Sprintf(insertIntoFuzzUpdate, id, 0), 0, false)
			require.NoError(t, err)
		}
	}
}

// generateAndExecuteTransaction generates the queries of the transaction and then executes them.
func (fz *fuzzer) generateAndExecuteTransaction(threadId int) {
	// Create a connection to the vtgate to run transactions.
	conn, err := mysql.Connect(context.Background(), &vtParams)
	if err != nil {
		return
	}
	defer conn.Close()
	// randomly generate an update set to use and the value to increment it by.
	updateSetVal := rand.Intn(fz.updateSets)
	incrementVal := rand.Int31()
	// We have to generate the update queries first. We can run the inserts only after the update queries.
	// Otherwise, our check to see that the ids in the twopc_fuzzer_insert table in all the shards are the exact same
	// for each update set ordered by the auto increment column will not be true.
	// That assertion depends on all the transactions running updates first to ensure that for any given update set,
	// no two transactions are running the insert queries.
	queries := []string{"begin"}
	queries = append(queries, fz.generateUpdateQueries(updateSetVal, incrementVal)...)
	queries = append(queries, fz.generateInsertQueries(updateSetVal, threadId)...)
	finalCommand := "commit"
	for _, query := range queries {
		_, err := conn.ExecuteFetch(query, 0, false)
		// If any command fails because of deadlocks or timeout or whatever, then we need to rollback the transaction.
		if err != nil {
			finalCommand = "rollback"
			break
		}
	}
	_, _ = conn.ExecuteFetch(finalCommand, 0, false)
}

// generateUpdateQueries generates the queries to run updates on the twopc_fuzzer_update table.
// It takes the update set index and the value to increment the set by.
func (fz *fuzzer) generateUpdateQueries(updateSet int, incrementVal int32) []string {
	var queries []string
	for _, id := range fz.updateRowsVals[updateSet] {
		queries = append(queries, fmt.Sprintf(updateFuzzUpdate, incrementVal, id))
	}
	rand.Shuffle(len(queries), func(i, j int) {
		queries[i], queries[j] = queries[j], queries[i]
	})
	return queries
}

// generateInsertQueries generates the queries to run inserts on the twopc_fuzzer_insert table.
// It takes the update set index and the thread id that is generating these inserts.
func (fz *fuzzer) generateInsertQueries(updateSet int, threadId int) []string {
	var queries []string
	for _, baseVal := range updateRowBaseVals {
		// In the twopc_fuzzer_insert table we are going to be inserting the following values -
		// 	- id: We use the updateRowBaseVals to ensure that the 3 insertions happen on 3 different shards.
		//	      This also allows us to read rows any of the shards without shard targeting by just filtering by this column.
		//  - updateSet: The update set index that these insertions correspond too.
		//  - threadId: The thread ID of the fuzzer thread that is running the transaction.
		queries = append(queries, fmt.Sprintf(insertIntoFuzzInsert, baseVal, updateSet, threadId))
	}
	rand.Shuffle(len(queries), func(i, j int) {
		queries[i], queries[j] = queries[j], queries[i]
	})
	return queries
}

// runClusterDisruptionThread runs the cluster level disruptions in a separate thread.
func (fz *fuzzer) runClusterDisruptionThread(t *testing.T) {
	// Whenever we finish running this thread, we should mark the thread has stopped.
	defer func() {
		fz.wg.Done()
	}()

	for {
		// If disruption thread is marked to be stopped, then we should exit this go routine.
		if fz.shouldStop.Load() == true {
			return
		}
		// Run a potential disruption
		fz.runClusterDisruption(t)
		time.Sleep(100 * time.Millisecond)
	}

}

// runClusterDisruption tries to run a single cluster disruption.
func (fz *fuzzer) runClusterDisruption(t *testing.T) {
	for idx, prob := range fz.disruptionProbability {
		if rand.Intn(100) < prob {
			fz.clusterDisruptions[idx](fz.t)
			return
		}
	}
}

/*
Cluster Level Disruptions for the fuzzer
*/

func prs(t *testing.T) {
	shards := clusterInstance.Keyspaces[0].Shards
	shard := shards[rand.Intn(len(shards))]
	vttablets := shard.Vttablets
	newPrimary := vttablets[rand.Intn(len(vttablets))]
	log.Errorf("Running PRS for - %v/%v with new primary - %v", keyspaceName, shard.Name, newPrimary.Alias)
	err := clusterInstance.VtctldClientProcess.PlannedReparentShard(keyspaceName, shard.Name, newPrimary.Alias)
	if err != nil {
		log.Errorf("error running PRS - %v", err)
	}
}

func ers(t *testing.T) {
	shards := clusterInstance.Keyspaces[0].Shards
	shard := shards[rand.Intn(len(shards))]
	vttablets := shard.Vttablets
	newPrimary := vttablets[rand.Intn(len(vttablets))]
	log.Errorf("Running ERS for - %v/%v with new primary - %v", keyspaceName, shard.Name, newPrimary.Alias)
	_, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("EmergencyReparentShard", fmt.Sprintf("%s/%s", keyspaceName, shard.Name), "--new-primary", newPrimary.Alias)
	if err != nil {
		log.Errorf("error running ERS - %v", err)
	}
}

func vttabletRestarts(t *testing.T) {
	shards := clusterInstance.Keyspaces[0].Shards
	shard := shards[rand.Intn(len(shards))]
	vttablets := shard.Vttablets
	tablet := vttablets[rand.Intn(len(vttablets))]
	log.Errorf("Restarting vttablet for - %v/%v - %v", keyspaceName, shard.Name, tablet.Alias)
	err := tablet.VttabletProcess.TearDown()
	if err != nil {
		log.Errorf("error stopping vttablet - %v", err)
		return
	}
	tablet.VttabletProcess.ServingStatus = "SERVING"
	for {
		err = tablet.VttabletProcess.Setup()
		if err == nil {
			return
		}
		// Sometimes vttablets fail to connect to the topo server due to a minor blip there.
		// We don't want to fail the test, so we retry setting up the vttablet.
		log.Errorf("error restarting vttablet - %v", err)
		time.Sleep(1 * time.Second)
	}
}

var orderedDDLFuzzer = []string{
	"alter table twopc_fuzzer_insert add column extra_col1 varchar(20)",
	"alter table twopc_fuzzer_insert add column extra_col2 varchar(20)",
	"alter table twopc_fuzzer_insert drop column extra_col1",
	"alter table twopc_fuzzer_insert drop column extra_col2",
}

// onlineDDLFuzzer runs an online DDL statement while ignoring any errors for the fuzzer.
func onlineDDLFuzzer(t *testing.T) {
	output, err := clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(keyspaceName, orderedDDLFuzzer[count%len(orderedDDLFuzzer)], cluster.ApplySchemaParams{
		DDLStrategy: "vitess --force-cut-over-after=1ms",
	})
	count++
	if err != nil {
		return
	}
	fmt.Println("Running online DDL with uuid: ", output)
	WaitForMigrationStatus(t, &vtParams, clusterInstance.Keyspaces[0].Shards, strings.TrimSpace(output), 2*time.Minute, schema.OnlineDDLStatusComplete, schema.OnlineDDLStatusFailed)
}

func mysqlRestarts(t *testing.T) {
	shards := clusterInstance.Keyspaces[0].Shards
	shard := shards[rand.Intn(len(shards))]
	vttablets := shard.Vttablets
	tablet := vttablets[rand.Intn(len(vttablets))]
	log.Errorf("Restarting MySQL for - %v/%v tablet - %v", keyspaceName, shard.Name, tablet.Alias)
	pidFile := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.pid", tablet.TabletUID))
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		// We can't read the file which means the PID file does not exist
		// The server must have stopped
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil {
		log.Errorf("Error in conversion to integer: %v", err)
		return
	}
	err = syscallutil.Kill(pid, syscall.SIGKILL)
	if err != nil {
		log.Errorf("Error in killing process: %v", err)
	}
}
