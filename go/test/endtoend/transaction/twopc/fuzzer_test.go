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

package transaction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"vitess.io/vitess/go/mysql"
)

var (
	// updateRowsVals are the rows that we use to ensure 1 update on each shard with the same increment.
	updateRowsVals = [][]int{
		{
			4, // 4 maps to 0x20 and ends up in the first shard (-40)
			6, // 6 maps to 0x60 and ends up in the second shard (40-80)
			9, // 9 maps to 0x90 and ends up in the third shard (80-)
			// We can increment all of these values by multiples of 16 and they'll always be in the same shard.
		},
	}

	insertIntoFuzzUpdate = "INSERT INTO twopc_fuzzer_update (id, col) VALUES (%d, %d)"
	updateFuzzUpdate     = "UPDATE twopc_fuzzer_update SET col = col + %d WHERE id = %d"
	insertIntoFuzzInsert = "INSERT INTO twopc_fuzzer_insert (id, updateSet) VALUES (%d, %d)"
)

// TestTwoPCFuzzTest tests 2PC transactions in a fuzzer environment.
// The testing strategy involves running many transactions and checking that they all must be atomic.
// To this end, we have a very unique strategy. We have two sharded tables `twopc_fuzzer_update`, and `twopc_fuzzer_insert` with the following columns.
//   - id: This is the sharding column. We use reverse_bits as the sharding vindex because it is easy to reason about where a row will end up.
//   - col in `twopc_fuzzer_insert`: An auto-increment column.
//   - col in `twopc_fuzzer_update`: This is a bigint value that we will use to increment on updates.
//   - updateSet: This column will store which update set the inserts where done for.
//
// The testing strategy is as follows -
// Every transaction will do 2 things -
//   - One, it will increment the `col` on 1 row in each of the shards of the `twopc_fuzzer_update` table.
//     To do this, we have sets of rows that each map to one shard. We prepopulate this before the test starts.
//     These sets are stored in updateRowsVals.
//   - Two, it will insert one row in each of the shards of the `twopc_fuzzer_insert` table and it will also store the update set that it updated the rows off.
//
// We can check that a transaction was atomic by basically checking that the `col` value for all the rows that were updated together should match.
// If any transaction was partially successful, then it would have missed an increment on one of the rows.
// Moreover, the order of rows for a given update set in the 3 shards should be the same to ensure that conflicting transactions got committed in the same exact order.
func TestTwoPCFuzzTest(t *testing.T) {
	testcases := []struct {
		name           string
		threads        int
		updateSets     int
		timeForTesting time.Duration
	}{
		{
			name:           "Single Thread - Single Set",
			threads:        1,
			updateSets:     1,
			timeForTesting: 5 * time.Second,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			conn, closer := start(t)
			defer closer()
			fz := newFuzzer(tt.threads, tt.updateSets)

			fz.initialize(t, conn)
			// Start the fuzzer.
			fz.start(t)

			// Wait for the timeForTesting so that the threads continue to run.
			time.Sleep(tt.timeForTesting)

			fz.stop()
		})
	}
}

// fuzzer runs threads that runs queries against the databases.
// It has parameters that define the way the queries are constructed.
type fuzzer struct {
	threads    int
	updateSets int

	// shouldStop is an internal state variable, that tells the fuzzer
	// whether it should stop or not.
	shouldStop atomic.Bool
	// wg is an internal state variable, that used to know whether the fuzzer threads are running or not.
	wg sync.WaitGroup
}

// newFuzzer creates a new fuzzer struct.
func newFuzzer(threads int, updateSets int) *fuzzer {
	fz := &fuzzer{
		threads:    threads,
		updateSets: updateSets,
		wg:         sync.WaitGroup{},
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
	fz.wg.Add(fz.threads)
	for i := 0; i < fz.threads; i++ {
		go func() {
			fz.runFuzzerThread(t, i)
		}()
	}
}

// runFuzzerThread is used to run a thread of the fuzzer.
func (fz *fuzzer) runFuzzerThread(t *testing.T, threadId int) {
	// Whenever we finish running this thread, we should mark the thread has stopped.
	defer func() {
		fz.wg.Done()
	}()

	// Create a connection to the vtgate to run transactions.
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	for {
		// If fuzzer thread is marked to be stopped, then we should exit this go routine.
		if fz.shouldStop.Load() == true {
			return
		}
		// Run an atomic transaction
		fz.generateAndExecuteTransaction(t, conn, threadId)
	}

}

// initialize initializes all the variables that will be needed for running the fuzzer.
// It also creates the rows for the `twopc_fuzzer_update` table.
func (fz *fuzzer) initialize(t *testing.T, conn *mysql.Conn) {
	for i := 1; i < fz.updateSets; i++ {
		updateRowsVals = append(updateRowsVals, []int{
			updateRowsVals[0][0] + i*16,
			updateRowsVals[0][1] + i*16,
			updateRowsVals[0][2] + i*16,
		})
	}

	for _, updateSet := range updateRowsVals {
		for _, id := range updateSet {
			_, err := conn.ExecuteFetch(fmt.Sprintf(insertIntoFuzzUpdate, id, 0), 0, false)
			require.NoError(t, err)
		}
	}
}

// generateAndExecuteTransaction generates the queries of the transaction and then executes them.
func (fz *fuzzer) generateAndExecuteTransaction(t *testing.T, conn *mysql.Conn, threadId int) {
	// randomly generate an update set to use and the value to increment it by.
	updateSetVal := rand.Intn(fz.updateSets)
	incrementVal := rand.Int31()
	// We have to generate the update queries first. We can run the inserts only after the update queries.
	// Otherwise, our check to see that the ids in the twopc_fuzzer_insert table in all the shards are the exact same
	// for each update set ordered by the auto increment column will not be true.
	// That assertion depends on all the transactions running updates first to ensure that for any given update set,
	// no two transactions are running the insert queries.
	queries := fz.generateUpdateQueries(updateSetVal, incrementVal)
	queries = append(queries, fz.generateInsertQueries(updateSetVal, threadId)...)
	_, err := conn.ExecuteFetch("begin", 0, false)
	require.NoError(t, err)
	for _, query := range queries {
		_, _ = conn.ExecuteFetch(query, 0, false)
	}
	_, _ = conn.ExecuteFetch("commit", 0, false)
}

// generateUpdateQueries generates the queries to run updates on the twopc_fuzzer_update table.
// It takes the update set index and the value to increment the set by.
func (fz *fuzzer) generateUpdateQueries(updateSet int, incrementVal int32) []string {
	var queries []string
	for _, id := range updateRowsVals[updateSet] {
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
	for _, id := range updateRowsVals[0] {
		queries = append(queries, fmt.Sprintf(insertIntoFuzzInsert, updateSet, threadId*16+id))
	}
	rand.Shuffle(len(queries), func(i, j int) {
		queries[i], queries[j] = queries[j], queries[i]
	})
	return queries
}
