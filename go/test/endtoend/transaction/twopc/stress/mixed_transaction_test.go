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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"

	"vitess.io/vitess/go/mysql"
)

// TestTwoPCMixedTxFuzzTest tests 2PC transactions in a fuzzer environment.
// The testing strategy involves running many single and multi shard transactions and checking that they all must be atomic.
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
func TestTwoPCMixedTxFuzzTest(t *testing.T) {
	testcases := []struct {
		name                  string
		singleShardThreads    int
		multiShardThreads     int
		updateSets            int
		timeForTesting        time.Duration
		clusterDisruptions    []func(*testing.T)
		disruptionProbability []int
	}{
		{
			name:               "Single Thread - Single Set",
			singleShardThreads: 1,
			multiShardThreads:  1,
			updateSets:         1,
			timeForTesting:     5 * time.Second,
		},
		{
			name:               "Multiple Threads - Single Set",
			singleShardThreads: 2,
			multiShardThreads:  2,
			updateSets:         1,
			timeForTesting:     5 * time.Second,
		},
		{
			name:               "Multiple Threads - Multiple Set",
			singleShardThreads: 15,
			multiShardThreads:  15,
			updateSets:         15,
			timeForTesting:     5 * time.Second,
		},
		{
			name:                  "Multiple Threads - Multiple Set - PRS, ERS and MySQL restart disruptions",
			singleShardThreads:    15,
			multiShardThreads:     15,
			updateSets:            15,
			timeForTesting:        90 * time.Minute,
			clusterDisruptions:    []func(*testing.T){prs, ers, mysqlRestarts},
			disruptionProbability: []int{25, 25, 50},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			conn, closer := start(t)
			defer closer()
			fz := newMixedTxFuzzer(tt.singleShardThreads, tt.multiShardThreads, tt.updateSets, tt.clusterDisruptions, tt.disruptionProbability)

			fz.initialize(t, conn)
			conn.Close()
			// Start the fuzzer.
			fz.start(t)

			// Wait for the timeForTesting so that the threads continue to run.
			time.Sleep(tt.timeForTesting)

			// Signal the fuzzer to stop.
			fz.stop()
		})
	}
}

// fuzzer runs threads that runs queries against the databases.
// It has parameters that define the way the queries are constructed.
type mixedTxFuzzer struct {
	// singleShardThreads is the number of threads that will run transactions that only touch a single shard.
	singleShardThreads int
	// crossShardThreads is the number of threads that will run transactions that touch multiple shards.
	crossShardThreads int
	updateSets        int

	// shouldStop is an internal state variable, that tells the fuzzer
	// whether it should stop or not.
	shouldStop atomic.Bool
	// wg is an internal state variable, that used to know whether the fuzzer threads are running or not.
	wg sync.WaitGroup
	// updateRowVals are the rows that we use to ensure 1 update on each shard with the same increment.
	updateRowsVals [][]int
	// clusterDisruptions are the cluster level disruptions that can happen in a running cluster.
	clusterDisruptions []func(*testing.T)
	// disruptionProbability is the chance for the disruption to happen. We check this every 100 milliseconds.
	disruptionProbability []int

	ticker             time.Ticker
	multiShardSuccess  atomic.Uint64
	singleShardSuccess atomic.Uint64
}

// newMixedTxFuzzer creates a new mixedTxFuzzer struct.
func newMixedTxFuzzer(singleShardThreads, crossShardThreads, updateSets int, clusterDisruptions []func(t *testing.T), disruptionProbability []int) *mixedTxFuzzer {
	fz := &mixedTxFuzzer{
		singleShardThreads:    singleShardThreads,
		crossShardThreads:     crossShardThreads,
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
func (fz *mixedTxFuzzer) stop() {
	// Mark the thread to be stopped.
	fz.shouldStop.Store(true)
	// Wait for the fuzzer thread to stop.
	fz.wg.Wait()
}

// start starts running the fuzzer.
func (fz *mixedTxFuzzer) start(t *testing.T) {
	// We mark the fuzzer thread to be running now.
	fz.shouldStop.Store(false)

	// fz.threads is the count of fuzzer threads, and one disruption thread.
	fz.wg.Add(fz.singleShardThreads + fz.crossShardThreads + 1)
	for i := 0; i < fz.singleShardThreads; i++ {
		go func() {
			fz.runSingleShardTransactions()
		}()
	}
	for i := 0; i < fz.crossShardThreads; i++ {
		go func() {
			fz.runCrossShardTransactions()
		}()
	}
	go func() {
		fz.runClusterDisruptionThread(t)
	}()
}

// initialize initializes all the variables that will be needed for running the fuzzer.
// It also creates the rows for the `twopc_fuzzer_update` table.
func (fz *mixedTxFuzzer) initialize(t *testing.T, conn *mysql.Conn) {
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

// runCrossShardTransactions is used to run a thread of the fuzzer.
func (fz *mixedTxFuzzer) runSingleShardTransactions() {
	// Whenever we finish running this thread, we should mark the thread has stopped.
	defer func() {
		fz.wg.Done()
	}()

	// If fuzzer thread is marked to be stopped, then we should exit this go routine.
	for !fz.shouldStop.Load() {
		// Run an atomic transaction
		err := fz.execSingleShard()
		if err == nil {
			fz.singleShardSuccess.Add(1)
			fmt.Printf("Single Shard Success: %d\n", fz.singleShardSuccess.Load())
		}
	}

}

// execSingleShard generates the queries of the transaction and then executes them.
func (fz *mixedTxFuzzer) execSingleShard() error {
	// Create a connection to the vtgate to run transactions.
	conn, err := mysql.Connect(context.Background(), &vtParams)
	if err != nil {
		return err
	}
	defer conn.Close()
	// randomly generate an update set to use and the value to increment it by.
	updateSetVal := rand.Intn(fz.updateSets)
	ids := fz.updateRowsVals[updateSetVal]
	id := ids[rand.Intn(len(ids))]
	incrementVal := rand.Int31()

	updQuery := fmt.Sprintf(updateSingleShard, incrementVal, id)
	_, err = conn.ExecuteFetch(updQuery, 0, false)
	return err

}

// runCrossShardTransactions is used to run a thread of the fuzzer.
func (fz *mixedTxFuzzer) runCrossShardTransactions() {
	// Whenever we finish running this thread, we should mark the thread has stopped.
	defer func() {
		fz.wg.Done()
	}()

	// If fuzzer thread is marked to be stopped, then we should exit this go routine.
	for !fz.shouldStop.Load() {
		// Run an atomic transaction
		err := fz.execMultiShard()
		if err == nil {
			fz.multiShardSuccess.Add(1)
			fmt.Printf("Multi Shard Success: %d\n", fz.multiShardSuccess.Load())
		}
	}

}

// execMultiShard generates the queries of the transaction and then executes them.
func (fz *mixedTxFuzzer) execMultiShard() error {
	// Create a connection to the vtgate to run transactions.
	conn, err := mysql.Connect(context.Background(), &vtParams)
	if err != nil {
		return err
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
	defer func() {
		conn.ExecuteFetch("rollback", 0, false)
		if err != nil {
			fmt.Println(err)
		}
	}()
	_, err = conn.ExecuteFetch("begin", 0, false)
	if err != nil {
		return err
	}
	selQuery := fz.generateSelForUpdQuery(updateSetVal)
	qr, err := conn.ExecuteFetch(selQuery, 3, false)
	if err != nil {
		return err
	}

	for _, row := range qr.Rows {
		var id, col int
		id, err = row[0].ToInt()
		if err != nil {
			return err
		}
		col, err = row[1].ToInt()
		if err != nil {
			return err
		}
		updQuery := fz.generateUpdQuery(id, col, int(incrementVal))
		_, err = conn.ExecuteFetch(updQuery, 3, false)
		if err != nil {
			return err
		}
	}
	_, err = conn.ExecuteFetch("commit", 0, false)
	return err
}

var (
	updateMultiShard  = "update twopc_fuzzer_update set col = col + %d where id = %d and col = %d"
	updateSingleShard = "update twopc_fuzzer_update set col = col + %d where id = %d"
	selectMultiShard  = "select id, col from twopc_fuzzer_update where id in (%d, %d, %d) for update"
)

func (fz *mixedTxFuzzer) generateSelForUpdQuery(updateSet int) string {
	ids := fz.updateRowsVals[updateSet]
	return fmt.Sprintf(selectMultiShard, ids[0], ids[1], ids[2])
}

func (fz *mixedTxFuzzer) generateUpdQuery(id, oldVal, incrementVal int) string {
	return fmt.Sprintf(updateMultiShard, incrementVal, id, oldVal)
}

func (fz *mixedTxFuzzer) generateUpdSingleShardQuery(updateSet, incrementVal int) string {
	ids := fz.updateRowsVals[updateSet]
	idx := rand.Intn(len(ids))
	return fmt.Sprintf(updateSingleShard, incrementVal, ids[idx])
}

// runClusterDisruptionThread runs the cluster level disruptions in a separate thread.
func (fz *mixedTxFuzzer) runClusterDisruptionThread(t *testing.T) {
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
		time.Sleep(2 * time.Minute)
	}

}

// runClusterDisruption tries to run a single cluster disruption.
func (fz *mixedTxFuzzer) runClusterDisruption(t *testing.T) {
	for idx, prob := range fz.disruptionProbability {
		if rand.Intn(100) < prob {
			fz.clusterDisruptions[idx](t)
			return
		}
	}
}
