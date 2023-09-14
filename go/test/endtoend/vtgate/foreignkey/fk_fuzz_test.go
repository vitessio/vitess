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

package foreignkey

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
)

// fuzzer runs threads that runs queries against the databases.
// It has parameters that define the way the queries are constructed.
type fuzzer struct {
	maxValForId  int
	maxValForCol int
	insertShare  int
	deleteShare  int
	updateShare  int
	concurrency  int

	// shouldStop is an internal state variable, that tells the fuzzer
	// whether it should stop or not.
	shouldStop atomic.Bool
	// wg is an internal state variable, that used to know whether the fuzzer threads are running or not.
	wg sync.WaitGroup
	// firstFailureInfo stores the information about the database state after the first failure occurs.
	firstFailureInfo *debugInfo
}

// debugInfo stores the debugging information we can collect after a failure happens.
type debugInfo struct {
	queryToFail string
	vitessState []*sqltypes.Result
	mysqlState  []*sqltypes.Result
}

// newFuzzer creates a new fuzzer struct.
func newFuzzer(concurrency int, maxValForId int, maxValForCol int, insertShare int, deleteShare int, updateShare int) *fuzzer {
	fz := &fuzzer{
		concurrency:  concurrency,
		maxValForId:  maxValForId,
		maxValForCol: maxValForCol,
		insertShare:  insertShare,
		deleteShare:  deleteShare,
		updateShare:  updateShare,
		wg:           sync.WaitGroup{},
	}
	// Initially the fuzzer thread is stopped.
	fz.shouldStop.Store(true)
	return fz
}

// generateQuery generates a query from the parameters for the fuzzer.
func (fz *fuzzer) generateQuery() string {
	val := rand.Intn(fz.insertShare + fz.updateShare + fz.deleteShare)
	if val < fz.insertShare {
		return fz.generateInsertQuery()
	}
	if val < fz.insertShare+fz.updateShare {
		return fz.generateUpdateQuery()
	}
	return fz.generateDeleteQuery()
}

// generateInsertQuery generates an INSERT query from the parameters for the fuzzer.
func (fz *fuzzer) generateInsertQuery() string {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	colValue := rand.Intn(1 + fz.maxValForCol)
	query := fmt.Sprintf("insert into %v (id, col) values (%v, %v)", fkTables[tableId], idValue, colValue)
	if colValue == 0 {
		query = fmt.Sprintf("insert into %v (id, col) values (%v, NULL)", fkTables[tableId], idValue)
	}
	return query
}

// generateUpdateQuery generates an UPDATE query from the parameters for the fuzzer.
func (fz *fuzzer) generateUpdateQuery() string {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	colValue := rand.Intn(1 + fz.maxValForCol)
	query := fmt.Sprintf("update %v set col = %v where id = %v", fkTables[tableId], colValue, idValue)
	if colValue == 0 {
		query = fmt.Sprintf("update %v set col = NULL where id = %v", fkTables[tableId], idValue)
	}
	return query
}

// generateDeleteQuery generates a DELETE query from the parameters for the fuzzer.
func (fz *fuzzer) generateDeleteQuery() string {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	query := fmt.Sprintf("delete from %v where id = %v", fkTables[tableId], idValue)
	return query
}

// start starts running the fuzzer.
func (fz *fuzzer) start(t *testing.T, sharded bool) {
	// We mark the fuzzer thread to be running now.
	fz.shouldStop.Store(false)
	fz.wg.Add(fz.concurrency)
	for i := 0; i < fz.concurrency; i++ {
		fuzzerThreadId := i
		go func() {
			fz.runFuzzerThread(t, sharded, fuzzerThreadId)
		}()
	}
}

// runFuzzerThread is used to run a thread of the fuzzer.
func (fz *fuzzer) runFuzzerThread(t *testing.T, sharded bool, fuzzerThreadId int) {
	// Whenever we finish running this thread, we should mark the thread has stopped.
	defer func() {
		fz.wg.Done()
	}()
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)
	// Set the correct keyspace to use from VtGates.
	if sharded {
		_ = utils.Exec(t, mcmp.VtConn, "use `ks`")
	} else {
		_ = utils.Exec(t, mcmp.VtConn, "use `uks`")
	}
	for {
		// If fuzzer thread is marked to be stopped, then we should exit this go routine.
		if fz.shouldStop.Load() == true {
			return
		}
		// Get a query and execute it.
		query := fz.generateQuery()
		// When the concurrency is 1, then we run the query both on MySQL and Vitess.
		if fz.concurrency == 1 {
			_, _ = mcmp.ExecAllowAndCompareError(query)
			// If t is marked failed, we have encountered our first failure.
			// Lets collect the required information and finish execution.
			if t.Failed() {
				fz.firstFailureInfo = &debugInfo{
					queryToFail: query,
					mysqlState:  collectFkTablesState(mcmp.MySQLConn),
					vitessState: collectFkTablesState(mcmp.VtConn),
				}
				return
			}
		} else {
			// When we are running concurrent threads, then we run all the queries on Vitess.
			_, _ = utils.ExecAllowError(t, mcmp.VtConn, query)
		}
	}
}

// stop stops the fuzzer and waits for it to finish execution.
func (fz *fuzzer) stop() {
	// Mark the thread to be stopped.
	fz.shouldStop.Store(true)
	// Wait for the fuzzer thread to stop.
	fz.wg.Wait()
}

// TestFkFuzzTest is a fuzzer test that works by querying the database concurrently.
// We have a pre-written set of query templates that we will use, but the data in the queries will
// be randomly generated. The intent is that we hammer the database as a real-world application would
// and check the correctness of data with MySQL.
// We are using the same schema for this test as we do for TestFkScenarios.
/*
 *                    fk_t1
 *                        │
 *                        │ On Delete Restrict
 *                        │ On Update Restrict
 *                        ▼
 *   ┌────────────────fk_t2────────────────┐
 *   │                                     │
 *   │On Delete Set Null                   │ On Delete Set Null
 *   │On Update Set Null                   │ On Update Set Null
 *   ▼                                     ▼
 * fk_t7                                fk_t3───────────────────┐
 *                                         │                    │
 *                                         │                    │ On Delete Set Null
 *                      On Delete Set Null │                    │ On Update Set Null
 *                      On Update Set Null │                    │
 *                                         ▼                    ▼
 *                                      fk_t4                fk_t6
 *                                         │
 *                                         │
 *                      On Delete Restrict │
 *                      On Update Restrict │
 *                                         │
 *                                         ▼
 *                                      fk_t5
 */
/*
 *                fk_t10
 *                   │
 * On Delete Cascade │
 * On Update Cascade │
 *                   │
 *                   ▼
 *                fk_t11──────────────────┐
 *                   │                    │
 *                   │                    │ On Delete Restrict
 * On Delete Cascade │                    │ On Update Restrict
 * On Update Cascade │                    │
 *                   │                    │
 *                   ▼                    ▼
 *                fk_t12               fk_t13
 */
/*
 *                 fk_t15
 *                    │
 *                    │
 *  On Delete Cascade │
 *  On Update Cascade │
 *                    │
 *                    ▼
 *                 fk_t16
 *                    │
 * On Delete Set Null │
 * On Update Set Null │
 *                    │
 *                    ▼
 *                 fk_t17──────────────────┐
 *                    │                    │
 *                    │                    │ On Delete Set Null
 *  On Delete Cascade │                    │ On Update Set Null
 *  On Update Cascade │                    │
 *                    │                    │
 *                    ▼                    ▼
 *                 fk_t18               fk_t19
 */
/*
	Self referenced foreign key from col2 to col in fk_t20
*/
func TestFkFuzzTest(t *testing.T) {
	// Wait for schema-tracking to be complete.
	waitForSchemaTrackingForFkTables(t)
	// Remove all the foreign key constraints for all the replicas.
	// We can then verify that the replica, and the primary have the same data, to ensure
	// that none of the queries ever lead to cascades/updates on MySQL level.
	for _, ks := range []string{shardedKs, unshardedKs} {
		replicas := getReplicaTablets(ks)
		for _, replica := range replicas {
			removeAllForeignKeyConstraints(t, replica, ks)
		}
	}

	testcases := []struct {
		name           string
		concurrency    int
		timeForTesting time.Duration
		maxValForId    int
		maxValForCol   int
		insertShare    int
		deleteShare    int
		updateShare    int
	}{
		{
			name:           "Single Thread - Only Inserts",
			concurrency:    1,
			timeForTesting: 5 * time.Second,
			maxValForCol:   5,
			maxValForId:    10,
			insertShare:    100,
			deleteShare:    0,
			updateShare:    0,
		},
		{
			name:           "Single Thread - Balanced Inserts and Deletes",
			concurrency:    1,
			timeForTesting: 5 * time.Second,
			maxValForCol:   5,
			maxValForId:    10,
			insertShare:    50,
			deleteShare:    50,
			updateShare:    0,
		},
		{
			name:           "Single Thread - Balanced Inserts and Updates",
			concurrency:    1,
			timeForTesting: 5 * time.Second,
			maxValForCol:   5,
			maxValForId:    10,
			insertShare:    50,
			deleteShare:    0,
			updateShare:    50,
		},
		{
			name:           "Single Thread - Balanced Inserts, Updates and Deletes",
			concurrency:    1,
			timeForTesting: 5 * time.Second,
			maxValForCol:   5,
			maxValForId:    10,
			insertShare:    50,
			deleteShare:    50,
			updateShare:    50,
		},
		{
			name:           "Multi Thread - Balanced Inserts, Updates and Deletes",
			concurrency:    30,
			timeForTesting: 5 * time.Second,
			maxValForCol:   5,
			maxValForId:    30,
			insertShare:    50,
			deleteShare:    50,
			updateShare:    50,
		},
	}

	for _, tt := range testcases {
		for _, testSharded := range []bool{false, true} {
			t.Run(getTestName(tt.name, testSharded), func(t *testing.T) {
				mcmp, closer := start(t)
				defer closer()
				// Set the correct keyspace to use from VtGates.
				if testSharded {
					t.Skip("Skip test since we don't have sharded foreign key support yet")
					_ = utils.Exec(t, mcmp.VtConn, "use `ks`")
				} else {
					_ = utils.Exec(t, mcmp.VtConn, "use `uks`")
				}

				// Create the fuzzer.
				fz := newFuzzer(tt.concurrency, tt.maxValForId, tt.maxValForCol, tt.insertShare, tt.deleteShare, tt.updateShare)

				// Start the fuzzer.
				fz.start(t, testSharded)

				// Wait for the timeForTesting so that the threads continue to run.
				time.Sleep(tt.timeForTesting)

				fz.stop()

				// We encountered an error while running the fuzzer. Let's print out the information!
				if fz.firstFailureInfo != nil {
					log.Errorf("Failing query - %v", fz.firstFailureInfo.queryToFail)
					for idx, table := range fkTables {
						log.Errorf("MySQL data for %v -\n%v", table, fz.firstFailureInfo.mysqlState[idx].Rows)
						log.Errorf("Vitess data for %v -\n%v", table, fz.firstFailureInfo.vitessState[idx].Rows)
					}
				}

				// Verify the consistency of the data.
				verifyDataIsCorrect(t, mcmp, tt.concurrency)
			})
		}
	}
}

// verifyDataIsCorrect verifies that the data in MySQL database matches the data in the Vitess database.
func verifyDataIsCorrect(t *testing.T, mcmp utils.MySQLCompare, concurrency int) {
	// For single concurrent thread, we run all the queries on both MySQL and Vitess, so we can verify correctness
	// by just checking if the data in MySQL and Vitess match.
	if concurrency == 1 {
		for _, table := range fkTables {
			query := fmt.Sprintf("SELECT * FROM %v ORDER BY id", table)
			mcmp.Exec(query)
		}
	} else {
		// For higher concurrency, we don't have MySQL data to verify everything is fine,
		// so we'll have to do something different.
		// We run LEFT JOIN queries on all the parent and child tables linked by foreign keys
		// to make sure that nothing is broken in the database.
		for _, reference := range fkReferences {
			query := fmt.Sprintf("select %v.id from %v left join %v on (%v.col = %v.col) where %v.col is null and %v.col is not null", reference.childTable, reference.childTable, reference.parentTable, reference.parentTable, reference.childTable, reference.parentTable, reference.childTable)
			res, err := mcmp.VtConn.ExecuteFetch(query, 1000, false)
			require.NoError(t, err)
			require.Zerof(t, len(res.Rows), "Query %v gave non-empty results", query)
		}
	}
	// We also verify that the results in Primary and Replica table match as is.
	for _, keyspace := range clusterInstance.Keyspaces {
		for _, shard := range keyspace.Shards {
			var primaryTab, replicaTab *cluster.Vttablet
			for _, vttablet := range shard.Vttablets {
				if vttablet.Type == "primary" {
					primaryTab = vttablet
				} else {
					replicaTab = vttablet
				}
			}
			require.NotNil(t, primaryTab)
			require.NotNil(t, replicaTab)
			primaryConn, err := utils.GetMySQLConn(primaryTab, fmt.Sprintf("vt_%v", keyspace.Name))
			require.NoError(t, err)
			replicaConn, err := utils.GetMySQLConn(replicaTab, fmt.Sprintf("vt_%v", keyspace.Name))
			require.NoError(t, err)
			primaryRes := collectFkTablesState(primaryConn)
			replicaRes := collectFkTablesState(replicaConn)
			verifyDataMatches(t, primaryRes, replicaRes)
		}
	}
}

// verifyDataMatches verifies that the two list of results are the same.
func verifyDataMatches(t *testing.T, resOne []*sqltypes.Result, resTwo []*sqltypes.Result) {
	require.EqualValues(t, len(resTwo), len(resOne), "Res 1 - %v, Res 2 - %v", resOne, resTwo)
	for idx, resultOne := range resOne {
		resultTwo := resTwo[idx]
		require.True(t, resultOne.Equal(resultTwo), "Rows 1 - %v, Rows 2 - %v", resultOne.Rows, resultTwo.Rows)
	}
}

// collectFkTablesState collects the data stored in the foreign key tables for the given connection.
func collectFkTablesState(conn *mysql.Conn) []*sqltypes.Result {
	var tablesData []*sqltypes.Result
	for _, table := range fkTables {
		query := fmt.Sprintf("SELECT * FROM %v ORDER BY id", table)
		res, _ := conn.ExecuteFetch(query, 10000, true)
		tablesData = append(tablesData, res)
	}
	return tablesData
}
