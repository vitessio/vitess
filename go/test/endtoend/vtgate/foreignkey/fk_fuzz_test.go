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

	"vitess.io/vitess/go/test/endtoend/utils"
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
	// idCollisionMap is used to make sure that we don't end up running two queries from two threads that are running on the same table
	// on the same primary column. This is going to be a challenge, because lets say we run two inserts on the same primary col value,
	// In this case, the two queries will race with each other, and Vitess and MySQL might end up breaking ties in differnt ways, causing
	// the data to be different in them. mu is the set of mutexes, protecting access to the map.
	mu             []sync.Mutex
	idCollisionMap []map[int]bool
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
	// Initialize the map and mutexes.
	for range fkTables {
		fz.mu = append(fz.mu, sync.Mutex{})
		fz.idCollisionMap = append(fz.idCollisionMap, map[int]bool{})
	}
	return fz
}

// generateQuery generates a query from the parameters for the fuzzer.
func (fz *fuzzer) generateQuery(tableId, idValue, colValue int) string {
	val := rand.Intn(fz.insertShare + fz.updateShare + fz.deleteShare)
	if val < fz.insertShare {
		return fz.generateInsertQuery(tableId, idValue, colValue)
	}
	if val < fz.insertShare+fz.updateShare {
		return fz.generateUpdateQuery(tableId, idValue, colValue)
	}
	return fz.generateDeleteQuery(tableId, idValue)
}

// generateInsertQuery generates an INSERT query from the parameters for the fuzzer.
func (fz *fuzzer) generateInsertQuery(tableId, idValue, colValue int) string {
	query := fmt.Sprintf("insert into %v (id, col) values (%v, %v)", fkTables[tableId], idValue, colValue)
	if colValue == 0 {
		query = fmt.Sprintf("insert into %v (id, col) values (%v, NULL)", fkTables[tableId], idValue)
	}
	return query
}

// generateUpdateQuery generates an UPDATE query from the parameters for the fuzzer.
func (fz *fuzzer) generateUpdateQuery(tableId, idValue, colValue int) string {
	query := fmt.Sprintf("update %v set col = %v where id = %v", fkTables[tableId], colValue, idValue)
	if colValue == 0 {
		query = fmt.Sprintf("update %v set col = NULL where id = %v", fkTables[tableId], idValue)
	}
	return query
}

// generateDeleteQuery generates a DELETE query from the parameters for the fuzzer.
func (fz *fuzzer) generateDeleteQuery(tableId, idValue int) string {
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
		tableId := rand.Intn(len(fkTables))
		idValue := 1 + rand.Intn(fz.maxValForId)
		colValue := rand.Intn(1 + fz.maxValForCol)
		safeToUse := fz.checkAndMarkValue(tableId, idValue)
		if !safeToUse {
			continue
		}
		// Get a query and execute it.
		query := fz.generateQuery(tableId, idValue, colValue)
		_, _ = mcmp.ExecAllowAndCompareError(query)
		fz.freeValue(tableId, idValue)
	}
}

// stop stops the fuzzer and waits for it to finish execution.
func (fz *fuzzer) stop() {
	// Mark the thread to be stopped.
	fz.shouldStop.Store(true)
	// Wait for the fuzzer thread to stop.
	fz.wg.Wait()
}

// checkAndMarkValue checks if a concurrent query is currently running on the given tableId and idValue.
// If not, then it marks it in the map and returns that it is safe to continue with this query.
func (fz *fuzzer) checkAndMarkValue(tableId int, idValue int) bool {
	fz.mu[tableId].Lock()
	defer fz.mu[tableId].Unlock()

	if !fz.idCollisionMap[tableId][idValue] {
		fz.idCollisionMap[tableId][idValue] = true
		return true
	}
	return false
}

// freeValue frees the value for the given table and id pair. This can now be used by other running threads.
func (fz *fuzzer) freeValue(tableId int, idValue int) {
	fz.mu[tableId].Lock()
	defer fz.mu[tableId].Unlock()

	fz.idCollisionMap[tableId][idValue] = false
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

				// Verify that the data in the MySQL database and Vitess database matches exactly.
				verifyDataIsCorrect(mcmp)
			})
		}
	}
}

// verifyDataIsCorrect verifies that the data in MySQL database matches the data in the Vitess database.
func verifyDataIsCorrect(mcmp utils.MySQLCompare) {
	for _, table := range fkTables {
		query := fmt.Sprintf("SELECT * FROM %v ORDER BY id", table)
		mcmp.Exec(query)
	}
}
