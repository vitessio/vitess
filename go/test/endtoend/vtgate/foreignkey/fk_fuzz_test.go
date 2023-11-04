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
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
)

type QueryFormat string

const (
	SQLQueries              QueryFormat = "SQLQueries"
	PreparedStatmentQueries QueryFormat = "PreparedStatmentQueries"
	PreparedStatementPacket QueryFormat = "PreparedStatementPacket"
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
	queryFormat  QueryFormat

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
	// This can be a list of queries for prepared statements.
	queryToFail []string
	vitessState []*sqltypes.Result
	mysqlState  []*sqltypes.Result
}

// newFuzzer creates a new fuzzer struct.
func newFuzzer(concurrency int, maxValForId int, maxValForCol int, insertShare int, deleteShare int, updateShare int, queryFormat QueryFormat) *fuzzer {
	fz := &fuzzer{
		concurrency:  concurrency,
		maxValForId:  maxValForId,
		maxValForCol: maxValForCol,
		insertShare:  insertShare,
		deleteShare:  deleteShare,
		updateShare:  updateShare,
		queryFormat:  queryFormat,
		wg:           sync.WaitGroup{},
	}
	// Initially the fuzzer thread is stopped.
	fz.shouldStop.Store(true)
	return fz
}

// generateQuery generates a query from the parameters for the fuzzer.
// The returned set is a list of strings, because for prepared statements, we have to run
// set queries first and then the final query eventually.
func (fz *fuzzer) generateQuery() []string {
	val := rand.Intn(fz.insertShare + fz.updateShare + fz.deleteShare)
	if val < fz.insertShare {
		switch fz.queryFormat {
		case SQLQueries:
			return []string{fz.generateInsertDMLQuery()}
		case PreparedStatmentQueries:
			return fz.getPreparedInsertQueries()
		default:
			panic("Unknown query type")
		}
	}
	if val < fz.insertShare+fz.updateShare {
		switch fz.queryFormat {
		case SQLQueries:
			return []string{fz.generateUpdateDMLQuery()}
		case PreparedStatmentQueries:
			return fz.getPreparedUpdateQueries()
		default:
			panic("Unknown query type")
		}
	}
	switch fz.queryFormat {
	case SQLQueries:
		return []string{fz.generateDeleteDMLQuery()}
	case PreparedStatmentQueries:
		return fz.getPreparedDeleteQueries()
	default:
		panic("Unknown query type")
	}
}

// generateInsertDMLQuery generates an INSERT query from the parameters for the fuzzer.
func (fz *fuzzer) generateInsertDMLQuery() string {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.Intn(1 + fz.maxValForCol)
		col2Value := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("insert into %v (id, col, col2) values (%v, %v, %v)", tableName, idValue, convertColValueToString(colValue), convertColValueToString(col2Value))
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.Intn(1 + fz.maxValForCol)
		colbValue := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("insert into %v (id, cola, colb) values (%v, %v, %v)", tableName, idValue, convertColValueToString(colaValue), convertColValueToString(colbValue))
	} else {
		colValue := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("insert into %v (id, col) values (%v, %v)", tableName, idValue, convertColValueToString(colValue))
	}
}

// convertColValueToString converts the given value to a string
func convertColValueToString(value int) string {
	if value == 0 {
		return "NULL"
	}
	return fmt.Sprintf("%d", value)
}

// generateUpdateDMLQuery generates an UPDATE query from the parameters for the fuzzer.
func (fz *fuzzer) generateUpdateDMLQuery() string {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.Intn(1 + fz.maxValForCol)
		col2Value := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set col = %v, col2 = %v where id = %v", tableName, convertColValueToString(colValue), convertColValueToString(col2Value), idValue)
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.Intn(1 + fz.maxValForCol)
		colbValue := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set cola = %v, colb = %v where id = %v", tableName, convertColValueToString(colaValue), convertColValueToString(colbValue), idValue)
	} else {
		colValue := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set col = %v where id = %v", tableName, convertColValueToString(colValue), idValue)
	}
}

// generateDeleteDMLQuery generates a DELETE query from the parameters for the fuzzer.
func (fz *fuzzer) generateDeleteDMLQuery() string {
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
	// Create a MySQL Compare that connects to both Vitess and MySQL and runs the queries against both.
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)
	var vitessDb, mysqlDb *sql.DB
	if fz.queryFormat == PreparedStatementPacket {
		// Open another connection to Vitess using the go-sql-driver so that we can send prepared statements as COM_STMT_PREPARE packets.
		vitessDb, err = sql.Open("mysql", fmt.Sprintf("@tcp(%s:%v)/%s", vtParams.Host, vtParams.Port, vtParams.DbName))
		require.NoError(t, err)
		defer vitessDb.Close()
		// Open a similar connection to MySQL
		mysqlDb, err = sql.Open("mysql", fmt.Sprintf("%v:%v@unix(%s)/%s", mysqlParams.Uname, mysqlParams.Pass, mysqlParams.UnixSocket, mysqlParams.DbName))
		require.NoError(t, err)
		defer mysqlDb.Close()
	}
	// Set the correct keyspace to use from VtGates.
	if sharded {
		_ = utils.Exec(t, mcmp.VtConn, "use `ks`")
		if vitessDb != nil {
			_, _ = vitessDb.Exec("use `ks`")
		}
	} else {
		_ = utils.Exec(t, mcmp.VtConn, "use `uks`")
		if vitessDb != nil {
			_, _ = vitessDb.Exec("use `uks`")
		}
	}
	for {
		// If fuzzer thread is marked to be stopped, then we should exit this go routine.
		if fz.shouldStop.Load() == true {
			return
		}
		switch fz.queryFormat {
		case SQLQueries, PreparedStatmentQueries:
			if fz.generateAndExecuteStatementQuery(t, mcmp) {
				return
			}
		case PreparedStatementPacket:
			if fz.generateAndExecutePreparedPacketQuery(t, mysqlDb, vitessDb, mcmp) {
				return
			}
		default:
			panic("Unknown query format")
		}

	}
}

// generateAndExecuteStatementQuery generates a query and runs it on Vitess (and possibly MySQL).
// In this function we send the queries to Vitess always using COM_QUERY packets.
// We handle 2 query formats in this function:
//  1. SQLQueries: DML queries are run as a single SQL query.
//  2. PreparedStatmentQueries: We execute a prepared statement as a SQL query, SET user defined variables and then Execute the DML.
func (fz *fuzzer) generateAndExecuteStatementQuery(t *testing.T, mcmp utils.MySQLCompare) (exit bool) {
	// Get a query and execute it.
	queries := fz.generateQuery()
	// We get a set of queries only when we are using prepared statements, which require running `SET` queries before running the actual DML query.
	for _, query := range queries {
		// When the concurrency is 1, then we run the query both on MySQL and Vitess.
		if fz.concurrency == 1 {
			_, _ = mcmp.ExecAllowAndCompareError(query)
			// If t is marked failed, we have encountered our first failure.
			// Let's collect the required information and finish execution.
			if t.Failed() {
				fz.firstFailureInfo = &debugInfo{
					queryToFail: queries,
					mysqlState:  collectFkTablesState(mcmp.MySQLConn),
					vitessState: collectFkTablesState(mcmp.VtConn),
				}
				return true
			}
		} else {
			// When we are running concurrent threads, then we run all the queries on Vitess.
			_, _ = utils.ExecAllowError(t, mcmp.VtConn, query)
		}
	}
	return false
}

// generateAndExecutePreparedPacketQuery generates a query and runs it on Vitess (and possibly MySQL).
// This function handles the query format PreparedStatementPacket. Here we send the prepared statement as a COM_STMT_PREPARE packet.
// Following which we execute it. To this end, we use the go-sql-driver.
func (fz *fuzzer) generateAndExecutePreparedPacketQuery(t *testing.T, mysqlDB *sql.DB, vitessDb *sql.DB, mcmp utils.MySQLCompare) bool {
	query, params := fz.generateParameterizedQuery()
	// When the concurrency is 1, then we run the query both on MySQL and Vitess.
	if fz.concurrency == 1 {
		// When the concurrency is 1, then we run the query both on MySQL and Vitess.
		fz.execAndCompareMySQlAndVitess(t, mysqlDB, vitessDb, query, params)
		// If t is marked failed, we have encountered our first failure.
		// Let's collect the required information and finish execution.
		if t.Failed() {
			fz.firstFailureInfo = &debugInfo{
				queryToFail: []string{query},
				mysqlState:  collectFkTablesState(mcmp.MySQLConn),
				vitessState: collectFkTablesState(mcmp.VtConn),
			}
			return true
		}
	} else {
		// When we are running concurrent threads, then we run all the queries on Vitess.
		_, _ = vitessDb.Exec(query, params...)
	}
	return false
}

// execAndCompareMySQlAndVitess executes the given query with the parameters on MySQL and Vitess and compares their results.
func (fz *fuzzer) execAndCompareMySQlAndVitess(t *testing.T, mysqlDB *sql.DB, vitessDb *sql.DB, query string, params []any) {
	mysqlRes, mysqlErr := mysqlDB.Exec(query, params...)
	vtRes, vtErr := vitessDb.Exec(query, params...)
	compareVitessAndMySQLErrors(t, vtErr, mysqlErr)
	compareVitessAndMySQLResults(t, vtRes, mysqlRes)
}

// stop stops the fuzzer and waits for it to finish execution.
func (fz *fuzzer) stop() {
	// Mark the thread to be stopped.
	fz.shouldStop.Store(true)
	// Wait for the fuzzer thread to stop.
	fz.wg.Wait()
}

// getPreparedDeleteQueries gets the list of queries to run for executing an DELETE using prepared statements.
func (fz *fuzzer) getPreparedDeleteQueries() []string {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	return []string{
		fmt.Sprintf("prepare stmt_del from 'delete from %v where id = ?'", fkTables[tableId]),
		fmt.Sprintf("SET @id = %v", idValue),
		"execute stmt_del using @id",
	}
}

// getPreparedInsertQueries gets the list of queries to run for executing an INSERT using prepared statements.
func (fz *fuzzer) getPreparedInsertQueries() []string {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.Intn(1 + fz.maxValForCol)
		col2Value := rand.Intn(1 + fz.maxValForCol)
		return []string{
			"prepare stmt_insert from 'insert into fk_t20 (id, col, col2) values (?, ?, ?)'",
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", convertColValueToString(colValue)),
			fmt.Sprintf("SET @col2 = %v", convertColValueToString(col2Value)),
			"execute stmt_insert using @id, @col, @col2",
		}
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.Intn(1 + fz.maxValForCol)
		colbValue := rand.Intn(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_insert from 'insert into %v (id, cola, colb) values (?, ?, ?)'", tableName),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @cola = %v", convertColValueToString(colaValue)),
			fmt.Sprintf("SET @colb = %v", convertColValueToString(colbValue)),
			"execute stmt_insert using @id, @cola, @colb",
		}
	} else {
		colValue := rand.Intn(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_insert from 'insert into %v (id, col) values (?, ?)'", tableName),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", convertColValueToString(colValue)),
			"execute stmt_insert using @id, @col",
		}
	}
}

// getPreparedUpdateQueries gets the list of queries to run for executing an UPDATE using prepared statements.
func (fz *fuzzer) getPreparedUpdateQueries() []string {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.Intn(1 + fz.maxValForCol)
		col2Value := rand.Intn(1 + fz.maxValForCol)
		return []string{
			"prepare stmt_update from 'update fk_t20 set col = ?, col2 = ? where id = ?'",
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", convertColValueToString(colValue)),
			fmt.Sprintf("SET @col2 = %v", convertColValueToString(col2Value)),
			"execute stmt_update using @col, @col2, @id",
		}
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.Intn(1 + fz.maxValForCol)
		colbValue := rand.Intn(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_update from 'update %v set cola = ?, colb = ? where id = ?'", tableName),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @cola = %v", convertColValueToString(colaValue)),
			fmt.Sprintf("SET @colb = %v", convertColValueToString(colbValue)),
			"execute stmt_update using @cola, @colb, @id",
		}
	} else {
		colValue := rand.Intn(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_update from 'update %v set col = ? where id = ?'", tableName),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", convertColValueToString(colValue)),
			"execute stmt_update using @col, @id",
		}
	}
}

// generateParameterizedQuery generates a parameterized query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedQuery() (query string, params []any) {
	val := rand.Intn(fz.insertShare + fz.updateShare + fz.deleteShare)
	if val < fz.insertShare {
		return fz.generateParameterizedInsertQuery()
	}
	if val < fz.insertShare+fz.updateShare {
		return fz.generateParameterizedUpdateQuery()
	}
	return fz.generateParameterizedDeleteQuery()
}

// generateParameterizedInsertQuery generates a parameterized INSERT query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedInsertQuery() (query string, params []any) {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.Intn(1 + fz.maxValForCol)
		col2Value := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("insert into %v (id, col, col2) values (?, ?, ?)", tableName), []any{idValue, convertColValueToString(colValue), convertColValueToString(col2Value)}
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.Intn(1 + fz.maxValForCol)
		colbValue := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("insert into %v (id, cola, colb) values (?, ?, ?)", tableName), []any{idValue, convertColValueToString(colaValue), convertColValueToString(colbValue)}
	} else {
		colValue := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("insert into %v (id, col) values (?, ?)", tableName), []any{idValue, convertColValueToString(colValue)}
	}
}

// generateParameterizedUpdateQuery generates a parameterized UPDATE query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedUpdateQuery() (query string, params []any) {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.Intn(1 + fz.maxValForCol)
		col2Value := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set col = ?, col2 = ? where id = ?", tableName), []any{convertColValueToString(colValue), convertColValueToString(col2Value), idValue}
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.Intn(1 + fz.maxValForCol)
		colbValue := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set cola = ?, colb = ? where id = ?", tableName), []any{convertColValueToString(colaValue), convertColValueToString(colbValue), idValue}
	} else {
		colValue := rand.Intn(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set col = ? where id = ?", tableName), []any{convertColValueToString(colValue), idValue}
	}
}

// generateParameterizedDeleteQuery generates a parameterized DELETE query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedDeleteQuery() (query string, params []any) {
	tableId := rand.Intn(len(fkTables))
	idValue := 1 + rand.Intn(fz.maxValForId)
	return fmt.Sprintf("delete from %v where id = ?", fkTables[tableId]), []any{idValue}
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
			for _, queryFormat := range []QueryFormat{SQLQueries, PreparedStatmentQueries, PreparedStatementPacket} {
				t.Run(getTestName(tt.name, testSharded)+fmt.Sprintf(" QueryFormat - %v", queryFormat), func(t *testing.T) {
					mcmp, closer := start(t)
					defer closer()
					// Set the correct keyspace to use from VtGates.
					if testSharded {
						t.Skip("Skip test since we don't have sharded foreign key support yet")
						_ = utils.Exec(t, mcmp.VtConn, "use `ks`")
					} else {
						_ = utils.Exec(t, mcmp.VtConn, "use `uks`")
					}
					// Ensure that the Vitess database is originally empty
					ensureDatabaseState(t, mcmp.VtConn, true)
					ensureDatabaseState(t, mcmp.MySQLConn, true)

					// Create the fuzzer.
					fz := newFuzzer(tt.concurrency, tt.maxValForId, tt.maxValForCol, tt.insertShare, tt.deleteShare, tt.updateShare, queryFormat)

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

					// ensure Vitess database has some data. This ensures not all the commands failed.
					ensureDatabaseState(t, mcmp.VtConn, false)
					// Verify the consistency of the data.
					verifyDataIsCorrect(t, mcmp, tt.concurrency)
				})
			}
		}
	}
}

// ensureDatabaseState ensures that the database is either empty or not.
func ensureDatabaseState(t *testing.T, vtconn *mysql.Conn, empty bool) {
	results := collectFkTablesState(vtconn)
	isEmpty := true
	for _, res := range results {
		if len(res.Rows) > 0 {
			isEmpty = false
		}
	}
	require.Equal(t, isEmpty, empty)
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
			if isMultiColFkTable(reference.childTable) {
				query = fmt.Sprintf("select %v.id from %v left join %v on (%v.cola = %v.cola and %v.colb = %v.colb) where %v.cola is null and %v.cola is not null and %v.colb is not null", reference.childTable, reference.childTable, reference.parentTable, reference.parentTable, reference.childTable, reference.parentTable, reference.childTable, reference.parentTable, reference.childTable, reference.childTable)
			}
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
			checkReplicationHealthy(t, replicaTab)
			cluster.WaitForReplicationPos(t, primaryTab, replicaTab, true, 60.0)
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
		require.True(t, resultOne.Equal(resultTwo), "Data for %v doesn't match\nRows 1\n%v\nRows 2\n%v", fkTables[idx], resultOne.Rows, resultTwo.Rows)
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
