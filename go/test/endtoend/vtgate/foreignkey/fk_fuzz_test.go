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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

type QueryFormat string

const (
	SQLQueries              QueryFormat = "SQLQueries"
	OlapSQLQueries          QueryFormat = "OlapSQLQueries"
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
	noFkSetVar   bool
	fkState      *bool

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
func newFuzzer(concurrency int, maxValForId int, maxValForCol int, insertShare int, deleteShare int, updateShare int, queryFormat QueryFormat, fkState *bool) *fuzzer {
	fz := &fuzzer{
		concurrency:  concurrency,
		maxValForId:  maxValForId,
		maxValForCol: maxValForCol,
		insertShare:  insertShare,
		deleteShare:  deleteShare,
		updateShare:  updateShare,
		queryFormat:  queryFormat,
		fkState:      fkState,
		noFkSetVar:   false,
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
	val := rand.IntN(fz.insertShare + fz.updateShare + fz.deleteShare)
	if val < fz.insertShare {
		switch fz.queryFormat {
		case OlapSQLQueries, SQLQueries:
			return []string{fz.generateInsertDMLQuery(getInsertType())}
		case PreparedStatmentQueries:
			return fz.getPreparedInsertQueries(getInsertType())
		default:
			panic("Unknown query type")
		}
	}
	if val < fz.insertShare+fz.updateShare {
		switch fz.queryFormat {
		case OlapSQLQueries, SQLQueries:
			return []string{fz.generateUpdateDMLQuery()}
		case PreparedStatmentQueries:
			return fz.getPreparedUpdateQueries()
		default:
			panic("Unknown query type")
		}
	}
	switch fz.queryFormat {
	case OlapSQLQueries, SQLQueries:
		return []string{fz.generateDeleteDMLQuery()}
	case PreparedStatmentQueries:
		return fz.getPreparedDeleteQueries()
	default:
		panic("Unknown query type")
	}
}

func getInsertType() string {
	return []string{"insert", "replace"}[rand.IntN(2)]
}

// generateInsertDMLQuery generates an INSERT query from the parameters for the fuzzer.
func (fz *fuzzer) generateInsertDMLQuery(insertType string) string {
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	tableName := fkTables[tableId]
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	if tableName == "fk_t20" {
		colValue := rand.IntN(1 + fz.maxValForCol)
		col2Value := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("%s %vinto %v (id, col, col2) values (%v, %v, %v)", insertType, setVarFkChecksVal, tableName, idValue, convertIntValueToString(colValue), convertIntValueToString(col2Value))
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.IntN(1 + fz.maxValForCol)
		colbValue := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("%s %vinto %v (id, cola, colb) values (%v, %v, %v)", insertType, setVarFkChecksVal, tableName, idValue, convertIntValueToString(colaValue), convertIntValueToString(colbValue))
	} else {
		colValue := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("%s %vinto %v (id, col) values (%v, %v)", insertType, setVarFkChecksVal, tableName, idValue, convertIntValueToString(colValue))
	}
}

// generateUpdateDMLQuery generates a UPDATE query from the parameters for the fuzzer.
func (fz *fuzzer) generateUpdateDMLQuery() string {
	multiTableUpdate := rand.IntN(2) + 1
	if multiTableUpdate == 1 {
		return fz.generateSingleUpdateDMLQuery()
	}
	return fz.generateMultiUpdateDMLQuery()
}

// generateSingleUpdateDMLQuery generates an UPDATE query from the parameters for the fuzzer.
func (fz *fuzzer) generateSingleUpdateDMLQuery() string {
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	tableName := fkTables[tableId]
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	updWithLimit := rand.IntN(2)
	limitCount := rand.IntN(3)
	if tableName == "fk_t20" {
		colValue := convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
		col2Value := convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
		if updWithLimit == 0 {
			return fmt.Sprintf("update %v%v set col = %v, col2 = %v where id = %v", setVarFkChecksVal, tableName, colValue, col2Value, idValue)
		}
		return fmt.Sprintf("update %v%v set col = %v, col2 = %v order by id limit %v", setVarFkChecksVal, tableName, colValue, col2Value, limitCount)
	} else if isMultiColFkTable(tableName) {
		if rand.IntN(2) == 0 {
			colaValue := convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
			colbValue := convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
			if fz.concurrency > 1 {
				colaValue = fz.generateExpression(rand.IntN(4)+1, "cola", "colb", "id")
				colbValue = fz.generateExpression(rand.IntN(4)+1, "cola", "colb", "id")
			}
			if updWithLimit == 0 {
				return fmt.Sprintf("update %v%v set cola = %v, colb = %v where id = %v", setVarFkChecksVal, tableName, colaValue, colbValue, idValue)
			}
			return fmt.Sprintf("update %v%v set cola = %v, colb = %v order by id limit %v", setVarFkChecksVal, tableName, colaValue, colbValue, limitCount)
		} else {
			colValue := fz.generateExpression(rand.IntN(4)+1, "cola", "colb", "id")
			colToUpdate := []string{"cola", "colb"}[rand.IntN(2)]
			if updWithLimit == 0 {
				return fmt.Sprintf("update %v set %v = %v where id = %v", tableName, colToUpdate, colValue, idValue)
			}
			return fmt.Sprintf("update %v set %v = %v order by id limit %v", tableName, colToUpdate, colValue, limitCount)
		}
	} else {
		colValue := fz.generateExpression(rand.IntN(4)+1, "col", "id")
		if updWithLimit == 0 {
			return fmt.Sprintf("update %v%v set col = %v where id = %v", setVarFkChecksVal, tableName, colValue, idValue)
		}
		return fmt.Sprintf("update %v%v set col = %v order by id limit %v", setVarFkChecksVal, tableName, colValue, limitCount)
	}
}

// generateMultiUpdateDMLQuery generates a UPDATE query using 2 tables from the parameters for the fuzzer.
func (fz *fuzzer) generateMultiUpdateDMLQuery() string {
	tableId := rand.IntN(len(fkTables))
	tableId2 := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	colValue := convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
	col2Value := convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	setExprs := fmt.Sprintf("%v.col = %v", fkTables[tableId], colValue)
	if rand.IntN(2)%2 == 0 {
		setExprs += ", " + fmt.Sprintf("%v.col = %v", fkTables[tableId2], col2Value)
	}
	query := fmt.Sprintf("update %v%v join %v using (id) set %s where %v.id = %v", setVarFkChecksVal, fkTables[tableId], fkTables[tableId2], setExprs, fkTables[tableId], idValue)
	return query
}

// generateDeleteDMLQuery generates a DELETE query using 1 table from the parameters for the fuzzer.
func (fz *fuzzer) generateSingleDeleteDMLQuery() string {
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	delWithLimit := rand.IntN(2)
	if delWithLimit == 0 {
		return fmt.Sprintf("delete %vfrom %v where id = %v", setVarFkChecksVal, fkTables[tableId], idValue)
	}
	limitCount := rand.IntN(3)
	return fmt.Sprintf("delete %vfrom %v order by id limit %v", setVarFkChecksVal, fkTables[tableId], limitCount)
}

// generateMultiDeleteDMLQuery generates a DELETE query using 2 tables from the parameters for the fuzzer.
func (fz *fuzzer) generateMultiDeleteDMLQuery() string {
	tableId := rand.IntN(len(fkTables))
	tableId2 := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	target := fkTables[tableId]
	if rand.IntN(2)%2 == 0 {
		target += ", " + fkTables[tableId2]
	}
	query := fmt.Sprintf("delete %v%v from %v join %v using (id) where %v.id = %v", setVarFkChecksVal, target, fkTables[tableId], fkTables[tableId2], fkTables[tableId], idValue)
	return query
}

// generateDeleteDMLQuery generates a DELETE query from the parameters for the fuzzer.
func (fz *fuzzer) generateDeleteDMLQuery() string {
	multiTableDelete := rand.IntN(2) + 1
	if multiTableDelete == 1 {
		return fz.generateSingleDeleteDMLQuery()
	}
	return fz.generateMultiDeleteDMLQuery()
}

// start starts running the fuzzer.
func (fz *fuzzer) start(t *testing.T, keyspace string) {
	// We mark the fuzzer thread to be running now.
	fz.shouldStop.Store(false)
	fz.wg.Add(fz.concurrency)
	for i := 0; i < fz.concurrency; i++ {
		fuzzerThreadId := i
		go func() {
			fz.runFuzzerThread(t, keyspace, fuzzerThreadId)
		}()
	}
}

// runFuzzerThread is used to run a thread of the fuzzer.
func (fz *fuzzer) runFuzzerThread(t *testing.T, keyspace string, fuzzerThreadId int) {
	// Whenever we finish running this thread, we should mark the thread has stopped.
	defer func() {
		fz.wg.Done()
	}()
	// Create a MySQL Compare that connects to both Vitess and MySQL and runs the queries against both.
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)
	if fz.fkState != nil {
		mcmp.Exec(fmt.Sprintf("SET FOREIGN_KEY_CHECKS=%v", sqlparser.FkChecksStateString(fz.fkState)))
	}
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
	_ = utils.Exec(t, mcmp.VtConn, fmt.Sprintf("use `%v`", keyspace))
	if vitessDb != nil {
		_, _ = vitessDb.Exec(fmt.Sprintf("use `%v`", keyspace))
	}
	if fz.queryFormat == OlapSQLQueries {
		_ = utils.Exec(t, mcmp.VtConn, "set workload = olap")
	}
	for {
		// If fuzzer thread is marked to be stopped, then we should exit this go routine.
		if fz.shouldStop.Load() == true {
			return
		}
		switch fz.queryFormat {
		case OlapSQLQueries, SQLQueries, PreparedStatmentQueries:
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
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	return []string{
		fmt.Sprintf("prepare stmt_del from 'delete from %v where id = ?'", fkTables[tableId]),
		fmt.Sprintf("SET @id = %v", idValue),
		"execute stmt_del using @id",
	}
}

// getPreparedInsertQueries gets the list of queries to run for executing an INSERT using prepared statements.
func (fz *fuzzer) getPreparedInsertQueries(insertType string) []string {
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.IntN(1 + fz.maxValForCol)
		col2Value := rand.IntN(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_insert from '%s into fk_t20 (id, col, col2) values (?, ?, ?)'", insertType),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", convertIntValueToString(colValue)),
			fmt.Sprintf("SET @col2 = %v", convertIntValueToString(col2Value)),
			"execute stmt_insert using @id, @col, @col2",
		}
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.IntN(1 + fz.maxValForCol)
		colbValue := rand.IntN(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_insert from '%s into %v (id, cola, colb) values (?, ?, ?)'", insertType, tableName),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @cola = %v", convertIntValueToString(colaValue)),
			fmt.Sprintf("SET @colb = %v", convertIntValueToString(colbValue)),
			"execute stmt_insert using @id, @cola, @colb",
		}
	} else {
		colValue := rand.IntN(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_insert from '%s into %v (id, col) values (?, ?)'", insertType, tableName),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", convertIntValueToString(colValue)),
			"execute stmt_insert using @id, @col",
		}
	}
}

// getPreparedUpdateQueries gets the list of queries to run for executing an UPDATE using prepared statements.
func (fz *fuzzer) getPreparedUpdateQueries() []string {
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.IntN(1 + fz.maxValForCol)
		col2Value := rand.IntN(1 + fz.maxValForCol)
		return []string{
			"prepare stmt_update from 'update fk_t20 set col = ?, col2 = ? where id = ?'",
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", convertIntValueToString(colValue)),
			fmt.Sprintf("SET @col2 = %v", convertIntValueToString(col2Value)),
			"execute stmt_update using @col, @col2, @id",
		}
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.IntN(1 + fz.maxValForCol)
		colbValue := rand.IntN(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_update from 'update %v set cola = ?, colb = ? where id = ?'", tableName),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @cola = %v", convertIntValueToString(colaValue)),
			fmt.Sprintf("SET @colb = %v", convertIntValueToString(colbValue)),
			"execute stmt_update using @cola, @colb, @id",
		}
	} else {
		colValue := rand.IntN(1 + fz.maxValForCol)
		return []string{
			fmt.Sprintf("prepare stmt_update from 'update %v set col = ? where id = ?'", tableName),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", convertIntValueToString(colValue)),
			"execute stmt_update using @col, @id",
		}
	}
}

// generateParameterizedQuery generates a parameterized query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedQuery() (query string, params []any) {
	val := rand.IntN(fz.insertShare + fz.updateShare + fz.deleteShare)
	if val < fz.insertShare {
		return fz.generateParameterizedInsertQuery(getInsertType())
	}
	if val < fz.insertShare+fz.updateShare {
		return fz.generateParameterizedUpdateQuery()
	}
	return fz.generateParameterizedDeleteQuery()
}

// generateParameterizedInsertQuery generates a parameterized INSERT query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedInsertQuery(insertType string) (query string, params []any) {
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.IntN(1 + fz.maxValForCol)
		col2Value := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("%s into %v (id, col, col2) values (?, ?, ?)", insertType, tableName), []any{idValue, convertIntValueToString(colValue), convertIntValueToString(col2Value)}
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.IntN(1 + fz.maxValForCol)
		colbValue := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("%s into %v (id, cola, colb) values (?, ?, ?)", insertType, tableName), []any{idValue, convertIntValueToString(colaValue), convertIntValueToString(colbValue)}
	} else {
		colValue := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("%s into %v (id, col) values (?, ?)", insertType, tableName), []any{idValue, convertIntValueToString(colValue)}
	}
}

// generateParameterizedUpdateQuery generates a parameterized UPDATE query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedUpdateQuery() (query string, params []any) {
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	tableName := fkTables[tableId]
	if tableName == "fk_t20" {
		colValue := rand.IntN(1 + fz.maxValForCol)
		col2Value := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set col = ?, col2 = ? where id = ?", tableName), []any{convertIntValueToString(colValue), convertIntValueToString(col2Value), idValue}
	} else if isMultiColFkTable(tableName) {
		colaValue := rand.IntN(1 + fz.maxValForCol)
		colbValue := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set cola = ?, colb = ? where id = ?", tableName), []any{convertIntValueToString(colaValue), convertIntValueToString(colbValue), idValue}
	} else {
		colValue := rand.IntN(1 + fz.maxValForCol)
		return fmt.Sprintf("update %v set col = ? where id = ?", tableName), []any{convertIntValueToString(colValue), idValue}
	}
}

// generateParameterizedDeleteQuery generates a parameterized DELETE query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedDeleteQuery() (query string, params []any) {
	tableId := rand.IntN(len(fkTables))
	idValue := 1 + rand.IntN(fz.maxValForId)
	return fmt.Sprintf("delete from %v where id = ?", fkTables[tableId]), []any{idValue}
}

// getSetVarFkChecksVal generates an optimizer hint to randomly set the foreign key checks to on or off or leave them unaltered.
func (fz *fuzzer) getSetVarFkChecksVal() string {
	if fz.concurrency != 1 || fz.noFkSetVar {
		return ""
	}
	val := rand.IntN(3)
	if val == 0 {
		return ""
	}
	if val == 1 {
		return "/*+ SET_VAR(foreign_key_checks=On) */ "
	}
	return "/*+ SET_VAR(foreign_key_checks=Off) */ "
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
		}, {
			name:           "Single Thread - Balanced Inserts and Deletes",
			concurrency:    1,
			timeForTesting: 5 * time.Second,
			maxValForCol:   5,
			maxValForId:    10,
			insertShare:    50,
			deleteShare:    50,
			updateShare:    0,
		}, {
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
			name:           "Multi Thread - Only Inserts",
			concurrency:    30,
			timeForTesting: 5 * time.Second,
			maxValForCol:   5,
			maxValForId:    30,
			insertShare:    100,
			deleteShare:    0,
			updateShare:    0,
		}, {
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

	valTrue := true
	valFalse := false
	for _, fkState := range []*bool{nil, &valTrue, &valFalse} {
		for _, tt := range testcases {
			for _, keyspace := range []string{unshardedKs, shardedKs} {
				for _, queryFormat := range []QueryFormat{OlapSQLQueries, SQLQueries, PreparedStatmentQueries, PreparedStatementPacket} {
					if fkState != nil && (queryFormat != SQLQueries || tt.concurrency != 1) {
						continue
					}
					t.Run(getTestName(tt.name, keyspace)+fmt.Sprintf(" FkState - %v QueryFormat - %v", sqlparser.FkChecksStateString(fkState), queryFormat), func(t *testing.T) {
						mcmp, closer := start(t)
						defer closer()
						if keyspace == shardedKs {
							t.Skip("Skip test since we don't have sharded foreign key support yet")
						}
						// Set the correct keyspace to use from VtGates.
						_ = utils.Exec(t, mcmp.VtConn, fmt.Sprintf("use `%v`", keyspace))

						// Ensure that the Vitess database is originally empty
						ensureDatabaseState(t, mcmp.VtConn, true)
						ensureDatabaseState(t, mcmp.MySQLConn, true)

						// Create the fuzzer.
						fz := newFuzzer(tt.concurrency, tt.maxValForId, tt.maxValForCol, tt.insertShare, tt.deleteShare, tt.updateShare, queryFormat, fkState)

						// Start the fuzzer.
						fz.start(t, keyspace)

						// Wait for the timeForTesting so that the threads continue to run.
						totalTime := time.After(tt.timeForTesting)
						done := false
						for !done {
							select {
							case <-totalTime:
								done = true
							case <-time.After(10 * time.Millisecond):
								validateReplication(t)
							}
						}

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
}

// BenchmarkFkFuzz benchmarks the performance of Vitess unmanaged, Vitess managed and vanilla MySQL performance on a given set of queries generated by the fuzzer.
func BenchmarkFkFuzz(b *testing.B) {
	maxValForCol := 10
	maxValForId := 10
	insertShare := 50
	deleteShare := 50
	updateShare := 50
	numQueries := 1000
	// Wait for schema-tracking to be complete.
	waitForSchemaTrackingForFkTables(b)
	for i := 0; i < b.N; i++ {
		queries, mysqlConn, vtConn, vtUnmanagedConn := setupBenchmark(b, maxValForId, maxValForCol, insertShare, deleteShare, updateShare, numQueries)

		// Now we run the benchmarks!
		b.Run("MySQL", func(b *testing.B) {
			startBenchmark(b)
			runQueries(b, mysqlConn, queries)
		})

		b.Run("Vitess Managed Foreign Keys", func(b *testing.B) {
			startBenchmark(b)
			runQueries(b, vtConn, queries)
		})

		b.Run("Vitess Unmanaged Foreign Keys", func(b *testing.B) {
			startBenchmark(b)
			runQueries(b, vtUnmanagedConn, queries)
		})
	}
}
