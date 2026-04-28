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
	insertTables []string
	updateTables []string
	deleteTables []string
	allowReplace bool
	// The FK fuzz schema includes unique-key FK relationships that Vitess
	// rejects for ON DUPLICATE KEY UPDATE. Dedicated tests cover supported
	// ODKU cases.
	allowOnDup bool
	// shardScoped disables query forms that mutate a vindex column
	// (UPDATE, REPLACE, INSERT ... ON DUPLICATE KEY UPDATE). Required when
	// running against the shard-scoped keyspace whose vschema uses the FK
	// columns as vindexes.
	shardScoped bool

	// shouldStop is an internal state variable, that tells the fuzzer
	// whether it should stop or not.
	shouldStop atomic.Bool
	// wg is an internal state variable, that used to know whether the fuzzer threads are running or not.
	wg sync.WaitGroup
	// nextInsertID keeps shard-scoped inserts globally unique because those
	// keyspaces route by FK columns, not by the primary key.
	nextInsertID atomic.Int64
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

var shardScopedFuzzerExcludedTables = map[string]struct{}{
	// fk_t20 is self-referential from col2 to col, but shard_scoped_vschema
	// routes it by col2. The parent side is therefore not shard-scoped, so
	// generated shard-scoped DML can fail in Vitess while MySQL accepts it.
	"fk_t20": {},
	// These tables have unique secondary indexes that are not covered by the
	// shard-scoped primary vindex, so duplicates can be accepted on different
	// shards while the single MySQL comparison database rejects them.
	"fk_multicol_t1": {},
	"fk_multicol_t4": {},
}

var shardScopedFuzzerDeleteTables = []string{
	// Deletes from non-leaf shard-scoped FK tables can require cascading or
	// SET NULL changes to FK columns that are also primary vindex columns.
	"fk_t5", "fk_t6", "fk_t7", "fk_t12", "fk_t13", "fk_t18", "fk_t19",
	"fk_multicol_t5", "fk_multicol_t6", "fk_multicol_t7", "fk_multicol_t12", "fk_multicol_t13", "fk_multicol_t18", "fk_multicol_t19",
}

func fuzzerTables(shardScoped bool) (insertTables, updateTables, deleteTables []string) {
	if !shardScoped {
		return fkTables, fkTables, fkTables
	}

	tables := make([]string, 0, len(fkTables)-len(shardScopedFuzzerExcludedTables))
	for _, table := range fkTables {
		if _, excluded := shardScopedFuzzerExcludedTables[table]; excluded {
			continue
		}
		tables = append(tables, table)
	}
	return tables, nil, shardScopedFuzzerDeleteTables
}

// newFuzzer creates a new fuzzer struct.
func newFuzzer(concurrency int, maxValForId int, maxValForCol int, insertShare int, deleteShare int, updateShare int, queryFormat QueryFormat, fkState *bool, shardScoped bool) *fuzzer {
	insertTables, updateTables, deleteTables := fuzzerTables(shardScoped)
	fz := &fuzzer{
		concurrency:  concurrency,
		maxValForId:  maxValForId,
		maxValForCol: maxValForCol,
		insertShare:  insertShare,
		deleteShare:  deleteShare,
		updateShare:  updateShare,
		queryFormat:  queryFormat,
		fkState:      fkState,
		insertTables: insertTables,
		updateTables: updateTables,
		deleteTables: deleteTables,
		allowReplace: !shardScoped,
		allowOnDup:   false,
		noFkSetVar:   false,
		shardScoped:  shardScoped,
		wg:           sync.WaitGroup{},
	}
	// Initially the fuzzer thread is stopped.
	fz.shouldStop.Store(true)
	return fz
}

func queryTable(tables []string) string {
	return tables[rand.IntN(len(tables))]
}

func (fz *fuzzer) queryInsertTable() string {
	return queryTable(fz.insertTables)
}

func (fz *fuzzer) queryUpdateTable() string {
	return queryTable(fz.updateTables)
}

func (fz *fuzzer) queryDeleteTable() string {
	return queryTable(fz.deleteTables)
}

func (fz *fuzzer) randomColValue() string {
	value := rand.IntN(1 + fz.maxValForCol)
	if fz.shardScoped && value == 0 {
		value = 1
	}
	return convertIntValueToString(value)
}

func (fz *fuzzer) insertIDValue() int {
	if fz.shardScoped {
		return int(fz.nextInsertID.Add(1))
	}
	return fz.randomIDValue()
}

func (fz *fuzzer) randomIDValue() int {
	if fz.shardScoped {
		max := int(fz.nextInsertID.Load())
		if max > 0 {
			return 1 + rand.IntN(max)
		}
	}
	return 1 + rand.IntN(fz.maxValForId)
}

func TestFuzzerQueryTables(t *testing.T) {
	fz := newFuzzer(1, 1, 1, 1, 1, 1, SQLQueries, nil, false)
	require.Equal(t, fkTables, fz.insertTables)
	require.Equal(t, fkTables, fz.updateTables)
	require.Equal(t, fkTables, fz.deleteTables)
	require.Contains(t, fz.insertTables, "fk_t20")
	require.True(t, fz.allowReplace)
	require.False(t, fz.allowOnDup)

	fz = newFuzzer(1, 1, 1, 1, 1, 1, SQLQueries, nil, true)
	require.NotContains(t, fz.insertTables, "fk_t20")
	require.NotContains(t, fz.insertTables, "fk_multicol_t1")
	require.NotContains(t, fz.insertTables, "fk_multicol_t4")
	require.Len(t, fz.insertTables, len(fkTables)-len(shardScopedFuzzerExcludedTables))
	require.Empty(t, fz.updateTables)
	require.ElementsMatch(t, shardScopedFuzzerDeleteTables, fz.deleteTables)
	require.False(t, fz.allowReplace)
	require.False(t, fz.allowOnDup)
	require.Empty(t, fz.getInsertOnDuplicateClause("insert", "fk_t10"))
	require.Equal(t, 1, fz.insertIDValue())
	require.Equal(t, 2, fz.insertIDValue())
	for range 100 {
		require.NotEqual(t, "NULL", fz.randomColValue())
	}
}

// generateQuery generates a query from the parameters for the fuzzer.
// The returned set is a list of strings, because for prepared statements, we have to run
// set queries first and then the final query eventually.
func (fz *fuzzer) generateQuery() []string {
	updateShare := fz.updateShare
	if len(fz.updateTables) == 0 {
		// In shard-scoped keyspaces, the FK columns are vindexes and
		// UPDATE/REPLACE that would change them is unsupported, so skip
		// UPDATEs entirely here.
		updateShare = 0
	}
	deleteShare := fz.deleteShare
	if len(fz.deleteTables) == 0 {
		deleteShare = 0
	}
	val := rand.IntN(fz.insertShare + updateShare + deleteShare)
	if val < fz.insertShare {
		switch fz.queryFormat {
		case OlapSQLQueries, SQLQueries:
			return []string{fz.generateInsertDMLQuery(fz.getInsertType())}
		case PreparedStatmentQueries:
			return fz.getPreparedInsertQueries(fz.getInsertType())
		default:
			panic("Unknown query type")
		}
	}
	if val < fz.insertShare+updateShare {
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

// getInsertType picks INSERT vs REPLACE. REPLACE is disabled in shard-scoped
// keyspaces because the deletion half can be routed to a different shard than
// the insertion half if the FK column value is changed.
func (fz *fuzzer) getInsertType() string {
	if !fz.allowReplace {
		return "insert"
	}
	return []string{"insert", "replace"}[rand.IntN(2)]
}

// generateInsertDMLQuery generates an INSERT query from the parameters for the fuzzer.
func (fz *fuzzer) generateInsertDMLQuery(insertType string) string {
	idValue := fz.insertIDValue()
	tableName := fz.queryInsertTable()
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	onDup := fz.getInsertOnDuplicateClause(insertType, tableName)
	if tableName == "fk_t20" {
		colValue := fz.randomColValue()
		col2Value := fz.randomColValue()
		return fmt.Sprintf("%s %vinto %v (id, col, col2) values (%v, %v, %v)%s", insertType, setVarFkChecksVal, tableName, idValue, colValue, col2Value, onDup)
	} else if isMultiColFkTable(tableName) {
		colaValue := fz.randomColValue()
		colbValue := fz.randomColValue()
		return fmt.Sprintf("%s %vinto %v (id, cola, colb) values (%v, %v, %v)%s", insertType, setVarFkChecksVal, tableName, idValue, colaValue, colbValue, onDup)
	} else {
		colValue := fz.randomColValue()
		return fmt.Sprintf("%s %vinto %v (id, col) values (%v, %v)%s", insertType, setVarFkChecksVal, tableName, idValue, colValue, onDup)
	}
}

// getInsertOnDuplicateClause randomly returns an ON DUPLICATE KEY UPDATE
// clause with a leading space (or empty string) for the given table shape.
// Only applies to INSERT (not REPLACE) when allowOnDup is enabled.
func (fz *fuzzer) getInsertOnDuplicateClause(insertType, tableName string) string {
	if !fz.allowOnDup || insertType != "insert" || rand.IntN(2) == 0 {
		return ""
	}
	if tableName == "fk_t20" {
		colValue := fz.randomColValue()
		col2Value := fz.randomColValue()
		return fmt.Sprintf(" on duplicate key update col = %v, col2 = %v", colValue, col2Value)
	}
	if isMultiColFkTable(tableName) {
		colaValue := fz.randomColValue()
		colbValue := fz.randomColValue()
		return fmt.Sprintf(" on duplicate key update cola = %v, colb = %v", colaValue, colbValue)
	}
	colValue := fz.randomColValue()
	return fmt.Sprintf(" on duplicate key update col = %v", colValue)
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
	idValue := fz.randomIDValue()
	tableName := fz.queryUpdateTable()
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
	tableName := fz.queryUpdateTable()
	tableName2 := fz.queryUpdateTable()
	idValue := fz.randomIDValue()
	colValue := convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
	col2Value := convertIntValueToString(rand.IntN(1 + fz.maxValForCol))
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	setExprs := fmt.Sprintf("%v.col = %v", tableName, colValue)
	if rand.IntN(2)%2 == 0 {
		setExprs += ", " + fmt.Sprintf("%v.col = %v", tableName2, col2Value)
	}
	query := fmt.Sprintf("update %v%v join %v using (id) set %s where %v.id = %v", setVarFkChecksVal, tableName, tableName2, setExprs, tableName, idValue)
	return query
}

// generateDeleteDMLQuery generates a DELETE query using 1 table from the parameters for the fuzzer.
func (fz *fuzzer) generateSingleDeleteDMLQuery() string {
	tableName := fz.queryDeleteTable()
	idValue := fz.randomIDValue()
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	delWithLimit := rand.IntN(2)
	if delWithLimit == 0 {
		return fmt.Sprintf("delete %vfrom %v where id = %v", setVarFkChecksVal, tableName, idValue)
	}
	limitCount := rand.IntN(3)
	return fmt.Sprintf("delete %vfrom %v order by id limit %v", setVarFkChecksVal, tableName, limitCount)
}

// generateMultiDeleteDMLQuery generates a DELETE query using 2 tables from the parameters for the fuzzer.
func (fz *fuzzer) generateMultiDeleteDMLQuery() string {
	tableName := fz.queryDeleteTable()
	tableName2 := fz.queryDeleteTable()
	idValue := fz.randomIDValue()
	setVarFkChecksVal := fz.getSetVarFkChecksVal()
	target := tableName
	if rand.IntN(2)%2 == 0 {
		target += ", " + tableName2
	}
	query := fmt.Sprintf("delete %v%v from %v join %v using (id) where %v.id = %v", setVarFkChecksVal, target, tableName, tableName2, tableName, idValue)
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
			_, _ = mcmp.ExecAllowAndCompareError(query, utils.CompareOptions{IgnoreRowsAffected: true})
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
	tableName := fz.queryDeleteTable()
	idValue := fz.randomIDValue()
	return []string{
		fmt.Sprintf("prepare stmt_del from 'delete from %v where id = ?'", tableName),
		fmt.Sprintf("SET @id = %v", idValue),
		"execute stmt_del using @id",
	}
}

// getPreparedInsertQueries gets the list of queries to run for executing an INSERT using prepared statements.
func (fz *fuzzer) getPreparedInsertQueries(insertType string) []string {
	idValue := fz.insertIDValue()
	tableName := fz.queryInsertTable()
	// Decide up-front whether this INSERT should carry an ON DUPLICATE KEY
	// UPDATE clause. REPLACE does not support ON DUPLICATE.
	useOnDup := fz.allowOnDup && insertType == "insert" && rand.IntN(2) == 1
	if tableName == "fk_t20" {
		colValue := fz.randomColValue()
		col2Value := fz.randomColValue()
		queries := []string{
			fmt.Sprintf("prepare stmt_insert from '%s into fk_t20 (id, col, col2) values (?, ?, ?)%s'", insertType, onDupPreparedSuffix(useOnDup, "col = ?, col2 = ?")),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", colValue),
			fmt.Sprintf("SET @col2 = %v", col2Value),
		}
		execArgs := "@id, @col, @col2"
		if useOnDup {
			newCol := fz.randomColValue()
			newCol2 := fz.randomColValue()
			queries = append(queries,
				fmt.Sprintf("SET @new_col = %v", newCol),
				fmt.Sprintf("SET @new_col2 = %v", newCol2),
			)
			execArgs += ", @new_col, @new_col2"
		}
		return append(queries, "execute stmt_insert using "+execArgs)
	} else if isMultiColFkTable(tableName) {
		colaValue := fz.randomColValue()
		colbValue := fz.randomColValue()
		queries := []string{
			fmt.Sprintf("prepare stmt_insert from '%s into %v (id, cola, colb) values (?, ?, ?)%s'", insertType, tableName, onDupPreparedSuffix(useOnDup, "cola = ?, colb = ?")),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @cola = %v", colaValue),
			fmt.Sprintf("SET @colb = %v", colbValue),
		}
		execArgs := "@id, @cola, @colb"
		if useOnDup {
			newCola := fz.randomColValue()
			newColb := fz.randomColValue()
			queries = append(queries,
				fmt.Sprintf("SET @new_cola = %v", newCola),
				fmt.Sprintf("SET @new_colb = %v", newColb),
			)
			execArgs += ", @new_cola, @new_colb"
		}
		return append(queries, "execute stmt_insert using "+execArgs)
	} else {
		colValue := fz.randomColValue()
		queries := []string{
			fmt.Sprintf("prepare stmt_insert from '%s into %v (id, col) values (?, ?)%s'", insertType, tableName, onDupPreparedSuffix(useOnDup, "col = ?")),
			fmt.Sprintf("SET @id = %v", idValue),
			fmt.Sprintf("SET @col = %v", colValue),
		}
		execArgs := "@id, @col"
		if useOnDup {
			newCol := fz.randomColValue()
			queries = append(queries, fmt.Sprintf("SET @new_col = %v", newCol))
			execArgs += ", @new_col"
		}
		return append(queries, "execute stmt_insert using "+execArgs)
	}
}

// onDupPreparedSuffix returns the ON DUPLICATE KEY UPDATE clause (with a
// leading space) to embed inside a `prepare stmt from '...'` string, or
// empty string when not applicable.
func onDupPreparedSuffix(useOnDup bool, assignments string) string {
	if !useOnDup {
		return ""
	}
	return " on duplicate key update " + assignments
}

// getPreparedUpdateQueries gets the list of queries to run for executing an UPDATE using prepared statements.
func (fz *fuzzer) getPreparedUpdateQueries() []string {
	idValue := fz.randomIDValue()
	tableName := fz.queryUpdateTable()
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
	updateShare := fz.updateShare
	if len(fz.updateTables) == 0 {
		updateShare = 0
	}
	deleteShare := fz.deleteShare
	if len(fz.deleteTables) == 0 {
		deleteShare = 0
	}
	val := rand.IntN(fz.insertShare + updateShare + deleteShare)
	if val < fz.insertShare {
		return fz.generateParameterizedInsertQuery(fz.getInsertType())
	}
	if val < fz.insertShare+updateShare {
		return fz.generateParameterizedUpdateQuery()
	}
	return fz.generateParameterizedDeleteQuery()
}

// generateParameterizedInsertQuery generates a parameterized INSERT query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedInsertQuery(insertType string) (query string, params []any) {
	idValue := fz.insertIDValue()
	tableName := fz.queryInsertTable()
	useOnDup := fz.allowOnDup && insertType == "insert" && rand.IntN(2) == 1
	if tableName == "fk_t20" {
		colValue := fz.randomColValue()
		col2Value := fz.randomColValue()
		query = fmt.Sprintf("%s into %v (id, col, col2) values (?, ?, ?)", insertType, tableName)
		params = []any{idValue, colValue, col2Value}
		if useOnDup {
			query += " on duplicate key update col = ?, col2 = ?"
			params = append(params,
				fz.randomColValue(),
				fz.randomColValue(),
			)
		}
		return query, params
	} else if isMultiColFkTable(tableName) {
		colaValue := fz.randomColValue()
		colbValue := fz.randomColValue()
		query = fmt.Sprintf("%s into %v (id, cola, colb) values (?, ?, ?)", insertType, tableName)
		params = []any{idValue, colaValue, colbValue}
		if useOnDup {
			query += " on duplicate key update cola = ?, colb = ?"
			params = append(params,
				fz.randomColValue(),
				fz.randomColValue(),
			)
		}
		return query, params
	} else {
		colValue := fz.randomColValue()
		query = fmt.Sprintf("%s into %v (id, col) values (?, ?)", insertType, tableName)
		params = []any{idValue, colValue}
		if useOnDup {
			query += " on duplicate key update col = ?"
			params = append(params, fz.randomColValue())
		}
		return query, params
	}
}

// generateParameterizedUpdateQuery generates a parameterized UPDATE query for the query format PreparedStatementPacket.
func (fz *fuzzer) generateParameterizedUpdateQuery() (query string, params []any) {
	idValue := fz.randomIDValue()
	tableName := fz.queryUpdateTable()
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
	tableName := fz.queryDeleteTable()
	idValue := fz.randomIDValue()
	return fmt.Sprintf("delete from %v where id = ?", tableName), []any{idValue}
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
	for _, ks := range []string{shardedKs, unshardedKs, shardScopedKs} {
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
			name:           "Multi Thread - Only Inserts",
			concurrency:    30,
			timeForTesting: 5 * time.Second,
			maxValForCol:   5,
			maxValForId:    30,
			insertShare:    100,
			deleteShare:    0,
			updateShare:    0,
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

	valTrue := true
	valFalse := false
	for _, fkState := range []*bool{nil, &valTrue, &valFalse} {
		for _, tt := range testcases {
			for _, keyspace := range []string{unshardedKs, shardedKs, shardScopedKs} {
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
						fz := newFuzzer(tt.concurrency, tt.maxValForId, tt.maxValForCol, tt.insertShare, tt.deleteShare, tt.updateShare, queryFormat, fkState, keyspace == shardScopedKs)

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
							log.Error(fmt.Sprintf("Failing query - %v", fz.firstFailureInfo.queryToFail))
							for idx, table := range fkTables {
								log.Error(fmt.Sprintf("MySQL data for %v -\n%v", table, fz.firstFailureInfo.mysqlState[idx].Rows))
								log.Error(fmt.Sprintf("Vitess data for %v -\n%v", table, fz.firstFailureInfo.vitessState[idx].Rows))
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
	for b.Loop() {
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
