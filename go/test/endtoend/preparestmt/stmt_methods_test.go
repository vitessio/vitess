/*
Copyright 2019 The Vitess Authors.

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

package preparestmt

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtgate/engine"
)

// TestDMLNone tests that impossible query run without an error.
func TestDMLNone(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()

	t.Run("delete none", func(t *testing.T) {
		dmlquery(t, dbo, "delete from sks.t1 where 1 = 0")
	})
	t.Run("update none", func(t *testing.T) {
		dmlquery(t, dbo, "update sks.t1 set age = 5 where 1 = 0")
	})
}

func dmlquery(t *testing.T, dbo *sql.DB, query string) {
	stmt, err := dbo.Prepare(query)
	require.NoError(t, err)
	defer stmt.Close()

	qr, err := stmt.Exec()
	require.NoError(t, err)

	ra, err := qr.RowsAffected()
	require.NoError(t, err)

	require.Zero(t, ra)
}

// TestSelect simple select the data without any condition.
func TestSelect(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()
	selectWhere(t, dbo, "")
}

func TestSelectDatabase(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()
	prepare, err := dbo.Prepare("select database()")
	require.NoError(t, err)
	rows, err := prepare.Query()
	require.NoError(t, err)
	defer rows.Close()
	var resultBytes sql.RawBytes
	require.True(t, rows.Next(), "no rows found")
	err = rows.Scan(&resultBytes)
	require.NoError(t, err)
	assert.Equal(t, string(resultBytes), "uks")
}

// TestInsertUpdateDelete validates all insert, update and
// delete method on prepared statements.
func TestInsertUpdateDelete(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()
	// prepare insert statement
	insertStmt := `insert into vt_prepare_stmt_test values( ?,  ?,  ?,  ?,  ?,  ?,  ?, ?,
		?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?, ?, ?,
		?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?, ?, ?);`

	textValue := gofakeit.Name()
	largeComment := gofakeit.LoremIpsumParagraph(1, 5, 20, " ")

	location, _ := time.LoadLocation("Local")
	// inserting multiple rows into test table
	for i := 1; i <= 100; i++ {
		// preparing value for the insert testing
		insertValue := []any{
			i, strconv.FormatInt(int64(i), 10) + "21", i * 100,
			127, 1, 32767, 8388607, 2147483647, 2.55, 64.9, 55.5,
			time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
			time.Date(2009, 5, 5, 0, 0, 0, 50000, location),
			time.Date(2009, 5, 5, 0, 0, 0, 50000, location),
			time.Now(),
			time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
			1, 1, 1, 1, 1, 1, 1, 1, 1, jsonExample, textValue, largeComment,
			-128, 127, 1, -1,
			-32768, 32767, 1, -1,
			-8388608, 8388607, 1, -1,
			-2147483648, 2147483647, 1, -1,
			-(1 << 63), (1 << 63) - 1, 1, -1,
		}
		exec(t, dbo, insertStmt, insertValue...)
	}
	// validate inserted data count
	testcount(t, dbo, 100)

	// select data with id 1 and validate the data accordingly
	// validate row count
	data := selectWhere(t, dbo, "id = ?", testingID)
	assert.Equal(t, 1, len(data))

	// validate value of msg column in data
	assert.Equal(t, fmt.Sprintf("%d21", testingID), data[0].Msg)

	// Validate a datetime field (without micros)
	//   The 50 microsecs we inserted should have been truncated
	assert.Equal(t, time.Date(2009, 5, 5, 0, 0, 0, 0, location), data[0].DateTime)

	// Validate a datetime field (with micros)
	assert.Equal(t, time.Date(2009, 5, 5, 0, 0, 0, 50000, location), data[0].DateTimeMicros)

	testReplica(t)
	// testing record update
	updateRecord(t, dbo)

	// testing record deletion
	deleteRecord(t, dbo)

	// testing reconnection and deleted data validation
	reconnectAndTest(t)
}

func testReplica(t *testing.T) {
	replicaConn := Connect(t, "")
	require.NotNil(t, replicaConn, "unable to connect")
	_, err := replicaConn.Exec(fmt.Sprintf("use %s@replica", dbInfo.KeyspaceName))
	require.NoError(t, err)
	tx, err := replicaConn.Begin()
	require.NoError(t, err, "error creating replica transaction")
	data := selectWhereWithTx(t, tx, "id = ?", testingID)
	assert.Equal(t, fmt.Sprintf("%d21", testingID), data[0].Msg)
	require.NoError(t, tx.Commit())
}

// testcount validates inserted rows count with expected count.
func testcount(t *testing.T, dbo *sql.DB, except int) {
	r, err := dbo.Query("SELECT count(1) FROM vt_prepare_stmt_test")
	require.Nil(t, err)

	r.Next()
	var i int
	err = r.Scan(&i)
	require.Nil(t, err)
	assert.Equal(t, except, i)
}

// TestAutoIncColumns test insertion of row without passing
// the value of auto increment columns (here it is id).
func TestAutoIncColumns(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()
	// insert a row without id
	insertStmt := "INSERT INTO vt_prepare_stmt_test" + ` (
		msg,keyspace_id,tinyint_unsigned,bool_signed,smallint_unsigned,
		mediumint_unsigned,int_unsigned,float_unsigned,double_unsigned,
		decimal_unsigned,t_date,t_datetime,t_datetime_micros,t_time,t_timestamp,c8,c16,c24,
		c32,c40,c48,c56,c63,c64,json_col,text_col,data,
		tinyint_min,tinyint_max,tinyint_pos,tinyint_neg,
		smallint_min,smallint_max,smallint_pos,smallint_neg,
		medint_min,medint_max,medint_pos,medint_neg,
		int_min,int_max,int_pos,int_neg,
		bigint_min,bigint_max,bigint_pos,bigint_neg
) VALUES (?,  ?,  ?,  ?,  ?, ?,
		  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?, ?, ?,
		  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?, ?, ?);`
	insertValue := []any{
		"21", 0,
		127, 1, 32767, 8388607, 2147483647, 2.55, 64.9, 55.5,
		time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
		time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
		time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
		time.Now(),
		time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
		1, 1, 1, 1, 1, 1, 1, 1, 1, jsonExample, gofakeit.DomainName(), gofakeit.LoremIpsumParagraph(1, 5, 20, " "),
		-(1 << 7), (1 << 7) - 1, 1, -1,
		-(1 << 15), (1 << 15) - 1, 1, -1,
		-(1 << 23), (1 << 23) - 1, 1, -1,
		-(1 << 31), (1 << 31) - 1, 1, -1,
		-(1 << 63), (1 << 63) - 1, 1, -1,
	}

	exec(t, dbo, insertStmt, insertValue...)
}

// deleteRecord test deletion operation corresponds to the testingID.
func deleteRecord(t *testing.T, dbo *sql.DB) {
	// delete the record with id 1
	exec(t, dbo, "DELETE FROM vt_prepare_stmt_test WHERE id = ?;", testingID)

	data := selectWhere(t, dbo, "id = ?", testingID)
	assert.Equal(t, 0, len(data))
}

// updateRecord test update operation corresponds to the testingID.
func updateRecord(t *testing.T, dbo *sql.DB) {
	// update the record with id 1
	updateData := "new data value"
	updateTextCol := "new text col value"
	updateQuery := "update vt_prepare_stmt_test set data = ? , text_col = ? where id = ?;"

	exec(t, dbo, updateQuery, updateData, updateTextCol, testingID)

	// validate the updated value
	// validate row count
	data := selectWhere(t, dbo, "id = ?", testingID)
	assert.Equal(t, 1, len(data))

	// validate value of msg column in data
	assert.Equal(t, updateData, data[0].Data)
	assert.Equal(t, updateTextCol, data[0].TextCol)
}

// reconnectAndTest creates new connection with database and validate.
func reconnectAndTest(t *testing.T) {
	// reconnect and try to select the record with id 1
	dbo := Connect(t)
	defer dbo.Close()
	data := selectWhere(t, dbo, "id = ?", testingID)
	assert.Equal(t, 0, len(data))
}

// TestColumnParameter query database using column
// parameter.
func TestColumnParameter(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()

	id := 1000
	parameter1 := "param1"
	message := "TestColumnParameter"
	insertStmt := "INSERT INTO vt_prepare_stmt_test (id, msg, keyspace_id) VALUES (?, ?, ?);"
	values := []any{
		id,
		message,
		2000,
	}
	exec(t, dbo, insertStmt, values...)

	var param, msg string
	var recID int

	selectStmt := "SELECT COALESCE(?, id), msg FROM vt_prepare_stmt_test WHERE msg = ? LIMIT ?"

	results1, err := dbo.Query(selectStmt, parameter1, message, 1)
	require.Nil(t, err)
	require.True(t, results1.Next())

	results1.Scan(&param, &msg)
	assert.Equal(t, parameter1, param)
	assert.Equal(t, message, msg)

	results2, err := dbo.Query(selectStmt, nil, message, 1)
	require.Nil(t, err)
	require.True(t, results2.Next())

	results2.Scan(&recID, &msg)
	assert.Equal(t, id, recID)
	assert.Equal(t, message, msg)
}

// TestWrongTableName query database using invalid
// tablename and validate error.
func TestWrongTableName(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()
	execWithError(t, dbo, []uint16{1146}, "select * from teseting_table;")
}

type columns struct {
	columnName             string
	dataType               string
	fullDataType           string
	characterMaximumLength sql.NullInt64
	numericPrecision       sql.NullInt64
	numericScale           sql.NullInt64
	datetimePrecision      sql.NullInt64
	columnDefault          sql.NullString
	isNullable             string
	extra                  string
	tableName              string
}

func (c *columns) ToString() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("|%s| \t |%s| \t |%s| \t |%s| \t |%s| \t |%s| \t |%s| \t |%s| \t |%s| \t |%s| \t |%s|",
		c.columnName,
		c.dataType,
		c.fullDataType,
		getIntToString(c.characterMaximumLength),
		getIntToString(c.numericPrecision),
		getIntToString(c.numericScale),
		getIntToString(c.datetimePrecision),
		getStringToString(c.columnDefault),
		c.isNullable,
		c.extra,
		c.tableName))
	return buf.String()
}

func getIntToString(x sql.NullInt64) string {
	if x.Valid {
		return strconv.FormatInt(x.Int64, 10)
	}
	return "NULL"
}

func getStringToString(x sql.NullString) string {
	if x.Valid {
		return x.String
	}
	return "NULL"
}

func TestSelectDBA(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()

	_, err := dbo.Exec("CREATE TABLE `a` (`one` int NOT NULL,`two` int NOT NULL,PRIMARY KEY(`one`, `two`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	require.NoError(t, err)

	prepare, err := dbo.Prepare(`SELECT
										column_name column_name,
										data_type data_type,
										column_type full_data_type,
										character_maximum_length character_maximum_length,
										numeric_precision numeric_precision,
										numeric_scale numeric_scale,
										datetime_precision datetime_precision,
										column_default column_default,
										is_nullable is_nullable,
										extra extra,
										table_name table_name
									   FROM information_schema.columns
									   WHERE table_schema = ? and table_name = ?
									   ORDER BY ordinal_position`)
	require.NoError(t, err)
	rows, err := prepare.Query("uks", "a")
	require.NoError(t, err)
	defer rows.Close()
	var rec columns
	rowCount := 0
	for rows.Next() {
		err := rows.Scan(
			&rec.columnName,
			&rec.dataType,
			&rec.fullDataType,
			&rec.characterMaximumLength,
			&rec.numericPrecision,
			&rec.numericScale,
			&rec.datetimePrecision,
			&rec.columnDefault,
			&rec.isNullable,
			&rec.extra,
			&rec.tableName)
		require.NoError(t, err)
		assert.True(t, rec.columnName == "one" || rec.columnName == "two")
		assert.Equal(t, "int", rec.dataType)
		assert.True(t, rec.fullDataType == "int" || rec.fullDataType == "int(11)")
		assert.False(t, rec.characterMaximumLength.Valid)
		assert.EqualValues(t, 10, rec.numericPrecision.Int64)
		assert.EqualValues(t, 0, rec.numericScale.Int64)
		assert.False(t, rec.datetimePrecision.Valid)
		assert.False(t, rec.columnDefault.Valid)
		assert.Equal(t, "NO", rec.isNullable)
		assert.Equal(t, "", rec.extra)
		assert.Equal(t, "a", rec.tableName)
		rowCount++
	}
	require.Equal(t, 2, rowCount)
}

func TestSelectLock(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()

	// Get Lock
	prepare, err := dbo.Prepare("select get_lock(?, ?)")
	require.NoError(t, err)

	rows, err := prepare.Query("a", 100000)
	require.NoError(t, err)

	var resultBytes sql.RawBytes
	require.True(t, rows.Next(), "no rows found")
	err = rows.Scan(&resultBytes)
	require.NoError(t, err)
	assert.Equal(t, "1", string(resultBytes))

	// for connection to be reused.
	err = rows.Close()
	require.NoError(t, err)

	// Release Lock
	prepare, err = dbo.Prepare("select release_lock(?)")
	require.NoError(t, err)

	rows, err = prepare.Query("a")
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "no rows found")
	err = rows.Scan(&resultBytes)
	require.NoError(t, err)
	assert.Equal(t, "1", string(resultBytes))
}

func TestShowColumns(t *testing.T) {
	dbo := Connect(t)
	defer dbo.Close()

	prepare, err := dbo.Prepare("show columns from vt_prepare_stmt_test where Field = 'id'")
	require.NoError(t, err)

	rows, err := prepare.Query()
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next(), "no rows found")
	cols, err := rows.Columns()
	if err != nil {
		return
	}
	require.Len(t, cols, 6)
	require.False(t, rows.Next())
}

func TestBinaryColumn(t *testing.T) {
	dbo := Connect(t, "interpolateParams=false")
	defer dbo.Close()

	_, err := dbo.Query(`SELECT DISTINCT
                BINARY table_info.table_name AS table_name,
                table_info.create_options AS create_options,
                table_info.table_comment AS table_comment
              FROM information_schema.tables AS table_info
              JOIN information_schema.columns AS column_info
                  ON BINARY column_info.table_name = BINARY table_info.table_name
              WHERE
                  table_info.table_schema = ?
                  AND column_info.table_schema = ?
                  -- Exclude views.
                  AND table_info.table_type = 'BASE TABLE'
              ORDER BY BINARY table_info.table_name`, uks, uks)
	require.NoError(t, err)
}

// TestInsertTest inserts a row with empty json array.
func TestInsertTest(t *testing.T) {
	dbo := Connect(t, "interpolateParams=false")
	defer dbo.Close()

	stmt, err := dbo.Prepare(`insert into vt_prepare_stmt_test(id, keyspace_id, json_col) values( null, ?, ?)`)
	require.NoError(t, err)

	res, err := stmt.Exec(1, "[]")
	require.NoError(t, err)

	ra, err := res.RowsAffected()
	require.NoError(t, err)

	assert.Equal(t, int64(1), ra)
}

// TestSpecializedPlan tests the specialized plan generation for the query.
func TestSpecializedPlan(t *testing.T) {
	dbInfo.KeyspaceName = sks
	dbo := Connect(t, "interpolateParams=false")
	defer dbo.Close()

	oMap := getVarValue[map[string]any](t, "OptimizedQueryExecutions", clusterInstance.VtgateProcess.GetVars)
	initExecCount := getVarValue[float64](t, "Passthrough", func() map[string]any {
		return oMap
	})

	queries := []struct {
		query string
		args  []any
	}{{
		query: `select 1 from t1 tbl1, t1 tbl2 where tbl1.id = ? and tbl2.id = ?`,
		args:  []any{1, 1},
	}, {
		query: `select 1 from t1 tbl1, t1 tbl2, t1 tbl3 where tbl1.id = ? and tbl2.id = ? and tbl3.id = ?`,
		args:  []any{1, 1, 1},
	}, {
		query: `select 1 from t1 tbl1, t1 tbl2, t1 tbl3, t1 tbl4 where tbl1.id = ? and tbl2.id = ? and tbl3.id = ? and tbl4.id = ?`,
		args:  []any{1, 1, 1, 1},
	}, {
		query: `SELECT e.id, e.name, s.age, ROW_NUMBER() OVER (PARTITION BY e.age ORDER BY s.name DESC) AS age_rank FROM t1 e, t1 s where e.id = ? and s.id = ?`,
		args:  []any{1, 1},
	}}

	for _, q := range queries {
		stmt, err := dbo.Prepare(q.query)
		require.NoError(t, err)

		for range 5 {
			rows, err := stmt.Query(q.args...)
			require.NoError(t, err)
			require.NoError(t, rows.Close())
		}
		require.NoError(t, stmt.Close())
	}
	oMap = getVarValue[map[string]any](t, "OptimizedQueryExecutions", clusterInstance.VtgateProcess.GetVars)
	finalExecCount := getVarValue[float64](t, "Passthrough", func() map[string]any {
		return oMap
	})
	require.EqualValues(t, 20, finalExecCount-initExecCount)

	randomExec(t, dbo)

	// Validate Join Query specialized plan.
	p := getPlanWhenReady(t, queries[0].query, 100*time.Millisecond, clusterInstance.VtgateProcess.ReadQueryPlans)
	require.NotNil(t, p, "plan not found")
	validateJoinSpecializedPlan(t, p)

	// Validate Window Function Query specialized plan with failing baseline plan.
	p = getPlanWhenReady(t, queries[3].query, 100*time.Millisecond, clusterInstance.VtgateProcess.ReadQueryPlans)
	require.NotNil(t, p, "plan not found")
	validateBaselineErrSpecializedPlan(t, p)
}

func validateJoinSpecializedPlan(t *testing.T, p map[string]any) {
	t.Helper()
	plan, exist := p["Instructions"]
	require.True(t, exist, "plan Instructions not found")

	pd, err := engine.PrimitiveDescriptionFromMap(plan.(map[string]any))
	require.NoError(t, err)
	require.Equal(t, "PlanSwitcher", pd.OperatorType)
	require.Len(t, pd.Inputs, 2, "Unexpected number of Inputs")

	require.Equal(t, "Baseline", pd.Inputs[0].InputName)
	require.Equal(t, "Optimized", pd.Inputs[1].InputName)
	require.Equal(t, "Route", pd.Inputs[1].OperatorType)
	require.Equal(t, "EqualUnique", pd.Inputs[1].Variant)
}

func validateBaselineErrSpecializedPlan(t *testing.T, p map[string]any) {
	t.Helper()
	plan, exist := p["Instructions"]
	require.True(t, exist, "plan Instructions not found")

	pm, ok := plan.(map[string]any)
	require.True(t, ok, "plan is not of type map[string]any")
	require.EqualValues(t, "PlanSwitcher", pm["OperatorType"])
	baselineErr := pm["BaselineErr"].(string)

	// v24+ uses new error message format
	// v23 and earlier uses old format
	expectedErr := "VT12001: unsupported: window functions are only supported for single-shard queries"
	if clusterInstance.VtGateMajorVersion < 24 {
		expectedErr = "VT12001: unsupported: OVER CLAUSE with sharded keyspace"
	}

	require.EqualValues(t, expectedErr, baselineErr)

	pd, err := engine.PrimitiveDescriptionFromMap(plan.(map[string]any))
	require.NoError(t, err)
	require.Equal(t, "PlanSwitcher", pd.OperatorType)
	require.Len(t, pd.Inputs, 1, "Only Specialized plan should be available")

	require.Equal(t, "Optimized", pd.Inputs[0].InputName)
	require.Equal(t, "Route", pd.Inputs[0].OperatorType)
	require.Equal(t, "EqualUnique", pd.Inputs[0].Variant)
}

// randomExec to make many plans so that plan cache is populated.
func randomExec(t *testing.T, dbo *sql.DB) {
	t.Helper()

	for i := 1; i < 101; i++ {
		// generate a random query
		query := fmt.Sprintf("SELECT %d", i)
		stmt, err := dbo.Prepare(query)
		require.NoError(t, err)

		rows, err := stmt.Query()
		require.NoError(t, err)
		require.NoError(t, rows.Close())
		time.Sleep(5 * time.Millisecond)
	}
}

// getPlanWhenReady polls for the query plan until it is ready or times out.
func getPlanWhenReady(t *testing.T, sql string, timeout time.Duration, plansFunc func() (map[string]any, error)) map[string]any {
	t.Helper()

	waitTimeout := time.After(timeout)
	for {
		select {
		case <-waitTimeout:
			require.Fail(t, "timeout waiting for plan for query: "+sql)
			return nil
		default:
			p, err := plansFunc()
			require.NoError(t, err, "failed to retrieve query plans")
			if len(p) > 0 {
				val, found := p[sql]
				if found {
					planMap, ok := val.(map[string]any)
					require.True(t, ok, "plan is not of type map[string]any")
					return planMap
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func getVarValue[T any](t *testing.T, key string, varFunc func() map[string]any) T {
	t.Helper()

	vars := varFunc()
	require.NotNil(t, vars)

	value, exists := vars[key]
	if !exists {
		return *new(T)
	}
	castValue, ok := value.(T)
	if !ok {
		t.Errorf("unexpected type, want: %T, got %T", new(T), value)
	}
	return castValue
}
