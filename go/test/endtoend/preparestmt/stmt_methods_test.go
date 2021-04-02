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

	"github.com/icrowley/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestSelect simple select the data without any condition.
func TestSelect(t *testing.T) {
	defer cluster.PanicHandler(t)
	dbo := Connect(t)
	defer dbo.Close()
	selectWhere(t, dbo, "")
}

func TestSelectDatabase(t *testing.T) {
	defer cluster.PanicHandler(t)
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
	assert.Equal(t, string(resultBytes), "test_keyspace")
}

// TestInsertUpdateDelete validates all insert, update and
// delete method on prepared statements.
func TestInsertUpdateDelete(t *testing.T) {
	defer cluster.PanicHandler(t)
	dbo := Connect(t)
	defer dbo.Close()
	// prepare insert statement
	insertStmt := `insert into ` + tableName + ` values( ?,  ?,  ?,  ?,  ?,  ?,  ?, ?,
		?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?, ?, ?,
		?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?, ?, ?);`

	textValue := fake.FullName()
	largeComment := fake.Paragraph()

	location, _ := time.LoadLocation("Local")
	// inserting multiple rows into test table
	for i := 1; i <= 100; i++ {
		// preparing value for the insert testing
		insertValue := []interface{}{
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
	defer cluster.PanicHandler(t)
	r, err := dbo.Query("SELECT count(1) FROM " + tableName)
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
	defer cluster.PanicHandler(t)
	dbo := Connect(t)
	defer dbo.Close()
	// insert a row without id
	insertStmt := "INSERT INTO " + tableName + ` (
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
	insertValue := []interface{}{
		"21", 0,
		127, 1, 32767, 8388607, 2147483647, 2.55, 64.9, 55.5,
		time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
		time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
		time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
		time.Now(),
		time.Date(2009, 5, 5, 0, 0, 0, 50000, time.UTC),
		1, 1, 1, 1, 1, 1, 1, 1, 1, jsonExample, fake.DomainName(), fake.Paragraph(),
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
	exec(t, dbo, "DELETE FROM "+tableName+" WHERE id = ?;", testingID)

	data := selectWhere(t, dbo, "id = ?", testingID)
	assert.Equal(t, 0, len(data))

}

// updateRecord test update operation corresponds to the testingID.
func updateRecord(t *testing.T, dbo *sql.DB) {
	// update the record with id 1
	updateData := "new data value"
	updateTextCol := "new text col value"
	updateQuery := "update " + tableName + " set data = ? , text_col = ? where id = ?;"

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
	defer cluster.PanicHandler(t)
	dbo := Connect(t)
	defer dbo.Close()

	id := 1000
	parameter1 := "param1"
	message := "TestColumnParameter"
	insertStmt := "INSERT INTO " + tableName + " (id, msg, keyspace_id) VALUES (?, ?, ?);"
	values := []interface{}{
		id,
		message,
		2000,
	}
	exec(t, dbo, insertStmt, values...)

	var param, msg string
	var recID int

	selectStmt := "SELECT COALESCE(?, id), msg FROM " + tableName + " WHERE msg = ? LIMIT ?"

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
	defer cluster.PanicHandler(t)
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
		return fmt.Sprintf("%d", x.Int64)
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
	defer cluster.PanicHandler(t)
	dbo := Connect(t)
	defer dbo.Close()

	_, err := dbo.Exec("use uks")
	require.NoError(t, err)

	_, err = dbo.Exec("CREATE TABLE `a` (`one` int NOT NULL,`two` int NOT NULL,PRIMARY KEY(`one`, `two`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
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
									   WHERE table_schema = ?
									   ORDER BY ordinal_position`)
	require.NoError(t, err)
	rows, err := prepare.Query("uks")
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
