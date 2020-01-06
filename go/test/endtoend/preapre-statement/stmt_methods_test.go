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
	"fmt"
	"testing"
	"time"

	"github.com/icrowley/fake"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
)

// tableData is tmp structure to select data of test table using gorm.
type tableData struct {
	Msg     string
	Data    string
	TextCol string
}

// testingID is id on which we perform the update and delete testing.
const testingID = 1

// TestSelect simple select the data without any condition.
func TestSelect(t *testing.T) {
	dbo := GetORM(t)
	defer dbo.Close()
	data := selectWhere(t, dbo, "")
	fmt.Println(data)
}

// TestInsertUpdateDelete validate all insert, update and
// delete method on prepared statements.
func TestInsertUpdateDelete(t *testing.T) {

	dbo := GetORM(t)
	defer dbo.Close()
	// prepared insert statement for the insert testing
	insertStmt := `insert into ` + tableName + ` values(?,  ?,  ?,  ?,  ?,  ?,  ?,  
		?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?,  ?,  ?,  ?,  ?,  ?, ?, ?, ?);`

	textValue := fake.FullName()
	largeComment := fake.Paragraph()

	// inserting the multiple data in to test table
	for i := 1; i <= 100; i++ {
		// preparing value for the insert testing
		insertValue := []interface{}{
			i, fmt.Sprint(i) + "21", i * 100,
			127, 1, 32767, 8388607, 2147483647, 2.55, 64.9, 55.5,
			time.Date(2009, 5, 5, 0, 0, 0, 0, time.UTC),
			time.Date(2009, 5, 5, 0, 0, 0, 0, time.UTC),
			time.Now(),
			time.Date(2009, 5, 5, 0, 0, 0, 0, time.UTC),
			1, 1, 1, 1, 1, 1, 1, 1, 1, jsonExample, textValue, largeComment,
		}
		exec(t, dbo, insertStmt, insertValue...)

	}

	// select data with id 1 and validate the data accordingly
	// validate row count
	data := selectWhere(t, dbo, "id = ?", testingID)
	assert.Equal(t, 1, len(data))

	// validate value of msg column in data
	assert.Equal(t, fmt.Sprintf("%d21", testingID), data[0].Msg)

	// testing record update
	updateRecord(t, dbo)

	// teseting record deletion
	deleteRecord(t, dbo)

	// testing recontion and deleted data validation
	reconnectAndTest(t)
}

// TestAutoIncColumns test insertion of row without passing
// the value of auto increment columns (here it is id).
func TestAutoIncColumns(t *testing.T) {
	dbo := GetORM(t)
	defer dbo.Close()
	// insert a row without id
	insertStmt := "INSERT INTO " + tableName + ` (
		msg,keyspace_id,tinyint_unsigned,bool_signed,smallint_unsigned,
		mediumint_unsigned,int_unsigned,float_unsigned,double_unsigned,
		decimal_unsigned,t_date,t_datetime,t_time,t_timestamp,c8,c16,c24,
		c32,c40,c48,c56,c63,c64,json_col,text_col,data) VALUES (?,  ?,  ?,  ?,  ?, ?,
		  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?,  ?,  ?,  ?,  ?,  ?, ?, ?, ?);`
	insertValue := []interface{}{
		"21", 0,
		127, 1, 32767, 8388607, 2147483647, 2.55, 64.9, 55.5,
		time.Date(2009, 5, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2009, 5, 5, 0, 0, 0, 0, time.UTC),
		time.Now(),
		time.Date(2009, 5, 5, 0, 0, 0, 0, time.UTC),
		1, 1, 1, 1, 1, 1, 1, 1, 1, jsonExample, fake.DomainName(), fake.Paragraph(),
	}

	exec(t, dbo, insertStmt, insertValue...)

	// data := selectWhere(t, " id = last_inserted_id()")
	// if len(data) != 1 {
	// 	t.Fatalf("expected 1 got %d", len(data))
	// }

	// // validate value of msg column in data
	// expectedMsg := "21"
	// if d := data[0]; d.Msg != expectedMsg {
	// 	t.Fatalf("Received incorrect value, wanted: %s, got %s", expectedMsg, d.Msg)
	// }
}

// deleteRecord test deletion opeation corresponds to the testingID
func deleteRecord(t *testing.T, dbo *gorm.DB) {
	// delete the record with id 1
	exec(t, dbo, "DELETE FROM "+tableName+" WHERE id = ?;", testingID)

	data := selectWhere(t, dbo, "id = ?", testingID)
	assert.Equal(t, 0, len(data))

}

// updateRecord test update opeation corresponds to the testingID
func updateRecord(t *testing.T, dbo *gorm.DB) {
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

func reconnectAndTest(t *testing.T) {
	// reconnect and try to select the record with id 1
	dbo := GetORM(t)
	defer dbo.Close()
	data := selectWhere(t, dbo, "id = ?", testingID)
	assert.Equal(t, 0, len(data))

}

func TestWrongTableName(t *testing.T) {
	dbo := GetORM(t)
	defer dbo.Close()
	execWithIgnore(t, dbo, []uint16{1105}, "select * from teseting_table;")
}
