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

package endtoend

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Test the SQL query part of the API.
func TestQueries(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}

	// Try a simple error case.
	_, err = conn.ExecuteFetch("select * from aa", 1000, true)
	if err == nil || !strings.Contains(err.Error(), "Table 'vttest.aa' doesn't exist") {
		t.Fatalf("expected error but got: %v", err)
	}

	// Try a simple DDL.
	result, err := conn.ExecuteFetch("create table a(id int, name varchar(128), primary key(id))", 0, false)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Errorf("create table returned RowsAffected %v, was expecting 0", result.RowsAffected)
	}

	// Try a simple insert.
	result, err = conn.ExecuteFetch("insert into a(id, name) values(10, 'nice name')", 1000, true)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for insert: %v", result)
	}

	// And re-read what we inserted.
	result, err = conn.ExecuteFetch("select * from a", 1000, true)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name:         "id",
				Type:         querypb.Type_INT32,
				Table:        "a",
				OrgTable:     "a",
				Database:     "vttest",
				OrgName:      "id",
				ColumnLength: 11,
				Charset:      mysql.CharacterSetBinary,
				Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG |
					querypb.MySqlFlag_PRI_KEY_FLAG |
					querypb.MySqlFlag_PART_KEY_FLAG |
					querypb.MySqlFlag_NUM_FLAG),
			},
			{
				Name:         "name",
				Type:         querypb.Type_VARCHAR,
				Table:        "a",
				OrgTable:     "a",
				Database:     "vttest",
				OrgName:      "name",
				ColumnLength: 384,
				Charset:      mysql.CharacterSetUtf8,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
			},
		},
	}
	if !result.Equal(expectedResult) {
		// MySQL 5.7 is adding the NO_DEFAULT_VALUE_FLAG to Flags.
		expectedResult.Fields[0].Flags |= uint32(querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)
		if !result.Equal(expectedResult) {
			t.Errorf("unexpected result for select, got:\n%v\nexpected:\n%v\n", result, expectedResult)
		}
	}

	// Insert a few rows.
	for i := 0; i < 100; i++ {
		result, err := conn.ExecuteFetch(fmt.Sprintf("insert into a(id, name) values(%v, 'nice name %v')", 1000+i, i), 1000, true)
		if err != nil {
			t.Fatalf("ExecuteFetch(%v) failed: %v", i, err)
		}
		if result.RowsAffected != 1 {
			t.Errorf("insert into returned RowsAffected %v, was expecting 1", result.RowsAffected)
		}
	}

	// And use a streaming query to read them back.
	// Do it twice to make sure state is reset properly.
	readRowsUsingStream(t, conn, 101)
	readRowsUsingStream(t, conn, 101)

	// And drop the table.
	result, err = conn.ExecuteFetch("drop table a", 0, false)
	if err != nil {
		t.Fatalf("drop table failed: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Errorf("insert into returned RowsAffected %v, was expecting 0", result.RowsAffected)
	}
}

func TestLargeQueries(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}

	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	randString := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		return string(b)
	}

	for i := 0; i < 2; i++ {
		for j := -2; j < 2; j++ {
			expectedString := randString((i+1)*mysql.MaxPacketSize + j)

			result, err := conn.ExecuteFetch(fmt.Sprintf("select \"%s\"", expectedString), -1, true)
			if err != nil {
				t.Fatalf("ExecuteFetch failed: %v", err)
			}
			if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0].IsNull() {
				t.Fatalf("ExecuteFetch on large query returned poorly-formed result. " +
					"Expected single row single column string.")
			}
			if result.Rows[0][0].ToString() != expectedString {
				t.Fatalf("Result row was incorrect. Suppressing large string")
			}
		}
	}
}

func readRowsUsingStream(t *testing.T, conn *mysql.Conn, expectedCount int) {
	// Start the streaming query.
	if err := conn.ExecuteStreamFetch("select * from a"); err != nil {
		t.Fatalf("ExecuteStreamFetch failed: %v", err)
	}

	// Check the fields.
	expectedFields := []*querypb.Field{
		{
			Name:         "id",
			Type:         querypb.Type_INT32,
			Table:        "a",
			OrgTable:     "a",
			Database:     "vttest",
			OrgName:      "id",
			ColumnLength: 11,
			Charset:      mysql.CharacterSetBinary,
			Flags: uint32(querypb.MySqlFlag_NOT_NULL_FLAG |
				querypb.MySqlFlag_PRI_KEY_FLAG |
				querypb.MySqlFlag_PART_KEY_FLAG |
				querypb.MySqlFlag_NUM_FLAG),
		},
		{
			Name:         "name",
			Type:         querypb.Type_VARCHAR,
			Table:        "a",
			OrgTable:     "a",
			Database:     "vttest",
			OrgName:      "name",
			ColumnLength: 384,
			Charset:      mysql.CharacterSetUtf8,
		},
	}
	fields, err := conn.Fields()
	if err != nil {
		t.Fatalf("Fields failed: %v", err)
	}
	if !sqltypes.FieldsEqual(fields, expectedFields) {
		// MySQL 5.7 is adding the NO_DEFAULT_VALUE_FLAG to Flags.
		expectedFields[0].Flags |= uint32(querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)
		if !sqltypes.FieldsEqual(fields, expectedFields) {
			t.Fatalf("fields are not right, got:\n%v\nexpected:\n%v", fields, expectedFields)
		}
	}

	// Read the rows.
	count := 0
	for {
		row, err := conn.FetchNext()
		if err != nil {
			t.Fatalf("FetchNext failed: %v", err)
		}
		if row == nil {
			// We're done.
			break
		}
		if len(row) != 2 {
			t.Fatalf("Unexpected row found: %v", row)
		}
		count++
	}
	if count != expectedCount {
		t.Errorf("Got unexpected count %v for query, was expecting %v", count, expectedCount)
	}
	conn.CloseResult()
}

func doTestWarnings(t *testing.T, disableClientDeprecateEOF bool) {
	ctx := context.Background()

	connParams.DisableClientDeprecateEOF = disableClientDeprecateEOF

	conn, err := mysql.Connect(ctx, &connParams)
	expectNoError(t, err)
	defer conn.Close()

	result, err := conn.ExecuteFetch("create table a(id int, val int not null, primary key(id))", 0, false)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Errorf("create table returned RowsAffected %v, was expecting 0", result.RowsAffected)
	}

	// Disable strict mode
	_, err = conn.ExecuteFetch("set session sql_mode=''", 0, false)
	if err != nil {
		t.Fatalf("disable strict mode failed: %v", err)
	}

	// Try a simple insert with a null value
	result, warnings, err := conn.ExecuteFetchWithWarningCount("insert into a(id) values(10)", 1000, true)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if result.RowsAffected != 1 || len(result.Rows) != 0 {
		t.Errorf("unexpected result for insert: %v", result)
	}
	if warnings != 1 {
		t.Errorf("unexpected result for warnings: %v", warnings)
	}

	_, err = conn.ExecuteFetch("drop table a", 0, false)
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}
}

func TestWarningsDeprecateEOF(t *testing.T) {
	doTestWarnings(t, false)
}

func TestWarningsNoDeprecateEOF(t *testing.T) {
	doTestWarnings(t, true)
}

func TestSysInfo(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("drop table if exists `a`", 1000, true)
	require.NoError(t, err)

	_, err = conn.ExecuteFetch("CREATE TABLE `a` (`one` int NOT NULL,`two` int NOT NULL,PRIMARY KEY (`one`,`two`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", 1000, true)
	require.NoError(t, err)
	defer conn.ExecuteFetch("drop table `a`", 1000, true)

	qr, err := conn.ExecuteFetch(`SELECT
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
	WHERE table_schema = 'vttest' and table_name = 'a'
	ORDER BY ordinal_position`, 1000, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(qr.Rows))

	// is_nullable
	assert.Equal(t, `VARCHAR("NO")`, qr.Rows[0][8].String())
	assert.Equal(t, `VARCHAR("NO")`, qr.Rows[1][8].String())

	// table_name
	assert.Equal(t, `VARCHAR("a")`, qr.Rows[0][10].String())
	assert.Equal(t, `VARCHAR("a")`, qr.Rows[1][10].String())

	assert.EqualValues(t, sqltypes.Uint64, qr.Fields[4].Type)
	assert.EqualValues(t, querypb.Type_UINT64, qr.Rows[0][4].Type())
}
