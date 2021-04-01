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
R442
*/

package mysqlserver

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/icrowley/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

// TestMultiStmt checks that multiStatements=True and multiStatements=False work properly.
func TestMultiStatement(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	// connect database with multiStatements=True
	db := connectDB(t, vtParams, "multiStatements=True", "timeout=90s", "collation=utf8mb4_unicode_ci")

	rows, err := db.QueryContext(ctx, "SELECT 1; SELECT 2; SELECT 3")
	require.Nilf(t, err, "multiple statements should be executed without error, got %v", err)
	var count int
	for rows.Next() || (rows.NextResultSet() && rows.Next()) {
		var i int
		rows.Scan(&i)
		count++
		assert.Equalf(t, count, i, "result of query %v query should be %v, got %v", count, count, i)
	}
	assert.Equalf(t, 3, count, "this query should affect 3 row, got %v", count)
	db.Close()

	// connect database with multiStatements=False
	db = connectDB(t, vtParams, "multiStatements=False", "timeout=90s", "collation=utf8mb4_unicode_ci")

	_, err = db.QueryContext(ctx, "SELECT 1; SELECT 2; SELECT 3")
	require.NotNilf(t, err, "error expected, got nil error")
	assert.Containsf(t, err.Error(), "syntax error", "expected syntax error, got %v", err)
}

// TestLargeComment add large comment in insert stmt and validate the insert process.
func TestLargeComment(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nilf(t, err, "unable to connect mysql: %v", err)
	defer conn.Close()

	// insert data with large comment
	_, err = conn.ExecuteFetch("insert into vt_insert_test (id, msg, keyspace_id, data) values(1, 'large blob', 123, 'LLL') /* "+fake.CharactersN(4*1024*1024)+" */", 1, false)
	require.Nilf(t, err, "insertion error: %v", err)

	qr, err := conn.ExecuteFetch("select * from vt_insert_test where id = 1", 1, false)
	require.Nilf(t, err, "select error: %v", err)
	assert.Equal(t, 1, len(qr.Rows))
	assert.Equal(t, "BLOB(\"LLL\")", qr.Rows[0][3].String())
}

// TestInsertLargerThenGrpcLimit insert blob larger then grpc limit and verify the error.
func TestInsertLargerThenGrpcLimit(t *testing.T) {
	defer cluster.PanicHandler(t)

	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nilf(t, err, "unable to connect mysql: %v", err)
	defer conn.Close()

	grpcLimit := os.Getenv("grpc_max_message_size")
	limit, err := strconv.Atoi(grpcLimit)
	require.Nilf(t, err, "int parsing error: %v", err)

	// insert data with large blob
	_, err = conn.ExecuteFetch("insert into vt_insert_test (id, msg, keyspace_id, data) values(2, 'huge blob', 123, '"+fake.CharactersN(limit+1)+"')", 1, false)
	require.NotNil(t, err, "error expected on insert")
	assert.Contains(t, err.Error(), "trying to send message larger than max")
}

// TestTimeout executes sleep(5) with query_timeout of 1 second, and verifies the error.
func TestTimeout(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nilf(t, err, "unable to connect mysql: %v", err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("SELECT SLEEP(5);", 1, false)
	require.NotNilf(t, err, "quiry timeout error expected")
	mysqlErr, ok := err.(*mysql.SQLError)
	require.Truef(t, ok, "invalid error type")
	assert.Equal(t, 1317, mysqlErr.Number(), err)
}

// TestInvalidField tries to fetch invalid column and verifies the error.
func TestInvalidField(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nilf(t, err, "unable to connect mysql: %v", err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("SELECT invalid_field from vt_insert_test;", 1, false)
	require.NotNil(t, err, "invalid field error expected")
	mysqlErr, ok := err.(*mysql.SQLError)
	require.Truef(t, ok, "invalid error type")
	assert.Equal(t, 1054, mysqlErr.Number(), err)
}

// TestWarnings validates the behaviour of SHOW WARNINGS.
func TestWarnings(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nilf(t, err, "unable to connect mysql: %v", err)
	defer conn.Close()

	// validate warning with invalid_field error as warning
	qr, err := conn.ExecuteFetch("SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS */ invalid_field from vt_insert_test;", 1, false)
	require.Nilf(t, err, "select error : %v", err)
	assert.Empty(t, qr.Rows, "number of rows")

	qr, err = conn.ExecuteFetch("SHOW WARNINGS;", 1, false)
	require.Nilf(t, err, "SHOW WARNINGS; execution failed: %v", err)
	assert.EqualValues(t, 1, len(qr.Rows), "number of rows")
	assert.Contains(t, qr.Rows[0][0].String(), "VARCHAR(\"Warning\")", qr.Rows)
	assert.Contains(t, qr.Rows[0][1].String(), "UINT16(1054)", qr.Rows)
	assert.Contains(t, qr.Rows[0][2].String(), "Unknown column", qr.Rows)

	// validate warning with query_timeout error as warning
	qr, err = conn.ExecuteFetch("SELECT /*vt+ SCATTER_ERRORS_AS_WARNINGS QUERY_TIMEOUT_MS=1 */ sleep(1) from vt_insert_test;", 1, false)
	require.Nilf(t, err, "insertion error : %v", err)
	assert.Empty(t, qr.Rows, "number of rows")

	qr, err = conn.ExecuteFetch("SHOW WARNINGS;", 1, false)
	require.Nilf(t, err, "SHOW WARNINGS; execution failed: %v", err)
	assert.EqualValues(t, 1, len(qr.Rows), "number of rows")
	assert.Contains(t, qr.Rows[0][0].String(), "VARCHAR(\"Warning\")", qr.Rows)
	assert.Contains(t, qr.Rows[0][1].String(), "UINT16(1317)", qr.Rows)
	assert.Contains(t, qr.Rows[0][2].String(), "context deadline exceeded", qr.Rows)

	// validate with 0 warnings
	_, err = conn.ExecuteFetch("SELECT 1 from vt_insert_test limit 1", 1, false)
	require.Nilf(t, err, "select error: %v", err)

	qr, err = conn.ExecuteFetch("SHOW WARNINGS;", 1, false)
	require.Nilf(t, err, "SHOW WARNINGS; execution failed: %v", err)
	assert.Empty(t, len(qr.Rows), "number of rows")
}

// TestSelectWithUnauthorizedUser verifies that an unauthorized user
// is not able to read from the table.
func TestSelectWithUnauthorizedUser(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	tmpVtParam := vtParams
	tmpVtParam.Uname = "testuser2"
	tmpVtParam.Pass = "testpassword2"

	conn, err := mysql.Connect(ctx, &tmpVtParam)
	require.Nilf(t, err, "unable to connect to mysql: %v", err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("SELECT * from vt_insert_test limit 1", 1, false)
	require.NotNilf(t, err, "error expected, got nil")
	assert.Contains(t, err.Error(), "table acl error")
	assert.Contains(t, err.Error(), "cannot run Select on table")
}

func connectDB(t *testing.T, vtParams mysql.ConnParams, params ...string) *sql.DB {
	connectionStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", vtParams.Uname, vtParams.Pass, vtParams.Host, vtParams.Port, keyspaceName, strings.Join(params, "&"))
	db, err := sql.Open("mysql", connectionStr)
	require.Nil(t, err)
	return db
}

// createConfig create file in to Tmp dir in vtdataroot and write the given data.
func createConfig(name, data string) error {
	// creating new file
	f, err := os.Create(clusterInstance.TmpDirectory + name)
	if err != nil {
		return err
	}

	if data == "" {
		return nil
	}

	// write the given data
	_, err = fmt.Fprint(f, data)
	return err
}
