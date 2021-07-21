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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"context"

	"vitess.io/vitess/go/mysql"
)

// TestKill opens a connection, issues a command that
// will sleep for a few seconds, waits a bit for MySQL to start
// executing it, then kills the connection (using another
// connection). We make sure we get the right error code.
func TestKill(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}

	// Create the kill connection first. It sometimes takes longer
	// than 10s
	killConn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer killConn.Close()

	errChan := make(chan error)
	go func() {
		_, err = conn.ExecuteFetch("select sleep(10) from dual", 1000, false)
		errChan <- err
		close(errChan)
	}()

	// Give extra time for the query to start executing.
	time.Sleep(2 * time.Second)
	if _, err := killConn.ExecuteFetch(fmt.Sprintf("kill %v", conn.ConnectionID), 1000, false); err != nil {
		t.Fatalf("Kill(%v) failed: %v", conn.ConnectionID, err)
	}

	// The error text will depend on what ExecuteFetch in the go
	// routine managed to do. Two cases:
	// 1. the connection was closed before the go routine's ExecuteFetch
	//   entered the ReadFull call. Then we get an error with
	//   'connection reset by peer' in it.
	// 2. the connection was closed while the go routine's ExecuteFetch
	//   was stuck on the read. Then we get io.EOF.
	// The code and sqlState needs to be right in any case, the text
	// will differ.
	err = <-errChan
	if strings.Contains(err.Error(), "EOF") {
		assertSQLError(t, err, mysql.CRServerLost, mysql.SSUnknownSQLState, "EOF", "select sleep(10) from dual")
	} else {
		assertSQLError(t, err, mysql.CRServerLost, mysql.SSUnknownSQLState, "", "connection reset by peer")
	}
}

// TestKill2006 opens a connection, kills the
// connection from the server side, then waits a bit, and tries to
// execute a command. We make sure we get the right error code.
func TestKill2006(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}

	// Kill the connection from the server side.
	killConn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer killConn.Close()

	if _, err := killConn.ExecuteFetch(fmt.Sprintf("kill %v", conn.ConnectionID), 1000, false); err != nil {
		t.Fatalf("Kill(%v) failed: %v", conn.ConnectionID, err)
	}

	// Now we should get a CRServerGone.  Since we are using a
	// unix socket, we will get a broken pipe when the server
	// closes the connection and we are trying to write the command.
	_, err = conn.ExecuteFetch("select sleep(10) from dual", 1000, false)
	assertSQLError(t, err, mysql.CRServerGone, mysql.SSUnknownSQLState, "broken pipe", "select sleep(10) from dual")
}

// TestDupEntry tests a duplicate key is properly raised.
func TestDupEntry(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch("create table dup_entry(id int, name int, primary key(id), unique index(name))", 0, false); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := conn.ExecuteFetch("insert into dup_entry(id, name) values(1, 10)", 0, false); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}
	_, err = conn.ExecuteFetch("insert into dup_entry(id, name) values(2, 10)", 0, false)
	assertSQLError(t, err, mysql.ERDupEntry, mysql.SSConstraintViolation, "Duplicate entry", "insert into dup_entry(id, name) values(2, 10)")
}

// TestClientFoundRows tests if the CLIENT_FOUND_ROWS flag works.
func TestClientFoundRows(t *testing.T) {
	params := connParams
	params.EnableClientFoundRows()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.ExecuteFetch("create table found_rows(id int, val int, primary key(id))", 0, false); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := conn.ExecuteFetch("insert into found_rows(id, val) values(1, 10)", 0, false); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	qr, err := conn.ExecuteFetch("update found_rows set val=11 where id=1", 0, false)
	require.NoError(t, err)
	assert.EqualValues(t, 1, qr.RowsAffected, "RowsAffected")

	qr, err = conn.ExecuteFetch("update found_rows set val=11 where id=1", 0, false)
	require.NoError(t, err)
	assert.EqualValues(t, 1, qr.RowsAffected, "RowsAffected")
}

func doTestMultiResult(t *testing.T, disableClientDeprecateEOF bool) {
	ctx := context.Background()
	connParams.DisableClientDeprecateEOF = disableClientDeprecateEOF

	conn, err := mysql.Connect(ctx, &connParams)
	expectNoError(t, err)
	defer conn.Close()

	qr, more, err := conn.ExecuteFetchMulti("select 1 from dual; set autocommit=1; select 1 from dual", 10, true)
	expectNoError(t, err)
	expectFlag(t, "ExecuteMultiFetch(multi result)", more, true)
	assert.EqualValues(t, 1, len(qr.Rows))

	qr, more, _, err = conn.ReadQueryResult(10, true)
	expectNoError(t, err)
	expectFlag(t, "ReadQueryResult(1)", more, true)
	assert.EqualValues(t, 0, len(qr.Rows))

	qr, more, _, err = conn.ReadQueryResult(10, true)
	expectNoError(t, err)
	expectFlag(t, "ReadQueryResult(2)", more, false)
	assert.EqualValues(t, 1, len(qr.Rows))

	qr, more, err = conn.ExecuteFetchMulti("select 1 from dual", 10, true)
	expectNoError(t, err)
	expectFlag(t, "ExecuteMultiFetch(single result)", more, false)
	assert.EqualValues(t, 1, len(qr.Rows))

	qr, more, err = conn.ExecuteFetchMulti("set autocommit=1", 10, true)
	expectNoError(t, err)
	expectFlag(t, "ExecuteMultiFetch(no result)", more, false)
	assert.EqualValues(t, 0, len(qr.Rows))

	// The ClientDeprecateEOF protocol change has a subtle twist in which an EOF or OK
	// packet happens to have the status flags in the same position if the affected_rows
	// and last_insert_id are both one byte long:
	//
	// https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
	// https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
	//
	// It turns out that there are no actual cases in which clients end up needing to make
	// this distinction. If either affected_rows or last_insert_id are non-zero, the protocol
	// sends an OK packet unilaterally which is properly parsed. If not, then regardless of the
	// negotiated version, it can properly send the status flags.
	//
	result, err := conn.ExecuteFetch("create table a(id int, name varchar(128), primary key(id))", 0, false)
	require.NoError(t, err)
	assert.Zero(t, result.RowsAffected, "create table RowsAffected ")

	for i := 0; i < 255; i++ {
		result, err := conn.ExecuteFetch(fmt.Sprintf("insert into a(id, name) values(%v, 'nice name %v')", 1000+i, i), 1000, true)
		require.NoError(t, err)
		assert.EqualValues(t, 1, result.RowsAffected, "insert into returned RowsAffected")
	}

	qr, more, err = conn.ExecuteFetchMulti("update a set name = concat(name, ' updated'); select * from a; select count(*) from a", 300, true)
	expectNoError(t, err)
	expectFlag(t, "ExecuteMultiFetch(multi result)", more, true)
	assert.EqualValues(t, 255, qr.RowsAffected)

	qr, more, _, err = conn.ReadQueryResult(300, true)
	expectNoError(t, err)
	expectFlag(t, "ReadQueryResult(1)", more, true)
	assert.EqualValues(t, 255, len(qr.Rows), "ReadQueryResult(1)")

	qr, more, _, err = conn.ReadQueryResult(300, true)
	expectNoError(t, err)
	expectFlag(t, "ReadQueryResult(2)", more, false)
	assert.EqualValues(t, 1, len(qr.Rows), "ReadQueryResult(1)")

	_, err = conn.ExecuteFetch("drop table a", 10, true)
	require.NoError(t, err)
}

func TestMultiResultDeprecateEOF(t *testing.T) {
	doTestMultiResult(t, false)
}
func TestMultiResultNoDeprecateEOF(t *testing.T) {
	doTestMultiResult(t, true)
}

func expectNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func expectFlag(t *testing.T, msg string, flag, want bool) {
	t.Helper()
	if flag != want {
		// We cannot continue the test if flag is incorrect.
		t.Fatalf("%s: %v, want: %v", msg, flag, want)
	}
}

// TestTLS tests our client can connect via SSL.
func TestTLS(t *testing.T) {
	params := connParams
	params.EnableSSL()

	// First make sure the official 'mysql' client can connect.
	output, ok := runMysql(t, &params, "status")
	if !ok {
		t.Fatalf("'mysql -e status' failed: %v", output)
	}
	if !strings.Contains(output, "Cipher in use is") {
		t.Fatalf("cannot connect via SSL: %v", output)
	}

	// Now connect with our client.
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	result, err := conn.ExecuteFetch("SHOW STATUS LIKE 'Ssl_cipher'", 10, true)
	if err != nil {
		t.Fatalf("SHOW STATUS LIKE 'Ssl_cipher' failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0].ToString() != "Ssl_cipher" ||
		result.Rows[0][1].ToString() == "" {
		t.Fatalf("SHOW STATUS LIKE 'Ssl_cipher' returned unexpected result: %v", result)
	}
}

func TestReplicationStatus(t *testing.T) {
	params := connParams
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	status, err := conn.ShowReplicationStatus()
	if err != mysql.ErrNotReplica {
		t.Errorf("Got unexpected result for ShowReplicationStatus: %v %v", status, err)
	}
}

func TestSessionTrackGTIDs(t *testing.T) {
	ctx := context.Background()
	params := connParams
	params.Flags |= mysql.CapabilityClientSessionTrack
	conn, err := mysql.Connect(ctx, &params)
	require.NoError(t, err)

	qr, err := conn.ExecuteFetch(`set session session_track_gtids='own_gtid'`, 1000, false)
	require.NoError(t, err)
	require.Empty(t, qr.SessionStateChanges)

	qr, err = conn.ExecuteFetch(`create table vttest.t1(id bigint primary key)`, 1000, false)
	require.NoError(t, err)
	require.NotEmpty(t, qr.SessionStateChanges)

	qr, err = conn.ExecuteFetch(`insert into vttest.t1 values (1)`, 1000, false)
	require.NoError(t, err)
	require.NotEmpty(t, qr.SessionStateChanges)
}

func TestCachingSha2Password(t *testing.T) {
	ctx := context.Background()

	// connect as an existing user to create a user account with caching_sha2_password
	params := connParams
	conn, err := mysql.Connect(ctx, &params)
	expectNoError(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(`select true from information_schema.PLUGINS where PLUGIN_NAME='caching_sha2_password' and PLUGIN_STATUS='ACTIVE'`, 1, false)
	if err != nil {
		t.Errorf("select true from information_schema.PLUGINS failed: %v", err)
	}

	if len(qr.Rows) != 1 {
		t.Skip("Server does not support caching_sha2_password plugin")
	}

	// create a user using caching_sha2_password password
	if _, err = conn.ExecuteFetch(`create user 'sha2user'@'localhost' identified with caching_sha2_password by 'password';`, 0, false); err != nil {
		t.Fatalf("Create user with caching_sha2_password failed: %v", err)
	}
	conn.Close()

	// connect as sha2user
	params.Uname = "sha2user"
	params.Pass = "password"
	params.DbName = "information_schema"
	conn, err = mysql.Connect(ctx, &params)
	expectNoError(t, err)
	defer conn.Close()

	if qr, err = conn.ExecuteFetch(`select user()`, 1, true); err != nil {
		t.Fatalf("select user() failed: %v", err)
	}

	if len(qr.Rows) != 1 || qr.Rows[0][0].ToString() != "sha2user@localhost" {
		t.Errorf("Logged in user is not sha2user")
	}
}
