package endtoend

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
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
	assertSQLError(t, err, mysql.ERDupEntry, mysql.SSDupKey, "Duplicate entry", "insert into dup_entry(id, name) values(2, 10)")
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
	if err != nil {
		t.Fatalf("first update failed: %v", err)
	}
	if qr.RowsAffected != 1 {
		t.Errorf("First update: RowsAffected: %d, want 1", qr.RowsAffected)
	}
	qr, err = conn.ExecuteFetch("update found_rows set val=11 where id=1", 0, false)
	if err != nil {
		t.Fatalf("second update failed: %v", err)
	}
	if qr.RowsAffected != 1 {
		t.Errorf("Second update: RowsAffected: %d, want 1", qr.RowsAffected)
	}
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
	expectRows(t, "ExecuteMultiFetch(multi result)", qr, 1)

	qr, more, _, err = conn.ReadQueryResult(10, true)
	expectNoError(t, err)
	expectFlag(t, "ReadQueryResult(1)", more, true)
	expectRows(t, "ReadQueryResult(1)", qr, 0)

	qr, more, _, err = conn.ReadQueryResult(10, true)
	expectNoError(t, err)
	expectFlag(t, "ReadQueryResult(2)", more, false)
	expectRows(t, "ReadQueryResult(2)", qr, 1)

	qr, more, err = conn.ExecuteFetchMulti("select 1 from dual", 10, true)
	expectNoError(t, err)
	expectFlag(t, "ExecuteMultiFetch(single result)", more, false)
	expectRows(t, "ExecuteMultiFetch(single result)", qr, 1)

	qr, more, err = conn.ExecuteFetchMulti("set autocommit=1", 10, true)
	expectNoError(t, err)
	expectFlag(t, "ExecuteMultiFetch(no result)", more, false)
	expectRows(t, "ExecuteMultiFetch(no result)", qr, 0)

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
	if err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Errorf("create table returned RowsAffected %v, was expecting 0", result.RowsAffected)
	}

	for i := 0; i < 255; i++ {
		result, err := conn.ExecuteFetch(fmt.Sprintf("insert into a(id, name) values(%v, 'nice name %v')", 1000+i, i), 1000, true)
		if err != nil {
			t.Fatalf("ExecuteFetch(%v) failed: %v", i, err)
		}
		if result.RowsAffected != 1 {
			t.Errorf("insert into returned RowsAffected %v, was expecting 1", result.RowsAffected)
		}
	}

	qr, more, err = conn.ExecuteFetchMulti("update a set name = concat(name, ' updated'); select * from a; select count(*) from a", 300, true)
	expectNoError(t, err)
	expectFlag(t, "ExecuteMultiFetch(multi result)", more, true)
	expectRows(t, "ExecuteMultiFetch(multi result)", qr, 255)

	qr, more, _, err = conn.ReadQueryResult(300, true)
	expectNoError(t, err)
	expectFlag(t, "ReadQueryResult(1)", more, true)
	expectRows(t, "ReadQueryResult(1)", qr, 255)

	qr, more, _, err = conn.ReadQueryResult(300, true)
	expectNoError(t, err)
	expectFlag(t, "ReadQueryResult(2)", more, false)
	expectRows(t, "ReadQueryResult(2)", qr, 1)

	_, err = conn.ExecuteFetch("drop table a", 10, true)
	if err != nil {
		t.Fatalf("drop table failed: %v", err)
	}
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

func expectRows(t *testing.T, msg string, result *sqltypes.Result, want int) {
	t.Helper()
	if int(result.RowsAffected) != want {
		t.Errorf("%s: %d, want %d", msg, result.RowsAffected, want)
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

func TestSlaveStatus(t *testing.T) {
	params := connParams
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	status, err := conn.ShowSlaveStatus()
	if err != mysql.ErrNotSlave {
		t.Errorf("Got unexpected result for ShowSlaveStatus: %v %v", status, err)
	}
}
