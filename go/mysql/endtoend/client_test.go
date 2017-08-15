package endtoend

import (
	"fmt"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
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

	errChan := make(chan error)
	go func() {
		_, err = conn.ExecuteFetch("select sleep(10) from dual", 1000, false)
		errChan <- err
		close(errChan)
	}()

	killConn, err := mysql.Connect(ctx, &connParams)
	if err != nil {
		t.Fatal(err)
	}
	defer killConn.Close()

	if _, err := killConn.ExecuteFetch(fmt.Sprintf("kill %v", conn.ConnectionID), 1000, false); err != nil {
		t.Fatalf("Kill(%v) failed: %v", conn.ConnectionID, err)
	}

	err = <-errChan
	assertSQLError(t, err, mysql.CRServerLost, mysql.SSUnknownSQLState, "EOF", "select sleep(10) from dual")
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
