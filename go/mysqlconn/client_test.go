package mysqlconn

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/vttest"
)

// assertSQLError makes sure we get the right error.
func assertSQLError(t *testing.T, err error, code int, sqlState string, subtext string) {
	if err == nil {
		t.Fatalf("was expecting SQLError %v / %v / %v but got no error.", code, sqlState, subtext)
	}
	serr, ok := err.(*sqldb.SQLError)
	if !ok {
		t.Fatalf("was expecting SQLError %v / %v / %v but got: %v", code, sqlState, subtext, err)
	}
	if serr.Num != code {
		t.Fatalf("was expecting SQLError %v / %v / %v but got code %v", code, sqlState, subtext, serr.Num)
	}
	if serr.State != sqlState {
		t.Fatalf("was expecting SQLError %v / %v / %v but got state %v", code, sqlState, subtext, serr.State)
	}
	if subtext != "" && !strings.Contains(serr.Message, subtext) {
		t.Fatalf("was expecting SQLError %v / %v / %v but got message %v", code, sqlState, subtext, serr.Message)

	}
}

// TestConnectTimeout runs connection failure scenarios against a
// server that's not listening or has trouble.  This test is not meant
// to use a valid server. So we do not test bad handshakes here.
func TestConnectTimeout(t *testing.T) {
	// Create a socket, but it's not accepting. So all Dial
	// attempts will timeout.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}
	host := listener.Addr().(*net.TCPAddr).IP.String()
	port := listener.Addr().(*net.TCPAddr).Port
	params := &sqldb.ConnParams{
		Host: host,
		Port: port,
	}
	defer listener.Close()

	// Test that canceling the context really interrupts the Connect.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, err := Connect(ctx, params)
		if err != context.Canceled {
			t.Errorf("Was expecting context.Canceled but got: %v", err)
		}
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Tests a connection timeout works.
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = Connect(ctx, params)
	cancel()
	if err != context.DeadlineExceeded {
		t.Errorf("Was expecting context.DeadlineExceeded but got: %v", err)
	}

	// Now the server will listen, but close all connections on accept.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Listener was closed.
				return
			}
			conn.Close()
		}
	}()
	ctx = context.Background()
	_, err = Connect(ctx, params)
	assertSQLError(t, err, CRServerLost, SSUnknownSQLState, "initial packet read failed")

	// Now close the listener. Connect should fail right away,
	// check the error.
	listener.Close()
	wg.Wait()
	_, err = Connect(ctx, params)
	assertSQLError(t, err, CRConnHostError, SSUnknownSQLState, "connection refused")

	// Tests a connection where Dial to a unix socket fails
	// properly returns the right error. To simulate exactly the
	// right failure, try to dial a Unix socket that's just a temp file.
	fd, err := ioutil.TempFile("", "mysqlconn")
	if err != nil {
		t.Fatalf("cannot create TemFile: %v", err)
	}
	name := fd.Name()
	fd.Close()
	params.UnixSocket = name
	ctx = context.Background()
	_, err = Connect(ctx, params)
	os.Remove(name)
	assertSQLError(t, err, CRConnectionError, SSUnknownSQLState, "connection refused")
}

// testKillWithRealDatabase opens a connection, issues a command that
// will sleep for a few seconds, waits a bit for MySQL to start
// executing it, then kills the connection (using another
// connection). We make sure we get the right error code.
func testKillWithRealDatabase(t *testing.T, params *sqldb.ConnParams) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	errChan := make(chan error)
	go func() {
		_, err = conn.ExecuteFetch("select sleep(10) from dual", 1000, false)
		errChan <- err
		close(errChan)
	}()

	killConn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	defer killConn.Close()

	if _, err := killConn.ExecuteFetch(fmt.Sprintf("kill %v", conn.ConnectionID), 1000, false); err != nil {
		t.Fatalf("Kill(%v) failed: %v", conn.ConnectionID, err)
	}

	err = <-errChan
	assertSQLError(t, err, CRServerLost, SSUnknownSQLState, "EOF")
}

// testDupEntryWithRealDatabase tests a duplicate key is properly raised.
func testDupEntryWithRealDatabase(t *testing.T, params *sqldb.ConnParams) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
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
	assertSQLError(t, err, ERDupEntry, SSDupKey, "Duplicate entry")
}

// TestWithRealDatabase runs a real MySQL database, and runs all kinds
// of tests on it. To minimize overhead, we only run one database, and
// run all the tests on it.
func TestWithRealDatabase(t *testing.T) {
	hdl, err := vttest.LaunchVitess(
		vttest.MySQLOnly("vttest"),
		vttest.NoStderr())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = hdl.TearDown()
		if err != nil {
			t.Error(err)
		}
	}()
	params, err := hdl.MySQLConnParams()
	if err != nil {
		t.Error(err)
	}

	// Kill tests the query part of the API.
	t.Run("Kill", func(t *testing.T) {
		testKillWithRealDatabase(t, &params)
	})

	// DupEntry tests a duplicate key returns the right error.
	t.Run("DupEntry", func(t *testing.T) {
		testDupEntryWithRealDatabase(t, &params)
	})

	// Queries tests the query part of the API.
	t.Run("Queries", func(t *testing.T) {
		testQueriesWithRealDatabase(t, &params)
	})

	// Test replication client gets the right error when closed.
	t.Run("ReplicationClosingError", func(t *testing.T) {
		testReplicationConnectionClosing(t, &params)
	})

	// Test SBR replication client is working properly.
	t.Run("SBR", func(t *testing.T) {
		testStatementReplicationWithRealDatabase(t, &params)
	})

	// Test RBR replication client is working properly.
	t.Run("RBR", func(t *testing.T) {
		testRowReplicationWithRealDatabase(t, &params)
	})

	// Test Schema queries work as intended.
	t.Run("Schema", func(t *testing.T) {
		testSchema(t, &params)
	})
}
