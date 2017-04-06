package mysqlconn

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/tlstest"
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

// testKill2006WithRealDatabase opens a connection, kills the
// connection from the server side, then waits a bit, and tries to
// execute a command. We make sure we get the right error code.
func testKill2006WithRealDatabase(t *testing.T, params *sqldb.ConnParams) {
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}

	// Kill the connection from the server side.
	killConn, err := Connect(ctx, params)
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
	assertSQLError(t, err, CRServerGone, SSUnknownSQLState, "broken pipe")
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

// testTLS tests our client can connect via SSL.
func testTLS(t *testing.T, params *sqldb.ConnParams) {
	// First make sure the official 'mysql' client can connect.
	output, ok := runMysql(t, params, "status")
	if !ok {
		t.Fatalf("'mysql -e status' failed: %v", output)
	}
	if !strings.Contains(output, "Cipher in use is") {
		t.Fatalf("cannot connect via SSL: %v", output)
	}

	// Now connect with our client.
	ctx := context.Background()
	conn, err := Connect(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	result, err := conn.ExecuteFetch("SHOW STATUS LIKE 'Ssl_cipher'", 10, true)
	if err != nil {
		t.Fatalf("SHOW STATUS LIKE 'Ssl_cipher' failed: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0].String() != "Ssl_cipher" ||
		result.Rows[0][1].String() == "" {
		t.Fatalf("SHOW STATUS LIKE 'Ssl_cipher' returned unexpected result: %v", result)
	}
}

// TestWithRealDatabase runs a real MySQL database, and runs all kinds
// of tests on it. To minimize overhead, we only run one database, and
// run all the tests on it.
func TestWithRealDatabase(t *testing.T) {
	// Create the certs.
	root, err := ioutil.TempDir("", "TestTLSServer")
	if err != nil {
		t.Fatalf("TempDir failed: %v", err)
	}
	defer os.RemoveAll(root)
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "localhost")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the extra SSL my.cnf lines.
	cnf := fmt.Sprintf(`
ssl-ca=%v/ca-cert.pem
ssl-cert=%v/server-cert.pem
ssl-key=%v/server-key.pem
`, root, root, root)
	extraMyCnf := path.Join(root, "ssl_my.cnf")
	if err := ioutil.WriteFile(extraMyCnf, []byte(cnf), os.ModePerm); err != nil {
		t.Fatalf("ioutil.WriteFile(%v) failed: %v", extraMyCnf, err)
	}

	// Launch MySQL.
	hdl, err := vttest.LaunchVitess(
		vttest.MySQLOnly("vttest"),
		vttest.NoStderr(),
		vttest.ExtraMyCnf(extraMyCnf))
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

	// Kill tests killing a running query returns the right error.
	t.Run("Kill", func(t *testing.T) {
		testKillWithRealDatabase(t, &params)
	})

	// Kill2006 tests killing a connection with no running query
	// returns the right error.
	t.Run("Kill2006", func(t *testing.T) {
		testKill2006WithRealDatabase(t, &params)
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

	// Test RBR types are working properly.
	t.Run("RBRTypes", func(t *testing.T) {
		testRowReplicationTypesWithRealDatabase(t, &params)
	})

	// Test Schema queries work as intended.
	t.Run("Schema", func(t *testing.T) {
		testSchema(t, &params)
	})

	// Test SSL. First we make sure a real 'mysql' client gets it.
	params.Flags = CapabilityClientSSL
	params.SslCa = path.Join(root, "ca-cert.pem")
	params.SslCert = path.Join(root, "client-cert.pem")
	params.SslKey = path.Join(root, "client-key.pem")
	t.Run("TLS", func(t *testing.T) {
		testTLS(t, &params)
	})

	// Uncomment to sleep and be able to connect to MySQL
	// fmt.Printf("Connect to MySQL using parameters: %v\n", params)
	// time.Sleep(10 * time.Minute)
}
