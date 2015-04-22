package tabletserver

import (
	"os"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/stats"
	"golang.org/x/net/context"
)

var (
	appParams = &sqldb.ConnParams{
		Uname:      "vt_app",
		DbName:     "sougou",
		UnixSocket: os.Getenv("VTDATAROOT") + "/vt_0000062347/mysql.sock",
		Charset:    "utf8",
	}
	dbaParams = &sqldb.ConnParams{
		Uname:      "vt_dba",
		DbName:     "sougou",
		UnixSocket: os.Getenv("VTDATAROOT") + "/vt_0000062347/mysql.sock",
		Charset:    "utf8",
	}
)

// TestConnectivity is a manual test that requires code changes.
// You first need to bring up a mysql instance to match the connection params.
// Setup: mysqlctl -tablet_uid=62347 -mysql_port=15001 init.
// Connect: mysql -S vt_0000062347/mysql.sock -u vt_dba
// Initialize: create database sougou; use sougou; create table a(id int, primary key(id));
// Run the test normally once. Then you add a 20s sleep in dbconn.execOnce
// and run it again. You also have to check the code coverage to see that all critical
// paths were covered.
// Shutdown: mysqlctl -tablet_uid=62347 -mysql_port=15001 teardown
// TODO(sougou): Figure out a way to automate this.
func TestConnectivity(t *testing.T) {
	t.Skip("manual test")
	ctx := context.Background()
	killStats = stats.NewCounters("TestKills")
	internalErrors = stats.NewCounters("TestInternalErrors")
	mysqlStats = stats.NewTimings("TestMySQLStats")
	pool := NewConnPool("p1", 1, 30*time.Second, false)
	pool.Open(appParams, dbaParams)

	conn, err := pool.Get(ctx)
	if err != nil {
		t.Error(err)
	}
	conn.Kill()

	newctx, cancel := withTimeout(ctx, 2*time.Second)
	_, err = conn.Exec(newctx, "select * from a", 1000, true)
	cancel()
	if err != nil {
		t.Error(err)
	}
	conn.Close()

	newctx, cancel = withTimeout(ctx, 2*time.Second)
	_, err = conn.Exec(newctx, "select * from a", 1000, true)
	cancel()
	// You'll get a timedout error in slow mode. Otherwise, this should succeed.
	timedout := "error: setDeadline: timed out"
	if err != nil && err.Error() != timedout {
		t.Errorf("got: %v, want nil or %s", err, timedout)
	}
	conn.Recycle()

	conn, err = pool.Get(ctx)
	if err != nil {
		t.Error(err)
	}
	ch := make(chan bool)
	go func() {
		newctx, cancel = withTimeout(ctx, 2*time.Millisecond)
		_, err = conn.Exec(newctx, "select sleep(1) from dual", 1000, true)
		cancel()
		lostConn := "error: Lost connection to MySQL server during query (errno 2013) during query: select sleep(1) from dual"
		if err == nil || err.Error() != lostConn {
			t.Errorf("got: %v, want %s", err, lostConn)
		}
		ch <- true
	}()
	time.Sleep(40 * time.Millisecond)
	conn.Kill()
	<-ch
	conn.Recycle()

	conn, err = pool.Get(ctx)
	if err != nil {
		t.Error(err)
	}
	ch = make(chan bool)
	go func() {
		newctx, cancel = withTimeout(ctx, 2*time.Millisecond)
		err = conn.Stream(newctx, "select sleep(1) from dual", func(*mproto.QueryResult) error {
			return nil
		}, 4096)
		cancel()
		lostConn := "Lost connection to MySQL server during query (errno 2013) during query: select sleep(1) from dual"
		if err == nil || err.Error() != lostConn {
			t.Errorf("got: %v, want %s", err, lostConn)
		}
		ch <- true
	}()
	time.Sleep(40 * time.Millisecond)
	conn.Kill()
	<-ch
	conn.Recycle()

	pool.Close()
}
