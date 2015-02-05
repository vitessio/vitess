package tabletserver

import (
	"os"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/stats"
)

var (
	appParams = &mysql.ConnectionParams{
		Uname:      "vt_app",
		DbName:     "sougou",
		UnixSocket: os.Getenv("VTDATAROOT") + "/vt_0000062347/mysql.sock",
		Charset:    "utf8",
	}
	dbaParams = &mysql.ConnectionParams{
		Uname:      "vt_dba",
		DbName:     "sougou",
		UnixSocket: os.Getenv("VTDATAROOT") + "/vt_0000062347/mysql.sock",
		Charset:    "utf8",
	}
)

// TestConnectivity is a manual test that requires code changes.
// You first need to bring up a mysql instance to match the connection params.
// Run the test normally once. Then you add a 20s sleep in dbconn.execOnce
// and run it again. You also have to check the code coverage to see that all critical
// paths were covered.
// TODO(sougou): Figure out a way to automoatae this.
func TestConnectivity(t *testing.T) {
	killStats = stats.NewCounters("TestKills")
	internalErrors = stats.NewCounters("TestInternalErrors")
	mysqlStats = stats.NewTimings("TestMySQLStats")
	pool := NewConnPool("p1", 1, 30*time.Second)
	pool.Open(appParams, dbaParams)
	conn, err := pool.Get(0)
	if err != nil {
		t.Error(err)
	}
	conn.Kill()
	_, err = conn.Exec("select * from a", 1000, true, NewDeadline(2*time.Second))
	if err != nil {
		t.Error(err)
	}
	conn.Close()
	_, err = conn.Exec("select * from a", 1000, true, NewDeadline(2*time.Second))
	// You'll get a timedout error in slow mode. Otherwise, this should succeed.
	timedout := "error: SetDeadline: timed out"
	if err != nil && err.Error() != timedout {
		t.Errorf("got: %v, want nil or %s", err, timedout)
	}
	conn.Recycle()
	conn, err = pool.Get(0)
	if err != nil {
		t.Error(err)
	}
	_, err = conn.Exec("select id from a", 1000, true, NewDeadline(2*time.Nanosecond))
	if err == nil || err.Error() != timedout {
		t.Errorf("got: %v, want %s", err, timedout)
	}
	conn.Recycle()
	conn, err = pool.Get(0)
	if err != nil {
		t.Error(err)
	}
	ch := make(chan bool)
	go func() {
		_, err = conn.Exec("select sleep(1) from dual", 1000, true, NewDeadline(20*time.Millisecond))
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
	pool.Close()
}
