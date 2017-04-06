package heartbeat

import (
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"golang.org/x/net/context"
)

// TestReaderReadHeartbeat tests that reading a heartbeat sets the appropriate
// fields on the object.
func TestReaderReadHeartbeat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	r := newReader(db, mockNowFunc)

	db.AddQuery(fmt.Sprintf(sqlFetchMostRecentHeartbeat, r.dbName), &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "ts", Type: sqltypes.Int64},
			{Name: "master_uid", Type: sqltypes.Uint32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", now.Add(-10*time.Second).UnixNano()))),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte("1111")),
		}},
	})

	lagNs.Set(0)
	readErrors.Set(0)
	reads.Set(0)

	r.readHeartbeat()
	lag, err := r.GetLatest()

	if err != nil {
		t.Fatalf("Should not be in error: %v", r.lastKnownError)
	}
	if got, want := lag, 10*time.Second; got != want {
		t.Fatalf("wrong latest lag: got = %v, want = %v", r.lastKnownLag, want)
	}
	if got, want := lagNs.Get(), 10*time.Second.Nanoseconds(); got != want {
		t.Fatalf("wrong cumulative lag: got = %v, want = %v", got, want)
	}
	if got, want := reads.Get(), int64(1); got != want {
		t.Fatalf("wrong read count: got = %v, want = %v", got, want)
	}
	if got, want := readErrors.Get(), int64(0); got != want {
		t.Fatalf("wrong read error count: got = %v, want = %v", got, want)
	}
}

func TestReaderReadHeartbeatError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	r := newReader(db, mockNowFunc)

	lagNs.Set(0)
	readErrors.Set(0)

	r.readHeartbeat()
	lag, err := r.GetLatest()

	if err == nil {
		t.Fatalf("Should be in error: %v", r.lastKnownError)
	}
	if got, want := lag, 0*time.Second; got != want {
		t.Fatalf("wrong lastKnownLag: got = %v, want = %v", got, want)
	}
	if got, want := lagNs.Get(), int64(0); got != want {
		t.Fatalf("wrong cumulative lag: got = %v, want = %v", got, want)
	}
	if got, want := readErrors.Get(), int64(1); got != want {
		t.Fatalf("wrong read error count: got = %v, want = %v", got, want)
	}
}

func newReader(db *fakesqldb.DB, nowFunc func() time.Time) *Reader {
	pool := dbconnpool.NewConnectionPool("HeartbeatPool", 1, 30*time.Second)
	pool.Open(dbconnpool.DBConnectionCreator(db.ConnParams(), stats.NewTimings("")))
	fakeMysql := mysqlctl.NewFakeMysqlDaemon(db)
	fakeMysql.DbAppConnectionFactory = func() (dbconnpool.PoolConnection, error) {
		return pool.Get(context.Background())
	}

	r := NewReader(fakeMysql, "_vt")
	r.now = nowFunc
	return r
}
