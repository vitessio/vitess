package heartbeat

import (
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconnpool"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletmanager/events"
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
	if err := r.readHeartbeat(context.Background()); err != nil {
		t.Fatalf("Should not be in error: %v", err)
	}

	if r.lastKnownError != nil {
		t.Fatalf("Should not be in error: %v", r.lastKnownError)
	}

	if want := 10 * time.Second; r.lastKnownLag != want {
		t.Fatalf("wrong lastKnownLag: got = %v, want = %v", r.lastKnownLag, want)
	}
}

// TestReaderTabletTypeChange tests that we can respond to a StateChange event,
// and that we do not check lag when running as a master.
func TestReaderTabletTypeChange(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	r := newReader(db, mockNowFunc)

	fetchQuery := fmt.Sprintf(sqlFetchMostRecentHeartbeat, r.dbName)
	db.AddQuery(fetchQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "ts", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", now.Add(-10*time.Second).UnixNano()))),
		}},
	})

	// Run twice to ensure we're accumulating appropriately
	lagNs.Set(0)
	ctx := context.Background()
	if err := r.readHeartbeat(ctx); err != nil {
		t.Fatalf("Should not be in error: %v", err)
	}
	if err := r.readHeartbeat(ctx); err != nil {
		t.Fatalf("Should not be in error: %v", err)
	}

	lastKnownLag, lastKnownErr := r.GetLatest()

	if got, want := lagNs.Get(), 20*time.Second; got != want.Nanoseconds() {
		t.Fatalf("wrong cumulative lagNs: got = %v, want = %v", got, want.Nanoseconds())
	}
	if lastKnownErr != nil {
		t.Fatalf("Should not be in error: %v", lastKnownErr)
	}
	if want := 10 * time.Second; lastKnownLag != want {
		t.Fatalf("wrong last known lag: got = %v, want = %v", lastKnownLag, want)
	}

	r.registerTabletStateListener()
	event.Dispatch(&events.StateChange{NewTablet: topodatapb.Tablet{Type: topodatapb.TabletType_MASTER}})

	// Wait for master to switch on object
	start := time.Now()
	for {
		if r.isMaster() {
			break
		}

		if time.Since(start) > 5*time.Second {
			t.Fatal("Reader did not update its isMaster state after event dispatch")
		}
		time.Sleep(1 * time.Millisecond)
	}

	// All values should be unchanged from before
	if err := r.readHeartbeat(ctx); err != nil {
		t.Fatalf("Should not be in error: %v", err)
	}

	lastKnownLag, lastKnownErr = r.GetLatest()

	if got, want := lagNs.Get(), 20*time.Second.Nanoseconds(); got != want {
		t.Fatalf("wrong cumulative lagNs: got = %v, want = %v", got, want)
	}
	if lastKnownErr != nil {
		t.Fatalf("Should not be in error: %v", lastKnownErr)
	}
	if want := 10 * time.Second; lastKnownLag != want {
		t.Fatalf("wrong last known lag: got = %v, want = %v", lastKnownLag, want)
	}
}

func newReader(db *fakesqldb.DB, nowFunc func() time.Time) *Reader {
	pool := dbconnpool.NewConnectionPool("HeartbeatPool", 1, 30*time.Second)
	pool.Open(dbconnpool.DBConnectionCreator(db.ConnParams(), stats.NewTimings("")))
	fakeMysql := mysqlctl.NewFakeMysqlDaemon(db)
	fakeMysql.DbAppConnectionFactory = func() (dbconnpool.PoolConnection, error) {
		return pool.Get(context.Background())
	}

	r := NewReader(fakeMysql, &topodatapb.Tablet{Keyspace: "test", Shard: "0"})
	r.now = nowFunc
	r.dbName = sqlparser.Backtick("_vt")
	return r
}
