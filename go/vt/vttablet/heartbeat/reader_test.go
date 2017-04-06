package heartbeat

import (
	"fmt"
	"testing"
	"time"

	"math/rand"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// TestReaderReadHeartbeat tests that reading a heartbeat sets the appropriate
// fields on the object.
func TestReaderReadHeartbeat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tr := newReader(db, mockNowFunc)
	defer tr.Close()

	db.AddQuery(fmt.Sprintf(sqlFetchMostRecentHeartbeat, tr.dbName), &sqltypes.Result{
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

	tr.readHeartbeat()
	lag, err := tr.GetLatest()

	if err != nil {
		t.Fatalf("Should not be in error: %v", tr.lastKnownError)
	}
	if got, want := lag, 10*time.Second; got != want {
		t.Fatalf("wrong latest lag: got = %v, want = %v", tr.lastKnownLag, want)
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

// TestReaderReadHeartbeatError tests that we properly account for errors
// encountered in the reading of heartbeat.
func TestReaderReadHeartbeatError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	tr := newReader(db, mockNowFunc)
	defer tr.Close()

	lagNs.Set(0)
	readErrors.Set(0)

	tr.readHeartbeat()
	lag, err := tr.GetLatest()

	if err == nil {
		t.Fatalf("Should be in error: %v", tr.lastKnownError)
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
	*enableHeartbeat = true
	randID := rand.Int63()
	config := tabletenv.DefaultQsConfig
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)
	dbc := dbconfigs.DBConfigs{
		App:           *db.ConnParams(),
		Dba:           *db.ConnParams(),
		SidecarDBName: "_vt",
	}

	tr := NewReader(&fakeMysqlChecker{}, config)
	tr.dbName = sqlparser.Backtick(dbc.SidecarDBName)
	tr.now = nowFunc
	tr.pool.Open(&dbc.App, &dbc.Dba)

	return tr
}
