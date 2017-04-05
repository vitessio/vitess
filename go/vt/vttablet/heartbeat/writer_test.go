package heartbeat

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"golang.org/x/net/context"
)

var (
	now         = time.Now()
	mockNowFunc = func() time.Time {
		return now
	}
)

// TestCreateSchema tests that our initial INSERT uses
// the proper arguments.
func TestCreateSchema(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	te := newTestWriter(db, mockNowFunc)
	defer te.Close()

	db.AddQuery(fmt.Sprintf(sqlCreateSidecarDB, te.dbName), &sqltypes.Result{})
	db.AddQuery(fmt.Sprintf(sqlCreateHeartbeatTable, te.dbName), &sqltypes.Result{})
	db.AddQuery(fmt.Sprintf(sqlInsertInitialRow, te.dbName, now.UnixNano(), 1111), &sqltypes.Result{})

	writes.Set(0)
	if err := te.initializeTables(db.ConnParams()); err != nil {
		t.Fatalf("Should not be in error: %v", err)
	}

	if numWrites, want := writes.Get(), int64(1); numWrites != want {
		t.Fatalf("wrong writes count: got = %v, want = %v", numWrites, want)
	}
}

// TestWriteHearbeat ensures the proper arguments for the UPDATE query
// and writes get recorded in counters.
func TestWriteHeartbeat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	te := newTestWriter(db, mockNowFunc)
	db.AddQuery(fmt.Sprintf(sqlUpdateHeartbeat, te.dbName, now.UnixNano(), 1111), &sqltypes.Result{})

	writes.Set(0)
	err := te.writeHeartbeat(context.Background())
	if err != nil {
		t.Fatalf("Should not be in error: %v", err)
	}

	if got, want := writes.Get(), int64(1); got != want {
		t.Fatalf("wrong writes count: got = %v; want = %v", got, want)
	}
}

func newTestWriter(db *fakesqldb.DB, nowFunc func() time.Time) *Writer {
	flag.Set("enable_heartbeat", "true")
	randID := rand.Int63()
	config := tabletenv.DefaultQsConfig
	config.PoolNamePrefix = fmt.Sprintf("Pool-%d-", randID)

	tw := NewWriter(&fakeMysqlChecker{}, topodatapb.TabletAlias{Cell: "test", Uid: 1111}, config)
	tw.now = nowFunc

	dbc := dbconfigs.DBConfigs{
		App:           *db.ConnParams(),
		Dba:           *db.ConnParams(),
		SidecarDBName: "_vt",
	}

	tw.dbName = sqlparser.Backtick(dbc.SidecarDBName)
	tw.pool.Open(&dbc.App, &dbc.Dba)

	return tw
}

type fakeMysqlChecker struct{}

func (f fakeMysqlChecker) CheckMySQL() {}
