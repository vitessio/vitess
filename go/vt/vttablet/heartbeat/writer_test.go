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

// TestCreateSchema tests that we retry until we succeed
// at initializing the schema and that our initial INSERT uses
// the proper arguments
func TestCreateSchema(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	te := newTestWriter(db, mockNowFunc)
	defer te.Close()

	db.AddQuery(fmt.Sprintf(sqlCreateHeartbeatTable, te.dbName), &sqltypes.Result{})
	db.AddQuery(fmt.Sprintf(sqlInsertInitialRow, te.dbName, now.UnixNano(), 1111), &sqltypes.Result{})

	// Override to force faster retries
	initTableRetryInterval = 50 * time.Millisecond

	// Give us enough time to retry then cancel to ensure we don't run forever
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := te.waitForTables(ctx, db.ConnParams())
	defer te.Close()
	if err == nil {
		t.Error("Should be in error")
	}

	createSQL := fmt.Sprintf(sqlCreateSidecarDB, te.dbName)
	if numCalls := db.GetQueryCalledNum(createSQL); numCalls < 2 {
		t.Errorf("Expected at least 2 calls (1 fail, 1+ failed retries before cancel) but got %v", numCalls)
	}

	db.AddQuery(createSQL, &sqltypes.Result{})

	ctx, cancel = context.WithCancel(context.Background())
	err = te.waitForTables(ctx, db.ConnParams())
	if err != nil {
		t.Errorf("Should not be in error: %v", err)
	}

	if numCalls := db.GetQueryCalledNum(createSQL); numCalls != 1 {
		t.Errorf("Expected at least 1 call but got %v", numCalls)
	}

	if numErrors := writeErrorCount.Get(); numErrors < 2 {
		t.Errorf("Expected at least 2 errors recorded, but got %v", numErrors)
	}
}

// TestWriteHearbeat ensures the proper arguments for the UPDATE query
// and writes get recorded in counters
func TestWriteHeartbeat(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	te := newTestWriter(db, mockNowFunc)
	db.AddQuery(fmt.Sprintf(sqlUpdateHeartbeat, te.dbName, now.UnixNano(), 1111), &sqltypes.Result{})

	err := te.writeHeartbeat(context.Background())
	if err != nil {
		t.Errorf("Should not be in error: %v", err)
	}

	if num := writeCount.Get(); num != 1 {
		t.Errorf("Expected 1 write recorded, but got %v", num)
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
