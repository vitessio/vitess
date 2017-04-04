package heartbeat

import (
	"fmt"
	"sync"
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
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletmanager/events"
	"golang.org/x/net/context"
)

var defaultUID = uint32(1111)

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

	ctx := context.Background()
	err := r.readHeartbeat(ctx)

	if err != nil {
		t.Errorf("Should not be in error: %v", err)
	}

	if r.lastKnownError != nil {
		t.Errorf("Should not be in error: %v", r.lastKnownError)
	}

	if r.lastKnownMaster != 1111 {
		t.Errorf("Expected lastKnownMaster of 1111, got %v", r.lastKnownError)
	}

	if r.lastKnownLag != 10*time.Second {
		t.Errorf("Expected lag of 10s, got %v", r.lastKnownLag)
	}
}

// TestReaderTabletTypeChange tests that we can respond to a StateChange event,
// and that we do not check lag
func TestReaderTabletTypeChange(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	r := newReader(db, mockNowFunc)

	fetchQuery := fmt.Sprintf(sqlFetchMostRecentHeartbeat, r.dbName)
	db.AddQuery(fetchQuery, &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "ts", Type: sqltypes.Int64},
			{Name: "master_uid", Type: sqltypes.Uint32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", now.Add(-10*time.Second).UnixNano()))),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", defaultUID))),
		}},
	})

	r.wg = &sync.WaitGroup{}

	setInterval(time.Duration(5 * time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	var lagAtSwitch int64

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		time.Sleep(50 * time.Millisecond)
		event.Dispatch(&events.StateChange{NewTablet: topodatapb.Tablet{Type: topodatapb.TabletType_MASTER}})
		// Brief wait to account for already running loops
		time.Sleep(5 * time.Millisecond)
		// Record the lag and reject future fetch queries. At this point we should be no-oping because we're a master
		lagAtSwitch = lagNsCount.Get()
		db.AddRejectedQuery(fetchQuery, fmt.Errorf("Should not execute queries while tablet type is MASTER. Reader thinks it's master: %v", r.isMaster))
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	r.wg.Add(1)
	r.watchHeartbeat(ctx)

	r.wg.Wait()
	lastKnownLag, lastKnownErr := r.GetLatest()
	cumulativeLagAtEnd := lagNsCount.Get()

	if lastKnownErr != nil {
		t.Errorf("Should not be in error: %v", lastKnownErr)
	}
	if lastKnownLag != 10*time.Second {
		t.Errorf("Expected 10s lag, but got %d", lagAtSwitch)
	}
	if cumulativeLagAtEnd != lagAtSwitch {
		t.Errorf("Expected %d cumulative lag at end, but got %d", lagAtSwitch, cumulativeLagAtEnd)
	}
}

// TestReaderWrongMasterError tests that we throw an error
// when the most recent lag value is associated with a master uid that
// doesn't match what the TopoServer expects
func TestReaderWrongMasterError(t *testing.T) {
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
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", defaultUID))),
		}},
	})

	r.lastKnownMaster = 0

	ctx := context.Background()
	err := r.readHeartbeat(ctx)

	if err != nil {
		t.Errorf("Should not be in error: %v", err)
	}
	if r.lastKnownMaster != 1111 {
		t.Errorf("Expected 1111 for lastKnownMaster, but got %d", r.lastKnownMaster)
	}

	if fake, ok := r.topoServer.Impl.(*fakeTopo); ok {
		fake.fakeUID = uint32(2222)
	}

	r.lastKnownMaster = 3333

	err = r.readHeartbeat(ctx)

	if err == nil {
		t.Errorf("Should be in error: %v", err)
	}
	if r.lastKnownMaster != 3333 {
		t.Errorf("Expected 1111 for lastKnownMaster, but got %d", r.lastKnownMaster)
	}
}

func setInterval(duration time.Duration) {
	interval = &duration
}

func newReader(db *fakesqldb.DB, nowFunc func() time.Time) *Reader {
	pool := dbconnpool.NewConnectionPool("HeartbeatPool", 1, 30*time.Second)
	pool.Open(dbconnpool.DBConnectionCreator(db.ConnParams(), stats.NewTimings("")))
	fakeMysql := mysqlctl.NewFakeMysqlDaemon(db)
	fakeMysql.DbAppConnectionFactory = func() (dbconnpool.PoolConnection, error) {
		return pool.Get(context.Background())
	}

	r := NewReader(newFakeTopo(), fakeMysql, &topodatapb.Tablet{Keyspace: "test", Shard: "0"})
	r.now = nowFunc
	r.dbName = sqlparser.Backtick("_vt")
	return r
}

func newFakeTopo() topo.Server {
	return topo.Server{
		Impl: &fakeTopo{fakeUID: defaultUID},
	}
}

type fakeTopo struct {
	faketopo.FakeTopo
	fakeUID uint32
}

func (f fakeTopo) GetShard(ctx context.Context, keyspace string, shard string) (*topodatapb.Shard, int64, error) {
	return &topodatapb.Shard{MasterAlias: &topodatapb.TabletAlias{Cell: "test", Uid: f.fakeUID}}, 0, nil
}
