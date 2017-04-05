package heartbeat

import (
	"fmt"
	"testing"
	"time"

	"reflect"

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

	if want := uint32(1111); r.lastKnownMaster != want {
		t.Fatalf("wrong lastKnownMaster: got = %v, want = %v", r.lastKnownMaster, want)
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
			{Name: "master_uid", Type: sqltypes.Uint32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", now.Add(-10*time.Second).UnixNano()))),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", defaultUID))),
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
	maxWait, _ := context.WithDeadline(ctx, time.Now().Add(5*time.Second))
	for {
		if isMaster := func() bool {
			r.mu.Lock()
			defer r.mu.Unlock()
			return r.isMaster
		}(); isMaster {
			break
		}

		if maxWait.Done() {
			t.Fatal("Master state failed to change after event dispatch")
		}
	}

	// All values should be unchanged from before
	if err := r.readHeartbeat(ctx); err != nil {
		t.Fatalf("Should not be in error: %v", err)
	}

	lastKnownLag, lastKnownErr = r.GetLatest()

	if got, want := lagNs.Get(), 20*time.Second; got != want.Nanoseconds() {
		t.Fatalf("wrong cumulative lagNs: got = %v, want = %v", got, want)
	}
	if lastKnownErr != nil {
		t.Fatalf("Should not be in error: %v", lastKnownErr)
	}
	if want := 10 * time.Second; lastKnownLag != want {
		t.Fatalf("wrong last known lag: got = %v, want = %v", lastKnownLag, want)
	}
}

// TestReaderWrongMasterError tests that we throw an error
// when the most recent lag value is associated with a master uid that
// doesn't match what the TopoServer expects.
func TestReaderWrongMasterError(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	r := newReader(db, mockNowFunc)

	laggedValue := now.Add(-10 * time.Second).UnixNano()
	db.AddQuery(fmt.Sprintf(sqlFetchMostRecentHeartbeat, r.dbName), &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "ts", Type: sqltypes.Int64},
			{Name: "master_uid", Type: sqltypes.Uint32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", laggedValue))),
			sqltypes.MakeTrusted(sqltypes.Int64, []byte(fmt.Sprintf("%d", defaultUID))),
		}},
	})

	r.lastKnownMaster = 0

	ctx := context.Background()
	err := r.readHeartbeat(ctx)

	if err != nil {
		t.Fatalf("Should not be in error: %v", err)
	}
	if want := uint32(1111); r.lastKnownMaster != want {
		t.Fatalf("wrong lastKnownMaster: got = %v, want = %v", r.lastKnownMaster, want)
	}

	if fake, ok := r.topoServer.Impl.(*fakeTopo); ok {
		fake.fakeUID = uint32(2222)
	}

	// readHeartbeat only checks the topo server if the query
	// result has a different masterUID than we currently know of.
	// Setting this to a different UID forces a call to GetShard,
	// at which point we will see that GetShard result 2222 does not
	// match query result 1111.
	r.lastKnownMaster = 3333

	err, want := r.readHeartbeat(ctx), fmt.Errorf("Latest heartbeat is not from known master %v, with ts=%v, master_uid=%v", 2222, laggedValue, 1111)
	if err == nil || !reflect.DeepEqual(err, want) {
		t.Fatalf("Should be in error: got = %v; want = %v", err, want)
	}
	if got, want := r.lastKnownMaster, uint32(3333); got != want {
		t.Fatalf("wrong lastKnownMaster: got = %v, want = %v", got, want)
	}
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
