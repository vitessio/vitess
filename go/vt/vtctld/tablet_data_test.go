package vtctld

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// streamHealthTabletServer is a local QueryService implementation to support the tests
type streamHealthTabletServer struct {
	fakes.ErrorQueryService
	t *testing.T

	// streamHealthMutex protects all the following fields
	streamHealthMutex sync.Mutex
	streamHealthIndex int
	streamHealthMap   map[int]chan<- *querypb.StreamHealthResponse
}

func newStreamHealthTabletServer(t *testing.T) *streamHealthTabletServer {
	return &streamHealthTabletServer{
		t:               t,
		streamHealthMap: make(map[int]chan<- *querypb.StreamHealthResponse),
	}
}

func (s *streamHealthTabletServer) StreamHealthRegister(c chan<- *querypb.StreamHealthResponse) (int, error) {
	s.streamHealthMutex.Lock()
	defer s.streamHealthMutex.Unlock()

	id := s.streamHealthIndex
	s.streamHealthIndex++
	s.streamHealthMap[id] = c
	return id, nil
}

func (s *streamHealthTabletServer) StreamHealthUnregister(id int) error {
	s.streamHealthMutex.Lock()
	defer s.streamHealthMutex.Unlock()

	delete(s.streamHealthMap, id)
	return nil
}

// BroadcastHealth will broadcast the current health to all listeners
func (s *streamHealthTabletServer) BroadcastHealth(terTimestamp int64, stats *querypb.RealtimeStats) {
	shr := &querypb.StreamHealthResponse{
		TabletExternallyReparentedTimestamp: terTimestamp,
		RealtimeStats:                       stats,
	}

	s.streamHealthMutex.Lock()
	defer s.streamHealthMutex.Unlock()
	for _, c := range s.streamHealthMap {
		c <- shr
	}
}

func TestTabletData(t *testing.T) {
	db := fakesqldb.Register()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	tablet1 := testlib.NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, db, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	tablet1.StartActionLoop(t, wr)
	defer tablet1.StopActionLoop(t)
	shsq := newStreamHealthTabletServer(t)
	grpcqueryservice.Register(tablet1.RPCServer, shsq)

	thc := newTabletHealthCache(ts)

	stats := &querypb.RealtimeStats{
		HealthError:         "testHealthError",
		SecondsBehindMaster: 72,
		CpuUsage:            1.1,
	}

	// Keep broadcasting until the first result goes through.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				shsq.BroadcastHealth(42, stats)
			}
		}
	}()

	// Start streaming and wait for the first result.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	result, err := thc.Get(ctx, tablet1.Tablet.Alias)
	cancel()
	close(stop)

	if err != nil {
		t.Fatalf("thc.Get failed: %v", err)
	}
	if got, want := result.RealtimeStats, stats; !proto.Equal(got, want) {
		t.Errorf("RealtimeStats = %#v, want %#v", got, want)
	}
}
