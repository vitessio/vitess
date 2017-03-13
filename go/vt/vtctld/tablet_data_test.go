package vtctld

import (
	"io"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttablet/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// streamHealthTabletServer is a local QueryService implementation to support the tests
type streamHealthTabletServer struct {
	queryservice.QueryService
	t *testing.T

	// streamHealthMutex protects all the following fields
	streamHealthMutex sync.Mutex
	streamHealthIndex int
	streamHealthMap   map[int]chan<- *querypb.StreamHealthResponse
}

func newStreamHealthTabletServer(t *testing.T) *streamHealthTabletServer {
	return &streamHealthTabletServer{
		QueryService:    fakes.ErrorQueryService,
		t:               t,
		streamHealthMap: make(map[int]chan<- *querypb.StreamHealthResponse),
	}
}

func (s *streamHealthTabletServer) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	id, ch := s.streamHealthRegister()
	defer s.streamHealthUnregister(id)
	for shr := range ch {
		if err := callback(shr); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *streamHealthTabletServer) streamHealthRegister() (id int, ch chan *querypb.StreamHealthResponse) {
	s.streamHealthMutex.Lock()
	defer s.streamHealthMutex.Unlock()

	id = s.streamHealthIndex
	s.streamHealthIndex++
	ch = make(chan *querypb.StreamHealthResponse, 10)
	s.streamHealthMap[id] = ch
	return id, ch
}

func (s *streamHealthTabletServer) streamHealthUnregister(id int) error {
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
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())

	if err := ts.CreateKeyspace(context.Background(), "ks", &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	tablet1 := testlib.NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil, testlib.TabletKeyspaceShard(t, "ks", "-80"))
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
