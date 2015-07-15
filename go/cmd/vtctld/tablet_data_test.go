package main

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/tabletserver/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"
	"github.com/youtube/vitess/go/vt/zktopo"

	pb "github.com/youtube/vitess/go/vt/proto/query"
)

// streamHealthSQLQuery is a local QueryService implementation to support the tests
type streamHealthSQLQuery struct {
	queryservice.ErrorQueryService
	t *testing.T

	// streamHealthMutex protects all the following fields
	streamHealthMutex sync.Mutex
	streamHealthIndex int
	streamHealthMap   map[int]chan<- *pb.StreamHealthResponse
}

func newStreamHealthSQLQuery(t *testing.T) *streamHealthSQLQuery {
	return &streamHealthSQLQuery{
		t:               t,
		streamHealthMap: make(map[int]chan<- *pb.StreamHealthResponse),
	}
}

func (s *streamHealthSQLQuery) count() int {
	s.streamHealthMutex.Lock()
	defer s.streamHealthMutex.Unlock()
	return len(s.streamHealthMap)
}

func (s *streamHealthSQLQuery) StreamHealthRegister(c chan<- *pb.StreamHealthResponse) (int, error) {
	s.streamHealthMutex.Lock()
	defer s.streamHealthMutex.Unlock()

	id := s.streamHealthIndex
	s.streamHealthIndex++
	s.streamHealthMap[id] = c
	return id, nil
}

func (s *streamHealthSQLQuery) StreamHealthUnregister(id int) error {
	s.streamHealthMutex.Lock()
	defer s.streamHealthMutex.Unlock()

	delete(s.streamHealthMap, id)
	return nil
}

// BroadcastHealth will broadcast the current health to all listeners
func (s *streamHealthSQLQuery) BroadcastHealth(terTimestamp int64, stats *pb.RealtimeStats) {
	shr := &pb.StreamHealthResponse{
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
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)

	tablet1 := testlib.NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	tablet1.StartActionLoop(t, wr)
	defer tablet1.StopActionLoop(t)
	shsq := newStreamHealthSQLQuery(t)
	grpcqueryservice.RegisterForTest(tablet1.RPCServer, shsq)

	thc := newTabletHealthCache(ts)

	stats := &pb.RealtimeStats{
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
