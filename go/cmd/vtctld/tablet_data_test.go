package main

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

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
		// do not block on any write
		select {
		case c <- shr:
		default:
		}
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

	// get the first result, it's not containing any data but the alias
	result, err := thc.get(tablet1.Tablet.Alias)
	if err != nil {
		t.Fatalf("thc.get failed: %v", err)
	}
	var unpacked TabletHealth
	if err := json.Unmarshal(result, &unpacked); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if unpacked.TabletAlias != tablet1.Tablet.Alias {
		t.Fatalf("wrong alias: %v", &unpacked)
	}
	if unpacked.Version != 1 {
		t.Errorf("wrong version, got %v was expecting 1", unpacked.Version)
	}

	// wait for the streaming RPC to be established
	timeout := 5 * time.Second
	for {
		if shsq.count() > 0 {
			break
		}
		timeout -= 10 * time.Millisecond
		if timeout < 0 {
			t.Fatalf("timeout waiting for streaming RPC to be established")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// feed some data from the tablet, with just a data marker
	shsq.BroadcastHealth(42, &pb.RealtimeStats{
		HealthError:         "testHealthError",
		SecondsBehindMaster: 72,
		CpuUsage:            1.1,
	})

	// and wait for the cache to pick it up
	timeout = 5 * time.Second
	for {
		result, err = thc.get(tablet1.Tablet.Alias)
		if err != nil {
			t.Fatalf("thc.get failed: %v", err)
		}
		if err := json.Unmarshal(result, &unpacked); err != nil {
			t.Fatalf("bad json: %v", err)
		}
		if unpacked.StreamHealthResponse != nil &&
			unpacked.StreamHealthResponse.RealtimeStats != nil &&
			unpacked.StreamHealthResponse.RealtimeStats.HealthError == "testHealthError" &&
			unpacked.StreamHealthResponse.RealtimeStats.SecondsBehindMaster == 72 &&
			unpacked.StreamHealthResponse.RealtimeStats.CpuUsage == 1.1 {
			if unpacked.Version != 2 {
				t.Errorf("wrong version, got %v was expecting 2", unpacked.Version)
			}
			break
		}
		timeout -= 10 * time.Millisecond
		if timeout < 0 {
			t.Fatalf("timeout waiting for streaming RPC to be established")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
