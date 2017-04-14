package txthrottler

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestDisabledThrottler(t *testing.T) {
	oldConfig := tabletenv.Config
	defer func() { tabletenv.Config = oldConfig }()
	tabletenv.Config.EnableTxThrottler = false
	throttler := CreateTxThrottlerFromTabletConfig(topo.Server{})
	if err := throttler.Open("keyspace", "shard"); err != nil {
		t.Fatalf("want: nil, got: %v", err)
	}
	if result := throttler.Throttle(); result != false {
		t.Errorf("want: false, got: %v", result)
	}
	throttler.Close()
}

func TestEnabledThrottler(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	defer resetTxThrottlerFactories()
	mockTopoServer, _ := NewMockServer(mockCtrl)

	mockHealthCheck := NewMockHealthCheck(mockCtrl)
	var hcListener discovery.HealthCheckStatsListener
	hcCall1 := mockHealthCheck.EXPECT().SetListener(gomock.Any(), false /* sendDownEvents */)
	hcCall1.Do(func(listener discovery.HealthCheckStatsListener, sendDownEvents bool) {
		// Record the listener we're given.
		hcListener = listener
	})
	hcCall2 := mockHealthCheck.EXPECT().Close()
	hcCall2.After(hcCall1)
	healthCheckFactory = func() discovery.HealthCheck { return mockHealthCheck }

	topologyWatcherFactory = func(topoServer topo.Server, tr discovery.TabletRecorder, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface {
		if mockTopoServer.Impl != topoServer.Impl {
			t.Errorf("want: %v, got: %v", mockTopoServer, topoServer)
		}
		if cell != "cell1" && cell != "cell2" {
			t.Errorf("want: cell1 or cell2, got: %v", cell)
		}
		if keyspace != "keyspace" {
			t.Errorf("want: keyspace, got: %v", keyspace)
		}
		if shard != "shard" {
			t.Errorf("want: shard, got: %v", shard)
		}
		result := NewMockTopologyWatcherInterface(mockCtrl)
		result.EXPECT().Stop()
		return result
	}

	mockThrottler := NewMockThrottlerInterface(mockCtrl)
	throttlerFactory = func(name, unit string, threadCount int, maxRate, maxReplicationLag int64) (ThrottlerInterface, error) {
		if threadCount != 1 {
			t.Errorf("want: 1, got: %v", threadCount)
		}
		return mockThrottler, nil
	}

	call0 := mockThrottler.EXPECT().UpdateConfiguration(gomock.Any(), true /* copyZeroValues */)
	call1 := mockThrottler.EXPECT().Throttle(0)
	call1.Return(0 * time.Second)
	tabletStats := &discovery.TabletStats{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}
	call2 := mockThrottler.EXPECT().RecordReplicationLag(gomock.Any(), tabletStats)
	call3 := mockThrottler.EXPECT().Throttle(0)
	call3.Return(1 * time.Second)
	call4 := mockThrottler.EXPECT().Close()
	call1.After(call0)
	call2.After(call1)
	call3.After(call2)
	call4.After(call3)

	oldConfig := tabletenv.Config
	defer func() { tabletenv.Config = oldConfig }()
	tabletenv.Config.EnableTxThrottler = true
	tabletenv.Config.TxThrottlerHealthCheckCells = []string{"cell1", "cell2"}

	throttler, err := tryCreateTxThrottler(mockTopoServer)
	if err != nil {
		t.Fatalf("want: nil, got: %v", err)
	}
	if err := throttler.Open("keyspace", "shard"); err != nil {
		t.Fatalf("want: nil, got: %v", err)
	}
	if result := throttler.Throttle(); result != false {
		t.Errorf("want: false, got: %v", result)
	}
	hcListener.StatsUpdate(tabletStats)
	rdonlyTabletStats := &discovery.TabletStats{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_RDONLY,
		},
	}
	// This call should not be forwarded to the go/vt/throttler.Throttler object.
	hcListener.StatsUpdate(rdonlyTabletStats)
	// The second throttle call should reject.
	if result := throttler.Throttle(); result != true {
		t.Errorf("want: true, got: %v", result)
	}
	throttler.Close()
}

// Commands to generate the mocks for this test.
//go:generate mockgen -destination mock_toposerver_impl_test.go -package txthrottler github.com/youtube/vitess/go/vt/topo Impl
// We need the following to fix the generated mock_impl.go, since mockgen imports the 'context'
// package from the wrong place.
// TODO(mberlin): Remove the next line once we use the Go 1.7 package 'context' everywhere.
//go:generate sed -i s,github.com/youtube/vitess/vendor/,,g mock_toposerver_impl_test.go
//go:generate mockgen -destination mock_healthcheck_test.go -package txthrottler github.com/youtube/vitess/go/vt/discovery HealthCheck
//go:generate mockgen -destination mock_throttler_test.go -package txthrottler github.com/youtube/vitess/go/vt/tabletserver/txthrottler ThrottlerInterface
//go:generate mockgen -destination mock_topology_watcher_test.go -package txthrottler github.com/youtube/vitess/go/vt/tabletserver/txthrottler TopologyWatcherInterface
