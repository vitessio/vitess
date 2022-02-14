package discovery

import (
	"testing"
	"time"

	"context"

	"vitess.io/vitess/go/vt/topo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestWaitForTablets(t *testing.T) {
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer shortCancel()
	waitAvailableTabletInterval = 20 * time.Millisecond

	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)

	hc := NewLegacyHealthCheck(1*time.Millisecond, 1*time.Hour)
	tsc := NewLegacyTabletStatsCache(hc, nil, "cell")
	hc.AddTablet(tablet, "")

	// this should time out
	if err := tsc.WaitForTablets(shortCtx, "keyspace", "shard", topodatapb.TabletType_REPLICA); err != context.DeadlineExceeded {
		t.Errorf("got wrong error: %v", err)
	}

	// this should fail, but return a non-timeout error
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tsc.WaitForTablets(cancelledCtx, "keyspace", "shard", topodatapb.TabletType_REPLICA); err == nil || err == context.DeadlineExceeded {
		t.Errorf("want: non-timeout error, got: %v", err)
	}

	// send the tablet in
	shr := &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "shard",
			TabletType: topodatapb.TabletType_REPLICA,
		},
		Serving:       true,
		RealtimeStats: &querypb.RealtimeStats{ReplicationLagSeconds: 1, CpuUsage: 0.2},
	}
	input <- shr

	// and ask again, with longer time outs so it's not flaky
	longCtx, longCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer longCancel()
	waitAvailableTabletInterval = 10 * time.Millisecond
	if err := tsc.WaitForTablets(longCtx, "keyspace", "shard", topodatapb.TabletType_REPLICA); err != nil {
		t.Errorf("got error: %v", err)
	}
}
