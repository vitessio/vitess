/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type benchHealthCheck struct {
	hc      *HealthCheckImpl
	tablets []*topodatapb.Tablet
	inputs  []chan *querypb.StreamHealthResponse
}

func (bhc *benchHealthCheck) send(ctx context.Context, i int, lag uint32) bool {
	tablet := bhc.tablets[i]
	shr := &querypb.StreamHealthResponse{
		TabletAlias:   tablet.Alias,
		Target:        &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type},
		Serving:       true,
		RealtimeStats: &querypb.RealtimeStats{ReplicationLagSeconds: lag},
	}
	if tablet.Type == topodatapb.TabletType_PRIMARY {
		shr.PrimaryTermStartTimestamp = 10
	}
	select {
	case bhc.inputs[i] <- shr:
		return true
	case <-ctx.Done():
		return false
	}
}

// newBenchHealthCheck builds a healthcheck with one serving primary and
// `replicas` serving replicas in k/s, all registered via the real AddTablet path.
func newBenchHealthCheck(tb testing.TB, replicas int) *benchHealthCheck {
	ctx := tb.Context()
	ts := memorytopo.NewServer(ctx, "cell")
	hc := createTestHc(ctx, ts)
	tb.Cleanup(func() {
		hc.Close()
		ts.Close()
	})

	bhc := &benchHealthCheck{hc: hc}
	for i := 0; i <= replicas; i++ {
		tablet := createTestTablet(uint32(1000+i), "cell", "a")
		if i == 0 {
			tablet.Type = topodatapb.TabletType_PRIMARY
		} else {
			tablet.Type = topodatapb.TabletType_REPLICA
		}
		input := make(chan *querypb.StreamHealthResponse)
		createFakeConn(tablet, input)
		bhc.tablets = append(bhc.tablets, tablet)
		bhc.inputs = append(bhc.inputs, input)
		hc.AddTablet(tablet)
		require.True(tb, bhc.send(ctx, i, 1))
	}

	replicaTarget := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA}
	primaryTarget := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_PRIMARY}
	require.Eventually(tb, func() bool {
		return len(hc.GetHealthyTabletStats(replicaTarget)) == replicas && len(hc.GetHealthyTabletStats(primaryTarget)) == 1
	}, 30*time.Second, time.Millisecond)
	return bhc
}

// startHealthUpdates continuously streams non-trivial health updates (changing
// replication lag) so readers contend with updateHealth/recomputeHealthy.
func (bhc *benchHealthCheck) startHealthUpdates(b *testing.B) {
	ctx := b.Context()
	done := make(chan struct{})
	b.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		ticker := time.NewTicker(50 * time.Microsecond)
		defer ticker.Stop()
		for n := uint32(0); ; n++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if !bhc.send(ctx, int(n)%len(bhc.tablets), n%5) {
				return
			}
		}
	}()
}

// healthySink keeps benchmark results reachable so the result copy escapes to
// the heap, as it does for real callers (tabletgateway).
var healthySink atomic.Value

func BenchmarkGetHealthyTabletStats(b *testing.B) {
	for _, tc := range []struct {
		name       string
		tabletType topodatapb.TabletType
		updates    bool
	}{
		{"primary", topodatapb.TabletType_PRIMARY, false},
		{"replica", topodatapb.TabletType_REPLICA, false},
		{"primary_with_updates", topodatapb.TabletType_PRIMARY, true},
		{"replica_with_updates", topodatapb.TabletType_REPLICA, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			bhc := newBenchHealthCheck(b, 3)
			if tc.updates {
				bhc.startHealthUpdates(b)
			}
			target := &querypb.Target{Keyspace: "k", Shard: "s", TabletType: tc.tabletType}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var ths []*TabletHealth
				for pb.Next() {
					ths = bhc.hc.GetHealthyTabletStats(target)
					if len(ths) == 0 {
						b.Error("no healthy tablets")
						return
					}
				}
				healthySink.Store(ths)
			})
		})
	}
}
