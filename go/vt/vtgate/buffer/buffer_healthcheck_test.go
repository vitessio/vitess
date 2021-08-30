package buffer

import (
	"time"

	"vitess.io/vitess/go/vt/discovery"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func (b *HealthCheckBuffer) testGetShardBuffer(keyspace, shard string) testShardBuffer {
	return b.getOrCreateBuffer(keyspace, shard)
}

func (b *HealthCheckBuffer) testGetPoolSlots() int {
	return b.bufferSizeSema.Size()
}

func (b *HealthCheckBuffer) testFailover(tablet *topodatapb.Tablet, target *querypb.Target, now time.Time) {
	b.StatsUpdate(&discovery.LegacyTabletStats{Tablet: tablet, Target: target, TabletExternallyReparentedTimestamp: now.Unix()})
}
