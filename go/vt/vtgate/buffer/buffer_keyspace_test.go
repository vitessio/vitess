package buffer

import (
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func (b *KeyspaceEventBuffer) testGetShardBuffer(keyspace, shard string) testShardBuffer {
	return b.getOrCreateBuffer(keyspace, shard)
}

func (b *KeyspaceEventBuffer) testGetPoolSlots() int {
	return b.bufferSizeSema.Size()
}

func (b *KeyspaceEventBuffer) testFailover(tablet *topodatapb.Tablet, target *querypb.Target, now time.Time) {
	sb := b.getOrCreateBuffer(target.Keyspace, target.Shard)
	// HACK: the first KeyspaceEvent in the cluster doesn't count as a reparenting,
	// because there was no previous parent.
	if sb.currentPrimary == nil {
		sb.recordKeyspaceEvent(tablet.Alias, true, time.Time{})
	} else {
		sb.recordKeyspaceEvent(tablet.Alias, true, now)
	}
}
