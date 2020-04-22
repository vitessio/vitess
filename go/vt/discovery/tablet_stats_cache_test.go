/*
Copyright 2019 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

// TestTabletStatsCache tests the functionality of the TabletStatsCache class.
func TestTabletStatsCache(t *testing.T) {
	// We want to unit test TabletStatsCache without a full-blown
	// HealthCheck object, so we can't call NewTabletStatsCache.
	// So we just construct this object here.
	tsc := &tabletStatsCache{
		entries: make(map[string]map[string]map[topodatapb.TabletType]*tabletStatsCacheEntry),
	}

	// empty
	a := tsc.getEntry("k", "s", topodatapb.TabletType_MASTER)
	assert.Nil(t, a)
	// add a tablet
	b := tsc.getOrCreateEntry(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	assert.NotNil(t, b)
	tablet1 := topo.NewTablet(10, "cell", "host1")
	ts1 := &tabletStats{
		Key:     "t1",
		Tablet:  tablet1,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	b.all[TabletToMapKey(tablet1)] = ts1

	// check it's there
	c := tsc.getOrCreateEntry(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	assert.NotNil(t, c)
	got := c.getTabletStats()
	assert.Equal(t, 1, len(got))
	assert.True(t, ts1.DeepEqual(&got[0]))

	// add a second tablet
	tablet2 := topo.NewTablet(11, "cell", "host2")
	ts2 := &tabletStats{
		Key:     "t2",
		Tablet:  tablet2,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:      true,
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 0, CpuUsage: 0.2},
	}
	d := tsc.getOrCreateEntry(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER})
	d.all[TabletToMapKey(tablet2)] = ts2

	d.updateHealthyMapForMaster(ts2)
	// should be in healthy tablet stats
	got = d.getHealthyTabletStats()
	assert.Equal(t, 1, len(got))
	assert.True(t, ts2.DeepEqual(&got[0]))
	// should be in all tabletStats
	got = d.getTabletStats()
	assert.Equal(t, 1, len(got))
	assert.True(t, ts2.DeepEqual(&got[0]))

	// master goes down
	ts2.Up = false
	d.updateHealthyMapForMaster(ts2)
	got = d.getHealthyTabletStats()
	// check it is not there
	assert.Equal(t, 0, len(got))

}
