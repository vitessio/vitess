package vtgate

import (
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func newFakeHealthCheck() *fakeHealthCheck {
	return &fakeHealthCheck{items: make(map[string]*fhcItem)}
}

type fhcItem struct {
	ts   *discovery.TabletStats
	conn tabletconn.TabletConn
}

// fakeHealthCheck implements discovery.HealthCheck.
type fakeHealthCheck struct {
	items map[string]*fhcItem

	// GetStatsFromTargetCounter counts GetTabletStatsFromTarget() being called.
	GetStatsFromTargetCounter int
	// GetStatsFromKeyspaceShardCounter counts GetTabletStatsFromKeyspaceShard() being called.
	GetStatsFromKeyspaceShardCounter int
}

// Reset cleans up the internal state.
func (fhc *fakeHealthCheck) Reset() {
	fhc.GetStatsFromTargetCounter = 0
	fhc.GetStatsFromKeyspaceShardCounter = 0
	fhc.items = make(map[string]*fhcItem)
}

// SetListener sets the listener for healthcheck updates.
func (fhc *fakeHealthCheck) SetListener(listener discovery.HealthCheckStatsListener) {
}

// AddTablet adds the tablet.
func (fhc *fakeHealthCheck) AddTablet(cell, name string, tablet *topodatapb.Tablet) {
	key := discovery.TabletToMapKey(tablet)
	item := &fhcItem{
		ts: &discovery.TabletStats{
			Tablet: tablet,
			Name:   name,
		},
	}
	fhc.items[key] = item
}

// RemoveTablet removes the tablet.
func (fhc *fakeHealthCheck) RemoveTablet(tablet *topodatapb.Tablet) {
	key := discovery.TabletToMapKey(tablet)
	delete(fhc.items, key)
}

// GetTabletStatsFromKeyspaceShard returns all TabletStats for the given keyspace/shard.
func (fhc *fakeHealthCheck) GetTabletStatsFromKeyspaceShard(keyspace, shard string) []*discovery.TabletStats {
	fhc.GetStatsFromKeyspaceShardCounter++
	var res []*discovery.TabletStats
	for _, item := range fhc.items {
		if item.ts.Target == nil {
			continue
		}
		if item.ts.Target.Keyspace == keyspace && item.ts.Target.Shard == shard {
			res = append(res, item.ts)
		}
	}
	return res
}

// GetTabletStatsFromTarget returns all TabletStats for the given target.
func (fhc *fakeHealthCheck) GetTabletStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*discovery.TabletStats {
	fhc.GetStatsFromTargetCounter++
	var res []*discovery.TabletStats
	for _, item := range fhc.items {
		if item.ts.Target == nil {
			continue
		}
		if item.ts.Target.Keyspace == keyspace && item.ts.Target.Shard == shard && item.ts.Target.TabletType == tabletType {
			res = append(res, item.ts)
		}
	}
	return res
}

// GetConnection returns the TabletConn of the given tablet.
func (fhc *fakeHealthCheck) GetConnection(tablet *topodatapb.Tablet) tabletconn.TabletConn {
	key := discovery.TabletToMapKey(tablet)
	if item := fhc.items[key]; item != nil {
		return item.conn
	}
	return nil
}

// CacheStatus returns a displayable version of the cache.
func (fhc *fakeHealthCheck) CacheStatus() discovery.TabletsCacheStatusList {
	return nil
}

// Close stops the healthcheck.
func (fhc *fakeHealthCheck) Close() error {
	return nil
}

// addTestTablet inserts a fake entry into fakeHealthCheck.
func (fhc *fakeHealthCheck) addTestTablet(cell, host string, port int32, keyspace, shard string, tabletType topodatapb.TabletType, serving bool, reparentTS int64, err error, conn tabletconn.TabletConn) *topodatapb.Tablet {
	if conn != nil {
		conn.SetTarget(keyspace, shard, tabletType)
	}
	t := topo.NewTablet(0, cell, host)
	t.PortMap["vt"] = port
	key := discovery.TabletToMapKey(t)
	item := fhc.items[key]
	if item == nil {
		fhc.AddTablet(cell, "", t)
		item = fhc.items[key]
	}
	item.ts.Target = &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	item.ts.Serving = serving
	item.ts.TabletExternallyReparentedTimestamp = reparentTS
	item.ts.Stats = &querypb.RealtimeStats{}
	item.ts.LastError = err
	item.conn = conn
	return t
}
