package discovery

import (
	"sync"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
)

func newFakeHealthCheck() *fakeHealthCheck {
	return &fakeHealthCheck{tablets: make(map[string]*topodatapb.Tablet)}
}

type fakeHealthCheck struct {
	mu      sync.RWMutex
	tablets map[string]*topodatapb.Tablet
}

// RegisterStats is part of the HealthCheck interface.
func (*fakeHealthCheck) RegisterStats() {
}

// SetListener sets the listener for healthcheck updates.
func (*fakeHealthCheck) SetListener(listener HealthCheckStatsListener) {
}

// AddTablet adds the tablet, and starts health check.
func (fhc *fakeHealthCheck) AddTablet(cell, name string, tablet *topodatapb.Tablet) {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	key := TabletToMapKey(tablet)
	fhc.tablets[key] = tablet
}

// RemoveTablet removes the tablet, and stops the health check.
func (fhc *fakeHealthCheck) RemoveTablet(tablet *topodatapb.Tablet) {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	key := TabletToMapKey(tablet)
	delete(fhc.tablets, key)
}

// GetTabletStatsFromKeyspaceShard returns all TabletStats for the given keyspace/shard.
func (*fakeHealthCheck) GetTabletStatsFromKeyspaceShard(keyspace, shard string) []*TabletStats {
	return nil
}

// GetTabletStatsFromTarget returns all TabletStats for the given target.
func (*fakeHealthCheck) GetTabletStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*TabletStats {
	return nil
}

// GetConnection returns the TabletConn of the given tablet.
func (*fakeHealthCheck) GetConnection(tablet *topodatapb.Tablet) tabletconn.TabletConn {
	return nil
}

// CacheStatus returns a displayable version of the cache.
func (*fakeHealthCheck) CacheStatus() TabletsCacheStatusList {
	return nil
}

// Close stops the healthcheck.
func (*fakeHealthCheck) Close() error {
	return nil
}

func (fhc *fakeHealthCheck) GetAllTablets() map[string]*topodatapb.Tablet {
	res := make(map[string]*topodatapb.Tablet)
	fhc.mu.RLock()
	defer fhc.mu.RUnlock()
	for key, t := range fhc.tablets {
		res[key] = t
	}
	return res
}
