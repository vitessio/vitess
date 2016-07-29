package discovery

import (
	"sync"

	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/tabletserver/sandboxconn"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the definitions for a FakeHealthCheck class to
// simulate a HealthCheck module. Note it is not in a sub-package because
// otherwise it couldn't be used in this package's tests because of
// circular dependencies.

// NewFakeHealthCheck returns the fake healthcheck object.
func NewFakeHealthCheck() *FakeHealthCheck {
	return &FakeHealthCheck{
		items: make(map[string]*fhcItem),
	}
}

// FakeHealthCheck implements discovery.HealthCheck.
type FakeHealthCheck struct {
	// mu protects the items map
	mu    sync.RWMutex
	items map[string]*fhcItem

	// GetStatsFromTargetCounter counts GetTabletStatsFromTarget() being called.
	// (it can be accessed concurrently by 'multiGo', so using atomic)
	GetStatsFromTargetCounter sync2.AtomicInt32
}

type fhcItem struct {
	ts   *TabletStats
	conn tabletconn.TabletConn
}

//
// discovery.HealthCheck interface methods
//

// RegisterStats is not implemented.
func (fhc *FakeHealthCheck) RegisterStats() {
}

// SetListener is not implemented.
func (fhc *FakeHealthCheck) SetListener(listener HealthCheckStatsListener, sendDownEvents bool) {
}

// AddTablet adds the tablet.
func (fhc *FakeHealthCheck) AddTablet(tablet *topodatapb.Tablet, name string) {
	key := TabletToMapKey(tablet)
	item := &fhcItem{
		ts: &TabletStats{
			Tablet: tablet,
			Name:   name,
		},
	}

	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	fhc.items[key] = item
}

// RemoveTablet removes the tablet.
func (fhc *FakeHealthCheck) RemoveTablet(tablet *topodatapb.Tablet) {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	key := TabletToMapKey(tablet)
	delete(fhc.items, key)
}

// GetTabletStatsFromTarget returns all TabletStats for the given target.
func (fhc *FakeHealthCheck) GetTabletStatsFromTarget(keyspace, shard string, tabletType topodatapb.TabletType) []*TabletStats {
	fhc.GetStatsFromTargetCounter.Add(1)

	fhc.mu.RLock()
	defer fhc.mu.RUnlock()
	var res []*TabletStats
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
func (fhc *FakeHealthCheck) GetConnection(tablet *topodatapb.Tablet) tabletconn.TabletConn {
	fhc.mu.RLock()
	defer fhc.mu.RUnlock()
	key := TabletToMapKey(tablet)
	if item := fhc.items[key]; item != nil {
		return item.conn
	}
	return nil
}

// CacheStatus is not implemented.
func (fhc *FakeHealthCheck) CacheStatus() TabletsCacheStatusList {
	return nil
}

// Close is not implemented.
func (fhc *FakeHealthCheck) Close() error {
	return nil
}

//
// Management methods
//

// Reset cleans up the internal state.
func (fhc *FakeHealthCheck) Reset() {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()

	fhc.GetStatsFromTargetCounter.Set(0)
	fhc.items = make(map[string]*fhcItem)
}

// AddTestTablet inserts a fake entry into FakeHealthCheck.
// The Tablet can be talked to using the provided connection.
func (fhc *FakeHealthCheck) AddTestTablet(cell, host string, port int32, keyspace, shard string, tabletType topodatapb.TabletType, serving bool, reparentTS int64, err error) *sandboxconn.SandboxConn {
	t := topo.NewTablet(0, cell, host)
	t.Keyspace = keyspace
	t.Shard = shard
	t.Type = tabletType
	t.PortMap["vt"] = port
	key := TabletToMapKey(t)

	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	item := fhc.items[key]
	if item == nil {
		item = &fhcItem{
			ts: &TabletStats{
				Tablet: t,
			},
		}
		fhc.items[key] = item
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
	conn := sandboxconn.NewSandboxConn(t)
	item.conn = conn
	return conn
}

// GetAllTablets returns all the tablets we have.
func (fhc *FakeHealthCheck) GetAllTablets() map[string]*topodatapb.Tablet {
	res := make(map[string]*topodatapb.Tablet)
	fhc.mu.RLock()
	defer fhc.mu.RUnlock()
	for key, t := range fhc.items {
		res[key] = t.ts.Tablet
	}
	return res
}
