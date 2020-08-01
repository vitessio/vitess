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
	"sort"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/logutil"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the definitions for a FakeLegacyHealthCheck class to
// simulate a LegacyHealthCheck module. Note it is not in a sub-package because
// otherwise it couldn't be used in this package's tests because of
// circular dependencies.

// NewFakeLegacyHealthCheck returns the fake healthcheck object.
func NewFakeLegacyHealthCheck() *FakeLegacyHealthCheck {
	return &FakeLegacyHealthCheck{
		items: make(map[string]*flhcItem),
	}
}

// FakeLegacyHealthCheck implements discovery.LegacyHealthCheck.
type FakeLegacyHealthCheck struct {
	listener LegacyHealthCheckStatsListener

	// mu protects the items map
	mu    sync.RWMutex
	items map[string]*flhcItem
}

type flhcItem struct {
	ts   *LegacyTabletStats
	conn queryservice.QueryService
}

//
// discovery.LegacyHealthCheck interface methods
//

// RegisterStats is not implemented.
func (fhc *FakeLegacyHealthCheck) RegisterStats() {
}

// SetListener is not implemented.
func (fhc *FakeLegacyHealthCheck) SetListener(listener LegacyHealthCheckStatsListener, sendDownEvents bool) {
	fhc.listener = listener
}

// WaitForInitialStatsUpdates is not implemented.
func (fhc *FakeLegacyHealthCheck) WaitForInitialStatsUpdates() {
}

// AddTablet adds the tablet and calls the listener.
func (fhc *FakeLegacyHealthCheck) AddTablet(tablet *topodatapb.Tablet, name string) {
	key := TabletToMapKey(tablet)
	item := &flhcItem{
		ts: &LegacyTabletStats{
			Key:    key,
			Tablet: tablet,
			Target: &querypb.Target{
				Keyspace:   tablet.Keyspace,
				Shard:      tablet.Shard,
				TabletType: tablet.Type,
			},
			Serving: true,
			Up:      true,
			Name:    name,
			Stats:   &querypb.RealtimeStats{},
		},
	}

	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	fhc.items[key] = item

	if fhc.listener != nil {
		fhc.listener.StatsUpdate(item.ts)
	}
}

// RemoveTablet removes the tablet.
func (fhc *FakeLegacyHealthCheck) RemoveTablet(tablet *topodatapb.Tablet) {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	key := TabletToMapKey(tablet)
	item, ok := fhc.items[key]
	if !ok {
		return
	}
	// Make sure the key still corresponds to the tablet we want to delete.
	// If it doesn't match, we should do nothing. The tablet we were asked to
	// delete is already gone, and some other tablet is using the key
	// (host:port) that the original tablet used to use, which is fine.
	if !topoproto.TabletAliasEqual(tablet.Alias, item.ts.Tablet.Alias) {
		return
	}
	delete(fhc.items, key)
}

// ReplaceTablet removes the old tablet and adds the new.
func (fhc *FakeLegacyHealthCheck) ReplaceTablet(old, new *topodatapb.Tablet, name string) {
	fhc.RemoveTablet(old)
	fhc.AddTablet(new, name)
}

// GetConnection returns the TabletConn of the given tablet.
func (fhc *FakeLegacyHealthCheck) GetConnection(key string) queryservice.QueryService {
	fhc.mu.RLock()
	defer fhc.mu.RUnlock()
	if item := fhc.items[key]; item != nil {
		return item.conn
	}
	return nil
}

// CacheStatus returns the status for each tablet
func (fhc *FakeLegacyHealthCheck) CacheStatus() LegacyTabletsCacheStatusList {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()

	stats := make(LegacyTabletsCacheStatusList, 0, len(fhc.items))
	for _, item := range fhc.items {
		stats = append(stats, &LegacyTabletsCacheStatus{
			Cell:         "FakeCell",
			Target:       item.ts.Target,
			TabletsStats: LegacyTabletStatsList{item.ts},
		})
	}
	sort.Sort(stats)
	return stats
}

// Close is not implemented.
func (fhc *FakeLegacyHealthCheck) Close() error {
	return nil
}

//
// Management methods
//

// Reset cleans up the internal state.
func (fhc *FakeLegacyHealthCheck) Reset() {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()

	fhc.items = make(map[string]*flhcItem)
}

// AddFakeTablet inserts a fake entry into FakeLegacyHealthCheck.
// The Tablet can be talked to using the provided connection.
// The Listener is called, as if AddTablet had been called.
// For flexibility the connection is created via a connFactory callback
func (fhc *FakeLegacyHealthCheck) AddFakeTablet(cell, host string, port int32, keyspace, shard string, tabletType topodatapb.TabletType, serving bool, reparentTS int64, err error, connFactory func(*topodatapb.Tablet) queryservice.QueryService) queryservice.QueryService {
	t := topo.NewTablet(0, cell, host)
	t.Keyspace = keyspace
	t.Shard = shard
	t.Type = tabletType
	t.PortMap["vt"] = port
	// reparentTS only has precision to seconds
	t.MasterTermStartTime = logutil.TimeToProto(time.Unix(reparentTS, 0))
	key := TabletToMapKey(t)

	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	item := fhc.items[key]
	if item == nil {
		item = &flhcItem{
			ts: &LegacyTabletStats{
				Key:    key,
				Tablet: t,
				Up:     true,
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
	conn := connFactory(t)
	item.conn = conn

	if fhc.listener != nil {
		fhc.listener.StatsUpdate(item.ts)
	}
	return conn
}

// AddTestTablet adds a fake tablet for tests using the SandboxConn and returns
// the fake connection
func (fhc *FakeLegacyHealthCheck) AddTestTablet(cell, host string, port int32, keyspace, shard string, tabletType topodatapb.TabletType, serving bool, reparentTS int64, err error) *sandboxconn.SandboxConn {
	conn := fhc.AddFakeTablet(cell, host, port, keyspace, shard, tabletType, serving, reparentTS, err, func(tablet *topodatapb.Tablet) queryservice.QueryService {
		return sandboxconn.NewSandboxConn(tablet)
	})
	return conn.(*sandboxconn.SandboxConn)
}

// GetAllTablets returns all the tablets we have.
func (fhc *FakeLegacyHealthCheck) GetAllTablets() map[string]*topodatapb.Tablet {
	res := make(map[string]*topodatapb.Tablet)
	fhc.mu.RLock()
	defer fhc.mu.RUnlock()
	for key, t := range fhc.items {
		res[key] = t.ts.Tablet
	}
	return res
}
