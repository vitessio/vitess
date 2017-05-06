/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"sync"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/sandboxconn"

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
	listener HealthCheckStatsListener

	// mu protects the items map
	mu    sync.RWMutex
	items map[string]*fhcItem
}

type fhcItem struct {
	ts   *TabletStats
	conn queryservice.QueryService
}

//
// discovery.HealthCheck interface methods
//

// RegisterStats is not implemented.
func (fhc *FakeHealthCheck) RegisterStats() {
}

// SetListener is not implemented.
func (fhc *FakeHealthCheck) SetListener(listener HealthCheckStatsListener, sendDownEvents bool) {
	fhc.listener = listener
}

// WaitForInitialStatsUpdates is not implemented.
func (fhc *FakeHealthCheck) WaitForInitialStatsUpdates() {
}

// AddTablet adds the tablet and calls the listener.
func (fhc *FakeHealthCheck) AddTablet(tablet *topodatapb.Tablet, name string) {
	key := TabletToMapKey(tablet)
	item := &fhcItem{
		ts: &TabletStats{
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
func (fhc *FakeHealthCheck) RemoveTablet(tablet *topodatapb.Tablet) {
	fhc.mu.Lock()
	defer fhc.mu.Unlock()
	key := TabletToMapKey(tablet)
	delete(fhc.items, key)
}

// GetConnection returns the TabletConn of the given tablet.
func (fhc *FakeHealthCheck) GetConnection(key string) queryservice.QueryService {
	fhc.mu.RLock()
	defer fhc.mu.RUnlock()
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

	fhc.items = make(map[string]*fhcItem)
}

// AddTestTablet inserts a fake entry into FakeHealthCheck.
// The Tablet can be talked to using the provided connection.
// The Listener is called, as if AddTablet had been called.
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
	conn := sandboxconn.NewSandboxConn(t)
	item.conn = conn

	if fhc.listener != nil {
		fhc.listener.StatsUpdate(item.ts)
	}
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
