/*
Copyright 2017 Google Inc.

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

package topo

import (
	"fmt"
	"sync"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/trace"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/events"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

// IsTrivialTypeChange returns if this db type be trivially reassigned
// without changes to the replication graph
func IsTrivialTypeChange(oldTabletType, newTabletType topodatapb.TabletType) bool {
	switch oldTabletType {
	case topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY, topodatapb.TabletType_SPARE, topodatapb.TabletType_BACKUP, topodatapb.TabletType_EXPERIMENTAL, topodatapb.TabletType_DRAINED:
		switch newTabletType {
		case topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY, topodatapb.TabletType_SPARE, topodatapb.TabletType_BACKUP, topodatapb.TabletType_EXPERIMENTAL, topodatapb.TabletType_DRAINED:
			return true
		}
	case topodatapb.TabletType_RESTORE:
		switch newTabletType {
		case topodatapb.TabletType_SPARE:
			return true
		}
	}
	return false
}

// IsInServingGraph returns if a tablet appears in the serving graph
func IsInServingGraph(tt topodatapb.TabletType) bool {
	switch tt {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
		return true
	}
	return false
}

// IsRunningQueryService returns if a tablet is running the query service
func IsRunningQueryService(tt topodatapb.TabletType) bool {
	switch tt {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY, topodatapb.TabletType_EXPERIMENTAL, topodatapb.TabletType_DRAINED:
		return true
	}
	return false
}

// IsSubjectToLameduck returns if a tablet is subject to being
// lameduck.  Lameduck is a transition period where we are still
// allowed to serve, but we tell the clients we are going away
// soon. Typically, a vttablet will still serve, but broadcast a
// non-serving state through its health check. then vtgate will ctahc
// that non-serving state, and stop sending queries.
//
// Masters are not subject to lameduck, as we usually want to transition
// them as fast as possible.
//
// Replica and rdonly will use lameduck when going from healthy to
// unhealhty (either because health check fails, or they're shutting down).
//
// Other types are probably not serving user visible traffic, so they
// need to transition as fast as possible too.
func IsSubjectToLameduck(tt topodatapb.TabletType) bool {
	switch tt {
	case topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
		return true
	}
	return false
}

// IsRunningUpdateStream returns if a tablet is running the update stream
// RPC service.
func IsRunningUpdateStream(tt topodatapb.TabletType) bool {
	switch tt {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
		return true
	}
	return false
}

// IsSlaveType returns if this type should be connected to a master db
// and actively replicating?
// MASTER is not obviously (only support one level replication graph)
// BACKUP, RESTORE, DRAINED may or may not be, but we don't know for sure
func IsSlaveType(tt topodatapb.TabletType) bool {
	switch tt {
	case topodatapb.TabletType_MASTER, topodatapb.TabletType_BACKUP, topodatapb.TabletType_RESTORE, topodatapb.TabletType_DRAINED:
		return false
	}
	return true
}

// TabletComplete validates and normalizes the tablet. If the shard name
// contains a '-' it is going to try to infer the keyrange from it.
func TabletComplete(tablet *topodatapb.Tablet) error {
	shard, kr, err := ValidateShardName(tablet.Shard)
	if err != nil {
		return err
	}
	tablet.Shard = shard
	tablet.KeyRange = kr
	return nil
}

// NewTablet create a new Tablet record with the given id, cell, and hostname.
func NewTablet(uid uint32, cell, host string) *topodatapb.Tablet {
	return &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		},
		Hostname: host,
		PortMap:  make(map[string]int32),
	}
}

// TabletEquality returns true iff two Tablet are representing the same tablet
// process: same uid/cell, running on the same host / ports.
func TabletEquality(left, right *topodatapb.Tablet) bool {
	if !topoproto.TabletAliasEqual(left.Alias, right.Alias) {
		return false
	}
	if left.Hostname != right.Hostname {
		return false
	}
	if left.MysqlHostname != right.MysqlHostname {
		return false
	}
	if left.MysqlPort != right.MysqlPort {
		return false
	}
	if len(left.PortMap) != len(right.PortMap) {
		return false
	}
	for key, lvalue := range left.PortMap {
		rvalue, ok := right.PortMap[key]
		if !ok {
			return false
		}
		if lvalue != rvalue {
			return false
		}
	}
	return true
}

// TabletInfo is the container for a Tablet, read from the topology server.
type TabletInfo struct {
	version int64 // node version - used to prevent stomping concurrent writes
	*topodatapb.Tablet
}

// String returns a string describing the tablet.
func (ti *TabletInfo) String() string {
	return fmt.Sprintf("Tablet{%v}", topoproto.TabletAliasString(ti.Alias))
}

// AliasString returns the string representation of the tablet alias
func (ti *TabletInfo) AliasString() string {
	return topoproto.TabletAliasString(ti.Alias)
}

// Addr returns hostname:vt port.
func (ti *TabletInfo) Addr() string {
	return netutil.JoinHostPort(ti.Hostname, int32(ti.PortMap["vt"]))
}

// MysqlAddr returns hostname:mysql port.
func (ti *TabletInfo) MysqlAddr() string {
	return netutil.JoinHostPort(topoproto.MysqlHostname(ti.Tablet), topoproto.MysqlPort(ti.Tablet))
}

// DbName is usually implied by keyspace. Having the shard information in the
// database name complicates mysql replication.
func (ti *TabletInfo) DbName() string {
	return topoproto.TabletDbName(ti.Tablet)
}

// Version returns the version of this tablet from last time it was read or
// updated.
func (ti *TabletInfo) Version() int64 {
	return ti.version
}

// IsInServingGraph returns if this tablet is in the serving graph
func (ti *TabletInfo) IsInServingGraph() bool {
	return IsInServingGraph(ti.Type)
}

// IsSlaveType returns if this tablet's type is a slave
func (ti *TabletInfo) IsSlaveType() bool {
	return IsSlaveType(ti.Type)
}

// NewTabletInfo returns a TabletInfo basing on tablet with the
// version set. This function should be only used by Server
// implementations.
func NewTabletInfo(tablet *topodatapb.Tablet, version int64) *TabletInfo {
	return &TabletInfo{version: version, Tablet: tablet}
}

// GetTablet is a high level function to read tablet data.
// It generates trace spans.
func (ts Server) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*TabletInfo, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.GetTablet")
	span.Annotate("tablet", topoproto.TabletAliasString(alias))
	defer span.Finish()

	value, version, err := ts.Impl.GetTablet(ctx, alias)
	if err != nil {
		return nil, err
	}
	return &TabletInfo{
		version: version,
		Tablet:  value,
	}, nil
}

// UpdateTablet updates the tablet data only - not associated replication paths.
// It also uses a span, and sends the event.
func (ts Server) UpdateTablet(ctx context.Context, tablet *TabletInfo) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateTablet")
	span.Annotate("tablet", topoproto.TabletAliasString(tablet.Alias))
	defer span.Finish()

	var version int64 = -1
	if tablet.version != 0 {
		version = tablet.version
	}

	newVersion, err := ts.Impl.UpdateTablet(ctx, tablet.Tablet, version)
	if err != nil {
		return err
	}
	tablet.version = newVersion

	event.Dispatch(&events.TabletChange{
		Tablet: *tablet.Tablet,
		Status: "updated",
	})
	return nil
}

// UpdateTabletFields is a high level helper to read a tablet record, call an
// update function on it, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update succeeds, it returns the updated tablet.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil,nil is returned.
func (ts Server) UpdateTabletFields(ctx context.Context, alias *topodatapb.TabletAlias, update func(*topodatapb.Tablet) error) (*topodatapb.Tablet, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateTabletFields")
	span.Annotate("tablet", topoproto.TabletAliasString(alias))
	defer span.Finish()

	for {
		ti, err := ts.GetTablet(ctx, alias)
		if err != nil {
			return nil, err
		}
		if err = update(ti.Tablet); err != nil {
			if err == ErrNoUpdateNeeded {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.UpdateTablet(ctx, ti); err != ErrBadVersion {
			return ti.Tablet, err
		}
	}
}

// Validate makes sure a tablet is represented correctly in the topology server.
func Validate(ctx context.Context, ts Server, tabletAlias *topodatapb.TabletAlias) error {
	// read the tablet record, make sure it parses
	tablet, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	if !topoproto.TabletAliasEqual(tablet.Alias, tabletAlias) {
		return fmt.Errorf("bad tablet alias data for tablet %v: %#v", topoproto.TabletAliasString(tabletAlias), tablet.Alias)
	}

	// Validate the entry in the shard replication nodes
	si, err := ts.GetShardReplication(ctx, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}

	if _, err = si.GetShardReplicationNode(tabletAlias); err != nil {
		return fmt.Errorf("tablet %v not found in cell %v shard replication: %v", tabletAlias, tablet.Alias.Cell, err)
	}

	return nil
}

// CreateTablet creates a new tablet and all associated paths for the
// replication graph.
func (ts Server) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	// Have the Server create the tablet
	err := ts.Impl.CreateTablet(ctx, tablet)
	if err != nil && err != ErrNodeExists {
		return err
	}

	// Update ShardReplication in any case, to be sure.  This is
	// meant to fix the case when a Tablet record was created, but
	// then the ShardReplication record was not (because for
	// instance of a startup timeout). Upon running this code
	// again, we want to fix ShardReplication.
	if updateErr := UpdateTabletReplicationData(ctx, ts, tablet); updateErr != nil {
		return updateErr
	}

	if err == nil {
		event.Dispatch(&events.TabletChange{
			Tablet: *tablet,
			Status: "created",
		})
	}
	return err
}

// DeleteTablet wraps the underlying Impl.DeleteTablet
// and dispatches the event.
func (ts Server) DeleteTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	// get the current tablet record, if any, to log the deletion
	tablet, _, tErr := ts.Impl.GetTablet(ctx, tabletAlias)

	if err := ts.Impl.DeleteTablet(ctx, tabletAlias); err != nil {
		return err
	}

	// Only try to log if we have the required info.
	if tErr == nil {
		// Only copy the identity info for the tablet. The rest has been deleted.
		event.Dispatch(&events.TabletChange{
			Tablet: topodatapb.Tablet{
				Alias:    tabletAlias,
				Keyspace: tablet.Keyspace,
				Shard:    tablet.Shard,
			},
			Status: "deleted",
		})
	}
	return nil
}

// UpdateTabletReplicationData creates or updates the replication
// graph data for a tablet
func UpdateTabletReplicationData(ctx context.Context, ts Server, tablet *topodatapb.Tablet) error {
	return UpdateShardReplicationRecord(ctx, ts, tablet.Keyspace, tablet.Shard, tablet.Alias)
}

// DeleteTabletReplicationData deletes replication data.
func DeleteTabletReplicationData(ctx context.Context, ts Server, tablet *topodatapb.Tablet) error {
	return RemoveShardReplicationRecord(ctx, ts, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard, tablet.Alias)
}

// GetTabletMap tries to read all the tablets in the provided list,
// and returns them all in a map.
// If error is ErrPartialResult, the results in the dictionary are
// incomplete, meaning some tablets couldn't be read.
// The map is indexed by topoproto.TabletAliasString(tablet alias).
func (ts Server) GetTabletMap(ctx context.Context, tabletAliases []*topodatapb.TabletAlias) (map[string]*TabletInfo, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("topo.GetTabletMap")
	span.Annotate("num_tablets", len(tabletAliases))
	defer span.Finish()

	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	tabletMap := make(map[string]*TabletInfo)
	var someError error

	for _, tabletAlias := range tabletAliases {
		wg.Add(1)
		go func(tabletAlias *topodatapb.TabletAlias) {
			defer wg.Done()
			tabletInfo, err := ts.GetTablet(ctx, tabletAlias)
			mutex.Lock()
			if err != nil {
				log.Warningf("%v: %v", tabletAlias, err)
				// There can be data races removing nodes - ignore them for now.
				if err != ErrNoNode {
					someError = ErrPartialResult
				}
			} else {
				tabletMap[topoproto.TabletAliasString(tabletAlias)] = tabletInfo
			}
			mutex.Unlock()
		}(tabletAlias)
	}
	wg.Wait()
	return tabletMap, someError
}
