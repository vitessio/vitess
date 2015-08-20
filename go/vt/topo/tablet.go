// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"reflect"
	"sync"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/trace"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo/events"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

const (
	// According to docs, the tablet uid / (mysql server id) is uint32.
	// However, zero appears to be a sufficiently degenerate value to use
	// as a marker for not having a parent server id.
	// http://dev.mysql.com/doc/refman/5.1/en/replication-options.html
	NO_TABLET = 0

	// ReplicationLag is the key in the health map to indicate high
	// replication lag
	ReplicationLag = "replication_lag"

	// ReplicationLagHigh is the value in the health map to indicate high
	// replication lag
	ReplicationLagHigh = "high"
)

// TabletType is the main type for a tablet. It has an implication on:
// - the replication graph
// - the services run by vttablet on a tablet
// - the uptime expectancy
//
// DEPRECATED: use the proto3 topodata.TabletType enum instead.
type TabletType string

//go:generate bsongen -file $GOFILE -type TabletType -o tablet_type_bson.go

const (
	// idle -  no keyspace, shard or type assigned
	TYPE_IDLE = TabletType("idle")

	// primary copy of data
	TYPE_MASTER = TabletType("master")

	// a slaved copy of the data ready to be promoted to master
	TYPE_REPLICA = TabletType("replica")

	// a slaved copy of the data for olap load patterns.
	// too many aliases for olap - need to pick one
	TYPE_RDONLY = TabletType("rdonly")
	TYPE_BATCH  = TabletType("batch")

	// a slaved copy of the data ready, but not serving query traffic
	// could be a potential master.
	TYPE_SPARE = TabletType("spare")

	// a slaved copy of the data ready, but not serving query traffic
	// implies something abnormal about the setup - don't consider it
	// a potential master and don't worry about lag when reparenting.
	TYPE_EXPERIMENTAL = TabletType("experimental")

	// a slaved copy of the data that was serving but is now applying
	// a schema change. Will go bak to its serving type after the
	// upgrade
	TYPE_SCHEMA_UPGRADE = TabletType("schema_apply")

	// a slaved copy of the data, but offline to queries other than backup
	// replication sql thread may be stopped
	TYPE_BACKUP = TabletType("backup")

	// A tablet that has not been in the replication graph and is restoring
	// from a snapshot.
	TYPE_RESTORE = TabletType("restore")

	// A tablet that is used by a worker process. It is probably
	// lagging in replication.
	TYPE_WORKER = TabletType("worker")

	// a machine with data that needs to be wiped
	TYPE_SCRAP = TabletType("scrap")
)

// IsTrivialTypeChange returns if this db type be trivially reassigned
// without changes to the replication graph
func IsTrivialTypeChange(oldTabletType, newTabletType pb.TabletType) bool {
	switch oldTabletType {
	case pb.TabletType_REPLICA, pb.TabletType_RDONLY, pb.TabletType_SPARE, pb.TabletType_BACKUP, pb.TabletType_EXPERIMENTAL, pb.TabletType_SCHEMA_UPGRADE, pb.TabletType_WORKER:
		switch newTabletType {
		case pb.TabletType_REPLICA, pb.TabletType_RDONLY, pb.TabletType_SPARE, pb.TabletType_BACKUP, pb.TabletType_EXPERIMENTAL, pb.TabletType_SCHEMA_UPGRADE, pb.TabletType_WORKER:
			return true
		}
	case pb.TabletType_SCRAP:
		return newTabletType == pb.TabletType_IDLE
	case pb.TabletType_RESTORE:
		switch newTabletType {
		case pb.TabletType_SPARE, pb.TabletType_IDLE:
			return true
		}
	}
	return false
}

// IsInServingGraph returns if a tablet appears in the serving graph
func IsInServingGraph(tt pb.TabletType) bool {
	switch tt {
	case pb.TabletType_MASTER, pb.TabletType_REPLICA, pb.TabletType_RDONLY:
		return true
	}
	return false
}

// IsRunningQueryService returns if a tablet is running the query service
func IsRunningQueryService(tt pb.TabletType) bool {
	switch tt {
	case pb.TabletType_MASTER, pb.TabletType_REPLICA, pb.TabletType_RDONLY, pb.TabletType_WORKER:
		return true
	}
	return false
}

// IsRunningUpdateStream returns if a tablet is running the update stream
// RPC service.
func IsRunningUpdateStream(tt pb.TabletType) bool {
	switch tt {
	case pb.TabletType_MASTER, pb.TabletType_REPLICA, pb.TabletType_RDONLY:
		return true
	}
	return false
}

// IsInReplicationGraph returns if this tablet appears in the replication graph
// Only IDLE and SCRAP are not in the replication graph.
// The other non-obvious types are BACKUP, SNAPSHOT_SOURCE, RESTORE:
// these have had a master at some point (or were the master), so they are
// in the graph.
func IsInReplicationGraph(tt pb.TabletType) bool {
	switch tt {
	case pb.TabletType_IDLE, pb.TabletType_SCRAP:
		return false
	}
	return true
}

// IsSlaveType returns if this type should be connected to a master db
// and actively replicating?
// MASTER is not obviously (only support one level replication graph)
// IDLE and SCRAP are not either
// BACKUP, RESTORE, TYPE_WORKER may or may not be, but we don't know for sure
func IsSlaveType(tt pb.TabletType) bool {
	switch tt {
	case pb.TabletType_MASTER, pb.TabletType_IDLE, pb.TabletType_SCRAP, pb.TabletType_BACKUP, pb.TabletType_RESTORE, pb.TabletType_WORKER:
		return false
	}
	return true
}

// TabletValidatePortMap returns an error if the tablet's portmap doesn't
// contain all the necessary ports for the tablet to be fully
// operational. We only care about vt port now, as mysql may not even
// be running.
func TabletValidatePortMap(tablet *pb.Tablet) error {
	if _, ok := tablet.PortMap["vt"]; !ok {
		return fmt.Errorf("no vt port available")
	}
	return nil
}

// TabletEndPoint returns an EndPoint associated with the tablet record
func TabletEndPoint(tablet *pb.Tablet) (*pb.EndPoint, error) {
	if err := TabletValidatePortMap(tablet); err != nil {
		return nil, err
	}

	entry := NewEndPoint(tablet.Alias.Uid, tablet.Hostname)
	for name, port := range tablet.PortMap {
		entry.PortMap[name] = int32(port)
	}

	if len(tablet.HealthMap) > 0 {
		entry.HealthMap = make(map[string]string, len(tablet.HealthMap))
		for k, v := range tablet.HealthMap {
			entry.HealthMap[k] = v
		}
	}
	return entry, nil
}

// TabletComplete validates and normalizes the tablet. If the shard name
// contains a '-' it is going to try to infer the keyrange from it.
func TabletComplete(tablet *pb.Tablet) error {
	shard, kr, err := ValidateShardName(tablet.Shard)
	if err != nil {
		return err
	}
	tablet.Shard = shard
	tablet.KeyRange = kr
	return nil
}

// TabletInfo is the container for a Tablet, read from the topology server.
type TabletInfo struct {
	version int64 // node version - used to prevent stomping concurrent writes
	*pb.Tablet
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
	return netutil.JoinHostPort(ti.Hostname, int32(ti.PortMap["mysql"]))
}

// IsAssigned returns if this tablet ever assigned data?
// A "scrap" node will show up as assigned even though its data
// cannot be used for serving.
func (ti *TabletInfo) IsAssigned() bool {
	return ti.Keyspace != "" && ti.Shard != ""
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

// IsInReplicationGraph returns if this tablet is in the replication graph.
func (ti *TabletInfo) IsInReplicationGraph() bool {
	return IsInReplicationGraph(ti.Type)
}

// IsSlaveType returns if this tablet's type is a slave
func (ti *TabletInfo) IsSlaveType() bool {
	return IsSlaveType(ti.Type)
}

// IsHealthEqual compares the two health maps, and
// returns true if they're equivalent.
func IsHealthEqual(left, right map[string]string) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}

	return reflect.DeepEqual(left, right)
}

// NewTabletInfo returns a TabletInfo basing on tablet with the
// version set. This function should be only used by Server
// implementations.
func NewTabletInfo(tablet *pb.Tablet, version int64) *TabletInfo {
	return &TabletInfo{version: version, Tablet: tablet}
}

// GetTablet is a high level function to read tablet data.
// It generates trace spans.
func (ts Server) GetTablet(ctx context.Context, alias *pb.TabletAlias) (*TabletInfo, error) {
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

// UpdateTabletFields is a high level wrapper for TopoServer.UpdateTabletFields
// that generates trace spans.
func (ts Server) UpdateTabletFields(ctx context.Context, alias *pb.TabletAlias, update func(*pb.Tablet) error) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateTabletFields")
	span.Annotate("tablet", topoproto.TabletAliasString(alias))
	defer span.Finish()

	tablet, err := ts.Impl.UpdateTabletFields(ctx, alias, update)
	if err != nil {
		return err
	}
	if tablet != nil {
		event.Dispatch(&events.TabletChange{
			Tablet: *tablet,
			Status: "updated",
		})
	}
	return nil
}

// Validate makes sure a tablet is represented correctly in the topology server.
func Validate(ctx context.Context, ts Server, tabletAlias *pb.TabletAlias) error {
	// read the tablet record, make sure it parses
	tablet, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	if !topoproto.TabletAliasEqual(tablet.Alias, tabletAlias) {
		return fmt.Errorf("bad tablet alias data for tablet %v: %#v", topoproto.TabletAliasString(tabletAlias), tablet.Alias)
	}

	// Some tablets have no information to generate valid replication paths.
	// We have three cases to handle:
	// - we are a tablet in the replication graph, and should have
	//   replication data (first case below)
	// - we are in scrap mode but used to be assigned in the graph
	//   somewhere (second case below)
	// Idle tablets are just not in any graph at all, we don't even know
	// their keyspace / shard to know where to check.
	if tablet.IsInReplicationGraph() {
		if err = ts.ValidateShard(ctx, tablet.Keyspace, tablet.Shard); err != nil {
			return err
		}

		si, err := ts.GetShardReplication(ctx, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard)
		if err != nil {
			return err
		}

		_, err = si.GetShardReplicationNode(tabletAlias)
		if err != nil {
			return fmt.Errorf("tablet %v not found in cell %v shard replication: %v", tabletAlias, tablet.Alias.Cell, err)
		}

	} else if tablet.IsAssigned() {
		// this case is to make sure a scrap node that used to be in
		// a replication graph doesn't leave a node behind.
		// However, while an action is running, there is some
		// time where this might be inconsistent.
		si, err := ts.GetShardReplication(ctx, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard)
		if err != nil {
			return err
		}

		node, err := si.GetShardReplicationNode(tabletAlias)
		if err != ErrNoNode {
			return fmt.Errorf("unexpected replication data found(possible pending action?): %v (%v)", node, tablet.Type)
		}
	}

	return nil
}

// CreateTablet creates a new tablet and all associated paths for the
// replication graph.
func (ts Server) CreateTablet(ctx context.Context, tablet *pb.Tablet) error {
	// Have the Server create the tablet
	err := ts.Impl.CreateTablet(ctx, tablet)
	if err != nil {
		return err
	}

	// Then add the tablet to the replication graphs
	if !IsInReplicationGraph(tablet.Type) {
		return nil
	}

	if err := UpdateTabletReplicationData(ctx, ts, tablet); err != nil {
		return err
	}

	event.Dispatch(&events.TabletChange{
		Tablet: *tablet,
		Status: "created",
	})
	return nil
}

// DeleteTablet wraps the underlying Impl.DeleteTablet
// and dispatches the event.
func (ts Server) DeleteTablet(ctx context.Context, tabletAlias *pb.TabletAlias) error {
	// get the current tablet record, if any, to log the deletion
	tablet, _, tErr := ts.Impl.GetTablet(ctx, tabletAlias)

	if err := ts.Impl.DeleteTablet(ctx, tabletAlias); err != nil {
		return err
	}

	// Only try to log if we have the required info.
	if tErr == nil {
		// Only copy the identity info for the tablet. The rest has been deleted.
		event.Dispatch(&events.TabletChange{
			Tablet: pb.Tablet{
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
func UpdateTabletReplicationData(ctx context.Context, ts Server, tablet *pb.Tablet) error {
	return UpdateShardReplicationRecord(ctx, ts, tablet.Keyspace, tablet.Shard, tablet.Alias)
}

// DeleteTabletReplicationData deletes replication data.
func DeleteTabletReplicationData(ctx context.Context, ts Server, tablet *pb.Tablet) error {
	return RemoveShardReplicationRecord(ctx, ts, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard, tablet.Alias)
}

// GetTabletMap tries to read all the tablets in the provided list,
// and returns them all in a map.
// If error is ErrPartialResult, the results in the dictionary are
// incomplete, meaning some tablets couldn't be read.
func (ts Server) GetTabletMap(ctx context.Context, tabletAliases []*pb.TabletAlias) (map[pb.TabletAlias]*TabletInfo, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("topo.GetTabletMap")
	span.Annotate("num_tablets", len(tabletAliases))
	defer span.Finish()

	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	tabletMap := make(map[pb.TabletAlias]*TabletInfo)
	var someError error

	for _, tabletAlias := range tabletAliases {
		wg.Add(1)
		go func(tabletAlias *pb.TabletAlias) {
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
				tabletMap[*tabletAlias] = tabletInfo
			}
			mutex.Unlock()
		}(tabletAlias)
	}
	wg.Wait()
	return tabletMap, someError
}
