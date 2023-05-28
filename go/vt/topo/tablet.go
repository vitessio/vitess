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

package topo

import (
	"context"
	"fmt"
	"path"
	"sort"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/key"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/events"
	"vitess.io/vitess/go/vt/topo/topoproto"
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
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
		return true
	}
	return false
}

// IsRunningQueryService returns if a tablet is running the query service
func IsRunningQueryService(tt topodatapb.TabletType) bool {
	switch tt {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY, topodatapb.TabletType_EXPERIMENTAL, topodatapb.TabletType_DRAINED:
		return true
	}
	return false
}

// IsSubjectToLameduck returns if a tablet is subject to being
// lameduck.  Lameduck is a transition period where we are still
// allowed to serve, but we tell the clients we are going away
// soon. Typically, a vttablet will still serve, but broadcast a
// non-serving state through its health check. then vtgate will catch
// that non-serving state, and stop sending queries.
//
// Primaries are not subject to lameduck, as we usually want to transition
// them as fast as possible.
//
// Replica and rdonly will use lameduck when going from healthy to
// unhealthy (either because health check fails, or they're shutting down).
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
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY:
		return true
	}
	return false
}

// IsReplicaType returns if this type should be connected to a primary db
// and actively replicating?
// PRIMARY is not obviously (only support one level replication graph)
// BACKUP, RESTORE, DRAINED may or may not be, but we don't know for sure
func IsReplicaType(tt topodatapb.TabletType) bool {
	switch tt {
	case topodatapb.TabletType_PRIMARY, topodatapb.TabletType_BACKUP, topodatapb.TabletType_RESTORE, topodatapb.TabletType_DRAINED:
		return false
	}
	return true
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
	version Version // node version - used to prevent stomping concurrent writes
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
	return netutil.JoinHostPort(ti.Tablet.MysqlHostname, ti.Tablet.MysqlPort)
}

// DbName is usually implied by keyspace. Having the shard information in the
// database name complicates mysql replication.
func (ti *TabletInfo) DbName() string {
	return topoproto.TabletDbName(ti.Tablet)
}

// Version returns the version of this tablet from last time it was read or
// updated.
func (ti *TabletInfo) Version() Version {
	return ti.version
}

// IsInServingGraph returns if this tablet is in the serving graph
func (ti *TabletInfo) IsInServingGraph() bool {
	return IsInServingGraph(ti.Type)
}

// IsReplicaType returns if this tablet's type is a replica
func (ti *TabletInfo) IsReplicaType() bool {
	return IsReplicaType(ti.Type)
}

// GetPrimaryTermStartTime returns the tablet's primary term start time as a Time value.
func (ti *TabletInfo) GetPrimaryTermStartTime() time.Time {
	return logutil.ProtoToTime(ti.Tablet.PrimaryTermStartTime)
}

// NewTabletInfo returns a TabletInfo basing on tablet with the
// version set. This function should be only used by Server
// implementations.
func NewTabletInfo(tablet *topodatapb.Tablet, version Version) *TabletInfo {
	return &TabletInfo{version: version, Tablet: tablet}
}

// GetTablet is a high level function to read tablet data.
// It generates trace spans.
func (ts *Server) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*TabletInfo, error) {
	conn, err := ts.ConnForCell(ctx, alias.Cell)
	if err != nil {
		log.Errorf("Unable to get connection for cell %s", alias.Cell)
		return nil, err
	}

	span, ctx := trace.NewSpan(ctx, "TopoServer.GetTablet")
	span.Annotate("tablet", topoproto.TabletAliasString(alias))
	defer span.Finish()

	tabletPath := path.Join(TabletsPath, topoproto.TabletAliasString(alias), TabletFile)
	data, version, err := conn.Get(ctx, tabletPath)
	if err != nil {
		log.Errorf("unable to connect to tablet %s: %s", alias, err)
		return nil, err
	}
	tablet := &topodatapb.Tablet{}
	if err := tablet.UnmarshalVT(data); err != nil {
		return nil, err
	}

	return &TabletInfo{
		version: version,
		Tablet:  tablet,
	}, nil
}

// GetTabletAliasesByCell returns all the tablet aliases in a cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no tablets in it.
func (ts *Server) GetTabletAliasesByCell(ctx context.Context, cell string) ([]*topodatapb.TabletAlias, error) {
	// If the cell doesn't exist, this will return ErrNoNode.
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	// List the directory, and parse the aliases
	children, err := conn.ListDir(ctx, TabletsPath, false /*full*/)
	if err != nil {
		if IsErrType(err, NoNode) {
			// directory doesn't exist, empty list, no error.
			return nil, nil
		}
		return nil, err
	}

	result := make([]*topodatapb.TabletAlias, len(children))
	for i, child := range children {
		result[i], err = topoproto.ParseTabletAlias(child.Name)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// GetTabletsByCell returns all the tablets in the cell.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no tablets in it.
func (ts *Server) GetTabletsByCell(ctx context.Context, cellAlias string) ([]*TabletInfo, error) {
	// If the cell doesn't exist, this will return ErrNoNode.
	cellConn, err := ts.ConnForCell(ctx, cellAlias)
	if err != nil {
		return nil, err
	}
	listResults, err := cellConn.List(ctx, TabletsPath)
	if err != nil || len(listResults) == 0 {
		// Currently the ZooKeeper and Memory topo implementations do not support scans
		// so we fall back to the more costly method of fetching the tablets one by one.
		if IsErrType(err, NoImplementation) {
			return ts.GetTabletsIndividuallyByCell(ctx, cellAlias)
		}
		if IsErrType(err, NoNode) {
			return nil, nil
		}
		return nil, err
	}

	tablets := make([]*TabletInfo, len(listResults))
	for n := range listResults {
		tablet := &topodatapb.Tablet{}
		if err := tablet.UnmarshalVT(listResults[n].Value); err != nil {
			return nil, err
		}
		tablets[n] = &TabletInfo{Tablet: tablet, version: listResults[n].Version}
	}

	return tablets, nil
}

// GetTabletsIndividuallyByCell returns a sorted list of tablets for topo servers that do not
// directly support the topoConn.List() functionality.
// It returns ErrNoNode if the cell doesn't exist.
// It returns (nil, nil) if the cell exists, but there are no tablets in it.
func (ts *Server) GetTabletsIndividuallyByCell(ctx context.Context, cell string) ([]*TabletInfo, error) {
	// If the cell doesn't exist, this will return ErrNoNode.
	aliases, err := ts.GetTabletAliasesByCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	sort.Sort(topoproto.TabletAliasList(aliases))

	tabletMap, err := ts.GetTabletMap(ctx, aliases)
	if err != nil {
		// we got another error than topo.ErrNoNode
		return nil, err
	}
	tablets := make([]*TabletInfo, 0, len(aliases))
	for _, tabletAlias := range aliases {
		tabletInfo, ok := tabletMap[topoproto.TabletAliasString(tabletAlias)]
		if !ok {
			// tablet disappeared on us (GetTabletMap ignores
			// topo.ErrNoNode), just echo a warning
			log.Warningf("failed to load tablet %v", tabletAlias)
		} else {
			tablets = append(tablets, tabletInfo)
		}
	}

	return tablets, nil
}

// UpdateTablet updates the tablet data only - not associated replication paths.
// It also uses a span, and sends the event.
func (ts *Server) UpdateTablet(ctx context.Context, ti *TabletInfo) error {
	conn, err := ts.ConnForCell(ctx, ti.Tablet.Alias.Cell)
	if err != nil {
		return err
	}

	span, ctx := trace.NewSpan(ctx, "TopoServer.UpdateTablet")
	span.Annotate("tablet", topoproto.TabletAliasString(ti.Alias))
	defer span.Finish()

	data, err := ti.Tablet.MarshalVT()
	if err != nil {
		return err
	}
	tabletPath := path.Join(TabletsPath, topoproto.TabletAliasString(ti.Tablet.Alias), TabletFile)
	newVersion, err := conn.Update(ctx, tabletPath, data, ti.version)
	if err != nil {
		return err
	}
	ti.version = newVersion

	event.Dispatch(&events.TabletChange{
		Tablet: ti.Tablet,
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
func (ts *Server) UpdateTabletFields(ctx context.Context, alias *topodatapb.TabletAlias, update func(*topodatapb.Tablet) error) (*topodatapb.Tablet, error) {
	span, ctx := trace.NewSpan(ctx, "TopoServer.UpdateTabletFields")
	span.Annotate("tablet", topoproto.TabletAliasString(alias))
	defer span.Finish()

	for {
		ti, err := ts.GetTablet(ctx, alias)
		if err != nil {
			return nil, err
		}
		if err = update(ti.Tablet); err != nil {
			if IsErrType(err, NoUpdateNeeded) {
				return nil, nil
			}
			return nil, err
		}
		if err = ts.UpdateTablet(ctx, ti); !IsErrType(err, BadVersion) {
			return ti.Tablet, err
		}
	}
}

// Validate makes sure a tablet is represented correctly in the topology server.
func Validate(ctx context.Context, ts *Server, tabletAlias *topodatapb.TabletAlias) error {
	// read the tablet record, make sure it parses
	tablet, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	if !topoproto.TabletAliasEqual(tablet.Alias, tabletAlias) {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "bad tablet alias data for tablet %v: %#v", topoproto.TabletAliasString(tabletAlias), tablet.Alias)
	}

	// Validate the entry in the shard replication nodes
	si, err := ts.GetShardReplication(ctx, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return err
	}

	if _, err = si.GetShardReplicationNode(tabletAlias); err != nil {
		return vterrors.Wrapf(err, "tablet %v not found in cell %v shard replication", tabletAlias, tablet.Alias.Cell)
	}

	return nil
}

// CreateTablet creates a new tablet and all associated paths for the
// replication graph.
func (ts *Server) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	conn, err := ts.ConnForCell(ctx, tablet.Alias.Cell)
	if err != nil {
		return err
	}

	data, err := tablet.MarshalVT()
	if err != nil {
		return err
	}
	tabletPath := path.Join(TabletsPath, topoproto.TabletAliasString(tablet.Alias), TabletFile)
	if _, err = conn.Create(ctx, tabletPath, data); err != nil {
		return err
	}

	if updateErr := UpdateTabletReplicationData(ctx, ts, tablet); updateErr != nil {
		return updateErr
	}

	if err == nil {
		event.Dispatch(&events.TabletChange{
			Tablet: tablet,
			Status: "created",
		})
	}
	return err
}

// DeleteTablet wraps the underlying conn.Delete
// and dispatches the event.
func (ts *Server) DeleteTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	conn, err := ts.ConnForCell(ctx, tabletAlias.Cell)
	if err != nil {
		return err
	}

	// get the current tablet record, if any, to log the deletion
	ti, tErr := ts.GetTablet(ctx, tabletAlias)

	tabletPath := path.Join(TabletsPath, topoproto.TabletAliasString(tabletAlias), TabletFile)
	if err := conn.Delete(ctx, tabletPath, nil); err != nil {
		return err
	}

	// Only try to log if we have the required info.
	if tErr == nil {
		// Only copy the identity info for the tablet. The rest has been deleted.
		event.Dispatch(&events.TabletChange{
			Tablet: &topodatapb.Tablet{
				Alias:    tabletAlias,
				Keyspace: ti.Tablet.Keyspace,
				Shard:    ti.Tablet.Shard,
			},
			Status: "deleted",
		})
	}
	return nil
}

// UpdateTabletReplicationData creates or updates the replication
// graph data for a tablet
func UpdateTabletReplicationData(ctx context.Context, ts *Server, tablet *topodatapb.Tablet) error {
	return UpdateShardReplicationRecord(ctx, ts, tablet.Keyspace, tablet.Shard, tablet.Alias)
}

// DeleteTabletReplicationData deletes replication data.
func DeleteTabletReplicationData(ctx context.Context, ts *Server, tablet *topodatapb.Tablet) error {
	return RemoveShardReplicationRecord(ctx, ts, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard, tablet.Alias)
}

// GetTabletMap tries to read all the tablets in the provided list,
// and returns them all in a map.
// If error is ErrPartialResult, the results in the dictionary are
// incomplete, meaning some tablets couldn't be read.
// The map is indexed by topoproto.TabletAliasString(tablet alias).
func (ts *Server) GetTabletMap(ctx context.Context, tabletAliases []*topodatapb.TabletAlias) (map[string]*TabletInfo, error) {
	span, ctx := trace.NewSpan(ctx, "topo.GetTabletMap")
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
				if !IsErrType(err, NoNode) {
					someError = NewError(PartialResult, "")
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

// InitTablet creates or updates a tablet. If no parent is specified
// in the tablet, and the tablet has a replica type, we will find the
// appropriate parent. If createShardAndKeyspace is true and the
// parent keyspace or shard don't exist, they will be created.  If
// allowUpdate is true, and a tablet with the same ID exists, just update it.
// If a tablet is created as primary, and there is already a different
// primary in the shard, allowPrimaryOverride must be set.
func (ts *Server) InitTablet(ctx context.Context, tablet *topodatapb.Tablet, allowPrimaryOverride, createShardAndKeyspace, allowUpdate bool) error {
	shard, kr, err := ValidateShardName(tablet.Shard)
	if err != nil {
		return err
	}
	tablet.Shard = shard
	tablet.KeyRange = kr

	// get the shard, possibly creating it
	var si *ShardInfo

	if createShardAndKeyspace {
		// create the parent keyspace and shard if needed
		si, err = ts.GetOrCreateShard(ctx, tablet.Keyspace, tablet.Shard)
	} else {
		si, err = ts.GetShard(ctx, tablet.Keyspace, tablet.Shard)
		if IsErrType(err, NoNode) {
			return fmt.Errorf("missing parent shard, use -parent option to create it, or CreateKeyspace / CreateShard")
		}
	}

	// get the shard, checks a couple things
	if err != nil {
		return fmt.Errorf("cannot get (or create) shard %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
	}
	if !key.KeyRangeEqual(si.KeyRange, tablet.KeyRange) {
		return fmt.Errorf("shard %v/%v has a different KeyRange: %v != %v", tablet.Keyspace, tablet.Shard, si.KeyRange, tablet.KeyRange)
	}
	if tablet.Type == topodatapb.TabletType_PRIMARY && si.HasPrimary() && !topoproto.TabletAliasEqual(si.PrimaryAlias, tablet.Alias) && !allowPrimaryOverride {
		// InitTablet is deprecated, so the flag has not been renamed
		return fmt.Errorf("creating this tablet would override old primary %v in shard %v/%v, use allow_master_override flag", topoproto.TabletAliasString(si.PrimaryAlias), tablet.Keyspace, tablet.Shard)
	}

	if tablet.Type == topodatapb.TabletType_PRIMARY {
		// we update primary_term_start_time even if the primary hasn't changed
		// because that means a new primary term with the same primary
		tablet.PrimaryTermStartTime = logutil.TimeToProto(time.Now())
	}

	err = ts.CreateTablet(ctx, tablet)
	if IsErrType(err, NodeExists) && allowUpdate {
		// Try to update then
		oldTablet, err := ts.GetTablet(ctx, tablet.Alias)
		if err != nil {
			return fmt.Errorf("failed reading existing tablet %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
		}

		// Check we have the same keyspace / shard, and if not,
		// require the allowDifferentShard flag.
		if oldTablet.Keyspace != tablet.Keyspace || oldTablet.Shard != tablet.Shard {
			return fmt.Errorf("old tablet has shard %v/%v. Cannot override with shard %v/%v. Delete and re-add tablet if you want to change the tablet's keyspace/shard", oldTablet.Keyspace, oldTablet.Shard, tablet.Keyspace, tablet.Shard)
		}
		oldTablet.Tablet = proto.Clone(tablet).(*topodatapb.Tablet)
		if err := ts.UpdateTablet(ctx, oldTablet); err != nil {
			return fmt.Errorf("failed updating tablet %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
		}
		return nil
	}
	return err
}

// ParseServingTabletType parses the tablet type into the enum, and makes sure
// that the enum is of serving type (PRIMARY, REPLICA, RDONLY/BATCH).
//
// Note: This function more closely belongs in topoproto, but that would create
// a circular import between packages topo and topoproto.
func ParseServingTabletType(param string) (topodatapb.TabletType, error) {
	servedType, err := topoproto.ParseTabletType(param)
	if err != nil {
		return topodatapb.TabletType_UNKNOWN, err
	}

	if !IsInServingGraph(servedType) {
		return topodatapb.TabletType_UNKNOWN, fmt.Errorf("served_type has to be in the serving graph, not %v", param)
	}

	return servedType, nil
}
