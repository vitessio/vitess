// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/trace"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// According to docs, the tablet uid / (mysql server id) is uint32.
	// However, zero appears to be a sufficiently degenerate value to use
	// as a marker for not having a parent server id.
	// http://dev.mysql.com/doc/refman/5.1/en/replication-options.html
	NO_TABLET = 0

	// Default name for databases is the prefix plus keyspace
	vtDbPrefix = "vt_"

	// ReplicationLag is the key in the health map to indicate high
	// replication lag
	ReplicationLag = "replication_lag"

	// ReplicationLagHigh is the value in the health map to indicate high
	// replication lag
	ReplicationLagHigh = "high"
)

// TabletAliasIsZero returns true iff cell and uid are empty
func TabletAliasIsZero(ta *pb.TabletAlias) bool {
	return ta == nil || (ta.Cell == "" && ta.Uid == 0)
}

// TabletAliasEqual returns true if two TabletAlias match
func TabletAliasEqual(left, right *pb.TabletAlias) bool {
	if left == nil {
		return right == nil
	}
	if right == nil {
		return false
	}
	return *left == *right
}

// TabletAliasString formats a TabletAlias
func TabletAliasString(tabletAlias *pb.TabletAlias) string {
	if tabletAlias == nil {
		return "<nil>"
	}
	return fmtAlias(tabletAlias.Cell, tabletAlias.Uid)
}

// TabletAliasUIDStr returns a string version of the uid
func TabletAliasUIDStr(ta *pb.TabletAlias) string {
	return tabletUIDStr(ta.Uid)
}

// ParseTabletAliasString returns a TabletAlias for the input string,
// of the form <cell>-<uid>
func ParseTabletAliasString(aliasStr string) (*pb.TabletAlias, error) {
	nameParts := strings.Split(aliasStr, "-")
	if len(nameParts) != 2 {
		return nil, fmt.Errorf("invalid tablet alias: %v", aliasStr)
	}
	uid, err := ParseUID(nameParts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid tablet uid %v: %v", aliasStr, err)
	}
	return &pb.TabletAlias{
		Cell: nameParts[0],
		Uid:  uid,
	}, nil
}

func tabletUIDStr(uid uint32) string {
	return fmt.Sprintf("%010d", uid)
}

// ParseUID parses just the uid (a number)
func ParseUID(value string) (uint32, error) {
	uid, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad tablet uid %v", err)
	}
	return uint32(uid), nil
}

func fmtAlias(cell string, uid uint32) string {
	return fmt.Sprintf("%v-%v", cell, tabletUIDStr(uid))
}

// TabletAliasList is used mainly for sorting
type TabletAliasList []*pb.TabletAlias

// Len is part of sort.Interface
func (tal TabletAliasList) Len() int {
	return len(tal)
}

// Less is part of sort.Interface
func (tal TabletAliasList) Less(i, j int) bool {
	if tal[i].Cell < tal[j].Cell {
		return true
	} else if tal[i].Cell > tal[j].Cell {
		return false
	}
	return tal[i].Uid < tal[j].Uid
}

// Swap is part of sort.Interface
func (tal TabletAliasList) Swap(i, j int) {
	tal[i], tal[j] = tal[j], tal[i]
}

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

// AllTabletTypes lists all the possible tablet types
var AllTabletTypes = []pb.TabletType{
	pb.TabletType_IDLE,
	pb.TabletType_MASTER,
	pb.TabletType_REPLICA,
	pb.TabletType_RDONLY,
	pb.TabletType_BATCH,
	pb.TabletType_SPARE,
	pb.TabletType_EXPERIMENTAL,
	pb.TabletType_SCHEMA_UPGRADE,
	pb.TabletType_BACKUP,
	pb.TabletType_RESTORE,
	pb.TabletType_WORKER,
	pb.TabletType_SCRAP,
}

// SlaveTabletTypes contains all the tablet type that can have replication
// enabled.
var SlaveTabletTypes = []pb.TabletType{
	pb.TabletType_REPLICA,
	pb.TabletType_RDONLY,
	pb.TabletType_BATCH,
	pb.TabletType_SPARE,
	pb.TabletType_EXPERIMENTAL,
	pb.TabletType_SCHEMA_UPGRADE,
	pb.TabletType_BACKUP,
	pb.TabletType_RESTORE,
	pb.TabletType_WORKER,
}

// ParseTabletType parses the tablet type into the enum
func ParseTabletType(param string) (pb.TabletType, error) {
	value, ok := pb.TabletType_value[strings.ToUpper(param)]
	if !ok {
		return pb.TabletType_UNKNOWN, fmt.Errorf("unknown TabletType %v", param)
	}
	return pb.TabletType(value), nil
}

// IsTypeInList returns true if the given type is in the list.
// Use it with AllTabletType and SlaveTabletType for instance.
func IsTypeInList(tabletType pb.TabletType, types []pb.TabletType) bool {
	for _, t := range types {
		if tabletType == t {
			return true
		}
	}
	return false
}

// MakeStringTypeList returns a list of strings that match the input list.
func MakeStringTypeList(types []pb.TabletType) []string {
	strs := make([]string, len(types))
	for i, t := range types {
		strs[i] = t.String()
	}
	sort.Strings(strs)
	return strs
}

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

// TabletAddr returns hostname:vt port associated with a tablet
func TabletAddr(tablet *pb.Tablet) string {
	return netutil.JoinHostPort(tablet.Hostname, tablet.PortMap["vt"])
}

// TabletDbName is usually implied by keyspace. Having the shard information in the
// database name complicates mysql replication.
func TabletDbName(tablet *pb.Tablet) string {
	if tablet.DbNameOverride != "" {
		return tablet.DbNameOverride
	}
	if tablet.Keyspace == "" {
		return ""
	}
	return vtDbPrefix + tablet.Keyspace
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
	return fmt.Sprintf("Tablet{%v}", TabletAliasString(ti.Alias))
}

// AliasString returns the string representation of the tablet alias
func (ti *TabletInfo) AliasString() string {
	return TabletAliasString(ti.Alias)
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
	return TabletDbName(ti.Tablet)
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
func GetTablet(ctx context.Context, ts Server, alias *pb.TabletAlias) (*TabletInfo, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.GetTablet")
	span.Annotate("tablet", TabletAliasString(alias))
	defer span.Finish()

	return ts.GetTablet(ctx, alias)
}

// UpdateTablet updates the tablet data only - not associated replication paths.
func UpdateTablet(ctx context.Context, ts Server, tablet *TabletInfo) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateTablet")
	span.Annotate("tablet", TabletAliasString(tablet.Alias))
	defer span.Finish()

	var version int64 = -1
	if tablet.version != 0 {
		version = tablet.version
	}

	newVersion, err := ts.UpdateTablet(ctx, tablet, version)
	if err == nil {
		tablet.version = newVersion
	}
	return err
}

// UpdateTabletFields is a high level wrapper for TopoServer.UpdateTabletFields
// that generates trace spans.
func UpdateTabletFields(ctx context.Context, ts Server, alias *pb.TabletAlias, update func(*pb.Tablet) error) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateTabletFields")
	span.Annotate("tablet", TabletAliasString(alias))
	defer span.Finish()

	return ts.UpdateTabletFields(ctx, alias, update)
}

// Validate makes sure a tablet is represented correctly in the topology server.
func Validate(ctx context.Context, ts Server, tabletAlias *pb.TabletAlias) error {
	// read the tablet record, make sure it parses
	tablet, err := ts.GetTablet(ctx, tabletAlias)
	if err != nil {
		return err
	}
	if !TabletAliasEqual(tablet.Alias, tabletAlias) {
		return fmt.Errorf("bad tablet alias data for tablet %v: %#v", TabletAliasString(tabletAlias), tablet.Alias)
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
func CreateTablet(ctx context.Context, ts Server, tablet *pb.Tablet) error {
	// Have the Server create the tablet
	err := ts.CreateTablet(ctx, tablet)
	if err != nil {
		return err
	}

	// Then add the tablet to the replication graphs
	if !IsInReplicationGraph(tablet.Type) {
		return nil
	}

	return UpdateTabletReplicationData(ctx, ts, tablet)
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
func GetTabletMap(ctx context.Context, ts Server, tabletAliases []*pb.TabletAlias) (map[pb.TabletAlias]*TabletInfo, error) {
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
