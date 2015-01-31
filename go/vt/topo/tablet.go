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
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/key"
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

// TabletAlias is the minimum required information to locate a tablet.
//
// Tablets are really globally unique, but crawling every cell to find
// out where it lives is time consuming and expensive. This is only
// needed during complex operations.  Tablet cell assignments don't
// change that often, thus using a TabletAlias is efficient.
type TabletAlias struct {
	Cell string
	Uid  uint32
}

// IsZero returns true iff cell and uid are empty
func (ta TabletAlias) IsZero() bool {
	return ta.Cell == "" && ta.Uid == 0
}

// String formats a TabletAlias
func (ta TabletAlias) String() string {
	return fmtAlias(ta.Cell, ta.Uid)
}

// TabletUIDStr returns a string version of the uid
func (ta TabletAlias) TabletUIDStr() string {
	return tabletUIDStr(ta.Uid)
}

// ParseTabletAliasString returns a TabletAlias for the input string,
// of the form <cell>-<uid>
func ParseTabletAliasString(aliasStr string) (result TabletAlias, err error) {
	nameParts := strings.Split(aliasStr, "-")
	if len(nameParts) != 2 {
		err = fmt.Errorf("invalid tablet alias: %v", aliasStr)
		return
	}
	result.Cell = nameParts[0]
	result.Uid, err = ParseUID(nameParts[1])
	if err != nil {
		err = fmt.Errorf("invalid tablet uid %v: %v", aliasStr, err)
		return
	}
	return
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
type TabletAliasList []TabletAlias

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

	// a slaved copy of the data intentionally lagged for pseudo backup
	TYPE_LAG = TabletType("lag")
	// when a reparent occurs, the tablet goes into lag_orphan state until
	// it can be reparented properly
	TYPE_LAG_ORPHAN = TabletType("lag_orphan")

	// a slaved copy of the data that was serving but is now applying
	// a schema change. Will go bak to its serving type after the
	// upgrade
	TYPE_SCHEMA_UPGRADE = TabletType("schema_apply")

	// a slaved copy of the data, but offline to queries other than backup
	// replication sql thread may be stopped
	TYPE_BACKUP = TabletType("backup")

	// a slaved copy of the data, where mysqld is *not* running,
	// and we are serving our data files to clone slaves
	// use 'vtctl Snapshot -server-mode ...' to get in this mode
	// use 'vtctl SnapshotSourceEnd ...' to get out of this mode
	TYPE_SNAPSHOT_SOURCE = TabletType("snapshot_source")

	// A tablet that has not been in the replication graph and is restoring
	// from a snapshot.  idle -> restore -> spare
	TYPE_RESTORE = TabletType("restore")

	// A tablet that is running a checker process. It is probably
	// lagging in replication.
	TYPE_CHECKER = TabletType("checker")

	// a machine with data that needs to be wiped
	TYPE_SCRAP = TabletType("scrap")
)

// AllTabletTypes lists all the possible tablet types
var AllTabletTypes = []TabletType{TYPE_IDLE,
	TYPE_MASTER,
	TYPE_REPLICA,
	TYPE_RDONLY,
	TYPE_BATCH,
	TYPE_SPARE,
	TYPE_EXPERIMENTAL,
	TYPE_LAG,
	TYPE_LAG_ORPHAN,
	TYPE_SCHEMA_UPGRADE,
	TYPE_BACKUP,
	TYPE_SNAPSHOT_SOURCE,
	TYPE_RESTORE,
	TYPE_CHECKER,
	TYPE_SCRAP,
}

// SlaveTabletTypes list all the types that are replication slaves
var SlaveTabletTypes = []TabletType{
	TYPE_REPLICA,
	TYPE_RDONLY,
	TYPE_BATCH,
	TYPE_SPARE,
	TYPE_EXPERIMENTAL,
	TYPE_LAG,
	TYPE_LAG_ORPHAN,
	TYPE_SCHEMA_UPGRADE,
	TYPE_BACKUP,
	TYPE_SNAPSHOT_SOURCE,
	TYPE_RESTORE,
	TYPE_CHECKER,
}

// IsTypeInList returns true if the given type is in the list.
// Use it with AllTabletType and SlaveTabletType for instance.
func IsTypeInList(tabletType TabletType, types []TabletType) bool {
	for _, t := range types {
		if tabletType == t {
			return true
		}
	}
	return false
}

// IsSlaveType returns true iff the type is a mysql replication slave.
func (tt TabletType) IsSlaveType() bool {
	return IsTypeInList(tt, SlaveTabletTypes)
}

// MakeStringTypeList returns a list of strings that match the input list.
func MakeStringTypeList(types []TabletType) []string {
	strs := make([]string, len(types))
	for i, t := range types {
		strs[i] = string(t)
	}
	sort.Strings(strs)
	return strs
}

// IsTrivialTypeChange returns if this db type be trivially reassigned
// without changes to the replication graph
func IsTrivialTypeChange(oldTabletType, newTabletType TabletType) bool {
	switch oldTabletType {
	case TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_SPARE, TYPE_LAG, TYPE_LAG_ORPHAN, TYPE_BACKUP, TYPE_SNAPSHOT_SOURCE, TYPE_EXPERIMENTAL, TYPE_SCHEMA_UPGRADE, TYPE_CHECKER:
		switch newTabletType {
		case TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_SPARE, TYPE_LAG, TYPE_LAG_ORPHAN, TYPE_BACKUP, TYPE_SNAPSHOT_SOURCE, TYPE_EXPERIMENTAL, TYPE_SCHEMA_UPGRADE, TYPE_CHECKER:
			return true
		}
	case TYPE_SCRAP:
		return newTabletType == TYPE_IDLE
	case TYPE_RESTORE:
		switch newTabletType {
		case TYPE_SPARE, TYPE_IDLE:
			return true
		}
	}
	return false
}

// IsValidTypeChange returns if we should we allow this transition at
// all.  Most transitions are allowed, but some don't make sense under
// any circumstances. If a transition could be forced, don't disallow
// it here.
func IsValidTypeChange(oldTabletType, newTabletType TabletType) bool {
	switch oldTabletType {
	case TYPE_SNAPSHOT_SOURCE:
		switch newTabletType {
		case TYPE_BACKUP, TYPE_SNAPSHOT_SOURCE:
			return false
		}
	}

	return true
}

// IsInServingGraph returns if a tablet appears in the serving graph
func IsInServingGraph(tt TabletType) bool {
	switch tt {
	case TYPE_MASTER, TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH:
		return true
	}
	return false
}

// IsRunningQueryService returns if a tablet is running the query service
func IsRunningQueryService(tt TabletType) bool {
	switch tt {
	case TYPE_MASTER, TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_CHECKER:
		return true
	}
	return false
}

// IsRunningUpdateStream returns if a tablet is running the update stream
// RPC service.
func IsRunningUpdateStream(tt TabletType) bool {
	switch tt {
	case TYPE_MASTER, TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH:
		return true
	}
	return false
}

// IsInReplicationGraph returns if this tablet appears in the replication graph
// Only IDLE and SCRAP are not in the replication graph.
// The other non-obvious types are BACKUP, SNAPSHOT_SOURCE, RESTORE
// and LAG_ORPHAN: these have had a master at some point (or were the
// master), so they are in the graph.
func IsInReplicationGraph(tt TabletType) bool {
	switch tt {
	case TYPE_IDLE, TYPE_SCRAP:
		return false
	}
	return true
}

// IsSlaveType returns if this type should be connected to a master db
// and actively replicating?
// MASTER is not obviously (only support one level replication graph)
// IDLE and SCRAP are not either
// BACKUP, RESTORE, LAG_ORPHAN, TYPE_CHECKER may or may not be, but we don't know for sure
func IsSlaveType(tt TabletType) bool {
	switch tt {
	case TYPE_MASTER, TYPE_IDLE, TYPE_SCRAP, TYPE_BACKUP, TYPE_RESTORE, TYPE_LAG_ORPHAN, TYPE_CHECKER:
		return false
	}
	return true
}

// TabletState describe if the tablet is read-only or read-write.
type TabletState string

const (
	// STATE_READ_WRITE is the normal state for a master
	STATE_READ_WRITE = TabletState("ReadWrite")

	// STATE_READ_ONLY is the normal state for a slave, or temporarily a master.
	// Not to be confused with type, which implies a workload.
	STATE_READ_ONLY = TabletState("ReadOnly")
)

// Tablet is a pure data struct for information serialized into json
// and stored into topo.Server
type Tablet struct {
	// What is this tablet?
	Alias TabletAlias

	// Location of the tablet
	Hostname string
	IPAddr   string

	// Named port names. Currently supported ports: vt, vts,
	// mysql.
	Portmap map[string]int

	// Tags contain freeform information about the tablet.
	Tags map[string]string

	// Health tracks how healthy the tablet is. Clients may decide
	// to use this information to make educated decisions on which
	// tablet to connect to.
	Health map[string]string

	// Information about the tablet inside a keyspace/shard
	Keyspace string
	Shard    string
	Type     TabletType

	// Is the tablet read-only?
	State TabletState

	// Normally the database name is implied by "vt_" + keyspace. I
	// really want to remove this but there are some databases that are
	// hard to rename.
	DbNameOverride string
	KeyRange       key.KeyRange
}

// ValidatePortmap returns an error if the tablet's portmap doesn't
// contain all the necessary ports for the tablet to be fully
// operational. We only care about vt port now, as mysql may not even
// be running.
func (tablet *Tablet) ValidatePortmap() error {
	if _, ok := tablet.Portmap["vt"]; !ok {
		return fmt.Errorf("no vt port available")
	}
	return nil
}

// EndPoint returns an EndPoint associated with the tablet record
func (tablet *Tablet) EndPoint() (*EndPoint, error) {
	entry := NewEndPoint(tablet.Alias.Uid, tablet.Hostname)
	if err := tablet.ValidatePortmap(); err != nil {
		return nil, err
	}

	entry.NamedPortMap = map[string]int{}

	if port, ok := tablet.Portmap["vt"]; ok {
		entry.NamedPortMap["_vtocc"] = port
		entry.NamedPortMap["vt"] = port
	}
	if port, ok := tablet.Portmap["mysql"]; ok {
		entry.NamedPortMap["mysql"] = port
	}
	if port, ok := tablet.Portmap["vts"]; ok {
		entry.NamedPortMap["vts"] = port
	}

	if len(tablet.Health) > 0 {
		entry.Health = make(map[string]string, len(tablet.Health))
		for k, v := range tablet.Health {
			entry.Health[k] = v
		}
	}
	return entry, nil
}

// Addr returns hostname:vt port.
func (tablet *Tablet) Addr() string {
	return netutil.JoinHostPort(tablet.Hostname, tablet.Portmap["vt"])
}

// MysqlAddr returns hostname:mysql port.
func (tablet *Tablet) MysqlAddr() string {
	return netutil.JoinHostPort(tablet.Hostname, tablet.Portmap["mysql"])
}

// MysqlIPAddr returns ip:mysql port.
func (tablet *Tablet) MysqlIPAddr() string {
	return netutil.JoinHostPort(tablet.IPAddr, tablet.Portmap["mysql"])
}

// DbName is usually implied by keyspace. Having the shard information in the
// database name complicates mysql replication.
func (tablet *Tablet) DbName() string {
	if tablet.DbNameOverride != "" {
		return tablet.DbNameOverride
	}
	if tablet.Keyspace == "" {
		return ""
	}
	return vtDbPrefix + tablet.Keyspace
}

// IsInServingGraph returns if this tablet is in the serving graph
func (tablet *Tablet) IsInServingGraph() bool {
	return IsInServingGraph(tablet.Type)
}

// IsRunningQueryService returns if this tablet should be running
// the query service.
func (tablet *Tablet) IsRunningQueryService() bool {
	return IsRunningQueryService(tablet.Type)
}

// IsInReplicationGraph returns if this tablet is in the replication graph.
func (tablet *Tablet) IsInReplicationGraph() bool {
	return IsInReplicationGraph(tablet.Type)
}

// IsSlaveType returns if this tablet's type is a slave
func (tablet *Tablet) IsSlaveType() bool {
	return IsSlaveType(tablet.Type)
}

// IsAssigned returns if this tablet ever assigned data? A "scrap" node will
// show up as assigned even though its data cannot be used for serving.
func (tablet *Tablet) IsAssigned() bool {
	return tablet.Keyspace != "" && tablet.Shard != ""
}

// String returns a string describing the tablet.
func (tablet *Tablet) String() string {
	return fmt.Sprintf("Tablet{%v}", tablet.Alias)
}

// JSON returns a json verison of the tablet.
func (tablet *Tablet) JSON() string {
	return jscfg.ToJson(tablet)
}

// TabletInfo is the container for a Tablet, read from the topology server.
type TabletInfo struct {
	version int64 // node version - used to prevent stomping concurrent writes
	*Tablet
}

// Version returns the version of this tablet from last time it was read or
// updated.
func (ti *TabletInfo) Version() int64 {
	return ti.version
}

// Complete validates and normalizes the tablet. If the shard name
// contains a '-' it is going to try to infer the keyrange from it.
func (tablet *Tablet) Complete() error {
	switch tablet.Type {
	case TYPE_MASTER:
		tablet.State = STATE_READ_WRITE
	case TYPE_IDLE:
		fallthrough
	default:
		tablet.State = STATE_READ_ONLY
	}

	var err error
	tablet.Shard, tablet.KeyRange, err = ValidateShardName(tablet.Shard)
	return err
}

// IsHealthEqual compares the tablet's health with the passed one, and
// returns true if they're equivalent.
func (tablet *Tablet) IsHealthEqual(health map[string]string) bool {
	if len(health) == 0 && len(tablet.Health) == 0 {
		return true
	}

	return reflect.DeepEqual(health, tablet.Health)
}

// NewTabletInfo returns a TabletInfo basing on tablet with the
// version set. This function should be only used by Server
// implementations.
func NewTabletInfo(tablet *Tablet, version int64) *TabletInfo {
	return &TabletInfo{version: version, Tablet: tablet}
}

// GetTablet is a high level function to read tablet data.
// It generates trace spans.
func GetTablet(ctx context.Context, ts Server, alias TabletAlias) (*TabletInfo, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.GetTablet")
	span.Annotate("tablet", alias.String())
	defer span.Finish()

	return ts.GetTablet(alias)
}

// UpdateTablet updates the tablet data only - not associated replication paths.
func UpdateTablet(ctx context.Context, ts Server, tablet *TabletInfo) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateTablet")
	span.Annotate("tablet", tablet.Alias.String())
	defer span.Finish()

	var version int64 = -1
	if tablet.version != 0 {
		version = tablet.version
	}

	newVersion, err := ts.UpdateTablet(tablet, version)
	if err == nil {
		tablet.version = newVersion
	}
	return err
}

// UpdateTabletFields is a high level wrapper for TopoServer.UpdateTabletFields
// that generates trace spans.
func UpdateTabletFields(ctx context.Context, ts Server, alias TabletAlias, update func(*Tablet) error) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateTabletFields")
	span.Annotate("tablet", alias.String())
	defer span.Finish()

	return ts.UpdateTabletFields(alias, update)
}

// Validate makes sure a tablet is represented correctly in the topology server.
func Validate(ts Server, tabletAlias TabletAlias) error {
	// read the tablet record, make sure it parses
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	// Some tablets have no information to generate valid replication paths.
	// We have three cases to handle:
	// - we are a master, in which case we may have an entry or not
	// (we are in the process of adding entries to the graph for masters)
	// - we are a slave in the replication graph, and should have
	// replication data (second case below)
	// - we are a master, or in scrap mode but used to be assigned
	// in the graph somewhere (third case below)
	// Idle tablets are just not in any graph at all, we don't even know
	// their keyspace / shard to know where to check.
	if tablet.Type == TYPE_MASTER {
		si, err := ts.GetShardReplication(tablet.Alias.Cell, tablet.Keyspace, tablet.Shard)
		if err != nil {
			log.Warningf("master tablet %v with no ShardReplication object, assuming it's because of transition", tabletAlias)
			return nil
		}

		_, err = si.GetReplicationLink(tabletAlias)
		if err != nil {
			log.Warningf("master tablet %v with no ReplicationLink entry, assuming it's because of transition", tabletAlias)
			return nil
		}

	} else if tablet.IsInReplicationGraph() {
		if err = ts.ValidateShard(tablet.Keyspace, tablet.Shard); err != nil {
			return err
		}

		si, err := ts.GetShardReplication(tablet.Alias.Cell, tablet.Keyspace, tablet.Shard)
		if err != nil {
			return err
		}

		_, err = si.GetReplicationLink(tabletAlias)
		if err != nil {
			return fmt.Errorf("tablet %v not found in cell %v shard replication: %v", tabletAlias, tablet.Alias.Cell, err)
		}

	} else if tablet.IsAssigned() {
		// this case is to make sure a scrap node that used to be in
		// a replication graph doesn't leave a node behind.
		// However, while an action is running, there is some
		// time where this might be inconsistent.
		si, err := ts.GetShardReplication(tablet.Alias.Cell, tablet.Keyspace, tablet.Shard)
		if err != nil {
			return err
		}

		rl, err := si.GetReplicationLink(tabletAlias)
		if err != ErrNoNode {
			return fmt.Errorf("unexpected replication data found(possible pending action?): %v (%v)", rl, tablet.Type)
		}
	}

	return nil
}

// CreateTablet creates a new tablet and all associated paths for the
// replication graph.
func CreateTablet(ts Server, tablet *Tablet) error {
	// Have the Server create the tablet
	err := ts.CreateTablet(tablet)
	if err != nil {
		return err
	}

	// Then add the tablet to the replication graphs
	if !tablet.IsInReplicationGraph() {
		return nil
	}

	return UpdateTabletReplicationData(context.TODO(), ts, tablet)
}

// UpdateTabletReplicationData creates or updates the replication
// graph data for a tablet
func UpdateTabletReplicationData(ctx context.Context, ts Server, tablet *Tablet) error {
	return UpdateShardReplicationRecord(ctx, ts, tablet.Keyspace, tablet.Shard, tablet.Alias)
}

// DeleteTabletReplicationData deletes replication data.
func DeleteTabletReplicationData(ts Server, tablet *Tablet) error {
	return RemoveShardReplicationRecord(ts, tablet.Alias.Cell, tablet.Keyspace, tablet.Shard, tablet.Alias)
}

// GetTabletMap tries to read all the tablets in the provided list,
// and returns them all in a map.
// If error is ErrPartialResult, the results in the dictionary are
// incomplete, meaning some tablets couldn't be read.
func GetTabletMap(ctx context.Context, ts Server, tabletAliases []TabletAlias) (map[TabletAlias]*TabletInfo, error) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("topo.GetTabletMap")
	span.Annotate("num_tablets", len(tabletAliases))
	defer span.Finish()

	wg := sync.WaitGroup{}
	mutex := sync.Mutex{}

	tabletMap := make(map[TabletAlias]*TabletInfo)
	var someError error

	for _, tabletAlias := range tabletAliases {
		wg.Add(1)
		go func(tabletAlias TabletAlias) {
			defer wg.Done()
			tabletInfo, err := ts.GetTablet(tabletAlias)
			mutex.Lock()
			if err != nil {
				log.Warningf("%v: %v", tabletAlias, err)
				// There can be data races removing nodes - ignore them for now.
				if err != ErrNoNode {
					someError = ErrPartialResult
				}
			} else {
				tabletMap[tabletAlias] = tabletInfo
			}
			mutex.Unlock()
		}(tabletAlias)
	}
	wg.Wait()
	return tabletMap, someError
}
