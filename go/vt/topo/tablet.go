// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/netutil"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
)

const (
	// According to docs, the tablet uid / (mysql server id) is uint32.
	// However, zero appears to be a sufficiently degenerate value to use
	// as a marker for not having a parent server id.
	// http://dev.mysql.com/doc/refman/5.1/en/replication-options.html
	NO_TABLET = 0

	// Default name for databases is the prefix plus keyspace
	vtDbPrefix = "vt_"
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

func (ta TabletAlias) String() string {
	return fmtAlias(ta.Cell, ta.Uid)
}

func (ta TabletAlias) TabletUidStr() string {
	return tabletUidStr(ta.Uid)
}

func ParseTabletAliasString(aliasStr string) (result TabletAlias, err error) {
	nameParts := strings.Split(aliasStr, "-")
	if len(nameParts) != 2 {
		err = fmt.Errorf("invalid tablet alias: %v", aliasStr)
		return
	}
	result.Cell = nameParts[0]
	result.Uid, err = ParseUid(nameParts[1])
	if err != nil {
		err = fmt.Errorf("invalid tablet uid %v: %v", aliasStr, err)
		return
	}
	return
}

func tabletUidStr(uid uint32) string {
	return fmt.Sprintf("%010d", uid)
}

func ParseUid(value string) (uint32, error) {
	uid, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad tablet uid %v", err)
	}
	return uint32(uid), nil
}

func fmtAlias(cell string, uid uint32) string {
	return fmt.Sprintf("%v-%v", cell, tabletUidStr(uid))
}

// TabletAliasList is used mainly for sorting
type TabletAliasList []TabletAlias

func (tal TabletAliasList) Len() int {
	return len(tal)
}

func (tal TabletAliasList) Less(i, j int) bool {
	if tal[i].Cell < tal[j].Cell {
		return true
	} else if tal[i].Cell > tal[j].Cell {
		return false
	}
	return tal[i].Uid < tal[j].Uid
}

func (tal TabletAliasList) Swap(i, j int) {
	tal[i], tal[j] = tal[j], tal[i]
}

// TabletType is the main type for a tablet. It has an implication on:
// - the replication graph
// - the services run by vttablet on a tablet
// - the uptime expectancy
type TabletType string

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

	// a machine with data that needs to be wiped
	TYPE_SCRAP = TabletType("scrap")
)

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
	TYPE_SCRAP,
}

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
}

var AllTabletTypeStrings []string
var SlaveTabletTypeStrings []string

func makeTypeList(types []TabletType) []string {
	strs := make([]string, len(types))
	for i, t := range types {
		strs[i] = string(t)
	}
	sort.Strings(strs)
	return strs
}

func init() {
	AllTabletTypeStrings = makeTypeList(AllTabletTypes)
	SlaveTabletTypeStrings = makeTypeList(SlaveTabletTypes)
}

// IsTrivialTypeChange returns if this db type be trivially reassigned
// without changes to the replication graph
func IsTrivialTypeChange(oldTabletType, newTabletType TabletType) bool {
	switch oldTabletType {
	case TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_SPARE, TYPE_LAG, TYPE_LAG_ORPHAN, TYPE_BACKUP, TYPE_SNAPSHOT_SOURCE, TYPE_EXPERIMENTAL, TYPE_SCHEMA_UPGRADE:
		switch newTabletType {
		case TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_SPARE, TYPE_LAG, TYPE_LAG_ORPHAN, TYPE_BACKUP, TYPE_SNAPSHOT_SOURCE, TYPE_EXPERIMENTAL, TYPE_SCHEMA_UPGRADE:
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
// any circumstances. If a transistion could be forced, don't disallow
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

// IsServingType returns if a tablet appears in the serving graph
func IsServingType(tt TabletType) bool {
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
// BACKUP, RESTORE, LAG_ORPHAN may or may not be, but we don't know for sure
func IsSlaveType(tt TabletType) bool {
	switch tt {
	case TYPE_MASTER, TYPE_IDLE, TYPE_SCRAP, TYPE_BACKUP, TYPE_RESTORE, TYPE_LAG_ORPHAN:
		return false
	}
	return true
}

// TabletState describe if the tablet is read-only or read-write.
type TabletState string

const (
	// The normal state for a master
	STATE_READ_WRITE = TabletState("ReadWrite")

	// The normal state for a slave, or temporarily a master. Not
	// to be confused with type, which implies a workload.
	STATE_READ_ONLY = TabletState("ReadOnly")
)

// Tablet is a pure data struct for information serialized into json
// and stored into topo.Server
type Tablet struct {
	Cell        string      // the cell this tablet is assigned to (doesn't change)
	Uid         uint32      // the server id for this instance
	Parent      TabletAlias // the globally unique alias for our replication parent - zero if this is the global master
	Addr        string      // host:port for queryserver
	SecureAddr  string      // host:port for queryserver using encrypted connection
	MysqlAddr   string      // host:port for the mysql instance
	MysqlIpAddr string      // ip:port for the mysql instance - needed to match slaves with tablets and preferable to relying on reverse dns

	Keyspace string
	Shard    string
	Type     TabletType

	State TabletState

	// Normally the database name is implied by "vt_" + keyspace. I
	// really want to remove this but there are some databases that are
	// hard to rename.
	DbNameOverride string
	KeyRange       key.KeyRange
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

// export per-tablet functions
func (tablet *Tablet) Alias() TabletAlias {
	return TabletAlias{tablet.Cell, tablet.Uid}
}

func (tablet *Tablet) IsServingType() bool {
	return IsServingType(tablet.Type)
}

func (tablet *Tablet) IsInReplicationGraph() bool {
	return IsInReplicationGraph(tablet.Type)
}

func (tablet *Tablet) IsSlaveType() bool {
	return IsSlaveType(tablet.Type)
}

// Was this tablet ever assigned data? A "scrap" node will show up as assigned
// even though its data cannot be used for serving.
func (tablet *Tablet) IsAssigned() bool {
	return tablet.Keyspace != "" && tablet.Shard != ""
}

func (tablet *Tablet) String() string {
	return fmt.Sprintf("Tablet{%v}", tablet.Uid)
}

func (tablet *Tablet) Json() string {
	return jscfg.ToJson(tablet)
}

func (tablet *Tablet) Hostname() string {
	host, _, err := netutil.SplitHostPort(tablet.Addr)
	if err != nil {
		panic(err) // should not happen, Addr was checked at creation
	}
	return host
}

type TabletInfo struct {
	version int // node version - used to prevent stomping concurrent writes
	*Tablet
}

// FIXME(alainjobart) when switch to topo.Server this will become useless
func (ti *TabletInfo) Path() string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%010d", ti.Cell, ti.Uid)
}

func (ti *TabletInfo) ReplicationPath() string {
	return tabletReplicationPath(ti.Tablet)
}

func tabletReplicationPath(tablet *Tablet) string {
	leaf := TabletAlias{tablet.Cell, tablet.Uid}.String()
	if tablet.Parent.Uid == NO_TABLET {
		return leaf
	}
	// FIXME(alainjobart) assumes one level of replication hierarchy
	return path.Join(tablet.Parent.String(), leaf)
}

func NewTablet(cell string, uid uint32, parent TabletAlias, vtAddr, mysqlAddr, keyspace, shardId string, tabletType TabletType) (*Tablet, error) {
	state := STATE_READ_ONLY
	if tabletType == TYPE_MASTER {
		state = STATE_READ_WRITE
		if parent.Uid != NO_TABLET {
			return nil, fmt.Errorf("master cannot have parent: %v", parent.Uid)
		}
	}

	// check the values for vtAddr and mysqlAddr are correct
	_, _, err := netutil.SplitHostPort(vtAddr)
	if err != nil {
		return nil, err
	}
	_, _, err = netutil.SplitHostPort(mysqlAddr)
	if err != nil {
		return nil, err
	}

	// These value will get resolved on tablet server startup.
	secureAddr := ""
	mysqlIpAddr := ""
	return &Tablet{cell, uid, parent, vtAddr, secureAddr, mysqlAddr, mysqlIpAddr, keyspace, shardId, tabletType, state, "", key.KeyRange{}}, nil
}

func TabletFromJson(data string) (*Tablet, error) {
	t := &Tablet{}
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func TabletInfoFromJson(data string, version int) (*TabletInfo, error) {
	tablet, err := TabletFromJson(data)
	if err != nil {
		return nil, err
	}
	return &TabletInfo{version, tablet}, nil
}

// UpdateTablet updates the tablet data only - not associated replication paths.
func UpdateTablet(ts Server, tablet *TabletInfo) error {
	version := -1
	if tablet.version != 0 {
		version = tablet.version
	}

	newVersion, err := ts.UpdateTablet(tablet, version)
	if err == nil {
		tablet.version = newVersion
	}
	return err
}

func Validate(ts Server, tabletAlias TabletAlias, tabletReplicationPath string) error {
	// read the tablet record, make sure it parses
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	// make sure the Server is good for this tablet
	if err = ts.ValidateTablet(tabletAlias); err != nil {
		return err
	}

	// Some tablets have no information to generate valid replication paths.
	// We have two cases to handle:
	// - we are in the replication graph, and should have a replication path
	//   (first case below)
	// - we are in scrap mode, but used to be assigned in the graph
	//   somewhere (second case below)
	// Idle tablets are just not in any graph at all, we don't even know
	// their keyspace / shard to know where to check.
	if tablet.IsInReplicationGraph() {
		if err = ts.ValidateShard(tablet.Keyspace, tablet.Shard); err != nil {
			return err
		}
		rp := tablet.ReplicationPath()
		if tabletReplicationPath != "" && tabletReplicationPath != rp {
			return fmt.Errorf("replication path mismatch, tablet expects %v but found %v",
				rp, tabletReplicationPath)
		}

		// Check we are in the replication graph
		_, err = ts.GetReplicationPaths(tablet.Keyspace, tablet.Shard, rp)
		if err != nil {
			return err
		}

	} else if tablet.IsAssigned() {
		// this case is to make sure a scrap node that used to be in
		// a replication graph doesn't leave a node behind.
		// However, while an action is running, there is some
		// time where this might be inconsistent.
		rp := tablet.ReplicationPath()
		_, err = ts.GetReplicationPaths(tablet.Keyspace, tablet.Shard, rp)
		if err != ErrNoNode {
			return fmt.Errorf("unexpected replication path found(possible pending action?): %v (%v)",
				rp, tablet.Type)
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

	return CreateTabletReplicationPaths(ts, tablet)
}

func CreateTabletReplicationPaths(ts Server, tablet *Tablet) error {
	relog.Debug("CreateTabletReplicationPaths %v", tablet.Alias())
	if err := ts.CreateShard(tablet.Keyspace, tablet.Shard); err != nil && err != ErrNodeExists {
		return err
	}

	trrp := tabletReplicationPath(tablet)
	err := ts.CreateReplicationPath(tablet.Keyspace, tablet.Shard, trrp)
	if err != nil && err != ErrNodeExists {
		return err
	}

	return nil
}
