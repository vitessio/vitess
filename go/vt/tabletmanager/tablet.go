// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

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

var allTabletTypes = []TabletType{TYPE_IDLE,
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

var slaveTabletTypes = []TabletType{
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
	AllTabletTypeStrings = makeTypeList(allTabletTypes)
	SlaveTabletTypeStrings = makeTypeList(slaveTabletTypes)
}

// Can this db type be trivially reassigned without changes to the replication graph?
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
		return newTabletType == TYPE_SPARE
	}
	return false
}

// Should we allow this transition at all?  Most transitions are
// allowed, but some don't make sense under any circumstances. If a
// transistion could be forced, don't disallow it here.
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

const (
	// According to docs, the tablet uid / (mysql server id) is uint32.
	// However, zero appears to be a sufficiently degenerate value to use
	// as a marker for not having a parent server id.
	// http://dev.mysql.com/doc/refman/5.1/en/replication-options.html
	NO_TABLET = 0
)

type TabletState string

const (
	// The normal state for a master
	STATE_READ_WRITE = TabletState("ReadWrite")
	// The normal state for a slave, or temporarily a master. Not to be confused with type, which implies a workload.
	STATE_READ_ONLY = TabletState("ReadOnly")
)

// Tablets are really globally unique, but crawling every cell to find out where
// it lives is time consuming and expensive. This is only needed during complex operations.
// Tablet cell assignments don't change that often, thus using a TabletAlias is efficient.
type TabletAlias struct {
	Cell string
	Uid  uint32
}

func (ta *TabletAlias) String() string {
	return fmtAlias(ta.Cell, ta.Uid)
}

const (
	vtDbPrefix = "vt_" // Default name for databases create"
)

// A pure data struct for information serialized into json and stored in zookeeper.
type Tablet struct {
	Cell        string      // the zk cell this tablet is assigned to (doesn't change)
	Uid         uint32      // the server id for this instance
	Parent      TabletAlias // the globally unique alias for our replication parent - zero if this is the global master
	Addr        string      // host:port for queryserver
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

// DbName is implied by keyspace. Having the shard information in the database name
// complicates mysql replication.
func (tablet *Tablet) DbName() string {
	if tablet.DbNameOverride != "" {
		return tablet.DbNameOverride
	}
	if tablet.Keyspace == "" {
		return ""
	}
	return vtDbPrefix + tablet.Keyspace
}

func (tablet *Tablet) Alias() TabletAlias {
	return TabletAlias{tablet.Cell, tablet.Uid}
}

func IsServingType(tt TabletType) bool {
	switch tt {
	case TYPE_MASTER, TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH:
		return true
	}
	return false
}

func (tablet *Tablet) IsServingType() bool {
	return IsServingType(tablet.Type)
}

// Should this tablet appear in the replication graph?
// Only IDLE and SCRAP are not in the replication graph.
// The other non-obvious types are BACKUP, SNAPSHOT_SOURCE, RESTORE
// and LAG_ORPHAN: these have had a master at some point (or were the
// master), so they are in the graph.
func (tablet *Tablet) IsInReplicationGraph() bool {
	switch tablet.Type {
	case TYPE_IDLE, TYPE_SCRAP:
		return false
	}
	return true
}

// Should this type be connected to a master db and actively replicating?
// MASTER is not obviously (only support one level replication graph)
// IDLE and SCRAP are not either
// BACKUP, RESTORE, LAG_ORPHAN may or may not be, but we don't know for sure
func (tablet *Tablet) IsSlaveType() bool {
	switch tablet.Type {
	case TYPE_MASTER, TYPE_IDLE, TYPE_SCRAP, TYPE_BACKUP, TYPE_RESTORE, TYPE_LAG_ORPHAN:
		return false
	}
	return true
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
	host, _, err := splitHostPort(tablet.Addr)
	if err != nil {
		panic(err) // should not happen, Addr was checked at creation
	}
	return host
}

type TabletInfo struct {
	zkVtRoot string // zk path to vt subtree - /zk/test/vt for instance
	version  int    // zk node version - used to prevent stomping concurrent writes
	*Tablet
}

func (ti *TabletInfo) Path() string {
	return TabletPath(ti.zkVtRoot, ti.Uid)
}

func (ti *TabletInfo) PidPath() string {
	return path.Join(TabletPath(ti.zkVtRoot, ti.Uid), "pid")
}

func (ti *TabletInfo) ShardPath() string {
	return ShardPath(ti.zkVtRoot, ti.Keyspace, ti.Shard)
}

func (ti *TabletInfo) KeyspacePath() string {
	return KeyspacePath(ti.zkVtRoot, ti.Keyspace)
}

// This is the path that indicates the tablet's position in the shard replication graph.
// This is too complicated for zk_path, so it's on this struct.
func (ti *TabletInfo) ReplicationPath() (string, error) {
	return TabletReplicationPath(ti.zkVtRoot, ti.Tablet)
}

func TabletReplicationPath(zkVtRoot string, tablet *Tablet) (string, error) {
	zkPath := ShardPath(zkVtRoot, tablet.Keyspace, tablet.Shard)
	cell, err := zk.ZkCellFromZkPath(zkVtRoot)
	if err != nil {
		return "", err
	}
	if cell == "local" || cell == "global" {
		return "", fmt.Errorf("invalid cell name for replication path: %v", cell)
	}
	if tablet.Parent.Uid == NO_TABLET {
		zkPath = path.Join(zkPath, fmtAlias(tablet.Cell, tablet.Uid))
	} else {
		// FIXME(msolomon) assumes one level of replication hierarchy
		zkPath = path.Join(zkPath, fmtAlias(tablet.Parent.Cell, tablet.Parent.Uid),
			fmtAlias(tablet.Cell, tablet.Uid))
	}
	return zkPath, nil
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
	_, _, err := splitHostPort(vtAddr)
	if err != nil {
		return nil, err
	}
	_, _, err = splitHostPort(mysqlAddr)
	if err != nil {
		return nil, err
	}

	// This value will get resolved on tablet server startup.
	mysqlIpAddr := ""
	return &Tablet{cell, uid, parent, vtAddr, mysqlAddr, mysqlIpAddr, keyspace, shardId, tabletType, state, "", key.KeyRange{}}, nil
}

func tabletFromJson(data string) (*Tablet, error) {
	t := &Tablet{}
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func ReadTablet(zconn zk.Conn, zkTabletPath string) (*TabletInfo, error) {
	if err := IsTabletPath(zkTabletPath); err != nil {
		return nil, err
	}
	data, stat, err := zconn.Get(zkTabletPath)
	if err != nil {
		return nil, err
	}
	tablet, err := tabletFromJson(data)
	if err != nil {
		return nil, err
	}
	zkVtRoot, err := VtRootFromTabletPath(zkTabletPath)
	if err != nil {
		return nil, err
	}
	return &TabletInfo{zkVtRoot, stat.Version(), tablet}, nil
}

// Update tablet data only - not associated paths.
func UpdateTablet(zconn zk.Conn, zkTabletPath string, tablet *TabletInfo) error {
	if err := IsTabletPath(zkTabletPath); err != nil {
		return err
	}
	version := -1
	if tablet.version != 0 {
		version = tablet.version
	}

	stat, err := zconn.Set(zkTabletPath, tablet.Json(), version)
	if err == nil {
		tablet.version = stat.Version()
	}
	return err
}

func Validate(zconn zk.Conn, zkTabletPath string, zkTabletReplicationPath string) error {
	if err := IsTabletPath(zkTabletPath); err != nil {
		return err
	}

	tablet, err := ReadTablet(zconn, zkTabletPath)
	if err != nil {
		return err
	}

	zkPaths := make([]string, 1, 2)
	zkPaths[0], err = TabletActionPath(zkTabletPath)
	if err != nil {
		return err
	}

	// Some tablets have no information to generate valid replication paths.
	// We have two cases to handle:
	// - we are in the replication graph, and should have a ZK path
	//   (first case below)
	// - we are in scrap mode, but used to be assigned in the graph
	//   somewhere (second case below)
	// Idle tablets are just not in any graph at all, we don't even know
	// their keyspace / shard to know where to check.
	if tablet.IsInReplicationGraph() {
		sap, err := ShardActionPath(tablet.ShardPath())
		if err != nil {
			return err
		}
		zkPaths = append(zkPaths, sap)
		rp, err := tablet.ReplicationPath()
		if err != nil {
			return err
		}
		if zkTabletReplicationPath != "" && zkTabletReplicationPath != rp {
			return fmt.Errorf("replication path mismatch, tablet expects %v but found %v",
				rp, zkTabletReplicationPath)
		}
		// Unless we are scrapped or idle, check we are in the replication graph
		zkPaths = append(zkPaths, rp)

	} else if tablet.IsAssigned() {
		// this case is to make sure a scrap node that used to be in
		// a replication graph doesn't leave a node behind.
		// However, while an action is running, there is some
		// time where this might be inconsistent.
		rp, err := tablet.ReplicationPath()
		if err != nil {
			return err
		}
		_, _, err = zconn.Get(rp)
		if !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return fmt.Errorf("unexpected replication path found(possible pending action?): %v (%v)",
				rp, tablet.Type)
		}
	}

	for _, zkPath := range zkPaths {
		_, _, err := zconn.Get(zkPath)
		if err != nil {
			return err
		}
	}

	return nil
}

// Create a new tablet and all associated global zk paths for the replication graph.
func CreateTablet(zconn zk.Conn, zkTabletPath string, tablet *Tablet) error {
	if err := IsTabletPath(zkTabletPath); err != nil {
		return err
	}

	// Create /vt/tablets/<uid>
	_, err := zk.CreateRecursive(zconn, zkTabletPath, tablet.Json(), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	// Create /vt/tablets/<uid>/action
	tap, err := TabletActionPath(zkTabletPath)
	if err != nil {
		return err
	}
	_, err = zconn.Create(tap, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	// Create /vt/tablets/<uid>/actionlog
	talp, err := TabletActionLogPath(zkTabletPath)
	if err != nil {
		return err
	}
	_, err = zconn.Create(talp, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	if !tablet.IsInReplicationGraph() {
		return nil
	}

	return CreateTabletReplicationPaths(zconn, zkTabletPath, tablet)
}

func CreateTabletReplicationPaths(zconn zk.Conn, zkTabletPath string, tablet *Tablet) error {
	relog.Debug("CreateTabletReplicationPaths %v", zkTabletPath)
	if err := IsTabletPath(zkTabletPath); err != nil {
		return err
	}

	zkVtRootPath, err := VtRootFromTabletPath(zkTabletPath)
	if err != nil {
		return err
	}

	shardPath := ShardPath(zkVtRootPath, tablet.Keyspace, tablet.Shard)
	// Create /vt/keyspaces/<keyspace>/shards/<shard id>
	_, err = zk.CreateRecursive(zconn, shardPath, newShard().Json(), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	shardActionPath, err := ShardActionPath(shardPath)
	if err != nil {
		return err
	}
	// Create /vt/keyspaces/<keyspace>/shards/<shard id>/action
	_, err = zk.CreateRecursive(zconn, shardActionPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	shardActionLogPath, err := ShardActionLogPath(shardPath)
	if err != nil {
		return err
	}
	// Create /vt/keyspaces/<keyspace>/shards/<shard id>/actionlog
	_, err = zk.CreateRecursive(zconn, shardActionLogPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	trp, err := TabletReplicationPath(zkVtRootPath, tablet)
	if err != nil {
		return err
	}
	_, err = zk.CreateRecursive(zconn, trp, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	return nil
}
