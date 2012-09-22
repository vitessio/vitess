// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"encoding/json"
	"fmt"
	"path"

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
	// a potential master.
	TYPE_EXPERIMENTAL = TabletType("experimental")

	// a slaved copy of the data intentionally lagged for pseudo backup
	TYPE_LAG = TabletType("lag")

	// a slaved copy of the data, but offline to queries other than backup
	// replication sql thread may be stopped
	TYPE_BACKUP = TabletType("backup")

	// A tablet that has not been in the replication graph and is restoring
	// from a snapshot.  idle -> restore -> spare
	TYPE_RESTORE = TabletType("restore")

	// a machine with data that needs to be wiped
	TYPE_SCRAP = TabletType("scrap")
)

// Can this db type be trivially reassigned without changes to the replication grpah?
func IsTrivialTypeChange(oldTabletType, newTabletType TabletType) bool {
	switch oldTabletType {
	case TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_SPARE, TYPE_LAG, TYPE_BACKUP, TYPE_EXPERIMENTAL:
		switch newTabletType {
		case TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH, TYPE_SPARE, TYPE_LAG, TYPE_BACKUP, TYPE_EXPERIMENTAL:
			return true
		}
	case TYPE_SCRAP:
		return newTabletType == TYPE_IDLE
	case TYPE_IDLE:
		return newTabletType == TYPE_RESTORE
	case TYPE_RESTORE:
		return newTabletType == TYPE_IDLE
	}
	return false
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
	Uid  uint
}

func (ta *TabletAlias) String() string {
	return fmtAlias(ta.Cell, ta.Uid)
}

const (
	vtDbPrefix = "vt_" // Default name for databases create"
)

// A pure data struct for information serialized into json and stored in zookeeper.
type Tablet struct {
	Cell      string      // the zk cell this tablet is assigned to (doesn't change)
	Uid       uint        // the server id for this instance
	Parent    TabletAlias // the globally unique alias for our replication parent - zero if this is the global master
	Addr      string      // host:port for queryserver
	MysqlAddr string      // host:port for the mysql instance

	Keyspace string
	Shard    string
	Type     TabletType

	State TabletState

	// Normally the database name is implied by "vt_" + keyspace. I
	// really want to remove this but there are some databases that are
	// hard to rename.
	DbNameOverride string
	key.KeyRange
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

func (tablet *Tablet) IsServingType() bool {
	switch tablet.Type {
	case TYPE_MASTER, TYPE_REPLICA, TYPE_RDONLY, TYPE_BATCH:
		return true
	}
	return false
}

func (tablet *Tablet) IsReplicatingType() bool {
	switch tablet.Type {
	case TYPE_IDLE, TYPE_SCRAP, TYPE_BACKUP, TYPE_RESTORE:
		return false
	}
	return true
}

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
	host, _ := splitHostPort(tablet.Addr)
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
func (ti *TabletInfo) ReplicationPath() string {
	return TabletReplicationPath(ti.zkVtRoot, ti.Tablet)
}

func TabletReplicationPath(zkVtRoot string, tablet *Tablet) string {
	zkPath := ShardPath(zkVtRoot, tablet.Keyspace, tablet.Shard)
	cell := zk.ZkCellFromZkPath(zkVtRoot)
	if cell == "local" || cell == "global" {
		panic(fmt.Errorf("invalid cell name for replication path: %v", cell))
	}
	if tablet.Parent.Uid == NO_TABLET {
		zkPath = path.Join(zkPath, fmtAlias(tablet.Cell, tablet.Uid))
	} else {
		// FIXME(msolomon) assumes one level of replication hierarchy
		zkPath = path.Join(zkPath, fmtAlias(tablet.Parent.Cell, tablet.Parent.Uid),
			fmtAlias(tablet.Cell, tablet.Uid))
	}
	return zkPath
}

func NewTablet(cell string, uid uint, parent TabletAlias, vtAddr, mysqlAddr, keyspace, shardId string, tabletType TabletType) *Tablet {
	state := STATE_READ_ONLY
	if tabletType == TYPE_MASTER {
		state = STATE_READ_WRITE
		if parent.Uid != NO_TABLET {
			panic(fmt.Errorf("master cannot have parent: %v", parent.Uid))
		}
	}

	return &Tablet{cell, uid, parent, vtAddr, mysqlAddr, keyspace, shardId, tabletType, state, "", key.KeyRange{}}
}

func tabletFromJson(data string) *Tablet {
	t := &Tablet{}
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		panic(err)
	}
	return t
}

func ReadTablet(zconn zk.Conn, zkTabletPath string) (*TabletInfo, error) {
	MustBeTabletPath(zkTabletPath)
	data, stat, err := zconn.Get(zkTabletPath)
	if err != nil {
		return nil, err
	}
	tablet := tabletFromJson(data)
	zkVtRoot := VtRootFromTabletPath(zkTabletPath)
	return &TabletInfo{zkVtRoot, stat.Version(), tablet}, nil
}

// Update tablet data only - not associated paths.
func UpdateTablet(zconn zk.Conn, zkTabletPath string, tablet *TabletInfo) error {
	MustBeTabletPath(zkTabletPath)
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
	MustBeTabletPath(zkTabletPath)

	tablet, err := ReadTablet(zconn, zkTabletPath)
	if err != nil {
		return err
	}

	zkPaths := []string{
		TabletActionPath(zkTabletPath),
	}

	// Some tablets have no information to generate valid replication paths.
	if tablet.IsReplicatingType() {
		zkPaths = append(zkPaths, ShardActionPath(tablet.ShardPath()))
		if zkTabletReplicationPath != "" && zkTabletReplicationPath != tablet.ReplicationPath() {
			return fmt.Errorf("replication path mismatch, tablet expects %v but found %v",
				tablet.ReplicationPath(), zkTabletReplicationPath)
		}
		// Unless we are scrapped or idle, check we are in the replication graph
		zkPaths = append(zkPaths, tablet.ReplicationPath())
	} else if tablet.IsAssigned() {
		// Scrap nodes should not appear in the replication graph. However, while
		// an action is running, there is some time where this will be inconsistent.
		_, _, err := zconn.Get(tablet.ReplicationPath())
		if !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return fmt.Errorf("unexpected replication path found(possible pending action?): %v (%v)",
				tablet.ReplicationPath(), tablet.Type)
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
	MustBeTabletPath(zkTabletPath)

	// Create /vt/tablets/<uid>
	_, err := zk.CreateRecursive(zconn, zkTabletPath, tablet.Json(), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	// Create /vt/tablets/<uid>/action
	_, err = zconn.Create(TabletActionPath(zkTabletPath), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		return err
	}

	if !tablet.IsReplicatingType() {
		return nil
	}

	return CreateTabletReplicationPaths(zconn, zkTabletPath, tablet)
}

func CreateTabletReplicationPaths(zconn zk.Conn, zkTabletPath string, tablet *Tablet) error {
	relog.Debug("CreateTabletReplicationPaths %v", zkTabletPath)
	MustBeTabletPath(zkTabletPath)

	zkVtRootPath := VtRootFromTabletPath(zkTabletPath)

	shardPath := ShardPath(zkVtRootPath, tablet.Keyspace, tablet.Shard)
	// Create /vt/keyspaces/<keyspace>/shards/<shard id>
	_, err := zk.CreateRecursive(zconn, shardPath, newShard().Json(), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	shardActionPath := ShardActionPath(shardPath)
	// Create /vt/keyspaces/<keyspace>/shards/<shard id>/action
	_, err = zk.CreateRecursive(zconn, shardActionPath, "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	_, err = zk.CreateRecursive(zconn, TabletReplicationPath(zkVtRootPath, tablet), "", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
		return err
	}

	return nil
}
