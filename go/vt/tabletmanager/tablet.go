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
	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
)

const (
	vtDbPrefix = "vt_" // Default name for databases create"
)

// A pure data struct for information serialized into json and stored in zookeeper.
type Tablet struct {
	Cell        string             // the zk cell this tablet is assigned to (doesn't change)
	Uid         uint32             // the server id for this instance
	Parent      naming.TabletAlias // the globally unique alias for our replication parent - zero if this is the global master
	Addr        string             // host:port for queryserver
	SecureAddr  string             // host:port for queryserver using encrypted connection
	MysqlAddr   string             // host:port for the mysql instance
	MysqlIpAddr string             // ip:port for the mysql instance - needed to match slaves with tablets and preferable to relying on reverse dns

	Keyspace string
	Shard    string
	Type     naming.TabletType

	State naming.TabletState

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

// export per-tablet functions (mirrors the naming functions)
func (tablet *Tablet) Alias() naming.TabletAlias {
	return naming.TabletAlias{tablet.Cell, tablet.Uid}
}

func (tablet *Tablet) IsServingType() bool {
	return naming.IsServingType(tablet.Type)
}

func (tablet *Tablet) IsInReplicationGraph() bool {
	return naming.IsInReplicationGraph(tablet.Type)
}

func (tablet *Tablet) IsSlaveType() bool {
	return naming.IsSlaveType(tablet.Type)
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
	version int // zk node version - used to prevent stomping concurrent writes
	*Tablet
}

// FIXME(alainjobart) when switch to TopologyServer this will become useless
func (ti *TabletInfo) Path() string {
	return TabletPath(ti.ZkVtRoot(), ti.Uid)
}

// FIXME(alainjobart) when switch to TopologyServer this will become useless
func (ti *TabletInfo) PidPath() string {
	return path.Join(TabletPath(ti.ZkVtRoot(), ti.Uid), "pid")
}

// FIXME(alainjobart) when switch to TopologyServer this will become useless
func (ti *TabletInfo) ShardPath() string {
	return ShardPath(ti.Keyspace, ti.Shard)
}

// FIXME(alainjobart) when switch to TopologyServer this will become useless
func (ti *TabletInfo) KeyspacePath() string {
	return KeyspacePath(ti.Keyspace)
}

// FIXME(alainjobart) when switch to TopologyServer this will become useless
func (ti *TabletInfo) ZkVtRoot() string {
	return fmt.Sprintf("/zk/%v/vt", ti.Cell)
}

// This is the path that indicates the tablet's position in the shard replication graph.
// This is too complicated for zk_path, so it's on this struct.
func (ti *TabletInfo) ReplicationPath() string {
	return TabletReplicationPath(ti.Tablet)
}

func TabletReplicationPath(tablet *Tablet) string {
	zkPath := ShardPath(tablet.Keyspace, tablet.Shard)
	if tablet.Parent.Uid == naming.NO_TABLET {
		zkPath = path.Join(zkPath, fmtAlias(tablet.Cell, tablet.Uid))
	} else {
		// FIXME(msolomon) assumes one level of replication hierarchy
		zkPath = path.Join(zkPath, fmtAlias(tablet.Parent.Cell, tablet.Parent.Uid),
			fmtAlias(tablet.Cell, tablet.Uid))
	}
	return zkPath
}

// TODO(alainjobart) remove old absolute implementation, rename this one
// from RelativeReplicationPath back to ReplicationPath
func (ti *TabletInfo) RelativeReplicationPath() string {
	return TabletRelativeReplicationPath(ti.Tablet)
}

func TabletRelativeReplicationPath(tablet *Tablet) string {
	if tablet.Parent.Uid == naming.NO_TABLET {
		return fmtAlias(tablet.Cell, tablet.Uid)
	}
	// FIXME(alainjobart) assumes one level of replication hierarchy
	return path.Join(fmtAlias(tablet.Parent.Cell, tablet.Parent.Uid), fmtAlias(tablet.Cell, tablet.Uid))
}

func NewTablet(cell string, uid uint32, parent naming.TabletAlias, vtAddr, mysqlAddr, keyspace, shardId string, tabletType naming.TabletType) (*Tablet, error) {
	state := naming.STATE_READ_ONLY
	if tabletType == naming.TYPE_MASTER {
		state = naming.STATE_READ_WRITE
		if parent.Uid != naming.NO_TABLET {
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

	// These value will get resolved on tablet server startup.
	secureAddr := ""
	mysqlIpAddr := ""
	return &Tablet{cell, uid, parent, vtAddr, secureAddr, mysqlAddr, mysqlIpAddr, keyspace, shardId, tabletType, state, "", key.KeyRange{}}, nil
}

func tabletFromJson(data string) (*Tablet, error) {
	t := &Tablet{}
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// Deprecated, use ReadTabletTs
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
	return &TabletInfo{stat.Version(), tablet}, nil
}

func ReadTabletTs(ts naming.TopologyServer, tabletAlias naming.TabletAlias) (*TabletInfo, error) {
	data, version, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}
	tablet, err := tabletFromJson(data)
	if err != nil {
		return nil, err
	}
	return &TabletInfo{version, tablet}, nil
}

// UpdateTablet updates the tablet data only - not associated replication paths.
func UpdateTablet(ts naming.TopologyServer, tablet *TabletInfo) error {
	version := -1
	if tablet.version != 0 {
		version = tablet.version
	}

	newVersion, err := ts.UpdateTablet(tablet.Alias(), tablet.Json(), version)
	if err == nil {
		tablet.version = newVersion
	}
	return err
}

func Validate(ts naming.TopologyServer, tabletAlias naming.TabletAlias, tabletReplicationPath string) error {
	// read the tablet record, make sure it parses
	tablet, err := ReadTabletTs(ts, tabletAlias)
	if err != nil {
		return err
	}

	// make sure the TopologyServer is good for this tablet
	if err = ts.ValidateTablet(tabletAlias); err != nil {
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
		if err = ts.ValidateShard(tablet.Keyspace, tablet.Shard); err != nil {
			return err
		}
		rp := tablet.RelativeReplicationPath()
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
		if err != naming.ErrNoNode {
			return fmt.Errorf("unexpected replication path found(possible pending action?): %v (%v)",
				rp, tablet.Type)
		}
	}

	return nil
}

// CreateTablet creates a new tablet and all associated paths for the
// replication graph.
func CreateTablet(ts naming.TopologyServer, tablet *Tablet) error {
	// Have the TopologyServer create the tablet
	err := ts.CreateTablet(tablet.Alias(), tablet.Json())
	if err != nil {
		return err
	}

	if !tablet.IsInReplicationGraph() {
		return nil
	}

	return CreateTabletReplicationPaths(ts, tablet)
}

func CreateTabletReplicationPaths(ts naming.TopologyServer, tablet *Tablet) error {
	relog.Debug("CreateTabletReplicationPaths %v", tablet.Alias())
	if err := ts.CreateShard(tablet.Keyspace, tablet.Shard, newShard().Json()); err != nil && err != naming.ErrNodeExists {
		return err
	}

	trrp := TabletRelativeReplicationPath(tablet)
	err := ts.CreateReplicationPath(tablet.Keyspace, tablet.Shard, trrp)
	if err != nil && err != naming.ErrNodeExists {
		return err
	}

	return nil
}
