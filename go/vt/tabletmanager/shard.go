// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

/*
Functions for dealing with shard representations in zookeeper.
*/

// A pure data struct for information serialized into json and stored in zookeeper
// FIXME(msolomon) More will be required here, but for now I don't know the best way
// to handle having ad-hoc db types beyond replica etc.
// This node is used to present a controlled view of the shard, unaware
// of every management action.
type Shard struct {
	// There can be only at most one master, but there may be none. (0)
	MasterAlias TabletAlias
	// Uids by type - could be a generic map.
	ReplicaAliases []TabletAlias
	RdonlyAliases  []TabletAlias
	// This must match the shard name based on our other conventions, but
	// helpful to have it decomposed here.
	KeyRange key.KeyRange
}

func (shard *Shard) Contains(tablet *Tablet) bool {
	alias := TabletAlias{tablet.Cell, tablet.Uid}
	switch tablet.Type {
	case TYPE_MASTER:
		return shard.MasterAlias == alias
	case TYPE_REPLICA:
		for _, replicaAlias := range shard.ReplicaAliases {
			if replicaAlias == alias {
				return true
			}
		}
	case TYPE_RDONLY:
		for _, rdonlyAlias := range shard.RdonlyAliases {
			if rdonlyAlias == alias {
				return true
			}
		}
	}
	return false
}

func (shard *Shard) Json() string {
	return jscfg.ToJson(shard)
}

func newShard() *Shard {
	return &Shard{ReplicaAliases: make([]TabletAlias, 0, 16),
		RdonlyAliases: make([]TabletAlias, 0, 16)}
}

func zkShardFromJson(data string) (*Shard, error) {
	shard := newShard()
	err := json.Unmarshal([]byte(data), shard)
	if err != nil {
		return nil, fmt.Errorf("bad shard data %v", err)
	}
	return shard, nil
}

/*
A meta struct that contains paths to give the zk data more context and convenience
This is the main way we interact with a shard.
*/
type ShardInfo struct {
	zkVtRoot  string // root path in zk for all vt nodes
	keyspace  string
	shardName string
	*Shard
}

func (si *ShardInfo) Json() string {
	return si.Shard.Json()
}

func (si *ShardInfo) ShardPath() string {
	return ShardPath(si.zkVtRoot, si.keyspace, si.shardName)
}

func (si *ShardInfo) TabletPath(alias TabletAlias) string {
	zkRoot := fmt.Sprintf("/zk/%v/vt", alias.Cell)
	return TabletPath(zkRoot, alias.Uid)
}

func (si *ShardInfo) MasterTabletPath() (string, error) {
	if si.Shard.MasterAlias.Uid == NO_TABLET {
		return "", fmt.Errorf("no master tablet for shard %v", si.ShardPath())
	}

	return si.TabletPath(si.Shard.MasterAlias), nil
}

func (si *ShardInfo) Rebuild(shardTablets []*TabletInfo) error {
	tmp := newShard()
	for i, ti := range shardTablets {
		tablet := ti.Tablet
		cell := tablet.Cell
		alias := TabletAlias{cell, tablet.Uid}
		switch tablet.Type {
		case TYPE_MASTER:
			tmp.MasterAlias = alias
		case TYPE_REPLICA:
			tmp.ReplicaAliases = append(tmp.ReplicaAliases, alias)
		case TYPE_RDONLY:
			tmp.RdonlyAliases = append(tmp.RdonlyAliases, alias)
		}

		if i == 0 {
			// copy the first KeyRange
			tmp.KeyRange = tablet.KeyRange
		} else {
			// verify the subsequent ones
			if tmp.KeyRange != tablet.KeyRange {
				return fmt.Errorf("inconsistent KeyRange: %v != %v", tmp.KeyRange, tablet.KeyRange)
			}
		}
	}
	si.Shard = tmp
	return nil
}

// shardData: JSON blob
// force: skip error on empty JSON data
func newShardInfo(zkShardPath, shardData string) (shardInfo *ShardInfo, err error) {
	if shardData == "" {
		return nil, fmt.Errorf("empty shard data: %v", zkShardPath)
	}

	zkVtRoot := VtRootFromShardPath(zkShardPath)
	pathParts := strings.Split(zkShardPath, "/")
	keyspace := pathParts[len(pathParts)-3]
	shardName := pathParts[len(pathParts)-1]

	var shard *Shard
	if shardData != "" {
		shard, err = zkShardFromJson(shardData)
		if err != nil {
			return nil, err
		}
	}

	return &ShardInfo{zkVtRoot, keyspace, shardName, shard}, nil
}

func ReadShard(zconn zk.Conn, zkShardPath string) (*ShardInfo, error) {
	MustBeShardPath(zkShardPath)
	data, _, err := zconn.Get(zkShardPath)
	if err != nil {
		return nil, err
	}
	shardInfo, err := newShardInfo(zkShardPath, data)
	if err != nil {
		return nil, err
	}
	return shardInfo, nil
}

func UpdateShard(zconn zk.Conn, si *ShardInfo) error {
	_, err := zconn.Set(si.ShardPath(), si.Json(), -1)
	return err
}

func FindAllTabletAliasesInShard(zconn zk.Conn, zkShardPath string) ([]TabletAlias, error) {
	children, err := zk.ChildrenRecursive(zconn, zkShardPath)
	if err != nil {
		return nil, err
	}

	aliases := make([]TabletAlias, 0, len(children))
	for _, child := range children {
		alias := path.Base(child)
		if strings.Contains(alias, "action") {
			continue
		}
		zkTabletReplicationPath := path.Join(zkShardPath, child)
		cell, uid, err := parseTabletReplicationPath(zkTabletReplicationPath)
		if err != nil {
			continue
		}
		aliases = append(aliases, TabletAlias{cell, uid})
	}

	return aliases, nil
}

/*
Update shard file with new master, replicas, etc.
/vt/keyspaces/<keyspace>/shards/<shard uid>
Write to zkns files?
Re-read from zk to make sure we are using the side effects of all actions.

This function should only be used with an action lock on the shard - otherwise the
consistency of the serving graph data can't be guaranteed.
*/
func RebuildShard(zconn zk.Conn, zkShardPath string) error {
	// NOTE(msolomon) nasty hack - pass non-empty string to bypass data check
	shardInfo, err := newShardInfo(zkShardPath, "{}")
	if err != nil {
		return err
	}
	aliases, err := FindAllTabletAliasesInShard(zconn, shardInfo.ShardPath())
	if err != nil {
		return err
	}
	tablets := make([]*TabletInfo, 0, len(aliases))
	for _, alias := range aliases {
		tablet, err := ReadTablet(zconn, shardInfo.TabletPath(alias))
		if err != nil {
			return err
		}
		tablets = append(tablets, tablet)
	}
	if err = shardInfo.Rebuild(tablets); err != nil {
		return err
	}
	if err = UpdateShard(zconn, shardInfo); err != nil {
		return err
	}

	return RebuildSrvShards(zconn, zkShardPath, shardInfo, tablets)
}

// Write serving graph data to /zk/local/vt/ns/...
func RebuildSrvShards(zconn zk.Conn, zkShardPath string, shardInfo *ShardInfo, tablets []*TabletInfo) error {
	// Get all existing db types so they can be removed if nothing had been editted.
	// This applies to all cells, which can't be determined until you walk through all the tablets.
	existingDbTypePaths := make(map[string]bool)

	// Update db type addresses in the serving graph
	pathAddrsMap := make(map[string]*naming.VtnsAddrs)
	for _, tablet := range tablets {
		zkSgShardPath := naming.ZkPathForVtShard(tablet.Tablet.Cell, tablet.Tablet.Keyspace, tablet.Shard)
		children, _, err := zconn.Children(zkSgShardPath)
		if err != nil {
			if !zookeeper.IsError(err, zookeeper.ZNONODE) {
				relog.Warning("unable to list existing db types: %v", err)
			}
		} else {
			for _, child := range children {
				existingDbTypePaths[path.Join(zkSgShardPath, child)] = true
			}
		}

		// Check IsServingType after we have populated existingDbTypePaths
		// so we properly prune data if the definition of serving type
		// changes.
		if !tablet.IsServingType() {
			continue
		}

		zkPath := naming.ZkPathForVtName(tablet.Tablet.Cell, tablet.Keyspace, tablet.Shard, string(tablet.Type))

		addrs, ok := pathAddrsMap[zkPath]
		if !ok {
			addrs = naming.NewAddrs()
			pathAddrsMap[zkPath] = addrs
		}

		entry := vtnsAddrForTablet(tablet.Tablet)
		addrs.Entries = append(addrs.Entries, *entry)
	}

	for zkPath, addrs := range pathAddrsMap {
		data := jscfg.ToJson(addrs)
		_, err := zk.CreateRecursive(zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				// Node already exists - just stomp away. Multiple writers shouldn't be here.
				// We use RetryChange here because it won't update the node unnecessarily.
				f := func(oldValue string, oldStat *zookeeper.Stat) (string, error) {
					return data, nil
				}
				err = zconn.RetryChange(zkPath, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
			}
		}
		if err != nil {
			return fmt.Errorf("writing endpoints failed: %v", err)
		}
	}

	// Delete any pre-existing paths that were not updated by this process.
	for zkDbTypePath, _ := range existingDbTypePaths {
		if _, ok := pathAddrsMap[zkDbTypePath]; !ok {
			relog.Info("removing stale db type from serving graph: %v", zkDbTypePath)
			if err := zconn.Delete(zkDbTypePath, -1); err != nil {
				relog.Warning("unable to remove stale db type from serving graph: %v", err)
			}
		}
	}

	// Update per-shard information per cell-specific serving path.
	srvShardByPath := make(map[string]*naming.SrvShard)
	for zkPath, addrs := range pathAddrsMap {
		// zkPath will be /zk/<cell>/vt/ns/<keyspace>/<shard>/<type>
		srvShardPath := path.Dir(zkPath)
		tabletType := TabletType(path.Base(zkPath))

		srvShard, ok := srvShardByPath[srvShardPath]
		if !ok {
			srvShard = &naming.SrvShard{KeyRange: shardInfo.KeyRange, AddrsByType: make(map[string]naming.VtnsAddrs)}
			srvShardByPath[srvShardPath] = srvShard
		}
		srvShard.AddrsByType[string(tabletType)] = *addrs
	}

	for srvPath, srvShard := range srvShardByPath {
		data := jscfg.ToJson(srvShard)
		// Stomp away - presume this update will be guarded by a lock node.
		_, err := zconn.Set(srvPath, data, -1)
		if err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}

// This function should only be used with an action lock on the shard - otherwise the
// consistency of the serving graph data can't be guaranteed.
//
// Take data from the global keyspace and rebuild the local serving copies in each
// cell.
func RebuildKeyspace(zconn zk.Conn, zkKeyspacePath string) error {
	// FIXME(msolomon) apply a deferred error decorator?

	vtRoot := VtRootFromKeyspacePath(zkKeyspacePath)
	keyspace := path.Base(zkKeyspacePath)
	shardNames, _, err := zconn.Children(path.Join(zkKeyspacePath, "shards"))
	if err != nil {
		return err
	}

	srvKeyspaceByPath := make(map[string]*naming.SrvKeyspace)

	zkShardPath := ShardPath(vtRoot, keyspace, shardNames[0])
	aliases, err := FindAllTabletAliasesInShard(zconn, zkShardPath)
	if err != nil {
		return err
	}

	for _, alias := range aliases {
		zkLocalKeyspace := naming.ZkPathForVtKeyspace(alias.Cell, keyspace)
		if _, ok := srvKeyspaceByPath[zkLocalKeyspace]; !ok {
			srvKeyspaceByPath[zkLocalKeyspace] = &naming.SrvKeyspace{Shards: make([]naming.SrvShard, 0, 16)}
		}
	}

	for srvPath, srvKeyspace := range srvKeyspaceByPath {
		for _, shardName := range shardNames {
			srvShard, err := naming.ReadSrvShard(zconn, path.Join(srvPath, shardName))
			if err != nil {
				return err
			}
			srvKeyspace.Shards = append(srvKeyspace.Shards, *srvShard)
		}
		naming.SrvShardArray(srvKeyspace.Shards).Sort()

		// check the first Start is MinKey, the last End is MaxKey,
		// and the values in between match: End[i] == Start[i+1]
		if srvKeyspace.Shards[0].KeyRange.Start != key.MinKey {
			return fmt.Errorf("Keyspace does not start with %v", key.MinKey)
		}
		if srvKeyspace.Shards[len(srvKeyspace.Shards)-1].KeyRange.End != key.MaxKey {
			return fmt.Errorf("Keyspace does not end with %v", key.MaxKey)
		}
		for i, _ := range srvKeyspace.Shards[0 : len(srvKeyspace.Shards)-1] {
			if srvKeyspace.Shards[i].KeyRange.End != srvKeyspace.Shards[i+1].KeyRange.Start {
				return fmt.Errorf("Non-contiguous KeyRange values at shard %v to %v: %v != %v", i, i+1, srvKeyspace.Shards[i].KeyRange.End, srvKeyspace.Shards[i+1].KeyRange.Start)
			}
		}
	}

	for srvPath, srvKeyspace := range srvKeyspaceByPath {
		data := jscfg.ToJson(srvKeyspace)
		// Stomp away - presume this update will be guarded by a lock node.
		_, err = zconn.Set(srvPath, data, -1)
		if err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}
