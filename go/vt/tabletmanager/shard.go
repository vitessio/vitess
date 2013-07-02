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
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
)

// Functions for dealing with shard representations in zookeeper.

// A pure data struct for information serialized into json and stored in zookeeper
// FIXME(msolomon) More will be required here, but for now I don't know the best way
// to handle having ad-hoc db types beyond replica etc.
// This node is used to present a controlled view of the shard, unaware
// of every management action.
type Shard struct {
	// There can be only at most one master, but there may be none. (0)
	MasterAlias naming.TabletAlias
	// Uids by type - could be a generic map.
	ReplicaAliases []naming.TabletAlias
	RdonlyAliases  []naming.TabletAlias
	// This must match the shard name based on our other conventions, but
	// helpful to have it decomposed here.
	KeyRange key.KeyRange
}

func (shard *Shard) Contains(tablet *Tablet) bool {
	alias := naming.TabletAlias{tablet.Cell, tablet.Uid}
	switch tablet.Type {
	case naming.TYPE_MASTER:
		return shard.MasterAlias == alias
	case naming.TYPE_REPLICA:
		for _, replicaAlias := range shard.ReplicaAliases {
			if replicaAlias == alias {
				return true
			}
		}
	case naming.TYPE_RDONLY:
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
	return &Shard{ReplicaAliases: make([]naming.TabletAlias, 0, 16),
		RdonlyAliases: make([]naming.TabletAlias, 0, 16)}
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

func (si *ShardInfo) Keyspace() string {
	return si.keyspace
}

func (si *ShardInfo) ShardName() string {
	return si.shardName
}

func (si *ShardInfo) Json() string {
	return si.Shard.Json()
}

func (si *ShardInfo) ShardPath() string {
	return ShardPath(si.zkVtRoot, si.keyspace, si.shardName)
}

func (si *ShardInfo) TabletPath(alias naming.TabletAlias) string {
	zkRoot := fmt.Sprintf("/zk/%v/vt", alias.Cell)
	return TabletPath(zkRoot, alias.Uid)
}

func (si *ShardInfo) MasterTabletPath() (string, error) {
	if si.Shard.MasterAlias.Uid == naming.NO_TABLET {
		return "", fmt.Errorf("no master tablet for shard %v", si.ShardPath())
	}

	return si.TabletPath(si.Shard.MasterAlias), nil
}

func (si *ShardInfo) Rebuild(shardTablets []*TabletInfo) error {
	tmp := newShard()
	for i, ti := range shardTablets {
		tablet := ti.Tablet
		cell := tablet.Cell
		alias := naming.TabletAlias{cell, tablet.Uid}
		switch tablet.Type {
		case naming.TYPE_MASTER:
			tmp.MasterAlias = alias
		case naming.TYPE_REPLICA:
			tmp.ReplicaAliases = append(tmp.ReplicaAliases, alias)
		case naming.TYPE_RDONLY:
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
func NewShardInfo(zkShardPath, shardData string) (shardInfo *ShardInfo, err error) {
	if shardData == "" {
		return nil, fmt.Errorf("empty shard data: %v", zkShardPath)
	}

	zkVtRoot, err := VtRootFromShardPath(zkShardPath)
	if err != nil {
		return nil, err
	}
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
	if err := IsShardPath(zkShardPath); err != nil {
		return nil, err
	}
	data, _, err := zconn.Get(zkShardPath)
	if err != nil {
		return nil, err
	}
	shardInfo, err := NewShardInfo(zkShardPath, data)
	if err != nil {
		return nil, err
	}
	return shardInfo, nil
}

func UpdateShard(zconn zk.Conn, si *ShardInfo) error {
	_, err := zconn.Set(si.ShardPath(), si.Json(), -1)
	return err
}

func FindAllTabletAliasesInShard(zconn zk.Conn, zkShardPath string) ([]naming.TabletAlias, error) {
	children, err := zk.ChildrenRecursive(zconn, zkShardPath)
	if err != nil {
		return nil, err
	}

	aliases := make([]naming.TabletAlias, 0, len(children))
	for _, child := range children {
		alias := path.Base(child)
		if strings.HasPrefix(alias, "action") {
			continue
		}
		zkTabletReplicationPath := path.Join(zkShardPath, child)
		cell, uid, err := ParseTabletReplicationPath(zkTabletReplicationPath)
		if err != nil {
			continue
		}
		aliases = append(aliases, naming.TabletAlias{cell, uid})
	}

	return aliases, nil
}
