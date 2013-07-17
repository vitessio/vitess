// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"encoding/json"
	"fmt"
	"path"
	"sync"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/vt/key"
)

// Functions for dealing with shard representations in topology.

// A pure data struct for information serialized into json and stored in topology server
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

func shardFromJson(data string) (*Shard, error) {
	shard := newShard()
	err := json.Unmarshal([]byte(data), shard)
	if err != nil {
		return nil, fmt.Errorf("bad shard data %v", err)
	}
	return shard, nil
}

/*
A meta struct that contains paths to give the data more context and convenience
This is the main way we interact with a shard.
*/
type ShardInfo struct {
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
func NewShardInfo(keyspace, shard, shardData string) (shardInfo *ShardInfo, err error) {
	if shardData == "" {
		return nil, fmt.Errorf("empty shard data for shard: %v/%v", keyspace, shard)
	}

	var s *Shard
	if shardData != "" {
		s, err = shardFromJson(shardData)
		if err != nil {
			return nil, err
		}
	}

	return &ShardInfo{keyspace, shard, s}, nil
}

func tabletAliasesRecursive(ts Server, keyspace, shard, repPath string) ([]TabletAlias, error) {
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	result := make([]TabletAlias, 0, 32)
	children, err := ts.GetReplicationPaths(keyspace, shard, repPath)
	if err != nil {
		return nil, err
	}

	for _, child := range children {
		wg.Add(1)
		go func(child TabletAlias) {
			childPath := path.Join(repPath, child.String())
			rChildren, subErr := tabletAliasesRecursive(ts, keyspace, shard, childPath)
			if subErr != nil {
				// If other processes are deleting
				// nodes, we need to ignore the
				// missing nodes.
				if subErr != ErrNoNode {
					mutex.Lock()
					err = subErr
					mutex.Unlock()
				}
			} else {
				mutex.Lock()
				result = append(result, child)
				for _, rChild := range rChildren {
					result = append(result, rChild)
				}
				mutex.Unlock()
			}
			wg.Done()
		}(child)
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func FindAllTabletAliasesInShard(ts Server, keyspace, shard string) ([]TabletAlias, error) {
	return tabletAliasesRecursive(ts, keyspace, shard, "")
}
