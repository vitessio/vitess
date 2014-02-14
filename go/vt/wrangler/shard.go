// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
)

// shard related methods for Wrangler

func (wr *Wrangler) lockShard(keyspace, shard string, actionNode *actionnode.ActionNode) (lockPath string, err error) {
	log.Infof("Locking shard %v/%v for action %v", keyspace, shard, actionNode.Action)
	return wr.ts.LockShardForAction(keyspace, shard, actionNode.ToJson(), wr.lockTimeout, interrupted)
}

func (wr *Wrangler) unlockShard(keyspace, shard string, actionNode *actionnode.ActionNode, lockPath string, actionError error) error {
	// first update the actionNode
	if actionError != nil {
		log.Infof("Unlocking shard %v/%v for action %v with error %v", keyspace, shard, actionNode.Action, actionError)
		actionNode.Error = actionError.Error()
		actionNode.State = actionnode.ACTION_STATE_FAILED
	} else {
		log.Infof("Unlocking shard %v/%v for successful action %v", keyspace, shard, actionNode.Action)
		actionNode.Error = ""
		actionNode.State = actionnode.ACTION_STATE_DONE
	}
	err := wr.ts.UnlockShardForAction(keyspace, shard, lockPath, actionNode.ToJson())
	if actionError != nil {
		if err != nil {
			// this will be masked
			log.Warningf("UnlockShardForAction failed: %v", err)
		}
		return actionError
	}
	return err
}

// SetShardServedTypes changes the ServedTypes parameter of a shard.
// It does not rebuild any serving graph or do any consistency check (yet).
func (wr *Wrangler) SetShardServedTypes(keyspace, shard string, servedTypes []topo.TabletType) error {

	actionNode := actionnode.SetShardServedTypes(servedTypes)
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.setShardServedTypes(keyspace, shard, servedTypes)
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) setShardServedTypes(keyspace, shard string, servedTypes []topo.TabletType) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	shardInfo.ServedTypes = servedTypes
	return wr.ts.UpdateShard(shardInfo)
}

// DeleteShard will do all the necessary changes in the topology server
// to entirely remove a shard. It can only work if there are no tablets
// in that shard.
func (wr *Wrangler) DeleteShard(keyspace, shard string) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := GetTabletMapForShard(wr.ts, keyspace, shard)
	if err != nil {
		return err
	}
	if len(tabletMap) > 0 {
		return fmt.Errorf("shard %v/%v still has %v tablets", len(tabletMap))
	}

	// remove the replication graph and serving graph in each cell
	for _, cell := range shardInfo.Cells {
		if err := wr.ts.DeleteShardReplication(cell, keyspace, shard); err != nil {
			log.Warningf("Cannot delete ShardReplication in cell %v for %v/%v: %v", cell, keyspace, shard, err)
		}

		for _, t := range topo.AllTabletTypes {
			if !topo.IsInServingGraph(t) {
				continue
			}

			if err := wr.ts.DeleteSrvTabletType(cell, keyspace, shard, t); err != nil && err != topo.ErrNoNode {
				log.Warningf("Cannot delete EndPoints in cell %v for %v/%v/%v: %v", cell, keyspace, shard, t, err)
			}
		}

		if err := wr.ts.DeleteSrvShard(cell, keyspace, shard); err != nil && err != topo.ErrNoNode {
			log.Warningf("Cannot delete SrvShard in cell %v for %v/%v: %v", cell, keyspace, shard, err)
		}
	}

	return wr.ts.DeleteShard(keyspace, shard)
}
