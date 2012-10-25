// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"fmt"
	"path"
	"sort"
	"time"

	"code.google.com/p/vitess/go/relog"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// Rebuild the serving data while locking out other changes.
func (wr *Wrangler) RebuildShard(zkShardPath string) (actionPath string, err error) {
	tm.MustBeShardPath(zkShardPath)

	actionPath, err = wr.ai.RebuildShard(zkShardPath)
	if err != nil {
		return
	}

	// Make sure two of these don't get scheduled at the same time.
	ok, err := zk.ObtainQueueLock(wr.zconn, actionPath, false)
	if err != nil {
		return
	}

	if !ok {
		// just clean up for now, in the future we may want to try harder, or wait
		wr.zconn.Delete(actionPath, -1)
		panic(fmt.Errorf("failed to obtain action lock: %v", actionPath))
	}

	rebuildErr := tm.RebuildShard(wr.zconn, zkShardPath)

	if err == nil {
		err = zk.DeleteRecursive(wr.zconn, actionPath, -1)
	} else {
		data, stat, err := wr.zconn.Get(actionPath)
		if err == nil {
			var actionNode *tm.ActionNode
			actionNode, err = tm.ActionNodeFromJson(data, actionPath)
			if err == nil {
				actionNode.Error = rebuildErr.Error()
				data = tm.ActionNodeToJson(actionNode)
				_, err = wr.zconn.Set(actionPath, data, stat.Version())
			}
		}
	}

	if rebuildErr != nil {
		return actionPath, rebuildErr
	}

	return
}

// Rebuild the serving data while locking out other changes.
// FIXME(msolomon) reduce copy-paste code from above
func (wr *Wrangler) RebuildKeyspace(zkKeyspacePath string) (actionPath string, err error) {
	tm.MustBeKeyspacePath(zkKeyspacePath)

	actionPath, err = wr.ai.RebuildKeyspace(zkKeyspacePath)
	if err != nil {
		return
	}

	// Make sure two of these don't get scheduled at the same time.
	ok, err := zk.ObtainQueueLock(wr.zconn, actionPath, false)
	if err != nil {
		return
	}

	if !ok {
		// just clean up for now, in the future we may want to try harder, or wait
		wr.zconn.Delete(actionPath, -1)
		panic(fmt.Errorf("failed to obtain action lock: %v", actionPath))
	}

	rebuildErr := tm.RebuildKeyspace(wr.zconn, zkKeyspacePath)

	if err == nil {
		err = zk.DeleteRecursive(wr.zconn, actionPath, -1)
	} else {
		data, stat, err := wr.zconn.Get(actionPath)
		if err == nil {
			var actionNode *tm.ActionNode
			actionNode, err = tm.ActionNodeFromJson(data, actionPath)
			if err == nil {
				actionNode.Error = rebuildErr.Error()
				data = tm.ActionNodeToJson(actionNode)
				_, err = wr.zconn.Set(actionPath, data, stat.Version())
			}
		}
	}

	if rebuildErr != nil {
		return actionPath, rebuildErr
	}

	return
}

// return a sorted list of tablets
func GetAllTablets(zconn zk.Conn, zkVtPath string) ([]*tm.TabletInfo, error) {
	zkTabletsPath := path.Join(zkVtPath, "tablets")
	children, _, err := zconn.Children(zkTabletsPath)
	if err != nil {
		return nil, err
	}

	sort.Strings(children)
	tabletPaths := make([]string, len(children))
	for i, child := range children {
		tabletPaths[i] = path.Join(zkTabletsPath, child)
	}

	tabletMap, _ := GetTabletMap(zconn, tabletPaths)
	tablets := make([]*tm.TabletInfo, 0, len(tabletPaths))
	for _, tabletPath := range tabletPaths {
		tabletInfo, ok := tabletMap[tabletPath]
		if !ok {
			relog.Warning("failed to load tablet %v", tabletPath)
		}
		tablets = append(tablets, tabletInfo)
	}

	return tablets, nil
}

func (wr *Wrangler) RebuildReplicationGraph(zkVtPaths []string, keyspaces []string, waitTime time.Duration) error {
	if zkVtPaths == nil || len(zkVtPaths) == 0 {
		return fmt.Errorf("must specify zkVtPaths to rebuild replication graph")
	}
	allTablets := make([]*tm.TabletInfo, 0, 1024)
	for _, zkVtPath := range zkVtPaths {
		tablets, err := GetAllTablets(wr.zconn, zkVtPath)
		if err != nil {
			return err
		}
		allTablets = append(allTablets, tablets...)
	}
	vtRoot := zkVtPaths[0]
	if keyspaces != nil && len(keyspaces) > 0 {
		for _, keyspace := range keyspaces {
			shardsPath := tm.KeyspaceShardsPath(tm.KeyspacePath(vtRoot, keyspace))
			relog.Debug("delete keyspace shards: %v", shardsPath)
			err := zk.DeleteRecursive(wr.zconn, shardsPath, -1)
			if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
				return err
			}
		}
	}

	shardPaths := make(map[string]bool)
	keyspacePaths := make(map[string]bool)
	hasErr := false
	for _, ti := range allTablets {
		if ti.Type == tm.TYPE_SCRAP || ti.Type == tm.TYPE_IDLE {
			continue
		}
		if keyspaces != nil && len(keyspaces) > 0 && !strInList(keyspaces, ti.Keyspace) {
			continue
		}
		shardPaths[ti.ShardPath()] = true
		keyspacePaths[ti.KeyspacePath()] = true
		err := tm.CreateTabletReplicationPaths(wr.zconn, ti.Path(), ti.Tablet)
		if err != nil {
			hasErr = true
			relog.Warning("failed creating replication path: %v", err)
		}
	}

	for shardPath, _ := range shardPaths {
		actionPath, err := wr.RebuildShard(shardPath)
		if err != nil {
			relog.Warning("RebuildShard failed: %v %v", shardPath, err)
			continue
		}
		wr.ai.WaitForCompletion(actionPath, waitTime)
	}

	for keyspacePath, _ := range keyspacePaths {
		actionPath, err := wr.RebuildKeyspace(keyspacePath)
		if err != nil {
			relog.Warning("RebuildKeyspace failed: %v %v", keyspacePath, err)
			continue
		}
		wr.ai.WaitForCompletion(actionPath, waitTime)
	}

	if hasErr {
		return fmt.Errorf("some errors occurred rebuilding replication graph, consult log")
	}
	return nil

}
