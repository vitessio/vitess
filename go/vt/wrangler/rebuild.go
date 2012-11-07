// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"fmt"
	"path"
	"sync"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/naming"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// Cleanup an action node or write back error data to zk.
// Only returns an error if something went wrong with zk.
func (wr *Wrangler) handleActionError(actionPath string, actionErr error) error {
	if actionErr == nil {
		return zk.DeleteRecursive(wr.zconn, actionPath, -1)
	}

	data, stat, err := wr.zconn.Get(actionPath)
	if err == nil {
		var actionNode *tm.ActionNode
		actionNode, err = tm.ActionNodeFromJson(data, actionPath)
		if err == nil {
			actionNode.Error = actionErr.Error()
			data = tm.ActionNodeToJson(actionNode)
			_, err = wr.zconn.Set(actionPath, data, stat.Version())
		}
	}
	return err
}

// Rebuild the serving and replication rollup data data while locking
// out other changes.
func (wr *Wrangler) RebuildShardGraph(zkShardPath string) (actionPath string, err error) {
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
		return "", fmt.Errorf("failed to obtain action lock: %v", actionPath)
	}

	rebuildErr := wr.rebuildShard(zkShardPath, false)
	err = wr.handleActionError(actionPath, rebuildErr)
	if rebuildErr != nil {
		if err != nil {
			relog.Warning("handleActionError failed: %v", err)
		}
		return actionPath, rebuildErr
	}
	return
}

// Update shard file with new master, replicas, etc.
//   /zk/global/vt/keyspaces/<keyspace>/shards/<shard uid>
//
// Re-read from zk to make sure we are using the side effects of all actions.
//
// This function should only be used with an action lock on the shard - otherwise the
// consistency of the serving graph data can't be guaranteed.
func (wr *Wrangler) rebuildShard(zkShardPath string, replicationGraphOnly bool) error {
	// NOTE(msolomon) nasty hack - pass non-empty string to bypass data check
	shardInfo, err := tm.NewShardInfo(zkShardPath, "{}")
	if err != nil {
		return err
	}

	tabletMap, err := GetTabletMapForShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	tablets := make([]*tm.TabletInfo, 0, len(tabletMap))
	for _, ti := range tabletMap {
		tablets = append(tablets, ti)
	}

	// Rebuild the rollup data in the replication graph.
	if err = shardInfo.Rebuild(tablets); err != nil {
		return err
	}
	if err = tm.UpdateShard(wr.zconn, shardInfo); err != nil {
		return err
	}
	if !replicationGraphOnly {
		return wr.rebuildShardSrvGraph(zkShardPath, shardInfo, tablets)
	}
	return nil
}

// Write to zkns files?
// Write serving graph data to /zk/local/vt/ns/...
func (wr *Wrangler) rebuildShardSrvGraph(zkShardPath string, shardInfo *tm.ShardInfo, tablets []*tm.TabletInfo) error {
	// Get all existing db types so they can be removed if nothing had been editted.
	// This applies to all cells, which can't be determined until you walk through all the tablets.
	existingDbTypePaths := make(map[string]bool)

	// Update db type addresses in the serving graph
	pathAddrsMap := make(map[string]*naming.VtnsAddrs)
	for _, tablet := range tablets {
		zkSgShardPath := naming.ZkPathForVtShard(tablet.Tablet.Cell, tablet.Tablet.Keyspace, tablet.Shard)
		children, _, err := wr.zconn.Children(zkSgShardPath)
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

		entry := tm.VtnsAddrForTablet(tablet.Tablet)
		addrs.Entries = append(addrs.Entries, *entry)
	}

	for zkPath, addrs := range pathAddrsMap {
		data := jscfg.ToJson(addrs)
		_, err := zk.CreateRecursive(wr.zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			if zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				// Node already exists - just stomp away. Multiple writers shouldn't be here.
				// We use RetryChange here because it won't update the node unnecessarily.
				f := func(oldValue string, oldStat *zookeeper.Stat) (string, error) {
					return data, nil
				}
				err = wr.zconn.RetryChange(zkPath, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), f)
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
			if err := wr.zconn.Delete(zkDbTypePath, -1); err != nil {
				relog.Warning("unable to remove stale db type from serving graph: %v", err)
			}
		}
	}

	// Update per-shard information per cell-specific serving path.
	srvShardByPath := make(map[string]*naming.SrvShard)
	for zkPath, addrs := range pathAddrsMap {
		// zkPath will be /zk/<cell>/vt/ns/<keyspace>/<shard>/<type>
		srvShardPath := path.Dir(zkPath)
		tabletType := tm.TabletType(path.Base(zkPath))

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
		_, err := wr.zconn.Set(srvPath, data, -1)
		if err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}

// Rebuild the serving graph data while locking out other changes.
func (wr *Wrangler) RebuildKeyspaceGraph(zkKeyspacePath string) (actionPath string, err error) {
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
		return "", fmt.Errorf("failed to obtain action lock: %v", actionPath)
	}

	rebuildErr := wr.rebuildKeyspace(zkKeyspacePath)
	err = wr.handleActionError(actionPath, rebuildErr)
	if rebuildErr != nil {
		if err != nil {
			relog.Warning("handleActionError failed: %v", err)
		}
		return actionPath, rebuildErr
	}
	return
}

// This function should only be used with an action lock on the shard - otherwise the
// consistency of the serving graph data can't be guaranteed.
//
// Take data from the global keyspace and rebuild the local serving
// copies in each cell.
func (wr *Wrangler) rebuildKeyspace(zkKeyspacePath string) error {
	vtRoot := tm.VtRootFromKeyspacePath(zkKeyspacePath)
	keyspace := path.Base(zkKeyspacePath)
	shardNames, _, err := wr.zconn.Children(path.Join(zkKeyspacePath, "shards"))
	if err != nil {
		return err
	}

	// Rebuild all shards in parallel.
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	var rebuildErr error
	for _, shardName := range shardNames {
		zkShardPath := tm.ShardPath(vtRoot, keyspace, shardName)
		wg.Add(1)
		go func() {
			if _, err := wr.RebuildShardGraph(zkShardPath); err != nil {
				relog.Error("RebuildShardGraph failed: %v %v", zkShardPath, err)
				mu.Lock()
				rebuildErr = fmt.Errorf("RebuildShardGraph failed on some shards")
				mu.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	mu.Lock()
	err = rebuildErr
	mu.Unlock()

	if err != nil {
		return err
	}

	// Scan the first shard to discover which cells need local serving data.
	zkShardPath := tm.ShardPath(vtRoot, keyspace, shardNames[0])
	aliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	srvKeyspaceByPath := make(map[string]*naming.SrvKeyspace)
	for _, alias := range aliases {
		zkLocalKeyspace := naming.ZkPathForVtKeyspace(alias.Cell, keyspace)
		if _, ok := srvKeyspaceByPath[zkLocalKeyspace]; !ok {
			srvKeyspaceByPath[zkLocalKeyspace] = &naming.SrvKeyspace{Shards: make([]naming.SrvShard, 0, 16)}
		}
	}

	for srvPath, srvKeyspace := range srvKeyspaceByPath {
		for _, shardName := range shardNames {
			srvShard, err := naming.ReadSrvShard(wr.zconn, path.Join(srvPath, shardName))
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
		_, err = wr.zconn.Set(srvPath, data, -1)
		if err != nil {
			return fmt.Errorf("writing serving data failed: %v", err)
		}
	}
	return nil
}

// This is a quick and dirty tool to resurrect the zk data from the
// canonical data stored in the tablet nodes.
//
// zkVtPaths: local vt paths to scan for all tablets
// keyspaces: list of keyspaces to rebuild
func (wr *Wrangler) RebuildReplicationGraph(zkVtPaths []string, keyspaces []string) error {
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
		actionPath, err := wr.RebuildShardGraph(shardPath)
		if err != nil {
			relog.Warning("RebuildShard failed: %v %v", shardPath, err)
			continue
		}
		wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	}

	for keyspacePath, _ := range keyspacePaths {
		actionPath, err := wr.RebuildKeyspaceGraph(keyspacePath)
		if err != nil {
			relog.Warning("RebuildKeyspace failed: %v %v", keyspacePath, err)
			continue
		}
		wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	}

	if hasErr {
		return fmt.Errorf("some errors occurred rebuilding replication graph, consult log")
	}
	return nil
}
