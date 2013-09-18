// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/topo"
)

func (wr *Wrangler) ShardExternallyReparented(keyspace, shard string, masterElectTabletAlias topo.TabletAlias, scrapStragglers bool, acceptSuccessPercents int) error {
	// grab the shard lock
	actionNode := wr.ai.ShardExternallyReparented(masterElectTabletAlias)
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// do the work
	err = wr.shardExternallyReparentedLocked(keyspace, shard, masterElectTabletAlias, scrapStragglers, acceptSuccessPercents)

	// release the lock in any case
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) shardExternallyReparentedLocked(keyspace, shard string, masterElectTabletAlias topo.TabletAlias, scrapStragglers bool, acceptSuccessPercents int) error {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := GetTabletMapForShard(wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	slaveTabletMap, foundMaster, err := slaveTabletMap(tabletMap)
	if err != nil {
		return err
	}

	if shardInfo.MasterAlias == masterElectTabletAlias {
		return fmt.Errorf("master-elect tablet %v is already master", masterElectTabletAlias)
	}

	masterElectTablet, ok := tabletMap[masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v not found in replication graph %v/%v %v", masterElectTabletAlias, keyspace, shard, mapKeys(tabletMap))
	}

	err = wr.reparentShardExternal(slaveTabletMap, foundMaster, masterElectTablet, scrapStragglers, acceptSuccessPercents)
	if err == nil {
		// only log if it works, if it fails we'll show the error
		log.Infof("reparentShardExternal finished")
	}
	return err
}

func (wr *Wrangler) reparentShardExternal(slaveTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterTablet, masterElectTablet *topo.TabletInfo, scrapStragglers bool, acceptSuccessPercents int) error {

	// we fix the new master in the replication graph
	err := wr.slaveWasPromoted(masterElectTablet)
	if err != nil {
		// This suggests that the master-elect is dead. This is bad.
		return fmt.Errorf("slaveWasPromoted(%v) failed: %v", masterElectTablet, err)
	}

	// Once the slave is promoted, remove it from our map
	delete(slaveTabletMap, masterElectTablet.Alias())

	// then fix all the slaves, including the old master
	err = wr.restartSlavesExternal(slaveTabletMap, masterTablet, masterElectTablet, scrapStragglers, acceptSuccessPercents)
	if err != nil {
		return err
	}

	// and rebuild the shard graph.  We rebuild all the cells, as
	// we may have taken tablets in and out of the graph.
	log.Infof("rebuilding shard serving graph data")
	return wr.rebuildShard(masterElectTablet.Keyspace, masterElectTablet.Shard, nil)
}

func (wr *Wrangler) restartSlavesExternal(slaveTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterTablet, masterElectTablet *topo.TabletInfo, scrapStragglers bool, acceptSuccessPercents int) error {
	recorder := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	swrd := tm.SlaveWasRestartedData{
		Parent:               masterElectTablet.Alias(),
		ExpectedMasterAddr:   masterElectTablet.MysqlAddr,
		ExpectedMasterIpAddr: masterElectTablet.MysqlIpAddr,
		ScrapStragglers:      scrapStragglers,
	}

	// do all the slaves
	for _, ti := range slaveTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			recorder.RecordError(wr.slaveWasRestarted(ti, &swrd))
			wg.Done()
		}(ti)
	}
	wg.Wait()

	// then do the old master if it hadn't been scrapped
	if masterTablet != nil {
		err := wr.slaveWasRestarted(masterTablet, &swrd)
		if err != nil {
			recorder.RecordError(err)
			// the old master can be annoying if left
			// around in the replication graph, so if we
			// can't restart it, we just scrap it
			log.Warningf("Old master %v is not restarting, scrapping it", masterTablet.Alias())
			if _, err := wr.Scrap(masterTablet.Alias() /*force*/, true /*skipRebuild*/, true); err != nil {
				log.Warningf("Failed to scrap old master %v: %v", masterTablet.Alias(), err)
			}
		}
	}

	// check the toplevel replication paths only contains the new master,
	// try to remove any old tablet aliases that don't make sense anymore
	toplevelAliases, err := wr.ts.GetReplicationPaths(masterElectTablet.Keyspace, masterElectTablet.Shard, "")
	if err != nil {
		log.Warningf("GetReplicationPaths() failed, cannot fix extra paths: %v", err)
	} else {
		for _, toplevelAlias := range toplevelAliases {
			if toplevelAlias == masterElectTablet.Alias() {
				continue
			}

			// if we can't read the tablet, or if it's not in the
			// replication graph, we remove the entry.
			if ti, err := wr.ts.GetTablet(toplevelAlias); err == nil && ti.Tablet.IsInReplicationGraph() {
				// we can read the entry and it belongs here,
				// keep it
				continue
			}

			log.Infof("Removing stale replication path %v", toplevelAlias.String())
			if err := topo.DeleteTabletReplicationData(wr.ts, masterElectTablet.Tablet, toplevelAlias.String()); err != nil {
				log.Warningf("DeleteTabletReplicationData(%v) failed: %v", toplevelAlias.String(), err)
			}
		}
	}

	if !recorder.HasErrors() {
		return nil
	}

	// report errors only above a threshold
	failurePercent := 100 * len(recorder.Errors) / (len(slaveTabletMap) + 1)
	if failurePercent < 100-acceptSuccessPercents {
		log.Warningf("Encountered %v%% failure, we keep going. Errors: %v", failurePercent, recorder.Error())
		return nil
	}

	return recorder.Error()
}

func (wr *Wrangler) slaveWasRestarted(ti *topo.TabletInfo, swrd *tm.SlaveWasRestartedData) (err error) {
	log.Infof("slaveWasRestarted(%v)", ti.Alias())
	actionPath, err := wr.ai.SlaveWasRestarted(ti.Alias(), swrd)
	if err != nil {
		return err
	}
	return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
}
