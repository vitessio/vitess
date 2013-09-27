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
	// read the shard, make sure the master is not already good
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}
	if shardInfo.MasterAlias == masterElectTabletAlias {
		return fmt.Errorf("master-elect tablet %v is already master", masterElectTabletAlias)
	}

	// read the tablets, make sure the master elect is known to us
	tabletMap, err := GetTabletMapForShard(wr.ts, keyspace, shard)
	if err != nil {
		return err
	}
	masterElectTablet, ok := tabletMap[masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v not found in replication graph %v/%v %v", masterElectTabletAlias, keyspace, shard, mapKeys(tabletMap))
	}

	// sort the tablets, and handle them
	slaveTabletMap, masterTabletMap := sortedTabletMap(tabletMap)
	err = wr.reparentShardExternal(slaveTabletMap, masterTabletMap, masterElectTablet, scrapStragglers, acceptSuccessPercents)
	if err != nil {
		log.Infof("Skipping shard rebuild with failed reparent")
		return err
	}

	// now update the master record in the shard object
	log.Infof("Updating Shard's MasterAlias record")
	shardInfo.MasterAlias = masterElectTabletAlias
	if err = wr.ts.UpdateShard(shardInfo); err != nil {
		return err
	}

	// and rebuild the shard serving graph (but do not change the
	// master record, we already did it)
	log.Infof("Rebuilding shard serving graph data")
	return wr.rebuildShard(masterElectTablet.Keyspace, masterElectTablet.Shard, nil, false)
}

func (wr *Wrangler) reparentShardExternal(slaveTabletMap, masterTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterElectTablet *topo.TabletInfo, scrapStragglers bool, acceptSuccessPercents int) error {
	// we fix the new master in the replication graph
	err := wr.slaveWasPromoted(masterElectTablet)
	if err != nil {
		// This suggests that the master-elect is dead. This is bad.
		return fmt.Errorf("slaveWasPromoted(%v) failed: %v", masterElectTablet, err)
	}

	// Once the slave is promoted, remove it from our maps
	delete(slaveTabletMap, masterElectTablet.Alias())
	delete(masterTabletMap, masterElectTablet.Alias())

	// then fix all the slaves, including the old master
	return wr.restartSlavesExternal(slaveTabletMap, masterTabletMap, masterElectTablet, scrapStragglers, acceptSuccessPercents)
}

func (wr *Wrangler) restartSlavesExternal(slaveTabletMap, masterTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterElectTablet *topo.TabletInfo, scrapStragglers bool, acceptSuccessPercents int) error {
	recorder := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	swrd := tm.SlaveWasRestartedData{
		Parent:               masterElectTablet.Alias(),
		ExpectedMasterAddr:   masterElectTablet.MysqlAddr,
		ExpectedMasterIpAddr: masterElectTablet.MysqlIpAddr,
		ScrapStragglers:      scrapStragglers,
	}

	// do all the slaves
	log.Infof("Making sure all slaves are correct:")
	for _, ti := range slaveTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			recorder.RecordError(wr.slaveWasRestarted(ti, &swrd))
			wg.Done()
		}(ti)
	}
	wg.Wait()

	// then do the old master and any straggler, if possible, but
	// do not record errors for these
	log.Infof("Fixing old master and other stragglers:")
	for _, ti := range masterTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			err := wr.slaveWasRestarted(ti, &swrd)
			if err != nil {
				// the old master can be annoying if left
				// around in the replication graph, so if we
				// can't restart it, we just scrap it
				log.Warningf("Old master %v is not restarting, scrapping it: %v", ti.Alias(), err)
				if _, err := wr.Scrap(ti.Alias(), true /*force*/, true /*skipRebuild*/); err != nil {
					log.Warningf("Failed to scrap old master %v: %v", ti.Alias(), err)
				}
			}
			wg.Done()
		}(ti)
	}
	wg.Wait()

	// TODO(alainjobart) remove this section when replication paths
	// are retired.
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
