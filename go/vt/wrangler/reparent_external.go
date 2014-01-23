// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/tabletmanager"
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
	// read the shard, make sure the master is not already good.
	// critical read, we want up to date info (and the shard is locked).
	shardInfo, err := wr.ts.GetShardCritical(keyspace, shard)
	if err != nil {
		return err
	}
	if shardInfo.MasterAlias == masterElectTabletAlias {
		return fmt.Errorf("master-elect tablet %v is already master", masterElectTabletAlias)
	}

	// Read the tablets, make sure the master elect is known to us.
	// Note we will keep going with a partial tablet map, which usually
	// happens when a cell is not reachable. After these checks, the
	// guarantees we'll have are:
	// - global cell is reachable (we just locked and read the shard)
	// - the local cell that contains the new master is reachable
	//   (as we're going to check the new master is in the list)
	// That should be enough.
	tabletMap, err := GetTabletMapForShard(wr.ts, keyspace, shard)
	partialTopology := false
	switch err {
	case nil:
		// keep going
	case topo.ErrPartialResult:
		partialTopology = true
		log.Warningf("Got topo.ErrPartialResult from GetTabletMapForShard, may need to re-init some tablets")
	default:
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
	return wr.rebuildShard(masterElectTablet.Keyspace, masterElectTablet.Shard,
		rebuildShardOptions{IgnorePartialResult: partialTopology, Critical: true})
}

func (wr *Wrangler) reparentShardExternal(slaveTabletMap, masterTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterElectTablet *topo.TabletInfo, scrapStragglers bool, acceptSuccessPercents int) error {
	// we fix the new master in the replication graph
	err := wr.slaveWasPromoted(masterElectTablet)
	if err != nil {
		// This suggests that the master-elect is dead. This is bad.
		return fmt.Errorf("slaveWasPromoted(%v) failed: %v", masterElectTablet, err)
	}

	// Once the slave is promoted, remove it from our maps
	delete(slaveTabletMap, masterElectTablet.Alias)
	delete(masterTabletMap, masterElectTablet.Alias)

	// Re-read the master elect tablet as its mysql port
	// may have been updated
	masterElectTablet, err = wr.TopoServer().GetTablet(masterElectTablet.Alias)
	if err != nil {
		return fmt.Errorf("Cannot re-read the master record, something is seriously wrong")
	}

	// then fix all the slaves, including the old master
	return wr.restartSlavesExternal(slaveTabletMap, masterTabletMap, masterElectTablet, scrapStragglers, acceptSuccessPercents)
}

func (wr *Wrangler) restartSlavesExternal(slaveTabletMap, masterTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterElectTablet *topo.TabletInfo, scrapStragglers bool, acceptSuccessPercents int) error {
	recorder := concurrency.AllErrorRecorder{}
	wg := sync.WaitGroup{}

	swrd := tabletmanager.SlaveWasRestartedData{
		Parent:               masterElectTablet.Alias,
		ExpectedMasterAddr:   masterElectTablet.GetMysqlAddr(),
		ExpectedMasterIpAddr: masterElectTablet.GetMysqlIpAddr(),
		ScrapStragglers:      scrapStragglers,
	}

	// The following two blocks of actions are very likely to time
	// out for some tablets (one random guy is dead, the old
	// master is dead, ...). We execute them all in parallel until
	// we get to wr.actionTimeout(). After this, no other action
	// with a timeout is executed, so even if we got to the
	// timeout, we're still good.
	log.Infof("Making sure all tablets have the right master:")

	// do all the slaves
	for _, ti := range slaveTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			recorder.RecordError(wr.slaveWasRestarted(ti, &swrd))
			wg.Done()
		}(ti)
	}

	// and do the old master and any straggler, if possible, but
	// do not record errors for these
	for _, ti := range masterTabletMap {
		wg.Add(1)
		go func(ti *topo.TabletInfo) {
			err := wr.slaveWasRestarted(ti, &swrd)
			if err != nil {
				// the old master can be annoying if left
				// around in the replication graph, so if we
				// can't restart it, we just scrap it.
				// We don't rebuild the Shard just yet though.
				log.Warningf("Old master %v is not restarting, scrapping it: %v", ti.Alias, err)
				if _, err := wr.Scrap(ti.Alias, true /*force*/, true /*skipRebuild*/); err != nil {
					log.Warningf("Failed to scrap old master %v: %v", ti.Alias, err)
				}
			}
			wg.Done()
		}(ti)
	}
	wg.Wait()

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

func (wr *Wrangler) slaveWasPromoted(ti *topo.TabletInfo) error {
	log.Infof("slaveWasPromoted(%v)", ti.Alias)
	if wr.UseRPCs {
		return wr.ai.RpcSlaveWasPromoted(ti, wr.actionTimeout())
	} else {
		actionPath, err := wr.ai.SlaveWasPromoted(ti.Alias)
		if err != nil {
			return err
		}
		err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
		if err != nil {
			return err
		}
	}
	return nil
}

func (wr *Wrangler) slaveWasRestarted(ti *topo.TabletInfo, swrd *tabletmanager.SlaveWasRestartedData) (err error) {
	log.Infof("slaveWasRestarted(%v)", ti.Alias)
	if wr.UseRPCs {
		return wr.ai.RpcSlaveWasRestarted(ti, swrd, wr.actionTimeout())
	} else {
		actionPath, err := wr.ai.SlaveWasRestarted(ti.Alias, swrd)
		if err != nil {
			return err
		}
		return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	}
}
