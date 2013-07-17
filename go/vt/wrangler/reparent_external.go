// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"sync"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/concurrency"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/vt/topo"
)

func (wr *Wrangler) ShardExternallyReparented(keyspace, shard string, masterElectTabletAlias topo.TabletAlias, scrapStragglers bool) error {
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

	currentMasterTabletAlias := shardInfo.MasterAlias
	if currentMasterTabletAlias == (topo.TabletAlias{}) {
		return fmt.Errorf("no master tablet for shard %v/%v", keyspace, shard)
	}
	if currentMasterTabletAlias == masterElectTabletAlias {
		return fmt.Errorf("master-elect tablet %v is already master", masterElectTabletAlias)
	}

	masterElectTablet, ok := tabletMap[masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v not found in replication graph %v/%v %v", masterElectTabletAlias, keyspace, shard, mapKeys(tabletMap))
	}

	// grab the shard lock
	actionNode := wr.ai.ShardExternallyReparented(masterElectTabletAlias)
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	err = wr.reparentShardExternal(slaveTabletMap, foundMaster, masterElectTablet, scrapStragglers)
	if err == nil {
		// only log if it works, if it fails we'll show the error
		relog.Info("reparentShardExternal finished")
	}
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) reparentShardExternal(slaveTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterTablet, masterElectTablet *topo.TabletInfo, scrapStragglers bool) error {

	// we fix the new master in the replication graph
	err := wr.slaveWasPromoted(masterElectTablet)
	if err != nil {
		// This suggests that the master-elect is dead. This is bad.
		return fmt.Errorf("slaveWasPromoted failed: %v", err, masterTablet.Alias())
	}

	// Once the slave is promoted, remove it from our map
	delete(slaveTabletMap, masterElectTablet.Alias())

	// then fix all the slaves, including the old master
	err = wr.restartSlavesExternal(slaveTabletMap, masterTablet, masterElectTablet, scrapStragglers)
	if err != nil {
		return err
	}

	// and rebuild the shard graph
	relog.Info("rebuilding shard serving graph data")
	return wr.rebuildShard(masterElectTablet.Keyspace, masterElectTablet.Shard, []string{masterTablet.Cell, masterElectTablet.Cell})
}

func (wr *Wrangler) restartSlavesExternal(slaveTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterTablet, masterElectTablet *topo.TabletInfo, scrapStragglers bool) error {
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

	// then do the master
	recorder.RecordError(wr.slaveWasRestarted(masterTablet, &swrd))
	return recorder.Error()
}

func (wr *Wrangler) slaveWasRestarted(ti *topo.TabletInfo, swrd *tm.SlaveWasRestartedData) (err error) {
	relog.Info("slaveWasRestarted(%v)", ti.Alias())
	actionPath, err := wr.ai.SlaveWasRestarted(ti.Alias(), swrd)
	if err != nil {
		return err
	}
	return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
}
