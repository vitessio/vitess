// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/topo"
)

func (wr *Wrangler) reparentShardGraceful(slaveTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterTablet, masterElectTablet *topo.TabletInfo, leaveMasterReadOnly bool) error {
	// Validate a bunch of assumptions we make about the replication graph.
	if masterTablet.Parent.Uid != topo.NO_TABLET {
		return fmt.Errorf("master tablet should not have a ParentUid: %v %v", masterTablet.Parent.Uid, masterTablet.Alias())
	}

	if masterTablet.Type != topo.TYPE_MASTER {
		return fmt.Errorf("master tablet should not be type: %v %v", masterTablet.Type, masterTablet.Alias())
	}

	if masterTablet.Uid == masterElectTablet.Uid {
		return fmt.Errorf("master tablet should not match master elect - this must be forced: %v", masterTablet.Alias())
	}

	if _, ok := slaveTabletMap[masterElectTablet.Alias()]; !ok {
		return fmt.Errorf("master elect tablet not in replication graph %v %v/%v %v", masterElectTablet.Alias(), masterTablet.Keyspace, masterTablet.Shard, mapKeys(slaveTabletMap))
	}

	if err := wr.ValidateShard(masterTablet.Keyspace, masterTablet.Shard, true); err != nil {
		return fmt.Errorf("ValidateShard verification failed: %v, if the master is dead, run: vtctl ScrapTablet -force %v", err, masterTablet.Alias())
	}

	// Make sure all tablets have the right parent and reasonable positions.
	err := wr.checkSlaveReplication(slaveTabletMap, masterTablet.Uid)
	if err != nil {
		return err
	}

	// Check the master-elect is fit for duty - call out for hardware checks.
	err = wr.checkMasterElect(masterElectTablet)
	if err != nil {
		return err
	}

	masterPosition, err := wr.demoteMaster(masterTablet)
	if err != nil {
		// FIXME(msolomon) This suggests that the master is dead and we
		// need to take steps. We could either pop a prompt, or make
		// retrying the action painless.
		return fmt.Errorf("demote master failed: %v, if the master is dead, run: vtctl -force ScrapTablet %v", err, masterTablet.Alias())
	}

	relog.Info("check slaves %v/%v", masterTablet.Keyspace, masterTablet.Shard)
	restartableSlaveTabletMap := restartableTabletMap(slaveTabletMap)
	err = wr.checkSlaveConsistency(restartableSlaveTabletMap, masterPosition)
	if err != nil {
		return fmt.Errorf("check slave consistency failed %v, demoted master is still read only, run: vtctl SetReadWrite %v", err, masterTablet.Alias())
	}

	rsd, err := wr.promoteSlave(masterElectTablet)
	if err != nil {
		// FIXME(msolomon) This suggests that the master-elect is dead.
		// We need to classify certain errors as temporary and retry.
		return fmt.Errorf("promote slave failed: %v, demoted master is still read only: vtctl SetReadWrite %v", err, masterTablet.Alias())
	}

	// Once the slave is promoted, remove it from our map
	delete(slaveTabletMap, masterElectTablet.Alias())

	majorityRestart, restartSlaveErr := wr.restartSlaves(slaveTabletMap, rsd)

	// For now, scrap the old master regardless of how many
	// slaves restarted.
	//
	// FIXME(msolomon) We could reintroduce it and reparent it and use
	// it as new replica.
	relog.Info("scrap demoted master %v", masterTablet.Alias())
	scrapActionPath, scrapErr := wr.ai.Scrap(masterTablet.Alias())
	if scrapErr == nil {
		scrapErr = wr.ai.WaitForCompletion(scrapActionPath, wr.actionTimeout())
	}
	if scrapErr != nil {
		// The sub action is non-critical, so just warn.
		relog.Warning("scrap demoted master failed: %v", scrapErr)
	}

	err = wr.finishReparent(masterTablet, masterElectTablet, majorityRestart, leaveMasterReadOnly)
	if err != nil {
		return err
	}

	if restartSlaveErr != nil {
		// This is more of a warning at this point.
		return restartSlaveErr
	}

	return nil
}
