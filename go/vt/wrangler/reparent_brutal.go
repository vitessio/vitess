package wrangler

import (
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
)

// Assume the master is dead and not coming back. Just push your way
// forward.  Force means we are reparenting to the same master
// (assuming the data has been externally synched).
func (wr *Wrangler) reparentShardBrutal(si *topo.ShardInfo, slaveTabletMap, masterTabletMap map[topo.TabletAlias]*topo.TabletInfo, masterElectTablet *topo.TabletInfo, leaveMasterReadOnly, force bool) error {
	log.Infof("Skipping ValidateShard - not a graceful situation")

	if _, ok := slaveTabletMap[masterElectTablet.Alias]; !ok && !force {
		return fmt.Errorf("master elect tablet not in replication graph %v %v/%v %v", masterElectTablet.Alias, si.Keyspace(), si.ShardName(), mapKeys(slaveTabletMap))
	}

	// Check the master-elect and slaves are in good shape when the action
	// has not been forced.
	if !force {
		// Make sure all tablets have the right parent and reasonable positions.
		if err := wr.checkSlaveReplication(slaveTabletMap, topo.NO_TABLET); err != nil {
			return err
		}

		// Check the master-elect is fit for duty - call out for hardware checks.
		if err := wr.checkMasterElect(masterElectTablet); err != nil {
			return err
		}

		log.Infof("check slaves %v/%v", masterElectTablet.Keyspace, masterElectTablet.Shard)
		restartableSlaveTabletMap := restartableTabletMap(slaveTabletMap)
		err := wr.checkSlaveConsistency(restartableSlaveTabletMap, nil)
		if err != nil {
			return err
		}
	} else {
		log.Infof("forcing reparent to same master %v", masterElectTablet.Alias)
		err := wr.breakReplication(slaveTabletMap, masterElectTablet)
		if err != nil {
			return err
		}
	}

	rsd, err := wr.promoteSlave(masterElectTablet)
	if err != nil {
		// FIXME(msolomon) This suggests that the master-elect is dead.
		// We need to classify certain errors as temporary and retry.
		return fmt.Errorf("promote slave failed: %v %v", err, masterElectTablet.Alias)
	}

	// Once the slave is promoted, remove it from our maps
	delete(slaveTabletMap, masterElectTablet.Alias)
	delete(masterTabletMap, masterElectTablet.Alias)

	majorityRestart, restartSlaveErr := wr.restartSlaves(slaveTabletMap, rsd)

	if !force {
		for _, failedMaster := range masterTabletMap {
			log.Infof("scrap dead master %v", failedMaster.Alias)
			// The master is dead so execute the action locally instead of
			// enqueing the scrap action for an arbitrary amount of time.
			if scrapErr := topotools.Scrap(wr.ts, failedMaster.Alias, false); scrapErr != nil {
				log.Warningf("scrapping failed master failed: %v", scrapErr)
			}
		}
	}

	err = wr.finishReparent(si, masterElectTablet, majorityRestart, leaveMasterReadOnly)
	if err != nil {
		return err
	}

	if restartSlaveErr != nil {
		// This is more of a warning at this point.
		return restartSlaveErr
	}

	return nil
}
