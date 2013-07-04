// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

/*
Assume a graph of mysql nodes.

Replace node N with X.

Connect to N and record file/position from "show master status"

On N: (Demote Master)
  SET GLOBAL READ_ONLY = 1;
  FLUSH TABLES WITH READ LOCK;
  UNLOCK TABLES;

While this is read-only, all the replicas should sync to the same point.

For all slaves of N:
  show slave status
    relay_master_log_file
    exec_master_log_pos

 Map file:pos to list of slaves that are in sync

There should be only one group (ideally).  If not, manually resolve, or pick
the largest group.

Select X from N - X is the new root node. Might not be a "master" in terms of
voltron, but it will be the data source for the rest of the nodes.

On X: (Promote Slave)
  STOP SLAVE;
  RESET MASTER;
  RESET SLAVE;
  SHOW MASTER STATUS;
    replication file,position
  INSERT INTO _vt.replication_log (time_created_ns, 'reparent check') VALUES (<time>);
  INSERT INTO _vt.reparent_log (time_created_ns, 'last post', 'new pos') VALUES ... ;
  SHOW MASTER STATUS;
    wait file,position

  SET GLOBAL READ_ONLY=0;

Disabling READ_ONLY mode here is a matter of opinion.
Realistically it is probably safer to do this later on and minimize
the potential of replaying rows. It expands the write unavailable window
slightly - probably by about 1 second.

For all slaves in majority N:
 if slave != X (Restart Slave)
    STOP SLAVE;
    RESET SLAVE;
    CHANGE MASTER TO X;
    START SLAVE;
    SELECT MASTER_POS_WAIT(file, pos, deadline)
    SELECT time_created FROM _vt.replication_log WHERE time_created_ns = <time>;

if no connection to N is available, ???

On X: (promoted slave)
  SET GLOBAL READ_ONLY=0;
*/

import (
	"fmt"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/naming"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

const (
	SLAVE_STATUS_DEADLINE = 10e9
)

// Create the reparenting action and launch a goroutine to coordinate
// the procedure.
//
//
// leaveMasterReadOnly: leave the master in read-only mode, even
//   though all the other necessary updates have been made.
// forceReparentToCurrentMaster: mostly for test setups, this can
//   cause data loss.
func (wr *Wrangler) ReparentShard(zkShardPath, zkMasterElectTabletPath string, leaveMasterReadOnly, forceReparentToCurrentMaster bool) error {
	if err := tm.IsShardPath(zkShardPath); err != nil {
		return err
	}
	if err := tm.IsTabletPath(zkMasterElectTabletPath); err != nil {
		return err
	}

	shardInfo, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	tabletMap, err := GetTabletMapForShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	slaveTabletMap, foundMaster, err := slaveTabletMap(tabletMap)
	if err != nil {
		return err
	}

	currentMasterTabletPath, err := shardInfo.MasterTabletPath()
	if err != nil {
		// There is no master - either it has been scrapped or there is some other degenerate case.
		currentMasterTabletPath = ""
	} else if currentMasterTabletPath != foundMaster.Path() {
		return fmt.Errorf("master tablet conflict in ShardInfo %v: %v, %v", zkShardPath, currentMasterTabletPath, foundMaster.Path())
	}

	if currentMasterTabletPath == zkMasterElectTabletPath && !forceReparentToCurrentMaster {
		return fmt.Errorf("master-elect tablet %v is already master - specify -force to override", zkMasterElectTabletPath)
	}

	masterElectTablet, ok := tabletMap[zkMasterElectTabletPath]
	if !ok {
		return fmt.Errorf("master-elect tablet not found in replication graph %v %v", zkMasterElectTabletPath, zkShardPath, mapKeys(tabletMap))
	}

	actionPath, err := wr.ai.ReparentShard(zkShardPath, zkMasterElectTabletPath)
	if err != nil {
		return err
	}

	// Make sure two of these don't get scheduled at the same time.
	if err = wr.obtainActionLock(actionPath); err != nil {
		return err
	}

	relog.Info("reparentShard starting masterElect:%v action:%v", masterElectTablet, actionPath)

	var reparentErr error
	if currentMasterTabletPath != "" && !forceReparentToCurrentMaster {
		reparentErr = wr.reparentShardGraceful(slaveTabletMap, foundMaster, masterElectTablet, leaveMasterReadOnly)
	} else {
		reparentErr = wr.reparentShardBrutal(slaveTabletMap, foundMaster, masterElectTablet, leaveMasterReadOnly, forceReparentToCurrentMaster)
	}
	if reparentErr == nil {
		// only log if it works, if it fails we'll show the error
		relog.Info("reparentShard finished")
	}

	err = wr.handleActionError(actionPath, reparentErr, false)
	if reparentErr != nil {
		if err != nil {
			relog.Warning("handleActionError failed: %v", err)
		}
		return reparentErr
	}
	return err
}

func (wr *Wrangler) ShardReplicationPositions(zkShardPath string) ([]*tm.TabletInfo, []*mysqlctl.ReplicationPosition, error) {
	if err := tm.IsShardPath(zkShardPath); err != nil {
		return nil, nil, err
	}

	shardInfo, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return nil, nil, err
	}

	actionPath, err := wr.ai.CheckShard(zkShardPath)
	if err != nil {
		return nil, nil, err
	}

	// Make sure two of these don't get scheduled at the same time.
	if err = wr.obtainActionLock(actionPath); err != nil {
		return nil, nil, err
	}

	tabletMap, posMap, slaveErr := wr.shardReplicationPositions(shardInfo)

	// regardless of error, just clean up
	err = wr.handleActionError(actionPath, nil, false)
	if slaveErr != nil {
		if err != nil {
			relog.Warning("handleActionError failed: %v", err)
		}
		return tabletMap, posMap, slaveErr
	}
	return tabletMap, posMap, err
}

func (wr *Wrangler) shardReplicationPositions(shardInfo *tm.ShardInfo) ([]*tm.TabletInfo, []*mysqlctl.ReplicationPosition, error) {
	// FIXME(msolomon) this assumes no hierarchical replication, which is currently the case.
	tabletMap, err := GetTabletMapForShard(wr.zconn, shardInfo.ShardPath())
	if err != nil {
		return nil, nil, err
	}
	tablets := CopyMapValues(tabletMap, []*tm.TabletInfo{}).([]*tm.TabletInfo)
	positions, err := wr.tabletReplicationPositions(tablets)
	return tablets, positions, err
}

// Attempt to reparent this tablet to the current master, based on the current
// replication position. If there is no match, it will fail.
func (wr *Wrangler) ReparentTablet(zkTabletPath string) error {
	// Get specified tablet.
	// Get current shard master tablet.
	// Sanity check they are in the same keyspace/shard.
	// Get slave position for specified tablet.
	// Get reparent position from master for the given slave position.
	// Issue a restart slave on the specified tablet.

	if err := tm.IsTabletPath(zkTabletPath); err != nil {
		return err
	}
	ti, err := wr.readTablet(zkTabletPath)
	if err != nil {
		return err
	}

	shardInfo, err := tm.ReadShard(wr.zconn, ti.ShardPath())
	if err != nil {
		return err
	}

	masterPath, err := shardInfo.MasterTabletPath()
	if err != nil {
		return err
	}
	masterTi, err := wr.readTablet(masterPath)
	if err != nil {
		return err
	}

	// Basic sanity checking.
	if masterTi.Type != naming.TYPE_MASTER {
		return fmt.Errorf("zk has inconsistent state for shard master %v", masterPath)
	}
	if masterTi.Keyspace != ti.Keyspace || masterTi.Shard != ti.Shard {
		return fmt.Errorf("master %v and potential slave not in same keyspace/shard", masterPath)
	}

	actionPath, err := wr.ai.SlavePosition(ti.Alias())
	if err != nil {
		return err
	}

	result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return err
	}
	pos := result.(*mysqlctl.ReplicationPosition)

	relog.Info("slave tablet position: %v %v %v", zkTabletPath, ti.MysqlAddr, pos.MapKey())

	actionPath, err = wr.ai.ReparentPosition(masterTi.Alias(), pos)
	if err != nil {
		return err
	}
	result, err = wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return err
	}
	rsd := result.(*tm.RestartSlaveData)

	relog.Info("master tablet position: %v %v %v", masterPath, masterTi.MysqlAddr, rsd.ReplicationState.ReplicationPosition.MapKey())
	// An orphan is already in the replication graph but it is
	// disconnected, hence we have to force this action.
	rsd.Force = ti.Type == naming.TYPE_LAG_ORPHAN
	actionPath, err = wr.ai.RestartSlave(ti.Alias(), rsd)
	if err != nil {
		return err
	}
	return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
}
