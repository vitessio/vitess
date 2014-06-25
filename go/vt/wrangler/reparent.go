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

	log "github.com/golang/glog"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler/events"
)

const (
	SLAVE_STATUS_DEADLINE = 10e9
)

// ReparentShard creates the reparenting action and launches a goroutine
// to coordinate the procedure.
//
//
// leaveMasterReadOnly: leave the master in read-only mode, even
//   though all the other necessary updates have been made.
// forceReparentToCurrentMaster: mostly for test setups, this can
//   cause data loss.
func (wr *Wrangler) ReparentShard(keyspace, shard string, masterElectTabletAlias topo.TabletAlias, leaveMasterReadOnly, forceReparentToCurrentMaster bool) error {
	// lock the shard
	actionNode := actionnode.ReparentShard(masterElectTabletAlias)
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return err
	}

	// do the work
	err = wr.reparentShardLocked(keyspace, shard, masterElectTabletAlias, leaveMasterReadOnly, forceReparentToCurrentMaster)

	// and unlock
	return wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) reparentShardLocked(keyspace, shard string, masterElectTabletAlias topo.TabletAlias, leaveMasterReadOnly, forceReparentToCurrentMaster bool) error {
	// critical read, we want up to date info (and the shard is locked).
	shardInfo, err := wr.ts.GetShardCritical(keyspace, shard)
	if err != nil {
		return err
	}

	tabletMap, err := topo.GetTabletMapForShard(wr.ts, keyspace, shard)
	if err != nil {
		return err
	}

	slaveTabletMap, masterTabletMap := sortedTabletMap(tabletMap)
	if shardInfo.MasterAlias == masterElectTabletAlias && !forceReparentToCurrentMaster {
		return fmt.Errorf("master-elect tablet %v is already master - specify -force to override", masterElectTabletAlias)
	}

	masterElectTablet, ok := tabletMap[masterElectTabletAlias]
	if !ok {
		return fmt.Errorf("master-elect tablet %v not found in replication graph %v/%v %v", masterElectTabletAlias, keyspace, shard, mapKeys(tabletMap))
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{
		Keyspace:  shardInfo.Keyspace(),
		Shard:     shardInfo.ShardName(),
		NewMaster: *masterElectTablet.Tablet,
	}

	if oldMasterTablet, ok := tabletMap[shardInfo.MasterAlias]; ok {
		ev.OldMaster = *oldMasterTablet.Tablet
	}

	if !shardInfo.MasterAlias.IsZero() && !forceReparentToCurrentMaster {
		err = wr.reparentShardGraceful(ev, shardInfo, slaveTabletMap, masterTabletMap, masterElectTablet, leaveMasterReadOnly)
	} else {
		err = wr.reparentShardBrutal(ev, shardInfo, slaveTabletMap, masterTabletMap, masterElectTablet, leaveMasterReadOnly, forceReparentToCurrentMaster)
	}

	if err == nil {
		// only log if it works, if it fails we'll show the error
		log.Infof("reparentShard finished")
	}
	return err
}

// ShardReplicationPositions returns the ReplicationPositions for all
// the tablets in a shard.
func (wr *Wrangler) ShardReplicationPositions(keyspace, shard string) ([]*topo.TabletInfo, []*myproto.ReplicationPosition, error) {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return nil, nil, err
	}

	// lock the shard
	actionNode := actionnode.CheckShard()
	lockPath, err := wr.lockShard(keyspace, shard, actionNode)
	if err != nil {
		return nil, nil, err
	}

	tabletMap, posMap, err := wr.shardReplicationPositions(shardInfo)
	return tabletMap, posMap, wr.unlockShard(keyspace, shard, actionNode, lockPath, err)
}

func (wr *Wrangler) shardReplicationPositions(shardInfo *topo.ShardInfo) ([]*topo.TabletInfo, []*myproto.ReplicationPosition, error) {
	// FIXME(msolomon) this assumes no hierarchical replication, which is currently the case.
	tabletMap, err := topo.GetTabletMapForShard(wr.ts, shardInfo.Keyspace(), shardInfo.ShardName())
	if err != nil {
		return nil, nil, err
	}
	tablets := CopyMapValues(tabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)
	positions, err := wr.tabletReplicationPositions(tablets)
	return tablets, positions, err
}

// ReparentTablet attempts to reparent this tablet to the current
// master, based on the current replication position. If there is no
// match, it will fail.
func (wr *Wrangler) ReparentTablet(tabletAlias topo.TabletAlias) error {
	// Get specified tablet.
	// Get current shard master tablet.
	// Sanity check they are in the same keyspace/shard.
	// Get slave position for specified tablet.
	// Get reparent position from master for the given slave position.
	// Issue a restart slave on the specified tablet.

	ti, err := wr.ts.GetTablet(tabletAlias)
	if err != nil {
		return err
	}

	shardInfo, err := wr.ts.GetShard(ti.Keyspace, ti.Shard)
	if err != nil {
		return err
	}
	if shardInfo.MasterAlias.IsZero() {
		return fmt.Errorf("no master tablet for shard %v/%v", ti.Keyspace, ti.Shard)
	}

	masterTi, err := wr.ts.GetTablet(shardInfo.MasterAlias)
	if err != nil {
		return err
	}

	// Basic sanity checking.
	if masterTi.Type != topo.TYPE_MASTER {
		return fmt.Errorf("TopologyServer has inconsistent state for shard master %v", shardInfo.MasterAlias)
	}
	if masterTi.Keyspace != ti.Keyspace || masterTi.Shard != ti.Shard {
		return fmt.Errorf("master %v and potential slave not in same keyspace/shard", shardInfo.MasterAlias)
	}

	pos, err := wr.ai.SlavePosition(ti, wr.actionTimeout())
	if err != nil {
		return err
	}
	log.Infof("slave tablet position: %v %v %v", tabletAlias, ti.MysqlAddr(), pos.MapKey())

	actionPath, err := wr.ai.ReparentPosition(masterTi.Alias, pos)
	if err != nil {
		return err
	}
	result, err := wr.WaitForCompletionReply(actionPath)
	if err != nil {
		return err
	}
	rsd := result.(*actionnode.RestartSlaveData)

	log.Infof("master tablet position: %v %v %v", shardInfo.MasterAlias, masterTi.MysqlAddr(), rsd.ReplicationState.ReplicationPosition.MapKey())
	// An orphan is already in the replication graph but it is
	// disconnected, hence we have to force this action.
	rsd.Force = ti.Type == topo.TYPE_LAG_ORPHAN
	actionPath, err = wr.ai.RestartSlave(ti.Alias, rsd)
	if err != nil {
		return err
	}
	return wr.WaitForCompletion(actionPath)
}
