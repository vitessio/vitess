// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

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
	"net/rpc"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	vtrpc "code.google.com/p/vitess/go/vt/rpc"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/zk"
)

const (
	SLAVE_STATUS_DEADLINE = 10e9
)

// Create the reparenting action and launch a goroutine to coordinate
// the procedure.
//
// leaveMasterReadOnly: leave the master in read-only mode, even
//   though all the other necessary updates have been made.
// forceReparentToCurrentMaster: mostly for test setups, this can
//   cause data loss.
func (wr *Wrangler) ReparentShard(zkShardPath, zkMasterElectTabletPath string, leaveMasterReadOnly, forceReparentToCurrentMaster bool) error {
	tm.MustBeShardPath(zkShardPath)
	tm.MustBeTabletPath(zkMasterElectTabletPath)

	shardInfo, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return err
	}

	currentMasterTabletPath, err := shardInfo.MasterTabletPath()
	if err != nil {
		return err
	}
	if currentMasterTabletPath == zkMasterElectTabletPath && !forceReparentToCurrentMaster {
		return fmt.Errorf("master-elect tablet %v is already master - specify -force to override", zkMasterElectTabletPath)
	}

	masterElectTablet, err := wr.readTablet(zkMasterElectTabletPath)
	if err != nil {
		return err
	}

	actionPath, err := wr.ai.ReparentShard(zkShardPath, zkMasterElectTabletPath)
	if err != nil {
		return err
	}

	// Make sure two of these don't get scheduled at the same time.
	ok, err := zk.ObtainQueueLock(wr.zconn, actionPath, false)
	if err != nil {
		return err
	}

	if !ok {
		// just clean up for now, in the future we may want to try harder, or wait
		wr.zconn.Delete(actionPath, -1)
		return fmt.Errorf("ReparentShard failed to obtain shard action lock")
	}

	relog.Info("reparentShard starting masterElect:%v action:%v", masterElectTablet, actionPath)
	reparentErr := wr.reparentShard(shardInfo, masterElectTablet, actionPath, leaveMasterReadOnly)
	relog.Info("reparentShard finished %v", reparentErr)

	err = wr.handleActionError(actionPath, reparentErr)
	if reparentErr != nil {
		if err != nil {
			relog.Warning("handleActionError failed: %v", err)
		}
		return reparentErr
	}
	return err
}

func (wr *Wrangler) reparentShard(shardInfo *tm.ShardInfo, masterElectTablet *tm.TabletInfo, zkShardActionPath string, leaveMasterReadOnly bool) error {
	// Get shard's master tablet.
	zkMasterTabletPath, err := shardInfo.MasterTabletPath()
	if err != nil {
		return err
	}
	masterTablet, err := wr.readTablet(zkMasterTabletPath)
	if err != nil {
		return err
	}

	// Validate a bunch of assumptions we make about the replication graph.
	if masterTablet.Parent.Uid != tm.NO_TABLET {
		return fmt.Errorf("masterTablet has ParentUid: %v", masterTablet.Parent.Uid)
	}

	// FIXME(msolomon) this assumes no hierarchical replication, which is currently the case.
	tabletAliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, shardInfo.ShardPath())
	if err != nil {
		return err
	}

	// FIXME(msolomon) Run validate shard first? What about the case when the
	// master is dead?

	// FIXME(msolomon) Ping all tablets before demote to minimize unavailaility.

	// FIXME(msolomon) Use GetTabletMap to parallelize.

	// FIXME(msolomon) this assumes that the replica nodes must all be
	// in a good state when the reparent happens. The better thing to
	// guarantee is that *enough* replica nodes are in a good state. In
	// fact, "enough" is probably a function of each datacenter. It's
	// complicated.
	slaveTabletMap := make(map[uint]*tm.TabletInfo)
	for _, alias := range tabletAliases {
		if alias.Uid == masterTablet.Uid {
			// skip master
			continue
		}
		tablet, err := wr.readTablet(shardInfo.TabletPath(alias))
		if err != nil {
			return fmt.Errorf("tablet unavailable: %v", err)
		}
		if tablet.Parent.Uid != masterTablet.Uid {
			return fmt.Errorf("tablet not slaved correctly, expected %v, found %v", masterTablet.Uid, tablet.Parent.Uid)
		}
		slaveTabletMap[alias.Uid] = tablet
	}

	var masterPosition *mysqlctl.ReplicationPosition
	// If the masterTablet type doesn't match, we can assume that it's been
	// removed by other operations. For instance, a DBA or health-check process
	// setting it's type to SCRAP.
	shouldDemoteMaster := masterTablet.Type == tm.TYPE_MASTER && masterTablet.Uid != masterElectTablet.Uid
	if shouldDemoteMaster {
		relog.Info("demote master %v", zkMasterTabletPath)
		actionPath, err := wr.ai.DemoteMaster(zkMasterTabletPath)
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
		}
		if err == nil {
			// masterPosition, err = getMasterPosition(masterTablet.Tablet)
			masterPosition, err = wr.getMasterPositionWithAction(masterTablet, zkShardActionPath)
		}
		if err != nil {
			// FIXME(msolomon) This suggests that the master is dead and we
			// need to take steps. We could either pop a prompt, or make
			// retrying the action painless.
			relog.Warning("demote master failed: vtctl -force ScrapTablet %v ?", zkMasterTabletPath)
			return err
		}
	}

	if masterTablet.Uid != masterElectTablet.Uid {
		relog.Info("check slaves %v", zkMasterTabletPath)
		// err = checkSlaveConsistency(slaveTabletMap, masterPosition, zkShardActionPath)
		err = wr.checkSlaveConsistencyWithActions(slaveTabletMap, masterPosition, zkShardActionPath)
		if err != nil {
			return err
		}
	} else {
		relog.Info("forcing reparent to same master %v", zkMasterTabletPath)
		// We are forcing a reparenting. Make sure that all slaves stop so
		// no data is accidentally replicated through before we call RestartSlave.
		// err = stopSlaves(slaveTabletMap)
		relog.Info("stop slaves %v", zkMasterTabletPath)
		err = wr.stopSlavesWithAction(slaveTabletMap, zkShardActionPath)
		if err != nil {
			return err
		}

		// Force slaves to break, just in case they were not advertised in
		// the replication graph.
		relog.Info("break slaves %v", zkMasterTabletPath)
		actionPath, err := wr.ai.BreakSlaves(zkMasterTabletPath)
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
		}
		if err != nil {
			return err
		}
	}

	zkMasterElectPath := masterElectTablet.Path()
	relog.Info("promote slave %v", zkMasterElectPath)
	actionPath, err := wr.ai.PromoteSlave(zkMasterElectPath, zkShardActionPath)
	if err == nil {
		err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	}
	if err != nil {
		// FIXME(msolomon) This suggests that the master-elect is dead.
		// We need to classify certain errors as temporary and retry.
		if shouldDemoteMaster {
			relog.Warning("promote slave failed, demoted master is still read only: vtctl SetReadWrite %v ?", zkMasterTabletPath)
		}
		return err
	}

	// Once the slave is promoted, remove it from our map
	if masterTablet.Uid != masterElectTablet.Uid {
		delete(slaveTabletMap, masterElectTablet.Uid)
	}

	restartSlaveErrors := make([]error, 0, len(slaveTabletMap))
	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)
	for _, slaveTablet := range slaveTabletMap {
		relog.Info("restart slave %v", slaveTablet.Path())
		wg.Add(1)
		f := func(zkSlavePath string) {
			actionPath, err := wr.ai.RestartSlave(zkSlavePath, zkShardActionPath)
			if err == nil {
				err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
			}
			if err != nil {
				// FIXME(msolomon) Don't bail early, just mark this phase as
				// failed. We might decide to proceed if enough of these
				// succeed.
				//
				// FIXME(msolomon) This is a somewhat delicate retry -
				// have to figure out why it failed on the tablet end. This
				// could lead to a nasty case of having to recompute where to
				// start replication. Practically speaking, that chance is
				// pretty low.
				relog.Warning("restart slave failed: %v %v", zkSlavePath, err)
				mu.Lock()
				restartSlaveErrors = append(restartSlaveErrors, err)
				mu.Unlock()
			}
			wg.Done()
		}
		go f(slaveTablet.Path())
	}
	wg.Wait()

	if masterTablet.Uid != masterElectTablet.Uid {
		relog.Info("scrap demoted master %v", zkMasterTabletPath)
		// If there is a master, scrap it for now.
		// We could reintroduce it and reparent it and use it as new replica.
		if shouldDemoteMaster {
			scrapActionPath, scrapErr := wr.ai.Scrap(zkMasterTabletPath)
			if scrapErr == nil {
				scrapErr = wr.ai.WaitForCompletion(scrapActionPath, wr.actionTimeout())
			}
			if scrapErr != nil {
				// The sub action is non-critical, so just warn.
				relog.Warning("waiting for scrap failed: %v", scrapErr)
			}
		} else {
			relog.Info("force scrap errant master %v", zkMasterTabletPath)
			// The master is dead so execute the action locally instead of
			// enqueing the scrap action for an arbitrary amount of time.
			if scrapErr := tm.Scrap(wr.zconn, zkMasterTabletPath, false); scrapErr != nil {
				relog.Warning("forcing scrap failed: %v", scrapErr)
			}
		}
	}

	// If the majority of slaves restarted, move ahead.
	majorityRestart := len(restartSlaveErrors) < (len(slaveTabletMap) / 2)
	if majorityRestart {
		if leaveMasterReadOnly {
			relog.Warning("leaving master read-only, vtctl SetReadWrite %v ?", zkMasterTabletPath)
		} else {
			relog.Info("marking master read-write %v", zkMasterTabletPath)
			actionPath, err := wr.ai.SetReadWrite(zkMasterElectPath)
			if err == nil {
				err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
			}
		}
		relog.Info("rebuilding shard data in zk")
		if err = wr.rebuildShard(shardInfo.ShardPath()); err != nil {
			return err
		}
	} else {
		relog.Warning("minority reparent, force serving graph rebuild: vtctl RebuildShardGraph %v ?", shardInfo.ShardPath())
	}

	if len(restartSlaveErrors) > 0 {
		msgs := make([]string, len(restartSlaveErrors))
		for i, e := range restartSlaveErrors {
			msgs[i] = e.Error()
		}
		// This is more of a warning at this point.
		// FIXME(msolomon) classify errors
		err = fmt.Errorf("restart slaves failed (%v): %v", len(msgs), strings.Join(msgs, ", "))
	}

	return err
}

func getMasterPosition(tablet *tm.Tablet) (*mysqlctl.ReplicationPosition, error) {
	timer := time.NewTimer(SLAVE_STATUS_DEADLINE)
	defer timer.Stop()

	callChan := make(chan *rpc.Call, 1)
	var client *rpc.Client
	go func() {
		var clientErr error
		client, clientErr := rpc.DialHTTP("tcp", tablet.Addr)
		if clientErr != nil {
			callChan <- &rpc.Call{Error: fmt.Errorf("dial failed: %v", clientErr)}
		} else {
			client.Go("TabletManager.MasterPosition", vtrpc.NilRequest, new(mysqlctl.ReplicationPosition), callChan)
		}
	}()

	var call *rpc.Call
	select {
	case <-timer.C:
	case call = <-callChan:
	}
	if client != nil {
		client.Close()
	}
	if call == nil {
		return nil, fmt.Errorf("TabletManager.MasterPosition deadline exceeded %v", tablet.Addr)
	}
	if call.Error != nil {
		return nil, call.Error
	}
	return call.Reply.(*mysqlctl.ReplicationPosition), nil
}

type rpcContext struct {
	tablet   *tm.TabletInfo
	client   *rpc.Client
	position *mysqlctl.ReplicationPosition
	err      error
}

func mapKeys(m interface{}) []interface{} {
	keys := make([]interface{}, 0, 16)
	mapVal := reflect.ValueOf(m)
	for _, kv := range mapVal.MapKeys() {
		keys = append(keys, kv.Interface())
	}
	return keys
}

func mapStrKeys(m interface{}) []string {
	keys := make([]string, 0, 16)
	mapVal := reflect.ValueOf(m)
	for _, kv := range mapVal.MapKeys() {
		keys = append(keys, fmt.Sprintf("%v", kv.Interface()))
	}
	return keys
}

// Check all the tablets to see if we can proceed with reparenting.
// masterPosition is supplied from the demoted master if we are doing
// this gracefully.
// FIXME(msolomon) This has been superceded by the action based version.
// This should be removed, but the RPC based version is much simpler to
// understand even though they are starting to diverge.
func checkSlaveConsistency(tabletMap map[uint]*tm.TabletInfo, masterPosition *mysqlctl.ReplicationPosition) error {
	relog.Debug("checkSlaveConsistency %v %#v", mapKeys(tabletMap), masterPosition)

	timer := time.NewTimer(SLAVE_STATUS_DEADLINE)
	defer timer.Stop()

	// FIXME(msolomon) Something still feels clumsy here and I can't put my finger on it.
	calls := make(chan *rpcContext, len(tabletMap))
	f := func(tablet *tm.TabletInfo) {
		ctx := &rpcContext{tablet: tablet}
		ctx.client, ctx.err = rpc.DialHTTP("tcp", tablet.Addr)
		if ctx.err == nil {
			ctx.position = new(mysqlctl.ReplicationPosition)
			if masterPosition != nil {
				// If the master position is known, do our best to wait for replication to catch up.
				args := &tm.SlavePositionReq{*masterPosition, int(SLAVE_STATUS_DEADLINE / 1e9)}
				ctx.err = ctx.client.Call("TabletManager.WaitSlavePosition", args, ctx.position)
			} else {
				ctx.err = ctx.client.Call("TabletManager.SlavePosition", vtrpc.NilRequest, ctx.position)
				if ctx.err == nil {
					// In the case where a master is down, look for the last bit of data copied and wait
					// for that to apply. That gives us a chance to wait for all data.
					lastDataPos := mysqlctl.ReplicationPosition{MasterLogFile: ctx.position.MasterLogFileIo,
						MasterLogPositionIo: ctx.position.MasterLogPositionIo}
					args := &tm.SlavePositionReq{lastDataPos, int(SLAVE_STATUS_DEADLINE / 1e9)}
					ctx.err = ctx.client.Call("TabletManager.WaitSlavePosition", args, ctx.position)
				}
			}
			ctx.client.Close()
		}
		calls <- ctx
	}

	for _, tablet := range tabletMap {
		// Pass loop variable explicitly so we don't have a concurrency issue.
		go f(tablet)
	}

	replies := make([]*rpcContext, 0, len(tabletMap))
	// wait for responses
wait:
	for i := 0; i < len(tabletMap); i++ {
		select {
		case <-timer.C:
			break wait
		case call := <-calls:
			replies = append(replies, call)
		}
	}

	replyErrorCount := len(tabletMap) - len(replies)
	// map positions to tablets
	positionMap := make(map[string][]uint)
	for _, ctx := range replies {
		if ctx.err != nil {
			replyErrorCount++
		} else {
			mapKey := ctx.position.MapKey()
			if _, ok := positionMap[mapKey]; !ok {
				positionMap[mapKey] = make([]uint, 0, 32)
			}
			positionMap[mapKey] = append(positionMap[mapKey], ctx.tablet.Uid)
		}
	}

	if len(positionMap) == 1 && replyErrorCount == 0 {
		// great, everyone agrees
		// demotedMasterReplicationState is nil if demotion failed
		if masterPosition != nil {
			demotedMapKey := masterPosition.MapKey()
			if _, ok := positionMap[demotedMapKey]; !ok {
				for slaveMapKey, _ := range positionMap {
					return fmt.Errorf("slave position doesn't match demoted master: %v != %v", demotedMapKey,
						slaveMapKey)
				}
			}
		}
	} else {
		// FIXME(msolomon) in the event of a crash, do you pick replica that is
		// furthest along or do you promote the majority? data loss vs availability
		// sounds like you pick the latest group and reclone.
		items := make([]string, 0, 32)
		for slaveMapKey, uids := range positionMap {
			items = append(items, fmt.Sprintf("%v %v", slaveMapKey, uids))
		}
		sort.Strings(items)
		// FIXME(msolomon) add instructions how to do so.
		return fmt.Errorf("inconsistent slaves, mark some offline (rpc error count: %v) %v", replyErrorCount, strings.Join(items, ", "))
	}
	return nil
}

// Shut off all replication.
func stopSlaves(tabletMap map[uint]*tm.TabletInfo) error {
	timer := time.NewTimer(SLAVE_STATUS_DEADLINE)

	// FIXME(msolomon) Something still feels clumsy here and I can't put my finger on it.
	errs := make(chan error, len(tabletMap))
	f := func(tablet *tm.TabletInfo) {
		client, err := rpc.DialHTTP("tcp", tablet.Addr)
		if err == nil {
			err = client.Call("TabletManager.StopSlave", vtrpc.NilRequest, vtrpc.NilResponse)
			client.Close()
		}
		if err != nil {
			relog.Debug("TabletManager.StopSlave failed: %v", err)
		}
		errs <- err
	}

	for _, tablet := range tabletMap {
		// Pass loop variable explicitly so we don't have a concurrency issue.
		go f(tablet)
	}

	// wait for responses
	var err error
wait:
	for i := 0; i < len(tabletMap); i++ {
		select {
		case <-timer.C:
			break wait
		case lastErr := <-errs:
			if err == nil && lastErr != nil {
				err = lastErr
			}
		}
	}

	return err
}
