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
  INSERT INTO _vt.replication_test (time_created_ns) VALUES (<time>);
  SHOW MASTER STATUS;
    wait file,position
  SET GLOBAL READ_ONLY=0;

For all slaves in majority N:
 if slave != X (Restart Slave)
    STOP SLAVE;
    RESET SLAVE;
    CHANGE MASTER TO X;
    START SLAVE;
    SELECT MASTER_POS_WAIT(file, pos, deadline)
    SELECT time_created FROM _vt.replication_test WHERE time_created_ns = <time>;

if no connection to N is available, ???

*/

import (
	"fmt"
	"net/rpc"
	"reflect"
	"sort"
	"strings"
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

type Wrangler struct {
	zconn zk.Conn
	ai    *tm.ActionInitiator
}

func NewWrangler(zconn zk.Conn, ai *tm.ActionInitiator) *Wrangler {
	return &Wrangler{zconn, ai}
}

func (wr *Wrangler) readTablet(zkTabletPath string) (*tm.TabletInfo, error) {
	return tm.ReadTablet(wr.zconn, zkTabletPath)
}

/*
 Create the reparenting action and launch a goroutine to coordinate the procedure.
 The actionNode can be watched for updates.
 TODO(msolomon): We could supply a channel of updates to cut out zk round trips.
force: true if we are trying to skip sanity checks - mostly for test setups
*/
func (wr *Wrangler) ReparentShard(zkShardPath, zkTabletPath string, force bool) (actionPath string, err error) {
	tm.MustBeShardPath(zkShardPath)
	tm.MustBeTabletPath(zkTabletPath)

	shardInfo, err := tm.ReadShard(wr.zconn, zkShardPath)
	if err != nil {
		return
	}

	currentMasterTabletPath, err := shardInfo.MasterTabletPath()
	if err != nil {
		return
	}
	if currentMasterTabletPath == zkTabletPath && !force {
		return "", fmt.Errorf("master-elect tablet %v is already master - specify -force to override", zkTabletPath)
	}

	tablet, err := wr.readTablet(zkTabletPath)
	if err != nil {
		return
	}

	actionPath, err = wr.ai.ReparentShard(zkShardPath, zkTabletPath)
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
		panic(fmt.Errorf("failed to obtain action lock: %v", actionPath))
	}

	go wr.reparentShardHandler(shardInfo, tablet, actionPath)
	return
}

/*
shardInfo for the shard we want to reparent.
masterElectTablet is the shart we want to promote when the time comes.
zkShardActionPath - zk path to the node representing this action.
*/
func (wr *Wrangler) reparentShardHandler(shardInfo *tm.ShardInfo, masterElectTablet *tm.TabletInfo, zkShardActionPath string) {
	relog.Debug("reparentShard starting %#v %v", masterElectTablet, zkShardActionPath)
	reparentErr := wr.reparentShard(shardInfo, masterElectTablet, zkShardActionPath)
	relog.Debug("reparentShard finished %v", reparentErr)

	var err error
	if reparentErr == nil {
		err = zk.DeleteRecursive(wr.zconn, zkShardActionPath, -1)
	} else {
		data, stat, err := wr.zconn.Get(zkShardActionPath)
		if err == nil {
			var actionNode *tm.ActionNode
			actionNode, err = tm.ActionNodeFromJson(data, zkShardActionPath)
			if err == nil {
				actionNode.Error = reparentErr.Error()
				data = tm.ActionNodeToJson(actionNode)
				_, err = wr.zconn.Set(zkShardActionPath, data, stat.Version())
			}
		}
	}
	if err != nil {
		relog.Error("action node update failed: %v", err)
		if reparentErr != nil {
			relog.Fatal("reparent failed: %v", reparentErr)
		}
	}
}

func (wr *Wrangler) reparentShard(shardInfo *tm.ShardInfo, masterElectTablet *tm.TabletInfo, zkShardActionPath string) error {
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
	tabletAliases, err := tm.FindAllTabletAliasesInShard(wr.zconn, shardInfo)
	relog.Debug("shardUids: %v", tabletAliases)
	if err != nil {
		return err
	}

	slaveTabletMap := make(map[uint]*tm.TabletInfo)
	// FIXME(msolomon) this assumes that the replica nodes must all be in a good
	// state when the reparent happens. the better thing to guarantee is that
	// *enough* replica nodes are in a good state. In fact, "enough" is probably a function
	// of each datacenter. It's complicated.
	// FIXME(msolomon) handle multiple datacenters
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

	relog.Debug("read all slave tablets")

	var masterPosition *mysqlctl.ReplicationPosition
	// If the masterTablet type doesn't match, we can assume that it's been
	// removed by other operations. For instance, a DBA or health-check process
	// setting it's type to SCRAP.
	if masterTablet.Type == tm.TYPE_MASTER && masterTablet.Uid != masterElectTablet.Uid {
		relog.Debug("demote master %v", zkMasterTabletPath)
		actionPath, err := wr.ai.DemoteMaster(zkMasterTabletPath)
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
		}
		if err != nil {
			// FIXME(msolomon) This suggests that the master is dead and we need to take steps.
			return err
		}

		masterPosition, err = getMasterPosition(masterTablet.Tablet)
		if err != nil {
			// FIXME(msolomon) handle the case where the master is failed, not demoted.
			// Suggest retry, or scrap master and force reparenting.
			return err
		}
	}

	if masterTablet.Uid != masterElectTablet.Uid {
		relog.Debug("check slaves %v", zkMasterTabletPath)
		err = checkSlaveConsistency(slaveTabletMap, masterPosition)
		if err != nil {
			return err
		}
	} else {
		// We are forcing a reparenting. Make sure that all slaves stop so
		// no data is accidentally replicated through before we call RestartSlave.
		err = stopSlaves(slaveTabletMap)
		if err != nil {
			return err
		}

		// Force slaves to break, just in case they were not advertised in
		// the replication graph.
		relog.Debug("break slaves %v", zkMasterTabletPath)
		actionPath, err := wr.ai.BreakSlaves(zkMasterTabletPath)
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
		}
		if err != nil {
			return err
		}
	}

	zkMasterElectPath := masterElectTablet.Path()
	relog.Debug("promote slave %v", zkMasterElectPath)
	actionPath, err := wr.ai.PromoteSlave(zkMasterElectPath, zkShardActionPath)
	if err == nil {
		err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
	}
	if err != nil {
		// FIXME(msolomon) This suggests that the master-elect is dead.
		// We need to classify certain errors as temporary and retry.
		return err
	}

	// Once the slave is promoted, remove it from our map
	if masterTablet.Uid != masterElectTablet.Uid {
		delete(slaveTabletMap, masterElectTablet.Uid)
	}

	// FIXME(msolomon) actions could block forever - set a reasonable deadline
	restartSlaveErrors := make([]error, 0, len(slaveTabletMap))
	// FIXME(msolomon) could be done in parallel
	for _, slaveTablet := range slaveTabletMap {
		relog.Debug("restart slave %v", slaveTablet.Path())
		actionPath, err := wr.ai.RestartSlave(slaveTablet.Path(), zkShardActionPath)
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
		}
		if err != nil {
			// FIXME(msolomon) Don't bail early, just mark this phase as failed. We might
			// decide to proceed if enough of these succeed.
			// FIXME(msolomon) This is a somewhat delicate retry - have to figure out
			// why it failed on the tablet end. This could lead to a nasty case of having
			// to recompute where to start replication. Practically speaking, that chance
			// is pretty low.
			relog.Warning("restart slave failed: %v", err)
			restartSlaveErrors = append(restartSlaveErrors, err)
		}
	}

	if masterTablet.Uid != masterElectTablet.Uid {
		relog.Debug("scrap demoted master %v", zkMasterTabletPath)
		// If there is a master, scrap it for now.
		// We could reintroduce it and reparent it and use it as new replica.
		if masterPosition != nil {
			scrapActionPath, scrapErr := wr.ai.Scrap(zkMasterTabletPath)
			if scrapErr != nil {
				relog.Warning("initiating scrap failed: %v", scrapErr)
			} else {
				err = wr.ai.WaitForCompletion(scrapActionPath, 1*time.Minute)
				if err != nil {
					relog.Warning("waiting for scrap failed: %v", err)
				}
			}
		} else {
			relog.Debug("forcing scrap: %v", zkMasterTabletPath)
			// The master is dead, so we have to take action ourselves since we presume
			// the actor will not get the initiation for some time.
			if scrapErr := tm.Scrap(wr.zconn, zkMasterTabletPath, false); scrapErr != nil {
				relog.Warning("forcing scrap failed: %v", scrapErr)
			}
		}
	}

	relog.Debug("update shard")
	err = tm.RebuildShard(wr.zconn, shardInfo.ShardPath())
	if err != nil {
		return err
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

/* Check all the tablets to see if we can proceed with reparenting.
   masterPosition is supplied from the demoted master if we are doing this gracefully.
*/
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
					lastDataPos := *ctx.position
					lastDataPos.MasterLogPosition = lastDataPos.ReadMasterLogPosition
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
	for i := 0; i < len(tabletMap); i++ {
		select {
		case <-timer.C:
			break
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

/* Shut off all replication. */
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
	for i := 0; i < len(tabletMap); i++ {
		select {
		case <-timer.C:
			break
		case lastErr := <-errs:
			if err == nil && lastErr != nil {
				err = lastErr
			}
		}
	}

	return err
}
