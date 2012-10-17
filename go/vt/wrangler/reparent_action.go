package zkwrangler

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"

	"launchpad.net/gozk/zookeeper"
)

// These functions reimplement a few actions that were originally
// implemented as direct RPCs.  This gives a consistent, if not slower
// mechanism for performing critical actions. It also leaves more
// centralize debug information in zk when a failure occurs.

func (wr *Wrangler) getMasterPositionWithAction(ti *tm.TabletInfo, zkShardActionPath string) (*mysqlctl.ReplicationPosition, error) {
	zkReplyPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_master_position_reply.json")
	actionPath, err := wr.ai.MasterPosition(ti.Path(), zkReplyPath)
	if err != nil {
		return nil, err
	}
	err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
	if err != nil {
		return nil, err
	}
	data, _, err := wr.zconn.Get(zkReplyPath)
	if err != nil {
		return nil, err
	}
	position := new(mysqlctl.ReplicationPosition)
	if err = json.Unmarshal([]byte(data), position); err != nil {
		return nil, err
	}
	return position, nil
}

func (wr *Wrangler) checkSlaveConsistencyWithActions(tabletMap map[uint]*tm.TabletInfo, masterPosition *mysqlctl.ReplicationPosition, zkShardActionPath string) error {
	relog.Debug("checkSlaveConsistencyWithActions %v %#v", mapKeys(tabletMap), masterPosition)

	timer := time.NewTimer(SLAVE_STATUS_DEADLINE)
	defer timer.Stop()

	// FIXME(msolomon) Something still feels clumsy here and I can't put my finger on it.
	calls := make(chan *rpcContext, len(tabletMap))

	f := func(ti *tm.TabletInfo) {
		ctx := &rpcContext{tablet: ti}
		if masterPosition != nil {
			zkArgsPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_slave_position_args.json")
			zkReplyPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_slave_position_reply.json")
			args := &tm.SlavePositionReq{*masterPosition, int(SLAVE_STATUS_DEADLINE / 1e9)}
			// If the master position is known, do our best to wait for replication to catch up.
			_, err := wr.zconn.Create(zkArgsPath, jscfg.ToJson(args), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
			if err != nil {
				ctx.err = err
				return
			}
			actionPath, err := wr.ai.WaitSlavePosition(ti.Path(), zkArgsPath, zkReplyPath)
			if err != nil {
				ctx.err = err
				return
			}
			err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
			if err != nil {
				ctx.err = err
				return
			}
			data, _, err := wr.zconn.Get(zkReplyPath)
			if err != nil {
				ctx.err = err
				return
			}
			ctx.position = new(mysqlctl.ReplicationPosition)
			if err = json.Unmarshal([]byte(data), ctx.position); err != nil {
				ctx.err = err
				return
			}
		} else {
			zkSlaveReplyPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_initial_slave_position_reply.json")
			zkArgsPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_slave_position_args.json")
			zkReplyPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_slave_position_reply.json")
			actionPath, err := wr.ai.SlavePosition(ti.Path(), zkSlaveReplyPath)
			if err != nil {
				ctx.err = err
				return
			}
			err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
			if err != nil {
				ctx.err = err
				return
			}
			data, _, err := wr.zconn.Get(zkSlaveReplyPath)
			if err != nil {
				ctx.err = err
				return
			}
			replPos := new(mysqlctl.ReplicationPosition)
			if err = json.Unmarshal([]byte(data), replPos); err != nil {
				ctx.err = err
				return
			}
			// In the case where a master is down, look for the last bit of data copied and wait
			// for that to apply. That gives us a chance to wait for all data.
			lastDataPos := *replPos
			lastDataPos.MasterLogPosition = lastDataPos.ReadMasterLogPosition
			args := &tm.SlavePositionReq{lastDataPos, int(SLAVE_STATUS_DEADLINE / 1e9)}
			_, err = wr.zconn.Create(zkArgsPath, jscfg.ToJson(args), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
			if err != nil {
				ctx.err = err
				return
			}
			actionPath, err = wr.ai.WaitSlavePosition(ti.Path(), zkArgsPath, zkReplyPath)
			if err != nil {
				ctx.err = err
				return
			}
			err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
			if err != nil {
				ctx.err = err
				return
			}
			data, _, err = wr.zconn.Get(zkReplyPath)
			if err != nil {
				ctx.err = err
				return
			}
			ctx.position = new(mysqlctl.ReplicationPosition)
			if err = json.Unmarshal([]byte(data), ctx.position); err != nil {
				ctx.err = err
				return
			}
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

// Shut off all replication.
func (wr *Wrangler) stopSlavesWithAction(tabletMap map[uint]*tm.TabletInfo, zkShardActionPath string) error {
	errs := make(chan error, len(tabletMap))
	f := func(ti *tm.TabletInfo) {
		actionPath, err := wr.ai.StopSlave(ti.Path())
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, 1*time.Minute)
		}
		if err != nil {
			relog.Debug("StopSlave failed: %v", err)
		}
		errs <- err
	}

	for _, tablet := range tabletMap {
		// Pass loop variable explicitly so we don't have a concurrency issue.
		go f(tablet)
	}

	// wait for responses
	for i := 0; i < len(tabletMap); i++ {
		if err := <-errs; err != nil {
			return err
		}
	}

	return nil
}
