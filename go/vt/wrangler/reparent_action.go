package zkwrangler

import (
	"fmt"
	"path"
	"sort"
	"strings"

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
	actionPath, err := wr.ai.MasterPosition(ti.Path())
	if err != nil {
		return nil, err
	}
	result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return nil, err
	}
	return result.(*mysqlctl.ReplicationPosition), nil
}

// Check all the tablets to see if we can proceed with reparenting.
// masterPosition is supplied from the demoted master if we are doing
// this gracefully.
func (wr *Wrangler) checkSlaveConsistencyWithActions(tabletMap map[uint32]*tm.TabletInfo, masterPosition *mysqlctl.ReplicationPosition, zkShardActionPath string) error {
	relog.Debug("checkSlaveConsistencyWithActions %v %#v", mapKeys(tabletMap), masterPosition)

	// FIXME(msolomon) Something still feels clumsy here and I can't put my finger on it.
	calls := make(chan *rpcContext, len(tabletMap))
	f := func(ti *tm.TabletInfo) {
		ctx := &rpcContext{tablet: ti}
		defer func() {
			calls <- ctx
		}()

		zkArgsPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_slave_position_args.json")
		var args *tm.SlavePositionReq
		if masterPosition != nil {
			// If the master position is known, do our best to wait for replication to catch up.
			args = &tm.SlavePositionReq{*masterPosition, int(wr.actionTimeout().Seconds())}
		} else {
			// In the case where a master is down, look for the last bit of data copied and wait
			// for that to apply. That gives us a chance to wait for all data.
			actionPath, err := wr.ai.SlavePosition(ti.Path())
			if err != nil {
				ctx.err = err
				return
			}
			result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
			if err != nil {
				ctx.err = err
				return
			}
			replPos := result.(*mysqlctl.ReplicationPosition)
			lastDataPos := mysqlctl.ReplicationPosition{MasterLogFile: replPos.MasterLogFileIo,
				MasterLogPositionIo: replPos.MasterLogPositionIo}
			args = &tm.SlavePositionReq{lastDataPos, int(wr.actionTimeout().Seconds())}
		}

		// This option waits for the SQL thread to apply all changes to this instance.
		_, err := wr.zconn.Create(zkArgsPath, jscfg.ToJson(args), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			ctx.err = err
			return
		}
		actionPath, err := wr.ai.WaitSlavePosition(ti.Path(), zkArgsPath)
		if err != nil {
			ctx.err = err
			return
		}
		result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
		if err != nil {
			ctx.err = err
			return
		}
		ctx.position = result.(*mysqlctl.ReplicationPosition)
	}

	for _, tablet := range tabletMap {
		// Pass loop variable explicitly so we don't have a concurrency issue.
		go f(tablet)
	}

	// map positions to tablets
	positionMap := make(map[string][]uint32)
	for i := 0; i < len(tabletMap); i++ {
		ctx := <-calls
		mapKey := "unavailable-tablet-error"
		if ctx.err == nil {
			mapKey = ctx.position.MapKey()
		}
		if _, ok := positionMap[mapKey]; !ok {
			positionMap[mapKey] = make([]uint32, 0, 32)
		}
		positionMap[mapKey] = append(positionMap[mapKey], ctx.tablet.Uid)
	}

	if len(positionMap) == 1 {
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
			tabletPaths := make([]string, len(uids))
			for i, uid := range uids {
				tabletPaths[i] = tabletMap[uid].Path()
			}
			items = append(items, fmt.Sprintf("%v %v", slaveMapKey, tabletPaths))
		}
		sort.Strings(items)
		return fmt.Errorf("inconsistent slaves, mark some offline with vtctl ScrapTablet {%v}", strings.Join(items, ", "))
	}
	return nil
}

// Shut off all replication.
func (wr *Wrangler) stopSlavesWithAction(tabletMap map[uint32]*tm.TabletInfo, zkShardActionPath string) error {
	errs := make(chan error, len(tabletMap))
	f := func(ti *tm.TabletInfo) {
		actionPath, err := wr.ai.StopSlave(ti.Path())
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
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

// Return a map of all tablets to the current replication position.
// Handles masters and slaves, but it's up to the caller to guarantee
// all tablets are in the same shard.
func (wr *Wrangler) tabletReplicationPositions(tabletMap map[uint32]*tm.TabletInfo, zkShardActionPath string) (map[uint32]*mysqlctl.ReplicationPosition, error) {
	relog.Debug("tabletReplicationPositions %v", mapKeys(tabletMap))

	calls := make(chan *rpcContext, len(tabletMap))
	f := func(ti *tm.TabletInfo) {
		ctx := &rpcContext{tablet: ti}
		defer func() {
			calls <- ctx
		}()

		var actionPath string
		if ti.Type == tm.TYPE_MASTER {
			actionPath, ctx.err = wr.ai.MasterPosition(ti.Path())
		} else {
			actionPath, ctx.err = wr.ai.SlavePosition(ti.Path())
		}
		if ctx.err != nil {
			return
		}
		var result interface{}
		if result, ctx.err = wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout()); ctx.err != nil {
			return
		}
		ctx.position = result.(*mysqlctl.ReplicationPosition)
	}

	for _, tablet := range tabletMap {
		go f(tablet)
	}

	someErrors := false
	positionMap := make(map[uint32]*mysqlctl.ReplicationPosition)
	for i := 0; i < len(tabletMap); i++ {
		ctx := <-calls
		if ctx.err == nil {
			positionMap[ctx.tablet.Uid] = ctx.position
		} else {
			positionMap[ctx.tablet.Uid] = nil
			relog.Warning("could not get replication position for tablet %v %v", ctx.tablet.Path(), ctx.err)
			someErrors = true
		}
	}
	if someErrors {
		return positionMap, fmt.Errorf("partial position map, some errors")
	}
	return positionMap, nil
}
