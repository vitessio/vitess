package zkwrangler

import (
	"encoding/json"
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
	zkReplyPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_master_position_reply.json")
	actionPath, err := wr.ai.MasterPosition(ti.Path(), zkReplyPath)
	if err != nil {
		return nil, err
	}
	err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
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

	// FIXME(msolomon) Something still feels clumsy here and I can't put my finger on it.
	calls := make(chan *rpcContext, len(tabletMap))
	f := func(ti *tm.TabletInfo) {
		ctx := &rpcContext{tablet: ti}
		defer func() {
			calls <- ctx
		}()
		zkArgsPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_slave_position_args.json")
		zkReplyPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_slave_position_reply.json")
		var args *tm.SlavePositionReq
		if masterPosition != nil {
			// If the master position is known, do our best to wait for replication to catch up.
			args = &tm.SlavePositionReq{*masterPosition, int(wr.actionTimeout().Seconds())}
		} else {
			// In the case where a master is down, look for the last bit of data copied and wait
			// for that to apply. That gives us a chance to wait for all data.
			zkSlaveReplyPath := path.Join(zkShardActionPath, path.Base(ti.Path())+"_initial_slave_position_reply.json")
			actionPath, err := wr.ai.SlavePosition(ti.Path(), zkSlaveReplyPath)
			if err != nil {
				ctx.err = err
				return
			}
			err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
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
			lastDataPos := mysqlctl.ReplicationPosition{MasterLogFile: replPos.MasterLogFileIo,
				MasterLogPositionIo: replPos.MasterLogPositionIo}
			args = &tm.SlavePositionReq{lastDataPos, int(wr.actionTimeout().Seconds())}
		}

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
		err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
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
	}

	reparentableSlaveCount := 0
	for _, tablet := range tabletMap {
		// These types are explicitly excluded from reparenting since you
		// will just wait forever for them to catch up.  A possible
		// improvement is waiting for the io thread to reach the same
		// position as the sql thread on a normal slave.
		if tablet.Type == tm.TYPE_LAG || tablet.Type == tm.TYPE_EXPERIMENTAL {
			continue
		}
		// Pass loop variable explicitly so we don't have a concurrency issue.
		go f(tablet)
		reparentableSlaveCount++
	}

	// map positions to tablets
	positionMap := make(map[string][]uint)
	for i := 0; i < reparentableSlaveCount; i++ {
		ctx := <-calls
		mapKey := "unavailable-tablet-error"
		if ctx.err == nil {
			mapKey = ctx.position.MapKey()
		}
		if _, ok := positionMap[mapKey]; !ok {
			positionMap[mapKey] = make([]uint, 0, 32)
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
func (wr *Wrangler) stopSlavesWithAction(tabletMap map[uint]*tm.TabletInfo, zkShardActionPath string) error {
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
