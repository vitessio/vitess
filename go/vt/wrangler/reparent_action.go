package wrangler

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

// helper struct to queue up results
type rpcContext struct {
	tablet   *tm.TabletInfo
	position *mysqlctl.ReplicationPosition
	err      error
}

// These functions reimplement a few actions that were originally
// implemented as direct RPCs.  This gives a consistent, if not slower
// mechanism for performing critical actions. It also leaves more
// centralize debug information in zk when a failure occurs.

func (wr *Wrangler) getMasterPosition(ti *tm.TabletInfo) (*mysqlctl.ReplicationPosition, error) {
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

// Check all the tablets replication positions to find if some
// will have a problem, and suggest a fix for them.
func (wr *Wrangler) checkSlaveReplication(tabletMap map[string]*tm.TabletInfo, masterTablet, masterElectTablet *tm.TabletInfo) error {
	relog.Info("Checking all replication positions will allow the transition:")
	// check everybody has the right parent
	for _, tablet := range tabletMap {
		// check the master is right
		if tablet.Parent.Uid != masterTablet.Uid {
			return fmt.Errorf("tablet not slaved correctly, expected %v, found %v", masterTablet.Uid, tablet.Parent.Uid)
		}
	}

	// now check all the replication positions will allow us to proceed
	var lastError error
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	for _, tablet := range tabletMap {
		wg.Add(1)
		go func(tablet *tm.TabletInfo) {
			defer wg.Done()
			if masterTablet.Uid != masterElectTablet.Uid {
				// will reparent to different tablet, skip LAG
				if tablet.Type == tm.TYPE_LAG {
					relog.Info("  skipping slave position check for %v tablet %v", tablet.Type, tablet.Path())
					return
				}
			}

			actionPath, err := wr.ai.SlavePosition(tablet.Path())
			if err != nil {
				mutex.Lock()
				lastError = err
				mutex.Unlock()
				relog.Error("  error asking tablet %v for slave position: %v", tablet.Path(), err)
				return
			}
			result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
			if err != nil {
				if masterTablet.Uid == masterElectTablet.Uid {
					// we are setting up replication on an empty keyspace, most likely
					relog.Info("  slave not configured, will set up replication on current keyspace data for %v (%v)", tablet.Path(), err)
					return
				}

				mutex.Lock()
				lastError = err
				mutex.Unlock()
				if tablet.Type == tm.TYPE_BACKUP {
					relog.Warning("  failed to get slave position from backup tablet %v, either wait for backup to finish or scrap tablet (%v)", tablet.Path(), err)
				} else {
					relog.Warning("  failed to get slave position from %v: %v", tablet.Path(), err)
				}
				return
			}
			replPos := result.(*mysqlctl.ReplicationPosition)
			var dur time.Duration = time.Duration(uint(time.Second) * replPos.SecondsBehindMaster)
			if dur > wr.actionTimeout() {
				err = fmt.Errorf("slave is too far behind to complete reparent in time (%v>%v), either increase timeout using 'vtctl -wait-time XXX ReparentShard ...' or scrap tablet %v", dur, wr.actionTimeout(), tablet.Path())
				relog.Error("  %v", err)
				mutex.Lock()
				lastError = err
				mutex.Unlock()
				return
			}

			relog.Debug("  slave is %v behind master (<%v), reparent should work for %v", dur, wr.actionTimeout(), tablet.Path())
		}(tablet)
	}
	wg.Wait()
	return lastError
}

// Check all the tablets to see if we can proceed with reparenting.
// masterPosition is supplied from the demoted master if we are doing
// this gracefully.
func (wr *Wrangler) checkSlaveConsistency(tabletMap map[uint32]*tm.TabletInfo, masterPosition *mysqlctl.ReplicationPosition) error {
	relog.Debug("checkSlaveConsistency %v %#v", mapKeys(tabletMap), masterPosition)

	// FIXME(msolomon) Something still feels clumsy here and I can't put my finger on it.
	calls := make(chan *rpcContext, len(tabletMap))
	f := func(ti *tm.TabletInfo) {
		ctx := &rpcContext{tablet: ti}
		defer func() {
			calls <- ctx
		}()

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
		actionPath, err := wr.ai.WaitSlavePosition(ti.Path(), args)
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
func (wr *Wrangler) stopSlaves(tabletMap map[string]*tm.TabletInfo) error {
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
func (wr *Wrangler) tabletReplicationPositions(tabletMap map[uint32]*tm.TabletInfo) (map[uint32]*mysqlctl.ReplicationPosition, error) {
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
