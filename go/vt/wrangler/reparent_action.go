package wrangler

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/hook"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/topo"
)

// helper struct to queue up results
type rpcContext struct {
	tablet   *topo.TabletInfo
	position *mysqlctl.ReplicationPosition
	err      error
}

// These functions reimplement a few actions that were originally
// implemented as direct RPCs.  This gives a consistent, if not slower
// mechanism for performing critical actions. It also leaves more
// centralized debug information in TopologyServer when a failure occurs.

func (wr *Wrangler) getMasterPosition(tabletAlias topo.TabletAlias) (*mysqlctl.ReplicationPosition, error) {
	actionPath, err := wr.ai.MasterPosition(tabletAlias)
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
func (wr *Wrangler) checkSlaveReplication(tabletMap map[topo.TabletAlias]*topo.TabletInfo, masterTabletUid uint32) error {
	log.Infof("Checking all replication positions will allow the transition:")
	masterIsDead := masterTabletUid == topo.NO_TABLET

	// Check everybody has the right master. If there is no master
	// (crash) just check that everyone has the same parent.
	for _, tablet := range tabletMap {
		if masterTabletUid == topo.NO_TABLET {
			masterTabletUid = tablet.Parent.Uid
		}
		if tablet.Parent.Uid != masterTabletUid {
			return fmt.Errorf("tablet %v not slaved correctly, expected %v, found %v", tablet.GetAlias(), masterTabletUid, tablet.Parent.Uid)
		}
	}

	// now check all the replication positions will allow us to proceed
	if masterIsDead {
		log.V(6).Infof("  master is dead, not checking Seconds Behind Master value")
	}
	var lastError error
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	for _, tablet := range tabletMap {
		wg.Add(1)
		go func(tablet *topo.TabletInfo) {
			defer wg.Done()

			if tablet.Type == topo.TYPE_LAG {
				log.Infof("  skipping slave position check for %v tablet %v", tablet.Type, tablet.GetAlias())
				return
			}

			actionPath, err := wr.ai.SlavePosition(tablet.GetAlias())
			if err != nil {
				mutex.Lock()
				lastError = err
				mutex.Unlock()
				log.Errorf("  error asking tablet %v for slave position: %v", tablet.GetAlias(), err)
				return
			}
			result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
			if err != nil {
				mutex.Lock()
				lastError = err
				mutex.Unlock()
				if tablet.Type == topo.TYPE_BACKUP {
					log.Warningf("  failed to get slave position from backup tablet %v, either wait for backup to finish or scrap tablet (%v)", tablet.GetAlias(), err)
				} else {
					log.Warningf("  failed to get slave position from %v: %v", tablet.GetAlias(), err)
				}
				return
			}

			if !masterIsDead {
				replPos := result.(*mysqlctl.ReplicationPosition)
				var dur time.Duration = time.Duration(uint(time.Second) * replPos.SecondsBehindMaster)
				if dur > wr.actionTimeout() {
					err = fmt.Errorf("slave is too far behind to complete reparent in time (%v>%v), either increase timeout using 'vtctl -wait-time XXX ReparentShard ...' or scrap tablet %v", dur, wr.actionTimeout(), tablet.GetAlias())
					log.Errorf("  %v", err)
					mutex.Lock()
					lastError = err
					mutex.Unlock()
					return
				}

				log.V(6).Infof("  slave is %v behind master (<%v), reparent should work for %v", dur, wr.actionTimeout(), tablet.GetAlias())
			}
		}(tablet)
	}
	wg.Wait()
	return lastError
}

// Check all the tablets to see if we can proceed with reparenting.
// masterPosition is supplied from the demoted master if we are doing
// this gracefully.
func (wr *Wrangler) checkSlaveConsistency(tabletMap map[uint32]*topo.TabletInfo, masterPosition *mysqlctl.ReplicationPosition) error {
	log.V(6).Infof("checkSlaveConsistency %v %#v", mapKeys(tabletMap), masterPosition)

	// FIXME(msolomon) Something still feels clumsy here and I can't put my finger on it.
	calls := make(chan *rpcContext, len(tabletMap))
	f := func(ti *topo.TabletInfo) {
		ctx := &rpcContext{tablet: ti}
		defer func() {
			calls <- ctx
		}()

		var args *tm.SlavePositionReq
		if masterPosition != nil {
			// If the master position is known, do our best to wait for replication to catch up.
			args = &tm.SlavePositionReq{ReplicationPosition: *masterPosition, WaitTimeout: int(wr.actionTimeout().Seconds())}
		} else {
			// In the case where a master is down, look for the last bit of data copied and wait
			// for that to apply. That gives us a chance to wait for all data.
			actionPath, err := wr.ai.SlavePosition(ti.GetAlias())
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
			args = &tm.SlavePositionReq{ReplicationPosition: lastDataPos, WaitTimeout: int(wr.actionTimeout().Seconds())}
		}

		// This option waits for the SQL thread to apply all changes to this instance.
		actionPath, err := wr.ai.WaitSlavePosition(ti.GetAlias(), args)
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
				for slaveMapKey := range positionMap {
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
				tabletPaths[i] = tabletMap[uid].GetAlias().String()
			}
			items = append(items, fmt.Sprintf("  %v\n    %v", slaveMapKey, strings.Join(tabletPaths, "\n    ")))
		}
		sort.Strings(items)
		return fmt.Errorf("inconsistent slaves, mark some offline with vtctl ScrapTablet\n%v", strings.Join(items, "\n"))
	}
	return nil
}

// Shut off all replication.
func (wr *Wrangler) stopSlaves(tabletMap map[topo.TabletAlias]*topo.TabletInfo) error {
	errs := make(chan error, len(tabletMap))
	f := func(ti *topo.TabletInfo) {
		actionPath, err := wr.ai.StopSlave(ti.GetAlias())
		if err == nil {
			err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
		}
		if err != nil {
			log.V(6).Infof("StopSlave failed: %v", err)
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

// Return a list of corresponding replication positions.
// Handles masters and slaves, but it's up to the caller to guarantee
// all tablets are in the same shard.
func (wr *Wrangler) tabletReplicationPositions(tablets []*topo.TabletInfo) ([]*mysqlctl.ReplicationPosition, error) {
	log.V(6).Infof("tabletReplicationPositions %v", tablets)
	calls := make([]*rpcContext, len(tablets))
	wg := sync.WaitGroup{}

	f := func(idx int) {
		defer wg.Done()
		ti := tablets[idx]
		ctx := &rpcContext{tablet: ti}
		calls[idx] = ctx

		var actionPath string
		if ti.Type == topo.TYPE_MASTER {
			actionPath, ctx.err = wr.ai.MasterPosition(ti.GetAlias())
		} else if ti.IsSlaveType() {
			actionPath, ctx.err = wr.ai.SlavePosition(ti.GetAlias())
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

	for i, tablet := range tablets {
		// Don't scan tablets that won't return something useful. Otherwise, you'll
		// end up waiting for a timeout.
		if tablet.Type == topo.TYPE_MASTER || tablet.IsSlaveType() {
			wg.Add(1)
			go f(i)
		} else {
			log.Infof("tabletReplicationPositions: skipping tablet %v type %v", tablet.GetAlias(), tablet.Type)
		}
	}
	wg.Wait()

	someErrors := false
	positions := make([]*mysqlctl.ReplicationPosition, len(tablets))
	for i, ctx := range calls {
		if ctx == nil {
			continue
		}
		if ctx.err != nil {
			log.Warningf("could not get replication position for tablet %v %v", ctx.tablet.GetAlias(), ctx.err)
			someErrors = true
		} else {
			positions[i] = ctx.position
		}
	}
	if someErrors {
		return positions, fmt.Errorf("partial position map, some errors")
	}
	return positions, nil
}

func (wr *Wrangler) demoteMaster(ti *topo.TabletInfo) (*mysqlctl.ReplicationPosition, error) {
	log.Infof("demote master %v", ti.GetAlias())
	actionPath, err := wr.ai.DemoteMaster(ti.GetAlias())
	if err != nil {
		return nil, err
	}
	err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	if err != nil {
		return nil, err
	}
	return wr.getMasterPosition(ti.GetAlias())
}

func (wr *Wrangler) promoteSlave(ti *topo.TabletInfo) (rsd *tm.RestartSlaveData, err error) {
	log.Infof("promote slave %v", ti.GetAlias())
	actionPath, err := wr.ai.PromoteSlave(ti.GetAlias())
	if err != nil {
		return
	}
	result, err := wr.ai.WaitForCompletionReply(actionPath, wr.actionTimeout())
	if err != nil {
		return
	}
	rsd = result.(*tm.RestartSlaveData)
	return
}

func (wr *Wrangler) restartSlaves(slaveTabletMap map[topo.TabletAlias]*topo.TabletInfo, rsd *tm.RestartSlaveData) (majorityRestart bool, err error) {
	wg := new(sync.WaitGroup)
	slaves := CopyMapValues(slaveTabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)
	errs := make([]error, len(slaveTabletMap))

	f := func(i int) {
		errs[i] = wr.restartSlave(slaves[i], rsd)
		if errs[i] != nil {
			// FIXME(msolomon) Don't bail early, just mark this phase as
			// failed. We might decide to proceed if enough of these
			// succeed.
			//
			// FIXME(msolomon) This is a somewhat delicate retry - have to
			// figure out why it failed on the tablet end. This could lead
			// to a nasty case of having to recompute where to start
			// replication. Practically speaking, that chance is pretty low.
			log.Warningf("restart slave failed: %v %v", slaves[i].GetAlias(), errs[i])
		}
		wg.Done()
	}

	for i := range slaves {
		wg.Add(1)
		go f(i)
	}
	wg.Wait()

	errCount := 0
	badTablets := make([]string, 0, 16)
	for i, err := range errs {
		if err != nil {
			errCount++
			badTablets = append(badTablets, slaves[i].GetAlias().String())
		}
	}
	// Phrase the question with multiplication so we don't get caught by int
	// division rounding.
	majorityRestart = errCount*2 < len(slaveTabletMap)

	if errCount > 0 {
		err = fmt.Errorf("restart slave failed on some tablets (%v): %v", errCount, strings.Join(badTablets, ", "))
	}
	return
}

func (wr *Wrangler) restartSlave(ti *topo.TabletInfo, rsd *tm.RestartSlaveData) (err error) {
	log.Infof("restart slave %v", ti.GetAlias())
	actionPath, err := wr.ai.RestartSlave(ti.GetAlias(), rsd)
	if err != nil {
		return err
	}
	return wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
}

func (wr *Wrangler) checkMasterElect(ti *topo.TabletInfo) error {
	// Check the master-elect is fit for duty - call out for hardware checks.
	// if the server was already serving live traffic, it's probably good
	if ti.IsServingType() {
		return nil
	}
	return wr.ExecuteOptionalTabletInfoHook(ti, hook.NewSimpleHook("preflight_serving_type"))
}

func (wr *Wrangler) finishReparent(si *topo.ShardInfo, masterElect *topo.TabletInfo, majorityRestart, leaveMasterReadOnly bool) error {
	// If the majority of slaves restarted, move ahead.
	if majorityRestart {
		if leaveMasterReadOnly {
			log.Warningf("leaving master-elect read-only, change with: vtctl SetReadWrite %v", masterElect.GetAlias())
		} else {
			log.Infof("marking master-elect read-write %v", masterElect.GetAlias())
			actionPath, err := wr.ai.SetReadWrite(masterElect.GetAlias())
			if err == nil {
				err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
			}
			if err != nil {
				log.Warningf("master master-elect read-write failed, leaving master-elect read-only, change with: vtctl SetReadWrite %v", masterElect.GetAlias())
			}
		}
	} else {
		log.Warningf("minority reparent, manual fixes are needed, leaving master-elect read-only, change with: vtctl SetReadWrite %v", masterElect.GetAlias())
	}

	// save the new master in the shard info
	si.MasterAlias = masterElect.GetAlias()
	if err := wr.ts.UpdateShard(si); err != nil {
		log.Errorf("Failed to save new master into shard: %v", err)
		return err
	}

	// We rebuild all the cells, as we may have taken tablets in and
	// out of the graph.
	log.Infof("rebuilding shard serving graph data")
	return wr.rebuildShard(masterElect.Keyspace, masterElect.Shard, nil)
}

func (wr *Wrangler) breakReplication(slaveMap map[topo.TabletAlias]*topo.TabletInfo, masterElect *topo.TabletInfo) error {
	// We are forcing a reparenting. Make sure that all slaves stop so
	// no data is accidentally replicated through before we call RestartSlave.
	log.Infof("stop slaves %v", masterElect.GetAlias())
	err := wr.stopSlaves(slaveMap)
	if err != nil {
		return err
	}

	// Force slaves to break, just in case they were not advertised in
	// the replication graph.
	log.Infof("break slaves %v", masterElect.GetAlias())
	actionPath, err := wr.ai.BreakSlaves(masterElect.GetAlias())
	if err == nil {
		err = wr.ai.WaitForCompletion(actionPath, wr.actionTimeout())
	}
	return err
}

func restartableTabletMap(slaves map[topo.TabletAlias]*topo.TabletInfo) map[uint32]*topo.TabletInfo {
	// Under normal circumstances, prune out lag as not restartable.
	// These types are explicitly excluded from reparenting since you
	// will just wait forever for them to catch up.  A possible
	// improvement is waiting for the io thread to reach the same
	// position as the sql thread on a normal slave.
	tabletMap := make(map[uint32]*topo.TabletInfo)
	for _, ti := range slaves {
		if ti.Type != topo.TYPE_LAG {
			tabletMap[ti.Uid] = ti
		} else {
			log.Infof("skipping reparent action for tablet %v %v", ti.Type, ti.GetAlias())
		}
	}
	return tabletMap
}

// sortedTabletMap returns two maps:
// - The slaveMap contains all the non-master non-scrapped hosts.
//   This can be used as a list of slaves to fix up for reparenting
// - The masterMap contains all the tablets without parents
//   (scrapped or not). This can be used to special case
//   the old master, and any tablet in a weird state, left over, ...
func sortedTabletMap(tabletMap map[topo.TabletAlias]*topo.TabletInfo) (map[topo.TabletAlias]*topo.TabletInfo, map[topo.TabletAlias]*topo.TabletInfo) {
	slaveMap := make(map[topo.TabletAlias]*topo.TabletInfo)
	masterMap := make(map[topo.TabletAlias]*topo.TabletInfo)
	for alias, ti := range tabletMap {
		if ti.Type != topo.TYPE_MASTER && ti.Type != topo.TYPE_SCRAP {
			slaveMap[alias] = ti
		} else if ti.Parent.Uid == topo.NO_TABLET {
			masterMap[alias] = ti
		}
	}
	return slaveMap, masterMap
}
