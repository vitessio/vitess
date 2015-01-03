package wrangler

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/hook"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
)

// helper struct to queue up results
type rpcContext struct {
	tablet *topo.TabletInfo
	status *myproto.ReplicationStatus
	err    error
}

// Check all the tablets replication positions to find if some
// will have a problem, and suggest a fix for them.
func (wr *Wrangler) checkSlaveReplication(tabletMap map[topo.TabletAlias]*topo.TabletInfo, masterTabletUID uint32) error {
	wr.logger.Infof("Checking all replication positions will allow the transition:")
	masterIsDead := masterTabletUID == topo.NO_TABLET

	// now check all the replication positions will allow us to proceed
	if masterIsDead {
		wr.logger.Infof("  master is dead, not checking Seconds Behind Master value")
	}
	var lastError error
	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	for _, tablet := range tabletMap {
		wg.Add(1)
		go func(tablet *topo.TabletInfo) {
			defer wg.Done()

			var err error
			defer func() {
				if err != nil {
					mutex.Lock()
					lastError = err
					mutex.Unlock()
				}
			}()

			if tablet.Type == topo.TYPE_LAG {
				wr.logger.Infof("  skipping slave position check for %v tablet %v", tablet.Type, tablet.Alias)
				return
			}

			status, err := wr.tmc.SlaveStatus(wr.ctx, tablet)
			if err != nil {
				if tablet.Type == topo.TYPE_BACKUP {
					wr.logger.Warningf("  failed to get slave position from backup tablet %v, either wait for backup to finish or scrap tablet (%v)", tablet.Alias, err)
				} else {
					wr.logger.Warningf("  failed to get slave position from %v: %v", tablet.Alias, err)
				}
				return
			}

			if !masterIsDead {
				if !status.SlaveRunning() {
					err = fmt.Errorf("slave %v is not replicating (Slave_IO or Slave_SQL not running), can't complete reparent in time", tablet.Alias)
					wr.logger.Errorf("  %v", err)
					return
				}

				var dur = time.Duration(uint(time.Second) * status.SecondsBehindMaster)
				if dur > wr.ActionTimeout() {
					err = fmt.Errorf("slave is too far behind to complete reparent in time (%v>%v), either increase timeout using 'vtctl -wait-time XXX ReparentShard ...' or scrap tablet %v", dur, wr.ActionTimeout(), tablet.Alias)
					wr.logger.Errorf("  %v", err)
					return
				}

				wr.logger.Infof("  slave is %v behind master (<%v), reparent should work for %v", dur, wr.ActionTimeout(), tablet.Alias)
			}
		}(tablet)
	}
	wg.Wait()
	return lastError
}

// Check all the tablets to see if we can proceed with reparenting.
// masterPosition is supplied from the demoted master if we are doing
// this gracefully.
func (wr *Wrangler) checkSlaveConsistency(tabletMap map[uint32]*topo.TabletInfo, masterPosition myproto.ReplicationPosition) error {
	wr.logger.Infof("checkSlaveConsistency %v %#v", topotools.MapKeys(tabletMap), masterPosition)

	// FIXME(msolomon) Something still feels clumsy here and I can't put my finger on it.
	calls := make(chan *rpcContext, len(tabletMap))
	f := func(ti *topo.TabletInfo) {
		ctx := &rpcContext{tablet: ti}
		defer func() {
			calls <- ctx
		}()

		if !masterPosition.IsZero() {
			// If the master position is known, do our best to wait for replication to catch up.
			status, err := wr.tmc.WaitSlavePosition(wr.ctx, ti, masterPosition, wr.ActionTimeout())
			if err != nil {
				ctx.err = err
				return
			}
			ctx.status = status
		} else {
			// If the master is down, just get the slave status.
			status, err := wr.tmc.SlaveStatus(wr.ctx, ti)
			if err != nil {
				ctx.err = err
				return
			}
			ctx.status = status
		}
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
			mapKey = ctx.status.Position.String()
		}
		if _, ok := positionMap[mapKey]; !ok {
			positionMap[mapKey] = make([]uint32, 0, 32)
		}
		positionMap[mapKey] = append(positionMap[mapKey], ctx.tablet.Alias.Uid)
	}

	if len(positionMap) == 1 {
		// great, everyone agrees
		// demotedMasterReplicationState is nil if demotion failed
		if !masterPosition.IsZero() {
			demotedMapKey := masterPosition.String()
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
				tabletPaths[i] = tabletMap[uid].Alias.String()
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
		err := wr.tmc.StopSlave(wr.ctx, ti)
		if err != nil {
			wr.logger.Infof("StopSlave failed: %v", err)
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

// tabletReplicationStatuses returns the ReplicationStatus of each tablet in
// tablets. It handles masters and slaves, but it's up to the caller to
// guarantee all tablets are in the same shard.
func (wr *Wrangler) tabletReplicationStatuses(tablets []*topo.TabletInfo) ([]*myproto.ReplicationStatus, error) {
	wr.logger.Infof("tabletReplicationStatuses: %v", tablets)
	calls := make([]*rpcContext, len(tablets))
	wg := sync.WaitGroup{}

	f := func(idx int) {
		defer wg.Done()
		ti := tablets[idx]
		ctx := &rpcContext{tablet: ti}
		calls[idx] = ctx
		if ti.Type == topo.TYPE_MASTER {
			pos, err := wr.tmc.MasterPosition(wr.ctx, ti)
			ctx.err = err
			if err == nil {
				ctx.status = &myproto.ReplicationStatus{Position: pos}
			}
		} else if ti.IsSlaveType() {
			ctx.status, ctx.err = wr.tmc.SlaveStatus(wr.ctx, ti)
		}
	}

	for i, tablet := range tablets {
		// Don't scan tablets that won't return something useful. Otherwise, you'll
		// end up waiting for a timeout.
		if tablet.Type == topo.TYPE_MASTER || tablet.IsSlaveType() {
			wg.Add(1)
			go f(i)
		} else {
			wr.logger.Infof("tabletReplicationPositions: skipping tablet %v type %v", tablet.Alias, tablet.Type)
		}
	}
	wg.Wait()

	someErrors := false
	stats := make([]*myproto.ReplicationStatus, len(tablets))
	for i, ctx := range calls {
		if ctx == nil {
			continue
		}
		if ctx.err != nil {
			wr.logger.Warningf("could not get replication status for tablet %v %v", ctx.tablet.Alias, ctx.err)
			someErrors = true
		} else {
			stats[i] = ctx.status
		}
	}
	if someErrors {
		return stats, fmt.Errorf("partial position map, some errors")
	}
	return stats, nil
}

func (wr *Wrangler) demoteMaster(ti *topo.TabletInfo) (myproto.ReplicationPosition, error) {
	wr.logger.Infof("demote master %v", ti.Alias)
	if err := wr.tmc.DemoteMaster(wr.ctx, ti); err != nil {
		return myproto.ReplicationPosition{}, err
	}
	return wr.tmc.MasterPosition(wr.ctx, ti)
}

func (wr *Wrangler) promoteSlave(ti *topo.TabletInfo) (rsd *actionnode.RestartSlaveData, err error) {
	wr.logger.Infof("promote slave %v", ti.Alias)
	return wr.tmc.PromoteSlave(wr.ctx, ti)
}

func (wr *Wrangler) restartSlaves(slaveTabletMap map[topo.TabletAlias]*topo.TabletInfo, rsd *actionnode.RestartSlaveData) (majorityRestart bool, err error) {
	wg := new(sync.WaitGroup)
	slaves := topotools.CopyMapValues(slaveTabletMap, []*topo.TabletInfo{}).([]*topo.TabletInfo)
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
			wr.logger.Warningf("restart slave failed: %v %v", slaves[i].Alias, errs[i])
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
			badTablets = append(badTablets, slaves[i].Alias.String())
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

func (wr *Wrangler) restartSlave(ti *topo.TabletInfo, rsd *actionnode.RestartSlaveData) (err error) {
	wr.logger.Infof("restart slave %v", ti.Alias)
	return wr.tmc.RestartSlave(wr.ctx, ti, rsd)
}

func (wr *Wrangler) checkMasterElect(ti *topo.TabletInfo) error {
	// Check the master-elect is fit for duty - call out for hardware checks.
	// if the server was already serving live traffic, it's probably good
	if ti.IsInServingGraph() {
		return nil
	}
	return wr.ExecuteOptionalTabletInfoHook(ti, hook.NewSimpleHook("preflight_serving_type"))
}

func (wr *Wrangler) finishReparent(si *topo.ShardInfo, masterElect *topo.TabletInfo, majorityRestart, leaveMasterReadOnly bool) error {
	// If the majority of slaves restarted, move ahead.
	if majorityRestart {
		if leaveMasterReadOnly {
			wr.logger.Warningf("leaving master-elect read-only, change with: vtctl SetReadWrite %v", masterElect.Alias)
		} else {
			wr.logger.Infof("marking master-elect read-write %v", masterElect.Alias)
			if err := wr.tmc.SetReadWrite(wr.ctx, masterElect); err != nil {
				wr.logger.Warningf("master master-elect read-write failed, leaving master-elect read-only, change with: vtctl SetReadWrite %v", masterElect.Alias)
			}
		}
	} else {
		wr.logger.Warningf("minority reparent, manual fixes are needed, leaving master-elect read-only, change with: vtctl SetReadWrite %v", masterElect.Alias)
	}

	// save the new master in the shard info
	si.MasterAlias = masterElect.Alias
	if err := topo.UpdateShard(wr.ctx, wr.ts, si); err != nil {
		wr.logger.Errorf("Failed to save new master into shard: %v", err)
		return err
	}

	// We rebuild all the cells, as we may have taken tablets in and
	// out of the graph.
	wr.logger.Infof("rebuilding shard serving graph data")
	_, err := wr.RebuildShardGraph(masterElect.Keyspace, masterElect.Shard, nil)
	return err
}

func (wr *Wrangler) breakReplication(slaveMap map[topo.TabletAlias]*topo.TabletInfo, masterElect *topo.TabletInfo) error {
	// We are forcing a reparenting. Make sure that all slaves stop so
	// no data is accidentally replicated through before we call RestartSlave.
	wr.logger.Infof("stop slaves %v", masterElect.Alias)
	err := wr.stopSlaves(slaveMap)
	if err != nil {
		return err
	}

	// Force slaves to break, just in case they were not advertised in
	// the replication graph.
	wr.logger.Infof("break slaves %v", masterElect.Alias)
	return wr.tmc.BreakSlaves(wr.ctx, masterElect)
}

func (wr *Wrangler) restartableTabletMap(slaves map[topo.TabletAlias]*topo.TabletInfo) map[uint32]*topo.TabletInfo {
	// Under normal circumstances, prune out lag as not restartable.
	// These types are explicitly excluded from reparenting since you
	// will just wait forever for them to catch up.  A possible
	// improvement is waiting for the io thread to reach the same
	// position as the sql thread on a normal slave.
	tabletMap := make(map[uint32]*topo.TabletInfo)
	for _, ti := range slaves {
		if ti.Type != topo.TYPE_LAG {
			tabletMap[ti.Alias.Uid] = ti
		} else {
			wr.logger.Infof("skipping reparent action for tablet %v %v", ti.Type, ti.Alias)
		}
	}
	return tabletMap
}
