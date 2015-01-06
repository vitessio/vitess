// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"flag"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
	"golang.org/x/net/context"
)

var (
	fastReparent = flag.Bool("fast_external_reparent", false, "Skip updating of fields in topology that aren't needed if all MySQL reparents are done by an external tool, instead of by Vitess directly.")

	finalizeReparentTimeout = flag.Duration("finalize_external_reparent_timeout", 10*time.Second, "Timeout for the finalize stage of a fast external reparent reconciliation.")
)

// fastTabletExternallyReparented completely replaces TabletExternallyReparented
// if the -fast_external_reparent flag is specified.
func (agent *ActionAgent) fastTabletExternallyReparented(ctx context.Context, externalID string) (err error) {
	tablet := agent.Tablet()

	// Check the global shard record.
	si, err := topo.GetShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("fastTabletExternallyReparented: failed to read global shard record for %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}
	if si.MasterAlias == tablet.Alias {
		// We may get called on the current master even when nothing has changed.
		// If the global shard record is already updated, it means we successfully
		// finished a previous reparent to this tablet.
		return nil
	}

	// Create a reusable Reparent event with available info.
	ev := &events.Reparent{
		ShardInfo:  *si,
		NewMaster:  *tablet.Tablet,
		OldMaster:  topo.Tablet{Alias: si.MasterAlias, Type: topo.TYPE_MASTER},
		ExternalID: externalID,
	}
	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()
	event.DispatchUpdate(ev, "starting external from tablet (fast)")

	// Execute state change to master by force-updating only the local copy of the
	// tablet record. The actual record in topo will be updated later.
	log.Infof("fastTabletExternallyReparented: executing change callback for state change to MASTER")
	oldTablet := *tablet.Tablet
	newTablet := oldTablet
	newTablet.Type = topo.TYPE_MASTER
	newTablet.State = topo.STATE_READ_WRITE
	newTablet.Health = nil
	agent.setTablet(topo.NewTabletInfo(&newTablet, -1))
	if err := agent.updateState(ctx, &oldTablet, "fastTabletExternallyReparented"); err != nil {
		return fmt.Errorf("fastTabletExternallyReparented: failed to change tablet state to MASTER: %v", err)
	}

	// Directly write the new master endpoint in the serving graph.
	// We will do a true rebuild in the background soon, but in the meantime,
	// this will be enough for clients to re-resolve the new master.
	event.DispatchUpdate(ev, "writing new master endpoint")
	log.Infof("fastTabletExternallyReparented: writing new master endpoint to serving graph")
	ep, err := tablet.EndPoint()
	if err != nil {
		return fmt.Errorf("fastTabletExternallyReparented: failed to generate EndPoint for tablet %v: %v", tablet.Alias, err)
	}
	err = topo.UpdateEndPoints(ctx, agent.TopoServer, tablet.Alias.Cell,
		si.Keyspace(), si.ShardName(), topo.TYPE_MASTER,
		&topo.EndPoints{Entries: []topo.EndPoint{*ep}})
	if err != nil {
		return fmt.Errorf("fastTabletExternallyReparented: failed to update master endpoint: %v", err)
	}

	// Start the finalize stage with a background context, but connect the trace.
	go agent.finalizeTabletExternallyReparented(
		trace.CopySpan(agent.batchCtx, ctx), si, ev)

	return nil
}

// finalizeTabletExternallyReparented performs slow, synchronized reconciliation
// tasks that ensure topology is self-consistent, and then marks the reparent as
// finished by updating the global shard record.
func (agent *ActionAgent) finalizeTabletExternallyReparented(ctx context.Context, si *topo.ShardInfo, ev *events.Reparent) (err error) {
	ctx, cancel := context.WithTimeout(ctx, *finalizeReparentTimeout)
	defer cancel()

	defer func() {
		if err != nil {
			log.Warningf("finalizeTabletExternallyReparented error: %v", err)
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	var wg sync.WaitGroup
	var errs concurrency.AllErrorRecorder
	oldMasterAlias := si.MasterAlias

	// Update the tablet records for the old and new master concurrently.
	// We don't need a lock to update them because they are the source of truth,
	// not derived values (like the serving graph).
	event.DispatchUpdate(ev, "updating old and new master tablet records")
	log.Infof("finalizeTabletExternallyReparented: updating tablet records")
	wg.Add(2)
	go func() {
		// Update our own record to master.
		err := topo.UpdateTabletFields(ctx, agent.TopoServer, agent.TabletAlias,
			func(tablet *topo.Tablet) error {
				tablet.Type = topo.TYPE_MASTER
				tablet.State = topo.STATE_READ_WRITE
				tablet.Health = nil
				return nil
			})
		errs.RecordError(err)
		wg.Done()
	}()
	go func() {
		// Force the old master to spare.
		var oldMasterTablet *topo.Tablet
		err := topo.UpdateTabletFields(ctx, agent.TopoServer, oldMasterAlias,
			func(tablet *topo.Tablet) error {
				tablet.Type = topo.TYPE_SPARE
				oldMasterTablet = tablet
				return nil
			})
		errs.RecordError(err)
		wg.Done()
		if err != nil {
			return
		}

		// Tell the old master to refresh its state. We don't need to wait for it.
		if oldMasterTablet != nil {
			tmc := tmclient.NewTabletManagerClient()
			tmc.RefreshState(ctx, topo.NewTabletInfo(oldMasterTablet, -1))
		}
	}()

	tablet := agent.Tablet()

	// Wait for the tablet records to be updated. At that point, any rebuild will
	// see the new master, so we're ready to mark the reparent as done in the
	// global shard record.
	wg.Wait()
	if errs.HasErrors() {
		return errs.Error()
	}

	// Update the master field in the global shard record. We don't use a lock
	// here anymore. The lock was only to ensure that the global shard record
	// didn't get modified between the time when we read it and the time when we
	// write it back. Now we use an update loop pattern to do that instead.
	event.DispatchUpdate(ev, "updating global shard record")
	log.Infof("finalizeTabletExternallyReparented: updating global shard record")
	topo.UpdateShardFields(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard, func(shard *topo.Shard) error {
		shard.MasterAlias = tablet.Alias
		return nil
	})

	// Rebuild the shard serving graph in the necessary cells.
	// If it's a cross-cell reparent, rebuild all cells (by passing nil).
	// If it's a same-cell reparent, we only need to rebuild the master cell.
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	var cells []string
	if oldMasterAlias.Cell == tablet.Alias.Cell {
		cells = []string{tablet.Alias.Cell}
	}
	logger := logutil.NewConsoleLogger()
	log.Infof("finalizeTabletExternallyReparented: rebuilding shard")
	if _, err = topotools.RebuildShard(ctx, logger, agent.TopoServer, tablet.Keyspace, tablet.Shard, cells, agent.LockTimeout); err != nil {
		return err
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}

// TabletExternallyReparented updates all topo records so the current
// tablet is the new master for this shard.
// Should be called under RPCWrapLock.
func (agent *ActionAgent) TabletExternallyReparented(ctx context.Context, externalID string) error {
	if *fastReparent {
		return agent.fastTabletExternallyReparented(ctx, externalID)
	}

	tablet := agent.Tablet()

	// fast quick check on the shard to see if we're not the master already
	shardInfo, err := topo.GetShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("TabletExternallyReparented: Cannot read the shard %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}
	if shardInfo.MasterAlias == agent.TabletAlias {
		// we are already the master, nothing more to do.
		return nil
	}

	// grab the shard lock
	actionNode := actionnode.ShardExternallyReparented(agent.TabletAlias)
	lockPath, err := actionNode.LockShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("TabletExternallyReparented: Cannot lock shard %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}

	// do the work
	runAfterAction, err := agent.tabletExternallyReparentedLocked(ctx, externalID)
	if err != nil {
		log.Warningf("TabletExternallyReparented: internal error: %v", err)
	}

	// release the lock in any case, and run refreshTablet if necessary
	err = actionNode.UnlockShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard, lockPath, err)
	if runAfterAction {
		if refreshErr := agent.refreshTablet(ctx, "RPC(TabletExternallyReparented)"); refreshErr != nil {
			if err == nil {
				// no error yet, now we have one
				err = refreshErr
			} else {
				//have an error already, keep the original one
				log.Warningf("refreshTablet failed with error: %v", refreshErr)
			}
		}
	}
	return err
}

// tabletExternallyReparentedLocked is called with the shard lock.
// It returns if agent.refreshTablet should be called, and the error.
// Note both are set independently (can have both true and an error).
func (agent *ActionAgent) tabletExternallyReparentedLocked(ctx context.Context, externalID string) (bool, error) {
	// re-read the tablet record to be sure we have the latest version
	tablet, err := topo.GetTablet(ctx, agent.TopoServer, agent.TabletAlias)
	if err != nil {
		return false, err
	}

	// read the shard, make sure again the master is not already good.
	shardInfo, err := topo.GetShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return false, err
	}
	if shardInfo.MasterAlias == tablet.Alias {
		log.Infof("TabletExternallyReparented: tablet became the master before we get the lock?")
		return false, nil
	}
	log.Infof("TabletExternallyReparented called and we're not the master, doing the work")

	// Read the tablets, make sure the master elect is known to the shard
	// (it's this tablet, so it better be!).
	// Note we will keep going with a partial tablet map, which usually
	// happens when a cell is not reachable. After these checks, the
	// guarantees we'll have are:
	// - global cell is reachable (we just locked and read the shard)
	// - the local cell that contains the new master is reachable
	//   (as we're going to check the new master is in the list)
	// That should be enough.
	tabletMap, err := topo.GetTabletMapForShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard)
	switch err {
	case nil:
		// keep going
	case topo.ErrPartialResult:
		log.Warningf("Got topo.ErrPartialResult from GetTabletMapForShard, may need to re-init some tablets")
	default:
		return false, err
	}
	masterElectTablet, ok := tabletMap[tablet.Alias]
	if !ok {
		return false, fmt.Errorf("this master-elect tablet %v not found in replication graph %v/%v %v", tablet.Alias, tablet.Keyspace, tablet.Shard, topotools.MapKeys(tabletMap))
	}

	// Create reusable Reparent event with available info
	ev := &events.Reparent{
		ShardInfo:  *shardInfo,
		NewMaster:  *tablet.Tablet,
		ExternalID: externalID,
	}

	if oldMasterTablet, ok := tabletMap[shardInfo.MasterAlias]; ok {
		ev.OldMaster = *oldMasterTablet.Tablet
	}

	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()

	// sort the tablets, and handle them
	slaveTabletMap, masterTabletMap := topotools.SortedTabletMap(tabletMap)
	event.DispatchUpdate(ev, "starting external from tablet")

	// We fix the new master in the replication graph.
	// Note after this call, we may have changed the tablet record,
	// so we will always return true, so the tablet record is re-read
	// by the agent.
	event.DispatchUpdate(ev, "mark ourself as new master")
	err = agent.updateReplicationGraphForPromotedSlave(ctx, tablet)
	if err != nil {
		// This suggests we can't talk to topo server. This is bad.
		return true, fmt.Errorf("updateReplicationGraphForPromotedSlave failed: %v", err)
	}

	// Once this tablet is promoted, remove it from our maps
	delete(slaveTabletMap, tablet.Alias)
	delete(masterTabletMap, tablet.Alias)

	// Then fix all the slaves, including the old master.  This
	// last step is very likely to time out for some tablets (one
	// random guy is dead, the old master is dead, ...). We
	// execute them all in parallel until we get to
	// wr.ActionTimeout(). After this, no other action with a
	// timeout is executed, so even if we got to the timeout,
	// we're still good.
	event.DispatchUpdate(ev, "restarting slaves")
	logger := logutil.NewConsoleLogger()
	tmc := tmclient.NewTabletManagerClient()
	topotools.RestartSlavesExternal(agent.TopoServer, logger, slaveTabletMap, masterTabletMap, masterElectTablet.Alias, func(ti *topo.TabletInfo, swrd *actionnode.SlaveWasRestartedArgs) error {
		return tmc.SlaveWasRestarted(ctx, ti, swrd)
	})

	// Compute the list of Cells we need to rebuild: old master and
	// all other cells if reparenting to another cell.
	cells := []string{shardInfo.MasterAlias.Cell}
	if shardInfo.MasterAlias.Cell != tablet.Alias.Cell {
		cells = nil
	}

	// now update the master record in the shard object
	event.DispatchUpdate(ev, "updating shard record")
	log.Infof("Updating Shard's MasterAlias record")
	shardInfo.MasterAlias = tablet.Alias
	if err = topo.UpdateShard(ctx, agent.TopoServer, shardInfo); err != nil {
		return true, err
	}

	// and rebuild the shard serving graph
	event.DispatchUpdate(ev, "rebuilding shard serving graph")
	log.Infof("Rebuilding shard serving graph data")
	if _, err = topotools.RebuildShard(ctx, logger, agent.TopoServer, tablet.Keyspace, tablet.Shard, cells, agent.LockTimeout); err != nil {
		return true, err
	}

	event.DispatchUpdate(ev, "finished")
	return true, nil
}
