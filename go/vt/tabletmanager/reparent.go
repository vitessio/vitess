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
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/topotools/events"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	finalizeReparentTimeout = flag.Duration("finalize_external_reparent_timeout", 10*time.Second, "Timeout for the finalize stage of a fast external reparent reconciliation.")

	externalReparentStats = stats.NewTimings("ExternalReparents", "NewMasterVisible", "FullRebuild")
)

// SetReparentFlags changes flag values. It should only be used in tests.
func SetReparentFlags(timeout time.Duration) {
	*finalizeReparentTimeout = timeout
}

// TabletExternallyReparented updates all topo records so the current
// tablet is the new master for this shard.
// Should be called under RPCWrapLock.
func (agent *ActionAgent) TabletExternallyReparented(ctx context.Context, externalID string) error {
	startTime := time.Now()

	// If there is a finalize step running, wait for it to finish or time out
	// before checking the global shard record again.
	if agent.finalizeReparentCtx != nil {
		select {
		case <-agent.finalizeReparentCtx.Done():
			agent.finalizeReparentCtx = nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	tablet := agent.Tablet()

	// Check the global shard record.
	si, err := topo.GetShard(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("fastTabletExternallyReparented: failed to read global shard record for %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}
	if si.MasterAlias != nil && *si.MasterAlias == *topo.TabletAliasToProto(tablet.Alias) {
		// We may get called on the current master even when nothing has changed.
		// If the global shard record is already updated, it means we successfully
		// finished a previous reparent to this tablet.
		return nil
	}

	// Create a reusable Reparent event with available info.
	ev := &events.Reparent{
		ShardInfo: *si,
		NewMaster: *tablet.Tablet,
		OldMaster: topo.Tablet{
			Alias: topo.ProtoToTabletAlias(si.MasterAlias),
			Type:  topo.TYPE_MASTER,
		},
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
	newTablet.Health = nil
	agent.setTablet(topo.NewTabletInfo(&newTablet, -1))
	if err := agent.updateState(ctx, &oldTablet, "fastTabletExternallyReparented"); err != nil {
		return fmt.Errorf("fastTabletExternallyReparented: failed to change tablet state to MASTER: %v", err)
	}

	agent.mutex.Lock()
	agent._tabletExternallyReparentedTime = time.Now()
	agent.mutex.Unlock()

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
		&pb.EndPoints{Entries: []*pb.EndPoint{ep}}, -1)
	if err != nil {
		return fmt.Errorf("fastTabletExternallyReparented: failed to update master endpoint: %v", err)
	}
	externalReparentStats.Record("NewMasterVisible", startTime)

	// Start the finalize stage with a background context, but connect the trace.
	bgCtx, cancel := context.WithTimeout(agent.batchCtx, *finalizeReparentTimeout)
	bgCtx = trace.CopySpan(bgCtx, ctx)
	agent.finalizeReparentCtx = bgCtx
	go func() {
		err := agent.finalizeTabletExternallyReparented(bgCtx, si, ev)
		cancel()

		if err != nil {
			log.Warningf("finalizeTabletExternallyReparented error: %v", err)
			event.DispatchUpdate(ev, "failed: "+err.Error())
			return
		}
		externalReparentStats.Record("FullRebuild", startTime)
	}()

	return nil
}

// finalizeTabletExternallyReparented performs slow, synchronized reconciliation
// tasks that ensure topology is self-consistent, and then marks the reparent as
// finished by updating the global shard record.
func (agent *ActionAgent) finalizeTabletExternallyReparented(ctx context.Context, si *topo.ShardInfo, ev *events.Reparent) (err error) {
	var wg sync.WaitGroup
	var errs concurrency.AllErrorRecorder
	oldMasterAlias := si.MasterAlias

	// Update the tablet records and serving graph for the old and new master concurrently.
	event.DispatchUpdate(ev, "updating old and new master tablet records")
	log.Infof("finalizeTabletExternallyReparented: updating tablet records")
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Update our own record to master.
		var updatedTablet *topo.Tablet
		err := topo.UpdateTabletFields(ctx, agent.TopoServer, agent.TabletAlias,
			func(tablet *topo.Tablet) error {
				tablet.Type = topo.TYPE_MASTER
				tablet.Health = nil
				updatedTablet = tablet
				return nil
			})
		if err != nil {
			errs.RecordError(err)
			return
		}

		// Update the serving graph for the tablet.
		if updatedTablet != nil {
			errs.RecordError(
				topotools.UpdateTabletEndpoints(ctx, agent.TopoServer, updatedTablet))
		}
	}()

	if !topo.TabletAliasIsZero(oldMasterAlias) {
		wg.Add(1)
		go func() {
			// Force the old master to spare.
			var oldMasterTablet *topo.Tablet
			err := topo.UpdateTabletFields(ctx, agent.TopoServer, topo.ProtoToTabletAlias(oldMasterAlias),
				func(tablet *topo.Tablet) error {
					tablet.Type = topo.TYPE_SPARE
					oldMasterTablet = tablet
					return nil
				})
			if err != nil {
				errs.RecordError(err)
				wg.Done()
				return
			}

			if oldMasterTablet != nil {
				// We now know more about the old master, so add it to event data.
				ev.OldMaster = *oldMasterTablet

				// Update the serving graph.
				errs.RecordError(
					topotools.UpdateTabletEndpoints(ctx, agent.TopoServer, oldMasterTablet))
				wg.Done()

				// Tell the old master to refresh its state. We don't need to wait for it.
				tmc := tmclient.NewTabletManagerClient()
				tmc.RefreshState(ctx, topo.NewTabletInfo(oldMasterTablet, -1))
			}
		}()
	}

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
	si, err = topo.UpdateShardFields(ctx, agent.TopoServer, tablet.Keyspace, tablet.Shard, func(shard *pb.Shard) error {
		shard.MasterAlias = topo.TabletAliasToProto(tablet.Alias)
		return nil
	})
	if err != nil {
		return err
	}

	// We already took care of updating the serving graph for the old and new masters.
	// All that's left now is in case of a cross-cell reparent, we need to update the
	// master cell setting in the SrvShard records of all cells.
	if oldMasterAlias == nil || oldMasterAlias.Cell != tablet.Alias.Cell {
		event.DispatchUpdate(ev, "rebuilding shard serving graph")
		log.Infof("finalizeTabletExternallyReparented: updating SrvShard in all cells for cross-cell reparent")
		if err := topotools.UpdateAllSrvShards(ctx, agent.TopoServer, si); err != nil {
			return err
		}
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}
