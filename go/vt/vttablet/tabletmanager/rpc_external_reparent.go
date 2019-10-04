/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"flag"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools/events"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	finalizeReparentTimeout = flag.Duration("finalize_external_reparent_timeout", 30*time.Second, "Timeout for the finalize stage of a fast external reparent reconciliation.")

	externalReparentStats = stats.NewTimings("ExternalReparents", "External reparenting time", "stage", "NewMasterVisible", "FullRebuild")
)

// SetReparentFlags changes flag values. It should only be used in tests.
func SetReparentFlags(timeout time.Duration) {
	*finalizeReparentTimeout = timeout
}

// TabletExternallyReparented updates all topo records so the current
// tablet is the new master for this shard.
func (agent *ActionAgent) TabletExternallyReparented(ctx context.Context, externalID string) error {
	if err := agent.lock(ctx); err != nil {
		return err
	}
	defer agent.unlock()

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
	si, err := agent.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		log.Warningf("fastTabletExternallyReparented: failed to read global shard record for %v/%v: %v", tablet.Keyspace, tablet.Shard, err)
		return err
	}

	// The external failover tool told us that we are still the MASTER.
	// Update the timestamp to the current time (start a new term).
	agent.setMasterTermStartTime(startTime)

	// Create a reusable Reparent event with available info.
	ev := &events.Reparent{
		ShardInfo: *si,
		NewMaster: *tablet,
		OldMaster: topodatapb.Tablet{
			Alias: si.MasterAlias,
			Type:  topodatapb.TabletType_MASTER,
		},
		ExternalID: externalID,
	}
	defer func() {
		if err != nil {
			event.DispatchUpdate(ev, "failed: "+err.Error())
		}
	}()
	event.DispatchUpdate(ev, "starting external from tablet (fast)")

	// We may get called on the current master multiple times in order to fix incomplete external reparents.
	// We update the tablet here only if it is not currently master
	if tablet.Type != topodatapb.TabletType_MASTER {
		log.Infof("fastTabletExternallyReparented: executing change callback for state change to MASTER")
		// Execute state change to master by force-updating only the local copy of the
		// tablet record. The actual record in topo will be updated later.
		newTablet := proto.Clone(tablet).(*topodatapb.Tablet)
		newTablet.Type = topodatapb.TabletType_MASTER

		// This is where updateState will block for gracePeriod, while it gives
		// vtgate a chance to stop sending replica queries.
		agent.updateState(ctx, newTablet, "fastTabletExternallyReparented")
	}
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
// tasks that ensure topology is self-consistent.
// It first updates new and old master tablet records, then updates
// the global shard record, then refreshes the old master.
// After that it attempts to detect and clean up any lingering old masters.
// Note that an up-to-date shard record does not necessarily mean that
// the reparent completed all the actions successfully
func (agent *ActionAgent) finalizeTabletExternallyReparented(ctx context.Context, si *topo.ShardInfo, ev *events.Reparent) (err error) {
	var wg sync.WaitGroup
	var errs concurrency.AllErrorRecorder
	var oldMasterTablet *topodatapb.Tablet
	oldMasterAlias := si.MasterAlias

	// Update the new and old master tablet records concurrently.
	event.DispatchUpdate(ev, "updating old and new master tablet records")
	log.Infof("finalizeTabletExternallyReparented: updating tablet records")
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Infof("finalizeTabletExternallyReparented: updating tablet record for new master: %v", agent.TabletAlias)
		// Update our own record to master if needed
		_, err := agent.TopoServer.UpdateTabletFields(ctx, agent.TabletAlias,
			func(tablet *topodatapb.Tablet) error {
				if tablet.Type != topodatapb.TabletType_MASTER {
					tablet.Type = topodatapb.TabletType_MASTER
					return nil
				}
				// returning NoUpdateNeeded avoids unnecessary calls to UpdateTablet
				return topo.NewError(topo.NoUpdateNeeded, agent.TabletAlias.String())
			})
		if err != nil {
			errs.RecordError(err)
		}
	}()

	// If TER is called twice, then oldMasterAlias is the same as agent.TabletAlias
	if !topoproto.TabletAliasIsZero(oldMasterAlias) && !topoproto.TabletAliasEqual(oldMasterAlias, agent.TabletAlias) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Infof("finalizeTabletExternallyReparented: updating tablet record for old master: %v", oldMasterAlias)

			// Forcibly demote the old master in topology, since we can't rely on the
			// old master to be up to change its own record.
			var err error
			oldMasterTablet, err = agent.TopoServer.UpdateTabletFields(ctx, oldMasterAlias,
				func(tablet *topodatapb.Tablet) error {
					if tablet.Type == topodatapb.TabletType_MASTER {
						tablet.Type = topodatapb.TabletType_REPLICA
						return nil
					}
					// returning NoUpdateNeeded avoids unnecessary calls to UpdateTablet
					return topo.NewError(topo.NoUpdateNeeded, oldMasterAlias.String())
				})
			if err != nil {
				errs.RecordError(err)
				return
			}

			// We now know more about the old master, so add it to event data.
			// oldMasterTablet will be nil if no update was needed
			if oldMasterTablet != nil {
				ev.OldMaster = *oldMasterTablet
			}
		}()
	}
	// Wait for the tablet records to be updated. At that point, any rebuild will
	// see the new master, so we're ready to mark the reparent as done in the
	// global shard record.
	wg.Wait()
	if errs.HasErrors() {
		return errs.Error()
	}

	masterTablet := agent.Tablet()

	event.DispatchUpdate(ev, "updating global shard record")
	log.Infof("finalizeTabletExternallyReparented: updating global shard record if needed")
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Update the master field in the global shard record. We don't use a lock
		// here anymore. The lock was only to ensure that the global shard record
		// didn't get modified between the time when we read it and the time when we
		// write it back. Now we use an update loop pattern to do that instead.
		_, err = agent.TopoServer.UpdateShardFields(ctx, masterTablet.Keyspace, masterTablet.Shard, func(currentSi *topo.ShardInfo) error {
			if topoproto.TabletAliasEqual(currentSi.MasterAlias, masterTablet.Alias) {
				// returning NoUpdateNeeded avoids unnecessary calls to UpdateTablet
				return topo.NewError(topo.NoUpdateNeeded, masterTablet.Alias.String())
			}
			if !topoproto.TabletAliasEqual(currentSi.MasterAlias, oldMasterAlias) {
				log.Warningf("old master alias (%v) not found in the global Shard record i.e. it has changed in the meantime."+
					" We're not overwriting the value with the new master (%v) because the current value is probably newer."+
					" (initial Shard record = %#v, current Shard record = %#v)",
					oldMasterAlias, masterTablet.Alias, si, currentSi)
				// returning NoUpdateNeeded avoids unnecessary calls to UpdateTablet
				return topo.NewError(topo.NoUpdateNeeded, oldMasterAlias.String())
			}
			currentSi.MasterAlias = masterTablet.Alias
			return nil
		})
		if err != nil {
			errs.RecordError(err)
		}
	}()

	tmc := tmclient.NewTabletManagerClient()
	defer tmc.Close()
	if !topoproto.TabletAliasIsZero(oldMasterAlias) && !topoproto.TabletAliasEqual(oldMasterAlias, agent.TabletAlias) && oldMasterTablet != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Tell the old master to re-read its tablet record and change its state.
			// We don't need to put error into errs if this fails, but we need to wait
			// for it to make sure that old master tablet is not stuck in the MASTER
			// state.
			if err := tmc.RefreshState(ctx, oldMasterTablet); err != nil {
				log.Warningf("Error calling RefreshState on old master %v: %v", topoproto.TabletAliasString(oldMasterTablet.Alias), err)
			}
		}()
	}

	wg.Wait()
	if errs.HasErrors() {
		return errs.Error()
	}

	// Look for any other tablets claiming to be master and fix them up on a best-effort basis
	tabletMap, err := agent.TopoServer.GetTabletMapForShard(ctx, masterTablet.Keyspace, masterTablet.Shard)
	if err != nil {
		log.Errorf("ignoring error %v from GetTabletMapForShard so that we can process any partial results", err)
	}

	for _, tabletInfo := range tabletMap {
		alias := tabletInfo.Tablet.Alias
		if !topoproto.TabletAliasEqual(alias, agent.TabletAlias) && !topoproto.TabletAliasEqual(alias, oldMasterAlias) && tabletInfo.Tablet.Type == topodatapb.TabletType_MASTER {
			log.Infof("finalizeTabletExternallyReparented: updating tablet record for another old master: %v", alias)
			wg.Add(1)
			go func(alias *topodatapb.TabletAlias) {
				defer wg.Done()
				var err error
				tab, err := agent.TopoServer.UpdateTabletFields(ctx, alias,
					func(tablet *topodatapb.Tablet) error {
						if tablet.Type == topodatapb.TabletType_MASTER {
							tablet.Type = topodatapb.TabletType_REPLICA
							return nil
						}
						return topo.NewError(topo.NoUpdateNeeded, alias.String())
					})
				if err != nil {
					errs.RecordError(err)
					return
				}
				// tab will be nil if no update was needed
				if tab != nil {
					log.Infof("finalizeTabletExternallyReparented: Refresh state for tablet: %v", topoproto.TabletAliasString(tab.Alias))
					if err := tmc.RefreshState(ctx, tab); err != nil {
						log.Warningf("Error calling RefreshState on old master %v: %v", topoproto.TabletAliasString(tab.Alias), err)
					}
				}
			}(alias)
		}
	}
	wg.Wait()
	if errs.HasErrors() {
		return errs.Error()
	}

	event.DispatchUpdate(ev, "finished")
	return nil
}
