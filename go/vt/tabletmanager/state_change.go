// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the agent state changes.

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager/events"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// constants for this module
	historyLength = 16

	// gracePeriod is the amount of time we pause after broadcasting to vtgate
	// that we're going to stop serving a particular target type (e.g. when going
	// spare, or when being promoted to master). During this period, we expect
	// vtgate to gracefully redirect traffic elsewhere, before we begin actually
	// rejecting queries for that target type.
	gracePeriod = flag.Duration("serving_state_grace_period", 0, "how long to pause after broadcasting health to vtgate, before enforcing a new serving state")
)

// Query rules from blacklist
const blacklistQueryRules string = "BlacklistQueryRules"

// loadBlacklistRules loads and builds the blacklist query rules
func (agent *ActionAgent) loadBlacklistRules(tablet *topodatapb.Tablet, blacklistedTables []string) (err error) {
	blacklistRules := tabletserver.NewQueryRules()
	if len(blacklistedTables) > 0 {
		// tables, first resolve wildcards
		tables, err := mysqlctl.ResolveTables(agent.MysqlDaemon, topoproto.TabletDbName(tablet), blacklistedTables)
		if err != nil {
			return err
		}

		// Verify that at least one table matches the wildcards, so
		// that we don't add a rule to blacklist all tables
		if len(tables) > 0 {
			log.Infof("Blacklisting tables %v", strings.Join(tables, ", "))
			qr := tabletserver.NewQueryRule("enforce blacklisted tables", "blacklisted_table", tabletserver.QRFailRetry)
			for _, t := range tables {
				qr.AddTableCond(t)
			}
			blacklistRules.Add(qr)
		}
	}

	loadRuleErr := agent.QueryServiceControl.SetQueryRules(blacklistQueryRules, blacklistRules)
	if loadRuleErr != nil {
		log.Warningf("Fail to load query rule set %s: %s", blacklistQueryRules, loadRuleErr)
	}
	return nil
}

// allowQueries tells QueryService to go in the serving state.
// Returns true if the state of QueryService or the tablet type changed.
func (agent *ActionAgent) allowQueries(tabletType topodatapb.TabletType) (bool, error) {
	return agent.QueryServiceControl.SetServingType(tabletType, true, nil)
}

// disallowQueries tells QueryService to go in the *not* serving state.
// Returns true if the state of QueryService or the tablet type changed.
func (agent *ActionAgent) disallowQueries(tabletType topodatapb.TabletType, reason string) (bool, error) {
	log.Infof("Agent is going to disallow queries, reason: %v", reason)

	return agent.QueryServiceControl.SetServingType(tabletType, false, nil)
}

func (agent *ActionAgent) enterLameduck(reason string) {
	log.Infof("Agent is entering lameduck, reason: %v", reason)

	agent.QueryServiceControl.EnterLameduck()
}

func (agent *ActionAgent) broadcastHealth() {
	// get the replication delays
	agent.mutex.Lock()
	replicationDelay := agent._replicationDelay
	healthError := agent._healthy
	terTime := agent._tabletExternallyReparentedTime
	agent.mutex.Unlock()

	// send it to our observers
	// FIXME(alainjobart,liguo) add CpuUsage
	stats := &querypb.RealtimeStats{
		SecondsBehindMaster: uint32(replicationDelay.Seconds()),
	}
	if agent.BinlogPlayerMap != nil {
		stats.SecondsBehindMasterFilteredReplication, stats.BinlogPlayersCount = agent.BinlogPlayerMap.StatusSummary()
	}
	if qss := agent.QueryServiceControl.QueryServiceStats(); qss != nil {
		stats.Qps = qss.QPSRates.TotalRate()
	}
	if healthError != nil {
		stats.HealthError = healthError.Error()
	}
	var ts int64
	if !terTime.IsZero() {
		ts = terTime.Unix()
	}
	go agent.QueryServiceControl.BroadcastHealth(ts, stats)
}

// refreshTablet needs to be run after an action may have changed the current
// state of the tablet.
func (agent *ActionAgent) refreshTablet(ctx context.Context, reason string) error {
	log.Infof("Executing post-action state refresh")

	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("ActionAgent.refreshTablet")
	span.Annotate("reason", reason)
	defer span.Finish()
	ctx = trace.NewContext(ctx, span)

	// Save the old tablet so callbacks can have a better idea of
	// the precise nature of the transition.
	oldTablet := agent.Tablet()

	// Actions should have side effects on the tablet, so reload the data.
	tablet, err := agent.updateTabletFromTopo(ctx)
	if err != nil {
		log.Warningf("Failed rereading tablet after %v - services may be inconsistent: %v", reason, err)
		return fmt.Errorf("Failed rereading tablet after %v: %v", reason, err)
	}

	if updatedTablet := agent.checkTabletMysqlPort(ctx, tablet); updatedTablet != nil {
		agent.setTablet(updatedTablet)
	}

	if err := agent.updateState(ctx, oldTablet, reason); err != nil {
		return err
	}
	log.Infof("Done with post-action state refresh")
	return nil
}

// updateState will use the provided tablet record as a base, the current
// tablet record as the new one, run changeCallback, and dispatch the event.
func (agent *ActionAgent) updateState(ctx context.Context, oldTablet *topodatapb.Tablet, reason string) error {
	newTablet := agent.Tablet()
	log.Infof("Running tablet callback because: %v", reason)
	if err := agent.changeCallback(ctx, oldTablet, newTablet); err != nil {
		return err
	}

	event.Dispatch(&events.StateChange{
		OldTablet: *oldTablet,
		NewTablet: *newTablet,
		Reason:    reason,
	})
	return nil
}

// changeCallback is run after every action that might
// have changed something in the tablet record or in the topology.
//
// It owns making changes to the BinlogPlayerMap. The input for this is the
// tablet type (has to be master), and the shard's SourceShards.
//
// It owns updating the blacklisted tables.
//
// It owns updating the stats record for 'TabletType'.
//
// It owns starting and stopping the update stream service.
//
// It owns reading the TabletControl for the current tablet, and storing it.
func (agent *ActionAgent) changeCallback(ctx context.Context, oldTablet, newTablet *topodatapb.Tablet) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("ActionAgent.changeCallback")
	defer span.Finish()

	allowQuery := topo.IsRunningQueryService(newTablet.Type)
	broadcastHealth := false

	// Read the shard to get SourceShards / TabletControlMap if
	// we're going to use it.
	var shardInfo *topo.ShardInfo
	var tabletControl *topodatapb.Shard_TabletControl
	var err error
	var disallowQueryReason string
	var blacklistedTables []string
	updateBlacklistedTables := true
	if allowQuery {
		shardInfo, err = agent.TopoServer.GetShard(ctx, newTablet.Keyspace, newTablet.Shard)
		if err != nil {
			log.Errorf("Cannot read shard for this tablet %v, might have inaccurate SourceShards and TabletControls: %v", newTablet.Alias, err)
			updateBlacklistedTables = false
		} else {
			if newTablet.Type == topodatapb.TabletType_MASTER {
				if len(shardInfo.SourceShards) > 0 {
					allowQuery = false
					disallowQueryReason = "master tablet with filtered replication on"
				}
			}
			if tc := shardInfo.GetTabletControl(newTablet.Type); tc != nil {
				if topo.InCellList(newTablet.Alias.Cell, tc.Cells) {
					if tc.DisableQueryService {
						allowQuery = false
						disallowQueryReason = "query service disabled by tablet control"
					}
					blacklistedTables = tc.BlacklistedTables
					tabletControl = tc
				}
			}
		}
	} else {
		disallowQueryReason = fmt.Sprintf("not a serving tablet type(%v)", newTablet.Type)
	}
	if updateBlacklistedTables {
		if err := agent.loadBlacklistRules(newTablet, blacklistedTables); err != nil {
			// FIXME(alainjobart) how to handle this error?
			log.Errorf("Cannot update blacklisted tables rule: %v", err)
		}
	}

	if allowQuery {
		// Query service should be running.
		if oldTablet.Type == topodatapb.TabletType_REPLICA &&
			newTablet.Type == topodatapb.TabletType_MASTER {
			// When promoting from replica to master, allow both master and replica
			// queries to be served during gracePeriod.
			if _, err := agent.QueryServiceControl.SetServingType(newTablet.Type,
				true, []topodatapb.TabletType{oldTablet.Type}); err == nil {
				// If successful, broadcast to vtgate and then wait.
				agent.broadcastHealth()
				time.Sleep(*gracePeriod)
			} else {
				log.Errorf("Can't start query service for MASTER+REPLICA mode: %v", err)
			}
		}

		if stateChanged, err := agent.allowQueries(newTablet.Type); err == nil {
			// If the state changed, broadcast to vtgate.
			// (e.g. this happens when the tablet was already master, but it just
			// changed from NOT_SERVING to SERVING due to
			// "vtctl MigrateServedFrom ... master".)
			if stateChanged {
				broadcastHealth = true
			}
		} else {
			log.Errorf("Cannot start query service: %v", err)
		}
	} else {
		// Query service should be stopped.
		if (oldTablet.Type == topodatapb.TabletType_REPLICA ||
			oldTablet.Type == topodatapb.TabletType_RDONLY) &&
			newTablet.Type == topodatapb.TabletType_SPARE {
			// When a non-MASTER serving type is going SPARE,
			// put query service in lameduck during gracePeriod.
			agent.enterLameduck(disallowQueryReason)
			agent.broadcastHealth()
			time.Sleep(*gracePeriod)
		}

		if stateChanged, err := agent.disallowQueries(newTablet.Type, disallowQueryReason); err == nil {
			// If the state changed, broadcast to vtgate.
			// (e.g. this happens when the tablet was already master, but it just
			// changed from NOT_SERVING to SERVING because filtered replication was
			// enabled.
			if stateChanged {
				broadcastHealth = true
			}
		} else {
			log.Errorf("disallowQueries failed: %v", err)
		}
	}

	// save the tabletControl we've been using, so the background
	// healthcheck makes the same decisions as we've been making.
	agent.setTabletControl(tabletControl)

	// update stream needs to be started or stopped too
	if topo.IsRunningUpdateStream(newTablet.Type) {
		agent.UpdateStream.Enable()
	} else {
		agent.UpdateStream.Disable()
	}

	// upate the stats to our current type
	if agent.exportStats {
		agent.statsTabletType.Set(strings.ToLower(newTablet.Type.String()))
	}

	// See if we need to start or stop any binlog player
	if agent.BinlogPlayerMap != nil {
		if newTablet.Type == topodatapb.TabletType_MASTER {
			agent.BinlogPlayerMap.RefreshMap(agent.batchCtx, newTablet, shardInfo)
		} else {
			agent.BinlogPlayerMap.StopAllPlayersAndReset()
		}
	}

	// Broadcast health changes to vtgate immediately.
	if broadcastHealth {
		agent.broadcastHealth()
	}
	return nil
}
