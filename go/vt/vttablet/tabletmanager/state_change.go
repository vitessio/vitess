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
	"github.com/youtube/vitess/go/vt/vttablet/tabletmanager/events"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
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
	blacklistRules := rules.New()
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
			qr := rules.NewQueryRule("enforce blacklisted tables", "blacklisted_table", rules.QRFailRetry)
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

// lameduck changes the QueryServiceControl state to lameduck,
// brodcasts the new health, then sleep for grace period, to give time
// to clients to get the new status.
func (agent *ActionAgent) lameduck(reason string) {
	log.Infof("Agent is entering lameduck, reason: %v", reason)
	agent.QueryServiceControl.EnterLameduck()
	agent.broadcastHealth()
	time.Sleep(*gracePeriod)
	log.Infof("Agent is leaving lameduck")
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
	stats.Qps = tabletenv.QPSRates.TotalRate()
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
	log.Infof("Executing post-action state refresh: %v", reason)

	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("ActionAgent.refreshTablet")
	span.Annotate("reason", reason)
	defer span.Finish()
	ctx = trace.NewContext(ctx, span)

	// Actions should have side effects on the tablet, so reload the data.
	ti, err := agent.TopoServer.GetTablet(ctx, agent.TabletAlias)
	if err != nil {
		log.Warningf("Failed rereading tablet after %v - services may be inconsistent: %v", reason, err)
		return fmt.Errorf("refreshTablet failed rereading tablet after %v: %v", reason, err)
	}
	tablet := ti.Tablet

	if updatedTablet := agent.checkTabletMysqlPort(ctx, tablet); updatedTablet != nil {
		tablet = updatedTablet
	}

	agent.updateState(ctx, tablet, reason)
	log.Infof("Done with post-action state refresh")
	return nil
}

// updateState will use the provided tablet record as the new tablet state,
// the current tablet as a base, run changeCallback, and dispatch the event.
func (agent *ActionAgent) updateState(ctx context.Context, newTablet *topodatapb.Tablet, reason string) {
	oldTablet := agent.Tablet()
	if oldTablet == nil {
		oldTablet = &topodatapb.Tablet{}
	}
	log.Infof("Running tablet callback because: %v", reason)
	agent.changeCallback(ctx, oldTablet, newTablet)
	agent.setTablet(newTablet)
	event.Dispatch(&events.StateChange{
		OldTablet: *oldTablet,
		NewTablet: *newTablet,
		Reason:    reason,
	})
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
func (agent *ActionAgent) changeCallback(ctx context.Context, oldTablet, newTablet *topodatapb.Tablet) {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("ActionAgent.changeCallback")
	defer span.Finish()

	allowQuery := topo.IsRunningQueryService(newTablet.Type)
	broadcastHealth := false
	runUpdateStream := allowQuery

	// Read the shard to get SourceShards / TabletControlMap if
	// we're going to use it.
	var shardInfo *topo.ShardInfo
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
						disallowQueryReason = "TabletControl.DisableQueryService set"
					}
					blacklistedTables = tc.BlacklistedTables
				}
			}
		}
	} else {
		disallowQueryReason = fmt.Sprintf("not a serving tablet type(%v)", newTablet.Type)
	}
	agent.setServicesDesiredState(disallowQueryReason, runUpdateStream)
	if updateBlacklistedTables {
		if err := agent.loadBlacklistRules(newTablet, blacklistedTables); err != nil {
			// FIXME(alainjobart) how to handle this error?
			log.Errorf("Cannot update blacklisted tables rule: %v", err)
		} else {
			agent.setBlacklistedTables(blacklistedTables)
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

		if stateChanged, err := agent.QueryServiceControl.SetServingType(newTablet.Type, true, nil); err == nil {
			// If the state changed, broadcast to vtgate.
			// (e.g. this happens when the tablet was already master, but it just
			// changed from NOT_SERVING to SERVING due to
			// "vtctl MigrateServedFrom ... master".)
			if stateChanged {
				broadcastHealth = true
			}
		} else {
			runUpdateStream = false
			log.Errorf("Cannot start query service: %v", err)
		}
	} else {
		// Query service should be stopped.
		if topo.IsSubjectToLameduck(oldTablet.Type) &&
			newTablet.Type == topodatapb.TabletType_SPARE &&
			*gracePeriod > 0 {
			// When a non-MASTER serving type is going SPARE,
			// put query service in lameduck during gracePeriod.
			agent.lameduck(disallowQueryReason)
		}

		log.Infof("Disabling query service on type change, reason: %v", disallowQueryReason)
		if stateChanged, err := agent.QueryServiceControl.SetServingType(newTablet.Type, false, nil); err == nil {
			// If the state changed, broadcast to vtgate.
			// (e.g. this happens when the tablet was already master, but it just
			// changed from SERVING to NOT_SERVING because filtered replication was
			// enabled.)
			if stateChanged {
				broadcastHealth = true
			}
		} else {
			log.Errorf("SetServingType(serving=false) failed: %v", err)
		}
	}

	// UpdateStream needs to be started or stopped too.
	if topo.IsRunningUpdateStream(newTablet.Type) && runUpdateStream {
		agent.UpdateStream.Enable()
	} else {
		agent.UpdateStream.Disable()
	}

	// Update the stats to our current type.
	if agent.exportStats {
		agent.statsTabletType.Set(topoproto.TabletTypeLString(newTablet.Type))
	}

	// See if we need to start or stop any binlog player.
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
}
