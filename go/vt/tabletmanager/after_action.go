// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the agent state changes.

import (
	"fmt"
	"strings"

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
		log.Infof("Blacklisting tables %v", strings.Join(tables, ", "))
		qr := tabletserver.NewQueryRule("enforce blacklisted tables", "blacklisted_table", tabletserver.QRFailRetry)
		for _, t := range tables {
			qr.AddTableCond(t)
		}
		blacklistRules.Add(qr)
	}

	loadRuleErr := agent.QueryServiceControl.SetQueryRules(blacklistQueryRules, blacklistRules)
	if loadRuleErr != nil {
		log.Warningf("Fail to load query rule set %s: %s", blacklistQueryRules, loadRuleErr)
	}
	return nil
}

func (agent *ActionAgent) allowQueries(tabletType topodatapb.TabletType) error {
	return agent.QueryServiceControl.SetServingType(tabletType, true, nil)
}

func (agent *ActionAgent) disallowQueries(tabletType topodatapb.TabletType, reason string) error {
	log.Infof("Agent is going to disallow queries, reason: %v", reason)

	return agent.QueryServiceControl.SetServingType(tabletType, false, nil)
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
	if healthError != nil {
		stats.HealthError = healthError.Error()
	}
	var ts int64
	if !terTime.IsZero() {
		ts = terTime.Unix()
	}
	defer func() {
		agent.QueryServiceControl.BroadcastHealth(ts, stats)
	}()
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
	oldTablet := agent.Tablet().Tablet

	// Actions should have side effects on the tablet, so reload the data.
	ti, err := agent.updateTabletFromTopo(ctx)
	if err != nil {
		log.Warningf("Failed rereading tablet after %v - services may be inconsistent: %v", reason, err)
		return fmt.Errorf("Failed rereading tablet after %v: %v", reason, err)
	}

	if updatedTablet := agent.checkTabletMysqlPort(ctx, ti); updatedTablet != nil {
		agent.mutex.Lock()
		agent._tablet = updatedTablet
		agent.mutex.Unlock()
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
	agent.mutex.Lock()
	newTablet := agent._tablet.Tablet
	agent.mutex.Unlock()
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
		if err := agent.allowQueries(newTablet.Type); err != nil {
			log.Errorf("Cannot start query service: %v", err)
		}
	} else {
		agent.disallowQueries(newTablet.Type, disallowQueryReason)
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
			// Read the keyspace on masters to get
			// ShardingColumnType, for binlog replication,
			// only if source shards are set.
			var keyspaceInfo *topo.KeyspaceInfo
			if shardInfo != nil && len(shardInfo.SourceShards) > 0 {
				keyspaceInfo, err = agent.TopoServer.GetKeyspace(ctx, newTablet.Keyspace)
				if err != nil {
					keyspaceInfo = nil
				}
			}
			agent.BinlogPlayerMap.RefreshMap(agent.batchCtx, newTablet, keyspaceInfo, shardInfo)
		} else {
			agent.BinlogPlayerMap.StopAllPlayersAndReset()
		}
	}
	return nil
}
