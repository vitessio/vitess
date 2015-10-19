// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the agent state changes.

import (
	"encoding/hex"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// the stats exported by this module
	statsType          = stats.NewString("TabletType")
	statsKeyspace      = stats.NewString("TabletKeyspace")
	statsShard         = stats.NewString("TabletShard")
	statsKeyRangeStart = stats.NewString("TabletKeyRangeStart")
	statsKeyRangeEnd   = stats.NewString("TabletKeyRangeEnd")

	// constants for this module
	historyLength = 16
)

// Query rules from blacklist
const blacklistQueryRules string = "BlacklistQueryRules"

func (agent *ActionAgent) allowQueries(tablet *pbt.Tablet, blacklistedTables []string) error {
	err := agent.loadBlacklistRules(tablet, blacklistedTables)
	if err != nil {
		return err
	}

	return agent.QueryServiceControl.SetServingType(tablet.Type, true)
}

// loadBlacklistRules loads and builds the blacklist query rules
func (agent *ActionAgent) loadBlacklistRules(tablet *pbt.Tablet, blacklistedTables []string) (err error) {
	blacklistRules := tabletserver.NewQueryRules()
	if len(blacklistedTables) > 0 {
		// tables, first resolve wildcards
		tables, err := mysqlctl.ResolveTables(agent.MysqlDaemon, topoproto.TabletDbName(tablet), blacklistedTables)
		if err != nil {
			return err
		}
		log.Infof("Blacklisting tables %v", strings.Join(tables, ", "))
		qr := tabletserver.NewQueryRule("enforce blacklisted tables", "blacklisted_table", tabletserver.QR_FAIL_RETRY)
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

func (agent *ActionAgent) disallowQueries(tablet *pbt.Tablet, reason string) error {
	log.Infof("Agent is going to disallow queries, reason: %v", reason)

	return agent.QueryServiceControl.SetServingType(tablet.Type, false)
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
	stats := &pb.RealtimeStats{
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

// changeCallback is run after every action that might
// have changed something in the tablet record or in the topology.
func (agent *ActionAgent) changeCallback(ctx context.Context, oldTablet, newTablet *pbt.Tablet) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartLocal("ActionAgent.changeCallback")
	defer span.Finish()

	allowQuery := topo.IsRunningQueryService(newTablet.Type)

	// Read the shard to get SourceShards / TabletControlMap if
	// we're going to use it.
	var shardInfo *topo.ShardInfo
	var tabletControl *pbt.Shard_TabletControl
	var blacklistedTables []string
	var err error
	var disallowQueryReason string
	if allowQuery {
		shardInfo, err = agent.TopoServer.GetShard(ctx, newTablet.Keyspace, newTablet.Shard)
		if err != nil {
			log.Errorf("Cannot read shard for this tablet %v, might have inaccurate SourceShards and TabletControls: %v", newTablet.Alias, err)
		} else {
			if newTablet.Type == pbt.TabletType_MASTER {
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

	if allowQuery {
		if err := agent.allowQueries(newTablet, blacklistedTables); err != nil {
			log.Errorf("Cannot start query service: %v", err)
		}
	} else {
		agent.disallowQueries(newTablet, disallowQueryReason)
	}

	// save the tabletControl we've been using, so the background
	// healthcheck makes the same decisions as we've been making.
	agent.setTabletControl(tabletControl)

	// update stream needs to be started or stopped too
	if agent.UpdateStream != nil {
		if topo.IsRunningUpdateStream(newTablet.Type) {
			agent.UpdateStream.Enable(agent.DBConfigs.App.DbName, agent.MysqlDaemon)
		} else {
			agent.UpdateStream.Disable()
		}
	}

	statsType.Set(strings.ToLower(newTablet.Type.String()))
	statsKeyspace.Set(newTablet.Keyspace)
	statsShard.Set(newTablet.Shard)
	if newTablet.KeyRange != nil {
		statsKeyRangeStart.Set(hex.EncodeToString(newTablet.KeyRange.Start))
		statsKeyRangeEnd.Set(hex.EncodeToString(newTablet.KeyRange.End))
	} else {
		statsKeyRangeStart.Set("")
		statsKeyRangeEnd.Set("")
	}

	// See if we need to start or stop any binlog player
	if agent.BinlogPlayerMap != nil {
		if newTablet.Type == pbt.TabletType_MASTER {
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
