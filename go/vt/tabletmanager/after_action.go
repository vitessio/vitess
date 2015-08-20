// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the agent state changes.

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
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

// Query rules from keyrange
const keyrangeQueryRules string = "KeyrangeQueryRules"

// Query rules from blacklist
const blacklistQueryRules string = "BlacklistQueryRules"

func (agent *ActionAgent) allowQueries(tablet *pbt.Tablet, blacklistedTables []string) error {
	// if the query service is already running, we're not starting it again
	if agent.QueryServiceControl.IsServing() {
		return nil
	}

	// only for real instances
	if agent.DBConfigs != nil {
		// Update our DB config to match the info we have in the tablet
		if agent.DBConfigs.App.DbName == "" {
			agent.DBConfigs.App.DbName = topoproto.TabletDbName(tablet)
		}
		agent.DBConfigs.App.Keyspace = tablet.Keyspace
		agent.DBConfigs.App.Shard = tablet.Shard
		if tablet.Type != pbt.TabletType_MASTER {
			agent.DBConfigs.App.EnableInvalidator = true
		} else {
			agent.DBConfigs.App.EnableInvalidator = false
		}
	}

	err := agent.loadKeyspaceAndBlacklistRules(tablet, blacklistedTables)
	if err != nil {
		return err
	}

	return agent.QueryServiceControl.AllowQueries(&pb.Target{
		Keyspace:   tablet.Keyspace,
		Shard:      tablet.Shard,
		TabletType: tablet.Type,
	}, agent.DBConfigs, agent.SchemaOverrides, agent.MysqlDaemon)
}

// loadKeyspaceAndBlacklistRules does what the name suggests:
// 1. load and build keyrange query rules
// 2. load and build blacklist query rules
func (agent *ActionAgent) loadKeyspaceAndBlacklistRules(tablet *pbt.Tablet, blacklistedTables []string) (err error) {
	// Keyrange rules
	keyrangeRules := tabletserver.NewQueryRules()
	if key.KeyRangeIsPartial(tablet.KeyRange) {
		log.Infof("Restricting to keyrange: %v", tablet.KeyRange)
		dmlPlans := []struct {
			planID   planbuilder.PlanType
			onAbsent bool
		}{
			{planbuilder.PLAN_INSERT_PK, true},
			{planbuilder.PLAN_INSERT_SUBQUERY, true},
			{planbuilder.PLAN_PASS_DML, false},
			{planbuilder.PLAN_DML_PK, false},
			{planbuilder.PLAN_DML_SUBQUERY, false},
		}
		for _, plan := range dmlPlans {
			qr := tabletserver.NewQueryRule(
				fmt.Sprintf("enforce keyspace_id range for %v", plan.planID),
				fmt.Sprintf("keyspace_id_not_in_range_%v", plan.planID),
				tabletserver.QR_FAIL,
			)
			qr.AddPlanCond(plan.planID)
			err := qr.AddBindVarCond("keyspace_id", plan.onAbsent, true, tabletserver.QR_NOTIN, tablet.KeyRange)
			if err != nil {
				return fmt.Errorf("Unable to add keyspace rule: %v", err)
			}
			keyrangeRules.Add(qr)
		}
	}

	// Blacklisted tables
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
	// Push all three sets of QueryRules to SqlQueryRpcService
	loadRuleErr := agent.QueryServiceControl.SetQueryRules(keyrangeQueryRules, keyrangeRules)
	if loadRuleErr != nil {
		log.Warningf("Fail to load query rule set %s: %s", keyrangeQueryRules, loadRuleErr)
	}

	loadRuleErr = agent.QueryServiceControl.SetQueryRules(blacklistQueryRules, blacklistRules)
	if loadRuleErr != nil {
		log.Warningf("Fail to load query rule set %s: %s", blacklistQueryRules, loadRuleErr)
	}
	return nil
}

func (agent *ActionAgent) disallowQueries() {
	agent.QueryServiceControl.DisallowQueries()
}

// changeCallback is run after every action that might
// have changed something in the tablet record.
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
	if allowQuery {
		shardInfo, err = agent.TopoServer.GetShard(ctx, newTablet.Keyspace, newTablet.Shard)
		if err != nil {
			log.Errorf("Cannot read shard for this tablet %v, might have inaccurate SourceShards and TabletControls: %v", newTablet.Alias, err)
		} else {
			if newTablet.Type == pbt.TabletType_MASTER {
				allowQuery = len(shardInfo.SourceShards) == 0
			}
			if tc := shardInfo.GetTabletControl(newTablet.Type); tc != nil {
				if topo.InCellList(newTablet.Alias.Cell, tc.Cells) {
					if tc.DisableQueryService {
						allowQuery = false
					}
					blacklistedTables = tc.BlacklistedTables
					tabletControl = tc
				}
			}
		}
	}

	// Read the keyspace on masters to get ShardingColumnType,
	// for binlog replication, only if source shards are set.
	var keyspaceInfo *topo.KeyspaceInfo
	if newTablet.Type == pbt.TabletType_MASTER && shardInfo != nil && len(shardInfo.SourceShards) > 0 {
		keyspaceInfo, err = agent.TopoServer.GetKeyspace(ctx, newTablet.Keyspace)
		if err != nil {
			log.Errorf("Cannot read keyspace for this tablet %v: %v", newTablet.Alias, err)
			keyspaceInfo = nil
		}
	}

	if allowQuery {
		// There are a few transitions when we need to restart the query service:
		switch {
		// If either InitMaster or InitSlave was called, because those calls
		// (or a prior call to ResetReplication) may have silently broken the
		// rowcache invalidator by executing RESET MASTER.
		// Note that we don't care about fixing it after ResetReplication itself
		// since that call breaks everything on purpose, and we don't expect
		// anything to start working until either InitMaster or InitSlave.
		case agent.initReplication:
			agent.initReplication = false
			agent.disallowQueries()

		// Transitioning from replica to master, so clients that were already
		// connected don't keep on using the master as replica or rdonly.
		case newTablet.Type == pbt.TabletType_MASTER && oldTablet.Type != pbt.TabletType_MASTER:
			agent.disallowQueries()

		// Having different parameters for the query service.
		// It needs to stop and restart with the new parameters.
		// That includes:
		//   - changing KeyRange
		//   - changing the BlacklistedTables list
		case (newTablet.KeyRange != oldTablet.KeyRange),
			!reflect.DeepEqual(blacklistedTables, agent.BlacklistedTables()):
			agent.disallowQueries()
		}

		if err := agent.allowQueries(newTablet, blacklistedTables); err != nil {
			log.Errorf("Cannot start query service: %v", err)
		}
	} else {
		agent.disallowQueries()
	}

	// save the tabletControl we've been using, so the background
	// healthcheck makes the same decisions as we've been making.
	agent.setTabletControl(tabletControl)

	// update stream needs to be started or stopped too
	if agent.DBConfigs != nil {
		if topo.IsRunningUpdateStream(newTablet.Type) {
			binlog.EnableUpdateStreamService(agent.DBConfigs.App.DbName, agent.MysqlDaemon)
		} else {
			binlog.DisableUpdateStreamService()
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
			agent.BinlogPlayerMap.RefreshMap(agent.batchCtx, newTablet, keyspaceInfo, shardInfo)
		} else {
			agent.BinlogPlayerMap.StopAllPlayersAndReset()
		}
	}
	return nil
}

func init() {
	// Register query rule sources under control of agent
	tabletserver.QueryRuleSources.RegisterQueryRuleSource(keyrangeQueryRules)
	tabletserver.QueryRuleSources.RegisterQueryRuleSource(blacklistQueryRules)
}
