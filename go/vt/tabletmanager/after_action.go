// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the agent state changes.

import (
	"fmt"
	"reflect"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/topo"
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

func (agent *ActionAgent) allowQueries(tablet *topo.Tablet, blacklistedTables []string) error {
	if agent.DBConfigs == nil {
		// test instance, do nothing
		return nil
	}

	// if the query service is already running, we're not starting it again
	if tabletserver.SqlQueryRpcService.GetState() == "SERVING" {
		return nil
	}

	// Update our DB config to match the info we have in the tablet
	if agent.DBConfigs.App.DbName == "" {
		agent.DBConfigs.App.DbName = tablet.DbName()
	}
	agent.DBConfigs.App.Keyspace = tablet.Keyspace
	agent.DBConfigs.App.Shard = tablet.Shard
	if tablet.Type != topo.TYPE_MASTER {
		agent.DBConfigs.App.EnableInvalidator = true
	} else {
		agent.DBConfigs.App.EnableInvalidator = false
	}

	qrs, err := agent.createQueryRules(tablet, blacklistedTables)
	if err != nil {
		return err
	}

	return tabletserver.AllowQueries(&agent.DBConfigs.App, agent.SchemaOverrides, qrs, agent.Mysqld, false)
}

// createQueryRules computes the query rules that match the tablet record
func (agent *ActionAgent) createQueryRules(tablet *topo.Tablet, blacklistedTables []string) (qrs *tabletserver.QueryRules, err error) {
	qrs = tabletserver.LoadCustomRules()

	// Keyrange rules
	if tablet.KeyRange.IsPartial() {
		log.Infof("Restricting to keyrange: %v", tablet.KeyRange)
		dml_plans := []struct {
			planID   planbuilder.PlanType
			onAbsent bool
		}{
			{planbuilder.PLAN_INSERT_PK, true},
			{planbuilder.PLAN_INSERT_SUBQUERY, true},
			{planbuilder.PLAN_PASS_DML, false},
			{planbuilder.PLAN_DML_PK, false},
			{planbuilder.PLAN_DML_SUBQUERY, false},
		}
		for _, plan := range dml_plans {
			qr := tabletserver.NewQueryRule(
				fmt.Sprintf("enforce keyspace_id range for %v", plan.planID),
				fmt.Sprintf("keyspace_id_not_in_range_%v", plan.planID),
				tabletserver.QR_FAIL,
			)
			qr.AddPlanCond(plan.planID)
			err := qr.AddBindVarCond("keyspace_id", plan.onAbsent, true, tabletserver.QR_NOTIN, tablet.KeyRange)
			if err != nil {
				return nil, fmt.Errorf("Unable to add keyspace rule: %v", err)
			}
			qrs.Add(qr)
		}
	}

	// Blacklisted tables
	if len(blacklistedTables) > 0 {
		// tables, first resolve wildcards
		tables, err := agent.Mysqld.ResolveTables(tablet.DbName(), blacklistedTables)
		if err != nil {
			return nil, err
		}
		log.Infof("Blacklisting tables %v", strings.Join(tables, ", "))
		qr := tabletserver.NewQueryRule("enforce blacklisted tables", "blacklisted_table", tabletserver.QR_FAIL_RETRY)
		for _, t := range tables {
			qr.AddTableCond(t)
		}
		qrs.Add(qr)
	}
	return qrs, nil
}

func (agent *ActionAgent) disallowQueries() {
	if agent.DBConfigs == nil {
		// test instance, do nothing
		return
	}
	tabletserver.DisallowQueries()
}

// changeCallback is run after every action that might
// have changed something in the tablet record.
func (agent *ActionAgent) changeCallback(oldTablet, newTablet *topo.Tablet) error {
	allowQuery := newTablet.IsRunningQueryService()

	// Read the shard to get SourceShards / TabletControlMap if
	// we're going to use it.
	var shardInfo *topo.ShardInfo
	var blacklistedTables []string
	var err error
	if allowQuery {
		shardInfo, err = agent.TopoServer.GetShard(newTablet.Keyspace, newTablet.Shard)
		if err != nil {
			log.Errorf("Cannot read shard for this tablet %v, might have inaccurate SourceShards and TabletControls: %v", newTablet.Alias, err)
		} else {
			if newTablet.Type == topo.TYPE_MASTER {
				allowQuery = len(shardInfo.SourceShards) == 0
			}
			if tc, ok := shardInfo.TabletControlMap[newTablet.Type]; ok {
				if topo.InCellList(newTablet.Alias.Cell, tc.Cells) {
					if tc.DisableQueryService {
						allowQuery = false
					}
					blacklistedTables = tc.BlacklistedTables
				}
			}
		}
	}

	// Read the keyspace on masters to get ShardingColumnType,
	// for binlog replication, only if source shards are set.
	var keyspaceInfo *topo.KeyspaceInfo
	if newTablet.Type == topo.TYPE_MASTER && shardInfo != nil && len(shardInfo.SourceShards) > 0 {
		keyspaceInfo, err = agent.TopoServer.GetKeyspace(newTablet.Keyspace)
		if err != nil {
			log.Errorf("Cannot read keyspace for this tablet %v: %v", newTablet.Alias, err)
			keyspaceInfo = nil
		}
	}

	if allowQuery {
		// There are a few transitions when we're
		// going to need to restart the query service:
		// - transitioning from replica to master, so clients
		//   that were already connected don't keep on using
		//   the master as replica or rdonly.
		// - having different parameters for the query
		//   service. It needs to stop and restart with the
		//   new parameters. That includes:
		//   - changing KeyRange
		//   - changing the BlacklistedTables list
		if (newTablet.Type == topo.TYPE_MASTER &&
			oldTablet.Type != topo.TYPE_MASTER) ||
			(newTablet.KeyRange != oldTablet.KeyRange) ||
			!reflect.DeepEqual(blacklistedTables, agent.BlacklistedTables()) {
			agent.disallowQueries()
		}
		if err := agent.allowQueries(newTablet, blacklistedTables); err != nil {
			log.Errorf("Cannot start query service: %v", err)
		} else {
			// allowQueries worked, save our blacklisted table list
			agent.setBlacklistedTables(blacklistedTables)
		}
	} else {
		agent.disallowQueries()
	}

	if agent.DBConfigs != nil {
		if topo.IsRunningUpdateStream(newTablet.Type) {
			binlog.EnableUpdateStreamService(agent.DBConfigs.App.DbName, agent.Mysqld)
		} else {
			binlog.DisableUpdateStreamService()
		}
	}

	statsType.Set(string(newTablet.Type))
	statsKeyspace.Set(newTablet.Keyspace)
	statsShard.Set(newTablet.Shard)
	statsKeyRangeStart.Set(string(newTablet.KeyRange.Start.Hex()))
	statsKeyRangeEnd.Set(string(newTablet.KeyRange.End.Hex()))

	// See if we need to start or stop any binlog player
	if agent.BinlogPlayerMap != nil {
		if newTablet.Type == topo.TYPE_MASTER {
			agent.BinlogPlayerMap.RefreshMap(newTablet, keyspaceInfo, shardInfo)
		} else {
			agent.BinlogPlayerMap.StopAllPlayersAndReset()
		}
	}
	return nil
}
