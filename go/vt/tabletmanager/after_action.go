// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

// This file handles the agent initialization.

import (
	"encoding/json"
	"reflect"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver"
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

func loadSchemaOverrides(overridesFile string) []tabletserver.SchemaOverride {
	var schemaOverrides []tabletserver.SchemaOverride
	if overridesFile == "" {
		return schemaOverrides
	}
	if err := jscfg.ReadJson(overridesFile, &schemaOverrides); err != nil {
		log.Warningf("can't read overridesFile %v: %v", overridesFile, err)
	} else {
		data, _ := json.MarshalIndent(schemaOverrides, "", "  ")
		log.Infof("schemaOverrides: %s\n", data)
	}
	return schemaOverrides
}

// InitAgent initializes the agent within vttablet.
func InitAgent(
	tabletAlias topo.TabletAlias,
	dbcfgs *dbconfigs.DBConfigs,
	mycnf *mysqlctl.Mycnf,
	port, securePort int,
	overridesFile string,
) (agent *ActionAgent, err error) {
	schemaOverrides := loadSchemaOverrides(overridesFile)

	topoServer := topo.GetServer()
	mysqld := mysqlctl.NewMysqld(mycnf, &dbcfgs.Dba, &dbcfgs.Repl)

	agent = &ActionAgent{
		TopoServer:      topoServer,
		TabletAlias:     tabletAlias,
		Mysqld:          mysqld,
		DBConfigs:       dbcfgs,
		SchemaOverrides: schemaOverrides,
		done:            make(chan struct{}),
		History:         history.New(historyLength),
		changeItems:     make(chan tabletChangeItem, 100),
	}

	// Start the binlog player services, not playing at start.
	agent.BinlogPlayerMap = NewBinlogPlayerMap(topoServer, &dbcfgs.App.ConnectionParams, mysqld)
	RegisterBinlogPlayerMap(agent.BinlogPlayerMap)

	if err := agent.Start(mysqld.Port(), port, securePort); err != nil {
		return nil, err
	}

	// register the RPC services from the agent
	agent.RegisterQueryService()

	// start health check if needed
	agent.initHeathCheck()

	return agent, nil
}

func (agent *ActionAgent) allowQueries(tablet *topo.Tablet) error {
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

	// Compute the query rules that match the tablet record
	qrs := tabletserver.LoadCustomRules()
	if tablet.KeyRange.IsPartial() {
		qr := tabletserver.NewQueryRule("enforce keyspace_id range", "keyspace_id_not_in_range", tabletserver.QR_FAIL_QUERY)
		qr.AddPlanCond(sqlparser.PLAN_INSERT_PK)
		err := qr.AddBindVarCond("keyspace_id", true, true, tabletserver.QR_NOTIN, tablet.KeyRange)
		if err != nil {
			log.Warningf("Unable to add keyspace rule: %v", err)
		} else {
			qrs.Add(qr)
		}
	}
	if len(tablet.BlacklistedTables) > 0 {
		log.Infof("Blacklisting tables %v", strings.Join(tablet.BlacklistedTables, ", "))
		qr := tabletserver.NewQueryRule("enforce blacklisted tables", "blacklisted_table", tabletserver.QR_FAIL_QUERY)
		for _, t := range tablet.BlacklistedTables {
			qr.AddTableCond(t)
		}
		qrs.Add(qr)
	}

	return tabletserver.AllowQueries(&agent.DBConfigs.App, agent.SchemaOverrides, qrs, agent.Mysqld, false)
}

func (agent *ActionAgent) disallowQueries() {
	tabletserver.DisallowQueries()
}

// changeCallback is run after every action that might
// have changed something in the tablet record.
func (agent *ActionAgent) changeCallback(oldTablet, newTablet topo.Tablet) {

	allowQuery := true
	var shardInfo *topo.ShardInfo
	var keyspaceInfo *topo.KeyspaceInfo
	if newTablet.Type == topo.TYPE_MASTER {
		// read the shard to get SourceShards
		var err error
		shardInfo, err = agent.TopoServer.GetShard(newTablet.Keyspace, newTablet.Shard)
		if err != nil {
			log.Errorf("Cannot read shard for this tablet %v: %v", newTablet.Alias, err)
		} else {
			allowQuery = len(shardInfo.SourceShards) == 0
		}

		// read the keyspace to get ShardingColumnType
		keyspaceInfo, err = agent.TopoServer.GetKeyspace(newTablet.Keyspace)
		switch err {
		case nil:
			// continue
		case topo.ErrNoNode:
			// backward compatible mode
			keyspaceInfo = topo.NewKeyspaceInfo(newTablet.Keyspace, &topo.Keyspace{})
		default:
			log.Errorf("Cannot read keyspace for this tablet %v: %v", newTablet.Alias, err)
			keyspaceInfo = nil
		}
	}

	if newTablet.IsRunningQueryService() && allowQuery {
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
			!reflect.DeepEqual(newTablet.BlacklistedTables, oldTablet.BlacklistedTables) {
			agent.disallowQueries()
		}
		if err := agent.allowQueries(&newTablet); err != nil {
			log.Errorf("Cannot start query service: %v", err)
		}

		// Disable before enabling to force existing streams to stop.
		binlog.DisableUpdateStreamService()
		binlog.EnableUpdateStreamService(agent.DBConfigs)
	} else {
		agent.disallowQueries()
		binlog.DisableUpdateStreamService()
	}

	statsType.Set(string(newTablet.Type))
	statsKeyspace.Set(newTablet.Keyspace)
	statsShard.Set(newTablet.Shard)
	statsKeyRangeStart.Set(string(newTablet.KeyRange.Start.Hex()))
	statsKeyRangeEnd.Set(string(newTablet.KeyRange.End.Hex()))

	// See if we need to start or stop any binlog player
	if newTablet.Type == topo.TYPE_MASTER {
		agent.BinlogPlayerMap.RefreshMap(newTablet, keyspaceInfo, shardInfo)
	} else {
		agent.BinlogPlayerMap.StopAllPlayersAndReset()
	}
}
