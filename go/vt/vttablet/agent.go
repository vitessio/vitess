// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vttablet contains the meat of the vttablet binary.
package vttablet

// This file handles the agent initialization.

import (
	"encoding/json"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/sqlparser"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	ts "github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	agent           *tm.ActionAgent
	binlogServer    *mysqlctl.BinlogServer
	binlogPlayerMap *BinlogPlayerMap
)

func loadSchemaOverrides(overridesFile string) []ts.SchemaOverride {
	var schemaOverrides []ts.SchemaOverride
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
	dbcfgs dbconfigs.DBConfigs,
	mycnf *mysqlctl.Mycnf,
	dbCredentialsFile string,
	port, securePort int,
	mycnfFile, overridesFile string) (err error) {
	schemaOverrides := loadSchemaOverrides(overridesFile)

	topoServer := topo.GetServer()
	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)

	// Start the binlog server service, disabled at start.
	binlogServer = mysqlctl.NewBinlogServer(mysqld)
	mysqlctl.RegisterBinlogServerService(binlogServer)

	// Start the binlog player services, not playing at start.
	binlogPlayerMap = NewBinlogPlayerMap(topoServer, dbcfgs.App.MysqlParams(), mysqld)
	RegisterBinlogPlayerMap(binlogPlayerMap)

	statsType := stats.NewString("TabletType")
	statsKeyspace := stats.NewString("TabletKeyspace")
	statsShard := stats.NewString("TabletShard")
	statsKeyRangeStart := stats.NewString("TabletKeyRangeStart")
	statsKeyRangeEnd := stats.NewString("TabletKeyRangeEnd")

	// Action agent listens to changes in zookeeper and makes
	// modifications to this tablet.
	agent, err = tm.NewActionAgent(topoServer, tabletAlias, mycnfFile, dbCredentialsFile)
	if err != nil {
		return err
	}
	agent.AddChangeCallback(func(oldTablet, newTablet topo.Tablet) {
		allowQuery := true
		var shardInfo *topo.ShardInfo
		if newTablet.Type == topo.TYPE_MASTER {
			// read the shard to get SourceShards
			shardInfo, err = topoServer.GetShard(newTablet.Keyspace, newTablet.Shard)
			if err != nil {
				log.Errorf("Cannot read shard for this tablet %v: %v", newTablet.GetAlias(), err)
			} else {
				allowQuery = len(shardInfo.SourceShards) == 0
			}
		}

		if newTablet.IsServingType() && allowQuery {
			if dbcfgs.App.DbName == "" {
				dbcfgs.App.DbName = newTablet.DbName()
			}
			dbcfgs.App.Keyspace = newTablet.Keyspace
			dbcfgs.App.Shard = newTablet.Shard
			// Transitioning from replica to master, first disconnect
			// existing connections. "false" indicateds that clients must
			// re-resolve their endpoint before reconnecting.
			if newTablet.Type == topo.TYPE_MASTER && oldTablet.Type != topo.TYPE_MASTER {
				ts.DisallowQueries()
			}
			qrs := ts.LoadCustomRules()
			if newTablet.KeyRange.IsPartial() {
				qr := ts.NewQueryRule("enforce keyspace_id range", "keyspace_id_not_in_range", ts.QR_FAIL_QUERY)
				qr.AddPlanCond(sqlparser.PLAN_INSERT_PK)
				err = qr.AddBindVarCond("keyspace_id", true, true, ts.QR_NOTIN, newTablet.KeyRange)
				if err != nil {
					log.Warningf("Unable to add keyspace rule: %v", err)
				} else {
					qrs.Add(qr)
				}
			}
			ts.AllowQueries(dbcfgs.App, schemaOverrides, qrs)
			mysqlctl.EnableUpdateStreamService(dbcfgs)
			if newTablet.Type != topo.TYPE_MASTER {
				ts.StartRowCacheInvalidation()
			}
		} else {
			ts.DisallowQueries()
			ts.StopRowCacheInvalidation()
			mysqlctl.DisableUpdateStreamService()
		}

		statsType.Set(string(newTablet.Type))
		statsKeyspace.Set(newTablet.Keyspace)
		statsShard.Set(newTablet.Shard)
		statsKeyRangeStart.Set(string(newTablet.KeyRange.Start.Hex()))
		statsKeyRangeEnd.Set(string(newTablet.KeyRange.End.Hex()))

		// BinlogServer is only enabled for replicas
		if newTablet.Type == topo.TYPE_REPLICA {
			if !mysqlctl.IsBinlogServerEnabled(binlogServer) {
				mysqlctl.EnableBinlogServerService(binlogServer, dbcfgs.App.DbName)
			}
		} else {
			if mysqlctl.IsBinlogServerEnabled(binlogServer) {
				mysqlctl.DisableBinlogServerService(binlogServer)
			}
		}

		// See if we need to start or stop any binlog player
		if newTablet.Type == topo.TYPE_MASTER {
			binlogPlayerMap.RefreshMap(newTablet, shardInfo)
		} else {
			binlogPlayerMap.StopAllPlayers()
		}
	})

	if err := agent.Start(mysqld.Port(), port, securePort); err != nil {
		return err
	}

	// register the RPC services from the agent
	agent.RegisterQueryService(mysqld)

	return nil
}

func CloseAgent() {
	if agent != nil {
		agent.Stop()
	}
	if binlogServer != nil {
		mysqlctl.DisableBinlogServerService(binlogServer)
	}
	if binlogPlayerMap != nil {
		binlogPlayerMap.StopAllPlayers()
	}
}
