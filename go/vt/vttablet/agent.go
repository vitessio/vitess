// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
vttablet package contains the meat of the vttablet binary.
*/
package vttablet

// This file handles the agent initialization

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io/ioutil"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/sqlparser"
	tm "github.com/youtube/vitess/go/vt/tabletmanager"
	ts "github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
)

func loadCustomRules(customrules string) *ts.QueryRules {
	if customrules == "" {
		return ts.NewQueryRules()
	}

	data, err := ioutil.ReadFile(customrules)
	if err != nil {
		relog.Fatal("Error reading file %v: %v", customrules, err)
	}

	qrs := ts.NewQueryRules()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		relog.Fatal("Error unmarshaling query rules %v", err)
	}
	return qrs
}

func loadSchemaOverrides(overridesFile string) []ts.SchemaOverride {
	var schemaOverrides []ts.SchemaOverride
	if err := jscfg.ReadJson(overridesFile, &schemaOverrides); err != nil {
		relog.Warning("can't read overridesFile %v: %v", overridesFile, err)
	} else {
		data, _ := json.MarshalIndent(schemaOverrides, "", "  ")
		relog.Info("schemaOverrides: %s\n", data)
	}
	return schemaOverrides
}

// InitAgent initializes the agent within vttablet.
func InitAgent(
	tabletAlias topo.TabletAlias,
	dbcfgs dbconfigs.DBConfigs,
	mycnf *mysqlctl.Mycnf,
	dbConfigsFile, dbCredentialsFile string,
	port, securePort int,
	mycnfFile, customRules string,
	overridesFile string) (agent *tm.ActionAgent, err error) {
	schemaOverrides := loadSchemaOverrides(overridesFile)

	topoServer := topo.GetServer()

	binlogServer := mysqlctl.NewBinlogServer(mycnf)
	mysqlctl.RegisterBinlogServerService(binlogServer)
	umgmt.AddCloseCallback(func() {
		mysqlctl.DisableBinlogServerService(binlogServer)
	})

	bindAddr := fmt.Sprintf(":%v", port)
	secureAddr := ""
	if securePort != 0 {
		secureAddr = fmt.Sprintf(":%v", securePort)
	}

	exportedType := expvar.NewString("tablet-type")

	// Action agent listens to changes in zookeeper and makes
	// modifications to this tablet.
	agent, err = tm.NewActionAgent(topoServer, tabletAlias, mycnfFile, dbConfigsFile, dbCredentialsFile)
	if err != nil {
		return nil, err
	}
	agent.AddChangeCallback(func(oldTablet, newTablet topo.Tablet) {
		if newTablet.IsServingType() {
			if dbcfgs.App.Dbname == "" {
				dbcfgs.App.Dbname = newTablet.DbName()
			}
			dbcfgs.App.KeyRange = newTablet.KeyRange
			dbcfgs.App.Keyspace = newTablet.Keyspace
			dbcfgs.App.Shard = newTablet.Shard
			// Transitioning from replica to master, first disconnect
			// existing connections. "false" indicateds that clients must
			// re-resolve their endpoint before reconnecting.
			if newTablet.Type == topo.TYPE_MASTER && oldTablet.Type != topo.TYPE_MASTER {
				ts.DisallowQueries(false)
			}
			qrs := loadCustomRules(customRules)
			if dbcfgs.App.KeyRange.IsPartial() {
				qr := ts.NewQueryRule("enforce keyspace_id range", "keyspace_id_not_in_range", ts.QR_FAIL_QUERY)
				qr.AddPlanCond(sqlparser.PLAN_INSERT_PK)
				err = qr.AddBindVarCond("keyspace_id", true, true, ts.QR_NOTIN, dbcfgs.App.KeyRange)
				if err != nil {
					relog.Warning("Unable to add keyspace rule: %v", err)
				} else {
					qrs.Add(qr)
				}
			}
			ts.AllowQueries(dbcfgs.App, schemaOverrides, qrs)
			mysqlctl.EnableUpdateStreamService(string(newTablet.Type), dbcfgs)
			if newTablet.Type != topo.TYPE_MASTER {
				ts.StartRowCacheInvalidation()
			}
		} else {
			ts.DisallowQueries(false)
			ts.StopRowCacheInvalidation()
			mysqlctl.DisableUpdateStreamService()
		}

		exportedType.Set(string(newTablet.Type))

		// BinlogServer is only enabled for replicas
		if newTablet.Type == topo.TYPE_REPLICA {
			if !mysqlctl.IsBinlogServerEnabled(binlogServer) {
				mysqlctl.EnableBinlogServerService(binlogServer, dbcfgs.App.Dbname)
			}
		} else {
			if mysqlctl.IsBinlogServerEnabled(binlogServer) {
				mysqlctl.DisableBinlogServerService(binlogServer)
			}
		}
	})

	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)
	if err := agent.Start(bindAddr, secureAddr, mysqld.Addr()); err != nil {
		return nil, err
	}

	// register the RPC services from the agent
	agent.RegisterQueryService(mysqld)

	return agent, nil
}
