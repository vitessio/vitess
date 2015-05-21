// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"

	// import mysql to register mysql connection function
	_ "github.com/youtube/vitess/go/mysql"
	// import memcache to register memcache connection function
	_ "github.com/youtube/vitess/go/memcache"
)

var (
	tabletPath     = flag.String("tablet-path", "", "tablet alias")
	enableRowcache = flag.Bool("enable-rowcache", false, "enable rowcacche")
	overridesFile  = flag.String("schema-override", "", "schema overrides file")
	tableAclConfig = flag.String("table-acl-config", "", "path to table access checker config file")
	lockTimeout    = flag.Duration("lock_timeout", actionnode.DefaultLockTimeout, "lock time for wrangler/topo operations")

	agent *tabletmanager.ActionAgent
)

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterDefaultSecureFlags()
	servenv.InitServiceMapForBsonRpcService("tabletmanager")
	servenv.InitServiceMapForBsonRpcService("queryservice")
	servenv.InitServiceMapForBsonRpcService("updatestream")
}

func main() {
	defer exit.Recover()

	flags := dbconfigs.AppConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
	dbconfigs.RegisterFlags(flags)
	mysqlctl.RegisterFlags()
	flag.Parse()
	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Errorf("vttablet doesn't take any positional arguments")
		exit.Return(1)
	}

	servenv.Init()

	if *tabletPath == "" {
		log.Errorf("tabletPath required")
		exit.Return(1)
	}
	tabletAlias, err := topo.ParseTabletAliasString(*tabletPath)

	if err != nil {
		log.Error(err)
		exit.Return(1)
	}

	mycnf, err := mysqlctl.NewMycnfFromFlags(tabletAlias.Uid)
	if err != nil {
		log.Errorf("mycnf read failed: %v", err)
		exit.Return(1)
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, flags)
	if err != nil {
		log.Warning(err)
	}
	dbcfgs.App.EnableRowcache = *enableRowcache

	if *tableAclConfig != "" {
		tableacl.Register("simpleacl", &simpleacl.Factory{})
		tableacl.Init(*tableAclConfig)
	}

	// creates and registers the query service
	qsc := tabletserver.NewQueryServiceControl()
	tabletserver.InitQueryService(qsc)
	binlog.RegisterUpdateStreamService(mycnf)

	// Create mysqld and register the health reporter (needs to be done
	// before initializing the agent, so the initial health check
	// done by the agent has the right reporter)
	mysqld := mysqlctl.NewMysqld("Dba", "App", mycnf, &dbcfgs.Dba, &dbcfgs.App.ConnParams, &dbcfgs.Repl)
	registerHealthReporter(mysqld)

	// Depends on both query and updateStream.
	agent, err = tabletmanager.NewActionAgent(context.Background(), mysqld, qsc, tabletAlias, dbcfgs, mycnf, *servenv.Port, *servenv.SecurePort, *overridesFile, *lockTimeout)
	if err != nil {
		log.Error(err)
		exit.Return(1)
	}

	servenv.OnRun(func() {
		addStatusParts(qsc)
	})
	servenv.OnTerm(func() {
		qsc.DisallowQueries()
		binlog.DisableUpdateStreamService()
		agent.Stop()
	})
	servenv.OnClose(func() {
		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		topo.CloseServers()
	})
	servenv.RunDefault()
}
