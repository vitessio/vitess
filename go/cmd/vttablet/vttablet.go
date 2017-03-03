// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tableacl/simpleacl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"
)

var (
	enforceTableACLConfig = flag.Bool("enforce-tableacl-config", false, "if this flag is true, vttablet will fail to start if a valid tableacl config does not exist")
	tableACLConfig        = flag.String("table-acl-config", "", "path to table access checker config file")
	tabletPath            = flag.String("tablet-path", "", "tablet alias")

	agent *tabletmanager.ActionAgent
)

func init() {
	servenv.RegisterDefaultFlags()
}

func main() {
	defer exit.Recover()

	dbconfigFlags := dbconfigs.AppConfig | dbconfigs.AllPrivsConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
	dbconfigs.RegisterFlags(dbconfigFlags)
	mysqlctl.RegisterFlags()
	flag.Parse()
	tabletenv.Init()
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
	tabletAlias, err := topoproto.ParseTabletAlias(*tabletPath)
	if err != nil {
		log.Error(err)
		exit.Return(1)
	}

	mycnf, err := mysqlctl.NewMycnfFromFlags(tabletAlias.Uid)
	if err != nil {
		log.Errorf("mycnf read failed: %v", err)
		exit.Return(1)
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, dbconfigFlags)
	if err != nil {
		log.Warning(err)
	}

	// creates and registers the query service
	ts := topo.Open()
	qsc := tabletserver.NewServer(ts)
	servenv.OnRun(func() {
		qsc.Register()
		addStatusParts(qsc)
	})
	servenv.OnClose(func() {
		// We now leave the queryservice running during lameduck,
		// so stop it in OnClose(), after lameduck is over.
		qsc.StopService()
	})

	if *tableACLConfig != "" {
		// To override default simpleacl, other ACL plugins must set themselves to be default ACL factory
		tableacl.Register("simpleacl", &simpleacl.Factory{})
	} else if *enforceTableACLConfig {
		log.Error("table acl config has to be specified with table-acl-config flag because enforce-tableacl-config is set.")
		exit.Return(1)
	}
	// tabletacl.Init loads ACL from file if *tableACLConfig is not empty
	err = tableacl.Init(
		*tableACLConfig,
		func() {
			qsc.ClearQueryPlanCache()
		},
	)
	if err != nil {
		log.Errorf("Fail to initialize Table ACL: %v", err)
		if *enforceTableACLConfig {
			log.Error("Need a valid initial Table ACL when enforce-tableacl-config is set, exiting.")
			exit.Return(1)
		}
	}

	// Create mysqld and register the health reporter (needs to be done
	// before initializing the agent, so the initial health check
	// done by the agent has the right reporter)
	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs, dbconfigFlags)
	servenv.OnClose(mysqld.Close)

	// Depends on both query and updateStream.
	gRPCPort := int32(0)
	if servenv.GRPCPort != nil {
		gRPCPort = int32(*servenv.GRPCPort)
	}
	agent, err = tabletmanager.NewActionAgent(context.Background(), ts, mysqld, qsc, tabletAlias, *dbcfgs, mycnf, int32(*servenv.Port), gRPCPort)
	if err != nil {
		log.Error(err)
		exit.Return(1)
	}

	servenv.OnClose(func() {
		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		ts.Close()
	})
	servenv.RunDefault()
}
