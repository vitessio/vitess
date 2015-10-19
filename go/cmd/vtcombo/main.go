// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vtcombo: a single binary that contains:
// - a ZK topology server based on an in-memory map.
// - one vtgate instance.
// - many vttablet instaces.
package main

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/planbuilder"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk/fakezk"
)

const (
	// cell is the cell to use for this fake cluster
	cell = "test"
)

var (
	topology           = flag.String("topology", "", "Define which shards exist in the test topology in the form <keyspace>/<shardrange>:<dbname>,... The dbname must be unique among all shards, since they share a MySQL instance in the test environment.")
	shardingColumnName = flag.String("sharding_column_name", "keyspace_id", "Specifies the column to use for sharding operations")
	shardingColumnType = flag.String("sharding_column_type", "", "Specifies the type of the column to use for sharding operations")
	vschema            = flag.String("vschema", "", "vschema file")
)

func init() {
	servenv.RegisterDefaultFlags()
}

func main() {
	defer exit.Recover()

	// flag parsing
	flags := dbconfigs.AppConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
	dbconfigs.RegisterFlags(flags)
	mysqlctl.RegisterFlags()
	flag.Parse()
	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Errorf("vtcombo doesn't take any positional arguments")
		exit.Return(1)
	}

	// register topo server
	topo.RegisterServer("fakezk", zktopo.NewServer(fakezk.NewConn()))
	ts := topo.GetServerByName("fakezk")

	servenv.Init()
	tabletserver.Init()

	// database configs
	mycnf, err := mysqlctl.NewMycnfFromFlags(0)
	if err != nil {
		log.Errorf("mycnf read failed: %v", err)
		exit.Return(1)
	}
	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, flags)
	if err != nil {
		log.Warning(err)
	}
	mysqld := mysqlctl.NewMysqld("Dba", "App", mycnf, &dbcfgs.Dba, &dbcfgs.App.ConnParams, &dbcfgs.Repl)

	// tablets configuration and init
	initTabletMap(ts, *topology, mysqld, dbcfgs, mycnf)

	// vschema
	var schema *planbuilder.Schema
	if *vschema != "" {
		schema, err = planbuilder.LoadFile(*vschema)
		if err != nil {
			log.Error(err)
			exit.Return(1)
		}
		log.Infof("v3 is enabled: loaded schema from file")
	}

	// vtgate configuration and init
	resilientSrvTopoServer := vtgate.NewResilientSrvTopoServer(ts, "ResilientSrvTopoServer")
	healthCheck := discovery.NewHealthCheck(30*time.Second /*connTimeoutTotal*/, 1*time.Millisecond /*retryDelay*/)
	vtgate.Init(healthCheck, ts, resilientSrvTopoServer, schema, cell, 1*time.Millisecond /*retryDelay*/, 2 /*retryCount*/, 30*time.Second /*connTimeoutTotal*/, 10*time.Second /*connTimeoutPerConn*/, 365*24*time.Hour /*connLife*/, 0 /*maxInFlight*/, "" /*testGateway*/)

	servenv.OnTerm(func() {
		// FIXME(alainjobart) stop vtgate, all tablets
		//		qsc.DisallowQueries()
		//		agent.Stop()
	})
	servenv.OnClose(func() {
		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		topo.CloseServers()
	})
	servenv.RunDefault()
}
