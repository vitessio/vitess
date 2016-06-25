// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vtcombo: a single binary that contains:
// - a ZK topology server based on an in-memory map.
// - one vtgate instance.
// - many vttablet instances.
// - a vtctld instance so it's easy to see the topology.
package main

import (
	"flag"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctld"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk/fakezk"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// cell is the cell to use for this fake cluster
	cell = "test"
)

var (
	protoTopo = flag.String("proto_topo", "", "vttest proto definition of the topology, encoded in compact text format. See vttest.proto for more information.")

	schemaDir = flag.String("schema_dir", "", "Schema base directory. Should contain one directory per keyspace, with a vschema.json file if necessary.")

	ts topo.Server
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

	// set discoverygateway flag to default value
	flag.Set("cells_to_watch", cell)

	// register topo server
	zkconn := fakezk.NewConn()
	topo.RegisterServer("fakezk", zktopo.NewServer(zkconn))
	ts = topo.GetServerByName("fakezk")

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
	servenv.OnClose(mysqld.Close)

	// tablets configuration and init
	if err := initTabletMap(ts, *protoTopo, mysqld, dbcfgs, *schemaDir, mycnf); err != nil {
		log.Errorf("initTabletMapProto failed: %v", err)
		exit.Return(1)
	}

	// vtgate configuration and init
	resilientSrvTopoServer := vtgate.NewResilientSrvTopoServer(ts, "ResilientSrvTopoServer")
	healthCheck := discovery.NewHealthCheck(30*time.Second /*connTimeoutTotal*/, 1*time.Millisecond /*retryDelay*/, 1*time.Hour /*healthCheckTimeout*/)
	tabletTypesToWait := []topodatapb.TabletType{
		topodatapb.TabletType_MASTER,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}
	vtgate.Init(context.Background(), healthCheck, ts, resilientSrvTopoServer, cell, 2 /*retryCount*/, tabletTypesToWait)

	// vtctld configuration and init
	vtctld.InitVtctld(ts)
	vtctld.HandleExplorer("zk", zktopo.NewZkExplorer(zkconn))

	servenv.OnTerm(func() {
		// FIXME(alainjobart): stop vtgate
	})
	servenv.OnClose(func() {
		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		topo.CloseServers()
	})
	servenv.RunDefault()
}
