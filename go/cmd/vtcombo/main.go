/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// vtcombo: a single binary that contains:
// - a ZK topology server based on an in-memory map.
// - one vtgate instance.
// - many vttablet instances.
// - a vtctld instance so it's easy to see the topology.
package main

import (
	"flag"
	"os"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vtctld"
	"github.com/youtube/vitess/go/vt/vtgate"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vttestpb "github.com/youtube/vitess/go/vt/proto/vttest"
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
	dbconfigFlags := dbconfigs.AppConfig | dbconfigs.AllPrivsConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
	dbconfigs.RegisterFlags(dbconfigFlags)
	mysqlctl.RegisterFlags()
	flag.Parse()

	if *servenv.Version {
		servenv.AppVersion.Print()
		os.Exit(0)
	}

	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Errorf("vtcombo doesn't take any positional arguments")
		exit.Return(1)
	}

	// parse the input topology
	tpb := &vttestpb.VTTestTopology{}
	if err := proto.UnmarshalText(*protoTopo, tpb); err != nil {
		log.Errorf("cannot parse topology: %v", err)
		exit.Return(1)
	}

	// default cell to "test" if unspecified
	if len(tpb.Cells) == 0 {
		tpb.Cells = append(tpb.Cells, "test")
	}

	// set discoverygateway flag to default value
	flag.Set("cells_to_watch", strings.Join(tpb.Cells, ","))

	// vtctld UI requires the cell flag
	flag.Set("cell", tpb.Cells[0])
	flag.Set("enable_realtime_stats", "true")
	flag.Set("log_dir", "$VTDATAROOT/tmp")

	// Create topo server. We use a 'memorytopo' implementation.
	ts = memorytopo.NewServer(tpb.Cells...)
	servenv.Init()
	tabletenv.Init()

	// database configs
	mycnf, err := mysqlctl.NewMycnfFromFlags(0)
	if err != nil {
		log.Errorf("mycnf read failed: %v", err)
		exit.Return(1)
	}
	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, dbconfigFlags)
	if err != nil {
		log.Warning(err)
	}
	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs, dbconfigFlags)
	servenv.OnClose(mysqld.Close)

	// tablets configuration and init
	if err := initTabletMap(ts, tpb, mysqld, *dbcfgs, *schemaDir, mycnf); err != nil {
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
	vtgate.Init(context.Background(), healthCheck, ts, resilientSrvTopoServer, tpb.Cells[0], 2 /*retryCount*/, tabletTypesToWait)

	// vtctld configuration and init
	vtctld.InitVtctld(ts)
	vtctld.HandleExplorer("memorytopo", vtctld.NewBackendExplorer(ts.Impl))

	servenv.OnTerm(func() {
		// FIXME(alainjobart): stop vtgate
	})
	servenv.OnClose(func() {
		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		ts.Close()
	})
	servenv.RunDefault()
}
