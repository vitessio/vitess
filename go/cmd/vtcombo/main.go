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
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtcombo"
	"vitess.io/vitess/go/vt/vtctld"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	protoTopo = flag.String("proto_topo", "", "vttest proto definition of the topology, encoded in compact text format. See vttest.proto for more information.")

	schemaDir = flag.String("schema_dir", "", "Schema base directory. Should contain one directory per keyspace, with a vschema.json file if necessary.")

	ts *topo.Server
)

func init() {
	servenv.RegisterDefaultFlags()
}

func main() {
	defer exit.Recover()

	// flag parsing
	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()
	servenv.ParseFlags("vtcombo")

	closer := trace.StartTracing("vtcombo")
	defer trace.LogErrorsWhenClosing(closer)

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
	if flag.Lookup("log_dir") == nil {
		flag.Set("log_dir", "$VTDATAROOT/tmp")
	}

	// Create topo server. We use a 'memorytopo' implementation.
	ts = memorytopo.NewServer(tpb.Cells...)
	servenv.Init()
	tabletenv.Init()

	dbcfgs, err := dbconfigs.Init("")
	if err != nil {
		log.Warning(err)
	}
	mysqld := mysqlctl.NewMysqld(dbcfgs)
	servenv.OnClose(mysqld.Close)

	// tablets configuration and init.
	// Send mycnf as nil because vtcombo won't do backups and restores.
	if err := vtcombo.InitTabletMap(ts, tpb, mysqld, dbcfgs, *schemaDir, nil); err != nil {
		log.Errorf("initTabletMapProto failed: %v", err)
		exit.Return(1)
	}

	// Now that we have fully initialized the tablets, rebuild the keyspace graph. This is what would normally happen in InitTablet.
	for _, ks := range tpb.Keyspaces {
		err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, ks.GetName(), tpb.Cells)
		if err != nil {
			log.Fatalf("Couldn't build srv keyspace for (%v: %v). Got error: %v", ks, tpb.Cells, err)
		}
	}

	// vtgate configuration and init
	resilientServer := srvtopo.NewResilientServer(ts, "ResilientSrvTopoServer")
	healthCheck := discovery.NewHealthCheck(1*time.Millisecond /*retryDelay*/, 1*time.Hour /*healthCheckTimeout*/)
	tabletTypesToWait := []topodatapb.TabletType{
		topodatapb.TabletType_MASTER,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}

	vtgate.QueryLogHandler = "/debug/vtgate/querylog"
	vtgate.QueryLogzHandler = "/debug/vtgate/querylogz"
	vtgate.QueryzHandler = "/debug/vtgate/queryz"
	vtgate.Init(context.Background(), healthCheck, resilientServer, tpb.Cells[0], 2 /*retryCount*/, tabletTypesToWait)

	// vtctld configuration and init
	vtctld.InitVtctld(ts)

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
