/*
Copyright 2019 The Vitess Authors.

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

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/dbconfigs"
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

	startMysql = flag.Bool("start_mysql", false, "Should vtcombo also start mysql")

	mysqlPort = flag.Int("mysql_port", 3306, "mysql port")

	ts              *topo.Server
	resilientServer *srvtopo.ResilientServer
)

func init() {
	servenv.RegisterDefaultFlags()
}

func startMysqld(uid uint32) (*mysqlctl.Mysqld, *mysqlctl.Mycnf) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	mycnfFile := mysqlctl.MycnfFile(uid)

	var mysqld *mysqlctl.Mysqld
	var cnf *mysqlctl.Mycnf
	var err error

	if _, statErr := os.Stat(mycnfFile); os.IsNotExist(statErr) {
		mysqld, cnf, err = mysqlctl.CreateMysqldAndMycnf(uid, "", int32(*mysqlPort))
		if err != nil {
			log.Errorf("failed to initialize mysql config :%v", err)
			exit.Return(1)
		}
		if err := mysqld.Init(ctx, cnf, ""); err != nil {
			log.Errorf("failed to initialize mysql :%v", err)
			exit.Return(1)
		}
	} else {
		mysqld, cnf, err = mysqlctl.OpenMysqldAndMycnf(uid)
		if err != nil {
			log.Errorf("failed to find mysql config: %v", err)
			exit.Return(1)
		}
		err = mysqld.RefreshConfig(ctx, cnf)
		if err != nil {
			log.Errorf("failed to refresh config: %v", err)
			exit.Return(1)
		}
		if err := mysqld.Start(ctx, cnf); err != nil {
			log.Errorf("Failed to start mysqld: %v", err)
			exit.Return(1)
		}
	}
	cancel()
	return mysqld, cnf
}

func main() {
	defer exit.Recover()

	// flag parsing
	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()
	servenv.ParseFlags("vtcombo")

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

	var mysqld *mysqlctl.Mysqld
	var cnf *mysqlctl.Mycnf
	if *startMysql {
		mysqld, cnf = startMysqld(1)
		servenv.OnClose(func() {
			mysqld.Shutdown(context.TODO(), cnf, true)
		})
		// We want to ensure we can write to this database
		mysqld.SetReadOnly(false)

	} else {
		dbconfigs.GlobalDBConfigs.InitWithSocket("")
		mysqld = mysqlctl.NewMysqld(&dbconfigs.GlobalDBConfigs)
		servenv.OnClose(mysqld.Close)
	}

	// tablets configuration and init.
	// Send mycnf as nil because vtcombo won't do backups and restores.
	if err := vtcombo.InitTabletMap(ts, tpb, mysqld, &dbconfigs.GlobalDBConfigs, *schemaDir, nil, *startMysql); err != nil {
		log.Errorf("initTabletMapProto failed: %v", err)
		// ensure we start mysql in the event we fail here
		if *startMysql {
			mysqld.Shutdown(context.TODO(), cnf, true)
		}
		exit.Return(1)
	}

	// Now that we have fully initialized the tablets, rebuild the keyspace graph.
	for _, ks := range tpb.Keyspaces {
		err := topotools.RebuildKeyspace(context.Background(), logutil.NewConsoleLogger(), ts, ks.GetName(), tpb.Cells, false)
		if err != nil {
			if *startMysql {
				mysqld.Shutdown(context.TODO(), cnf, true)
			}
			log.Fatalf("Couldn't build srv keyspace for (%v: %v). Got error: %v", ks, tpb.Cells, err)
		}
	}

	// vtgate configuration and init
	resilientServer = srvtopo.NewResilientServer(ts, "ResilientSrvTopoServer")
	tabletTypesToWait := []topodatapb.TabletType{
		topodatapb.TabletType_MASTER,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}

	vtgate.QueryLogHandler = "/debug/vtgate/querylog"
	vtgate.QueryLogzHandler = "/debug/vtgate/querylogz"
	vtgate.QueryzHandler = "/debug/vtgate/queryz"
	vtg := vtgate.Init(context.Background(), resilientServer, tpb.Cells[0], tabletTypesToWait)

	// vtctld configuration and init
	vtctld.InitVtctld(ts)

	servenv.OnRun(func() {
		addStatusParts(vtg)
	})

	servenv.OnTerm(func() {
		log.Error("Terminating")
		// FIXME(alainjobart): stop vtgate
	})
	servenv.OnClose(func() {
		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		ts.Close()
	})
	servenv.RunDefault()
}
