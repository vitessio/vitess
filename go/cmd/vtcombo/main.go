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
	"context"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/mysql"
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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	flags              = pflag.NewFlagSet("vtcombo", pflag.ContinueOnError)
	schemaDir          = flags.String("schema_dir", "", "Schema base directory. Should contain one directory per keyspace, with a vschema.json file if necessary.")
	startMysql         = flags.Bool("start_mysql", false, "Should vtcombo also start mysql")
	mysqlPort          = flags.Int("mysql_port", 3306, "mysql port")
	externalTopoServer = flags.Bool("external_topo_server", false, "Should vtcombo use an external topology server instead of starting its own in-memory topology server. "+
		"If true, vtcombo will use the flags defined in topo/server.go to open topo server")
	plannerName = flags.String("planner-version", "", "Sets the default planner to use when the session has not changed it. Valid values are: V3, V3Insert, Gen4, Gen4Greedy and Gen4Fallback. Gen4Fallback tries the gen4 planner and falls back to the V3 planner if the gen4 fails.")

	tpb             vttestpb.VTTestTopology
	ts              *topo.Server
	resilientServer *srvtopo.ResilientServer
)

func init() {
	flags.Var(vttest.TextTopoData(&tpb), "proto_topo", "vttest proto definition of the topology, encoded in compact text format. See vttest.proto for more information.")
	flags.Var(vttest.JSONTopoData(&tpb), "json_topo", "vttest proto definition of the topology, encoded in json format. See vttest.proto for more information.")

	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()
}

func startMysqld(uid uint32) (*mysqlctl.Mysqld, *mysqlctl.Mycnf) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	mycnfFile := mysqlctl.MycnfFile(uid)

	var mysqld *mysqlctl.Mysqld
	var cnf *mysqlctl.Mycnf
	var err error

	if _, statErr := os.Stat(mycnfFile); os.IsNotExist(statErr) {
		mysqld, cnf, err = mysqlctl.CreateMysqldAndMycnf(uid, "", *mysqlPort)
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
	var globalFlags *pflag.FlagSet
	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()
	servenv.OnParseFor("vtcombo", func(fs *pflag.FlagSet) {
		// We're going to force the value later, so don't even bother letting
		// the user know about this flag.
		fs.MarkHidden("tablet_protocol")

		// Add the vtcombo flags declared above in var/init sections to the
		// global flags.
		fs.AddFlagSet(flags)
		// Save for later -- see comment directly after ParseFlags for why.
		globalFlags = fs

		acl.RegisterFlags(fs)
	})

	servenv.ParseFlags("vtcombo")

	// At this point, servenv.ParseFlags has invoked _flag.Parse, which has
	// combined all the flags everywhere into the globalFlags variable we
	// stashed a reference to earlier in our OnParseFor callback function.
	//
	// We now take those flags and make them available to our `flags` instance,
	// which we call `Set` on various flags to force their values further down
	// in main().
	//
	// N.B.: we could just as easily call Set on globalFlags on everything
	// (including our local flags), but we need to save a reference either way,
	// and that in particular (globalFlags.Set on a local flag) feels more
	// potentially confusing than its inverse (flags.Set on a global flag), so
	// we go this way.
	flags.AddFlagSet(globalFlags)

	// Stash away a copy of the topology that vtcombo was started with.
	//
	// We will use this to determine the shard structure when keyspaces
	// get recreated.
	originalTopology := proto.Clone(&tpb).(*vttestpb.VTTestTopology)

	// default cell to "test" if unspecified
	if len(tpb.Cells) == 0 {
		tpb.Cells = append(tpb.Cells, "test")
	}

	flags.Set("cells_to_watch", strings.Join(tpb.Cells, ","))

	// vtctld UI requires the cell flag
	flags.Set("cell", tpb.Cells[0])
	if flags.Lookup("log_dir") == nil {
		flags.Set("log_dir", "$VTDATAROOT/tmp")
	}

	if *externalTopoServer {
		// Open topo server based on the command line flags defined at topo/server.go
		// do not create cell info as it should be done by whoever sets up the external topo server
		ts = topo.Open()
	} else {
		// Create topo server. We use a 'memorytopo' implementation.
		ts = memorytopo.NewServer(tpb.Cells...)
	}

	// attempt to load any routing rules specified by tpb
	if err := vtcombo.InitRoutingRules(context.Background(), ts, tpb.GetRoutingRules()); err != nil {
		log.Errorf("Failed to load routing rules: %v", err)
		exit.Return(1)
	}

	servenv.Init()
	tabletenv.Init()

	mysqld := &vtcomboMysqld{}
	var cnf *mysqlctl.Mycnf
	if *startMysql {
		mysqld.Mysqld, cnf = startMysqld(1)
		servenv.OnClose(func() {
			mysqld.Shutdown(context.TODO(), cnf, true)
		})
		// We want to ensure we can write to this database
		mysqld.SetReadOnly(false)

	} else {
		dbconfigs.GlobalDBConfigs.InitWithSocket("")
		mysqld.Mysqld = mysqlctl.NewMysqld(&dbconfigs.GlobalDBConfigs)
		servenv.OnClose(mysqld.Close)
	}

	// Tablet configuration and init.
	// Send mycnf as nil because vtcombo won't do backups and restores.
	//
	// Also force the `--tablet_manager_protocol` and `--tablet_protocol` flags
	// to be the "internal" protocol that InitTabletMap registers.
	flags.Set("tablet_manager_protocol", "internal")
	flags.Set("tablet_protocol", "internal")
	uid, err := vtcombo.InitTabletMap(ts, &tpb, mysqld, &dbconfigs.GlobalDBConfigs, *schemaDir, *startMysql)
	if err != nil {
		log.Errorf("initTabletMapProto failed: %v", err)
		// ensure we start mysql in the event we fail here
		if *startMysql {
			mysqld.Shutdown(context.TODO(), cnf, true)
		}
		exit.Return(1)
	}

	globalCreateDb = func(ctx context.Context, ks *vttestpb.Keyspace) error {
		// Check if we're recreating a keyspace that was previously deleted by looking
		// at the original topology definition.
		//
		// If we find a matching keyspace, we create it with the same sharding
		// configuration. This ensures that dropping and recreating a keyspace
		// will end up with the same number of shards.
		for _, originalKs := range originalTopology.Keyspaces {
			if originalKs.Name == ks.Name {
				ks = proto.Clone(originalKs).(*vttestpb.Keyspace)
			}
		}

		wr := wrangler.New(logutil.NewConsoleLogger(), ts, nil)
		newUID, err := vtcombo.CreateKs(ctx, ts, &tpb, mysqld, &dbconfigs.GlobalDBConfigs, *schemaDir, ks, true, uid, wr)
		if err != nil {
			return err
		}
		uid = newUID
		tpb.Keyspaces = append(tpb.Keyspaces, ks)
		return nil
	}

	globalDropDb = func(ctx context.Context, ksName string) error {
		if err := vtcombo.DeleteKs(ctx, ts, ksName, mysqld, &tpb); err != nil {
			return err
		}

		// Rebuild the SrvVSchema object
		if err := ts.RebuildSrvVSchema(ctx, tpb.Cells); err != nil {
			return err
		}

		return nil
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
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}
	plannerVersion, _ := plancontext.PlannerNameToVersion(*plannerName)

	vtgate.QueryLogHandler = "/debug/vtgate/querylog"
	vtgate.QueryLogzHandler = "/debug/vtgate/querylogz"
	vtgate.QueryzHandler = "/debug/vtgate/queryz"
	// pass nil for healthcheck, it will get created
	vtg := vtgate.Init(context.Background(), nil, resilientServer, tpb.Cells[0], tabletTypesToWait, plannerVersion)

	// vtctld configuration and init
	err = vtctld.InitVtctld(ts)
	if err != nil {
		exit.Return(1)
	}

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

// vtcomboMysqld is a wrapper on top of mysqlctl.Mysqld.
// We need this wrapper because vtcombo runs with a single MySQL instance
// which all the tablets connect to. (replica, primary, all). This means that we shouldn't
// be trying to run any replication related commands on it, otherwise they fail.
type vtcomboMysqld struct {
	*mysqlctl.Mysqld
}

// SetReplicationSource implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) SetReplicationSource(ctx context.Context, host string, port int32, stopReplicationBefore bool, startReplicationAfter bool) error {
	return nil
}

// StartReplication implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) StartReplication(hookExtraEnv map[string]string) error {
	return nil
}

// RestartReplication implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) RestartReplication(hookExtraEnv map[string]string) error {
	return nil
}

// StartReplicationUntilAfter implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) StartReplicationUntilAfter(ctx context.Context, pos mysql.Position) error {
	return nil
}

// StopReplication implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) StopReplication(hookExtraEnv map[string]string) error {
	return nil
}

// SetSemiSyncEnabled implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) SetSemiSyncEnabled(source, replica bool) error {
	return nil
}

// SemiSyncExtensionLoaded implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) SemiSyncExtensionLoaded() (bool, error) {
	return true, nil
}
