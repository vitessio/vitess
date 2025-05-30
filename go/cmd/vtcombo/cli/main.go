/*
Copyright 2023 The Vitess Authors.

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
package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtcombo"
	"vitess.io/vitess/go/vt/vtctld"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttest"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	Main = &cobra.Command{
		Use:   "vtcombo",
		Short: "vtcombo is a single binary containing several vitess components.",
		Long: `vtcombo is a single binary containing several vitess components.

In particular, it contains:
- A topology server based on an in-memory map.
- One vtgate instance.
- Many vttablet instances.
- A vtctld instance so it's easy to see the topology.`,
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}
	schemaDir             string
	startMysql            bool
	mysqlPort             = 3306
	externalTopoServer    bool
	plannerName           string
	vschemaPersistenceDir string

	tpb               vttestpb.VTTestTopology
	ts                *topo.Server
	resilientServer   *srvtopo.ResilientServer
	tabletTypesToWait []topodatapb.TabletType

	env *vtenv.Environment

	srvTopoCounts *stats.CountersWithSingleLabel
)

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterFlags()
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()

	dbconfigs.RegisterFlags(dbconfigs.All...)
	mysqlctl.RegisterFlags()

	servenv.MoveFlagsToCobraCommand(Main)

	acl.RegisterFlags(Main.Flags())

	Main.Flags().StringVar(&schemaDir, "schema_dir", schemaDir, "Schema base directory. Should contain one directory per keyspace, with a vschema.json file if necessary.")
	Main.Flags().BoolVar(&startMysql, "start_mysql", startMysql, "Should vtcombo also start mysql")
	utils.SetFlagIntVar(Main.Flags(), &mysqlPort, "mysql-port", mysqlPort, "mysql port")
	utils.SetFlagBoolVar(Main.Flags(), &externalTopoServer, "external-topo-server", externalTopoServer, "Should vtcombo use an external topology server instead of starting its own in-memory topology server. "+
		"If true, vtcombo will use the flags defined in topo/server.go to open topo server")
	Main.Flags().StringVar(&plannerName, "planner-version", plannerName, "Sets the default planner to use when the session has not changed it. Valid values are: Gen4, Gen4Greedy, Gen4Left2Right")
	Main.Flags().StringVar(&vschemaPersistenceDir, "vschema-persistence-dir", vschemaPersistenceDir, "If set, per-keyspace vschema will be persisted in this directory "+
		"and reloaded into the in-memory topology server across restarts. Bookkeeping is performed using a simple watcher goroutine. "+
		"This is useful when running vtcombo as an application development container (e.g. vttestserver) where you want to keep the same "+
		"vschema even if developer's machine reboots. This works in tandem with vttestserver's --persistent_mode flag. Needless to say, "+
		"this is neither a perfect nor a production solution for vschema persistence. Consider using the --external-topo-server flag if "+
		"you require a more complete solution. This flag is ignored if --external-topo-server is set.")

	utils.SetFlagVar(Main.Flags(), vttest.TextTopoData(&tpb), "proto-topo", "vttest proto definition of the topology, encoded in compact text format. See vttest.proto for more information.")
	utils.SetFlagVar(Main.Flags(), vttest.JSONTopoData(&tpb), "json-topo", "vttest proto definition of the topology, encoded in json format. See vttest.proto for more information.")

	utils.SetFlagVar(Main.Flags(), (*topoproto.TabletTypeListFlag)(&tabletTypesToWait), "tablet-types-to-wait", "Wait till connected for specified tablet types during Gateway initialization. Should be provided as a comma-separated set of tablet types.")

	// We're going to force the value later, so don't even bother letting the
	// user know about this flag.
	Main.Flags().MarkHidden("tablet-protocol")

	srvTopoCounts = stats.NewCountersWithSingleLabel("ResilientSrvTopoServer", "Resilient srvtopo server operations", "type")
}

func startMysqld(ctx context.Context, uid uint32) (mysqld *mysqlctl.Mysqld, cnf *mysqlctl.Mycnf, err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	mycnfFile := mysqlctl.MycnfFile(uid)

	if _, statErr := os.Stat(mycnfFile); os.IsNotExist(statErr) {
		mysqld, cnf, err = mysqlctl.CreateMysqldAndMycnf(uid, "", mysqlPort, env.CollationEnv())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize mysql config :%w", err)
		}
		if err := mysqld.Init(ctx, cnf, ""); err != nil {
			return nil, nil, fmt.Errorf("failed to initialize mysql :%w", err)
		}
	} else {
		mysqld, cnf, err = mysqlctl.OpenMysqldAndMycnf(uid, env.CollationEnv())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to find mysql config: %w", err)
		}
		err = mysqld.RefreshConfig(ctx, cnf)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to refresh config: %w", err)
		}
		if err := mysqld.Start(ctx, cnf); err != nil {
			return nil, nil, fmt.Errorf("Failed to start mysqld: %w", err)
		}
	}

	return mysqld, cnf, nil
}

func run(cmd *cobra.Command, args []string) (err error) {
	// Stash away a copy of the topology that vtcombo was started with.
	//
	// We will use this to determine the shard structure when keyspaces
	// get recreated.
	originalTopology := (&tpb).CloneVT()

	// default cell to "test" if unspecified
	if len(tpb.Cells) == 0 {
		tpb.Cells = append(tpb.Cells, "test")
	}

	cmd.Flags().Set("cells_to_watch", strings.Join(tpb.Cells, ","))

	// vtctld UI requires the cell flag
	cmd.Flags().Set("cell", tpb.Cells[0])
	if f := cmd.Flags().Lookup("log_dir"); f != nil && !f.Changed {
		cmd.Flags().Set("log_dir", "$VTDATAROOT/tmp")
	}

	env, err = vtenv.New(vtenv.Options{
		MySQLServerVersion: servenv.MySQLServerVersion(),
		TruncateUILen:      servenv.TruncateUILen,
		TruncateErrLen:     servenv.TruncateErrLen,
	})
	if err != nil {
		log.Fatalf("unable to initialize env: %v", err)
	}

	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()
	if externalTopoServer {
		// Open topo server based on the command line flags defined at topo/server.go
		// do not create cell info as it should be done by whoever sets up the external topo server
		ts = topo.Open()
	} else {
		// Create topo server. We use a 'memorytopo' implementation.
		ts = memorytopo.NewServer(ctx, tpb.Cells...)
	}
	defer ts.Close()

	// attempt to load any routing rules specified by tpb
	if err := vtcombo.InitRoutingRules(ctx, ts, tpb.GetRoutingRules()); err != nil {
		return fmt.Errorf("Failed to load routing rules: %w", err)
	}

	// attempt to load any mirror rules specified by tpb
	if err := vtcombo.InitMirrorRules(context.Background(), ts, tpb.GetMirrorRules()); err != nil {
		return fmt.Errorf("Failed to load mirror rules: %w", err)
	}

	servenv.Init()
	tabletenv.Init()

	var (
		mysqld = &vtcomboMysqld{}
		cnf    *mysqlctl.Mycnf
	)

	if startMysql {
		mysqld.Mysqld, cnf, err = startMysqld(ctx, 1)
		if err != nil {
			return err
		}
		servenv.OnClose(func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(cmd.Context(), mysqlctl.DefaultShutdownTimeout+10*time.Second)
			defer shutdownCancel()
			mysqld.Shutdown(shutdownCtx, cnf, true, mysqlctl.DefaultShutdownTimeout)
		})
		// We want to ensure we can write to this database
		mysqld.SetReadOnly(ctx, false)

	} else {
		dbconfigs.GlobalDBConfigs.InitWithSocket("", env.CollationEnv())
		mysqld.Mysqld = mysqlctl.NewMysqld(&dbconfigs.GlobalDBConfigs)
		servenv.OnClose(mysqld.Close)
	}

	// Tablet configuration and init.
	// Send mycnf as nil because vtcombo won't do backups and restores.
	//
	// Also force the `--tablet-manager-protocol` and `--tablet-protocol` flags
	// to be the "internal" protocol that InitTabletMap registers.

	cmd.Flags().Set("tablet-manager-protocol", "internal")
	cmd.Flags().Set("tablet-protocol", "internal")
	uid, err := vtcombo.InitTabletMap(env, ts, &tpb, mysqld, &dbconfigs.GlobalDBConfigs, schemaDir, startMysql, srvTopoCounts)
	if err != nil {
		// ensure we start mysql in the event we fail here
		if startMysql {
			startCtx, startCancel := context.WithTimeout(ctx, mysqlctl.DefaultShutdownTimeout+10*time.Second)
			defer startCancel()
			mysqld.Shutdown(startCtx, cnf, true, mysqlctl.DefaultShutdownTimeout)
		}

		return fmt.Errorf("initTabletMapProto failed: %w", err)
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
				ks = originalKs.CloneVT()
			}
		}

		wr := wrangler.New(env, logutil.NewConsoleLogger(), ts, nil)
		newUID, err := vtcombo.CreateKs(ctx, env, ts, &tpb, mysqld, &dbconfigs.GlobalDBConfigs, schemaDir, ks, true, uid, wr, srvTopoCounts)
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
		err := topotools.RebuildKeyspace(cmd.Context(), logutil.NewConsoleLogger(), ts, ks.GetName(), tpb.Cells, false)
		if err != nil {
			if startMysql {
				shutdownCtx, shutdownCancel := context.WithTimeout(cmd.Context(), mysqlctl.DefaultShutdownTimeout+10*time.Second)
				defer shutdownCancel()
				mysqld.Shutdown(shutdownCtx, cnf, true, mysqlctl.DefaultShutdownTimeout)
			}

			return fmt.Errorf("Couldn't build srv keyspace for (%v: %v). Got error: %w", ks, tpb.Cells, err)
		}
	}

	// vtgate configuration and init

	resilientServer = srvtopo.NewResilientServer(ctx, ts, srvTopoCounts)

	tabletTypes := make([]topodatapb.TabletType, 0, 1)
	if len(tabletTypesToWait) != 0 {
		for _, tt := range tabletTypesToWait {
			if topoproto.IsServingType(tt) {
				tabletTypes = append(tabletTypes, tt)
			}
		}

		if len(tabletTypes) == 0 {
			log.Exitf("tablet-types-to-wait should contain at least one serving tablet type")
		}
	} else {
		tabletTypes = append(tabletTypes, topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY)
	}

	plannerVersion, _ := plancontext.PlannerNameToVersion(plannerName)

	vtgate.QueryLogHandler = "/debug/vtgate/querylog"
	vtgate.QueryLogzHandler = "/debug/vtgate/querylogz"
	vtgate.QueryzHandler = "/debug/vtgate/queryz"

	// pass nil for healthcheck, it will get created
	vtg := vtgate.Init(ctx, env, nil, resilientServer, tpb.Cells[0], tabletTypes, plannerVersion)

	// vtctld configuration and init
	err = vtctld.InitVtctld(env, ts)
	if err != nil {
		return err
	}

	if vschemaPersistenceDir != "" && !externalTopoServer {
		startVschemaWatcher(ctx, vschemaPersistenceDir, ts)
	}

	servenv.OnRun(func() {
		addStatusParts(vtg)
	})

	servenv.RunDefault()

	return nil
}

// vtcomboMysqld is a wrapper on top of mysqlctl.Mysqld.
// We need this wrapper because vtcombo runs with a single MySQL instance
// which all the tablets connect to. (replica, primary, all). This means that we shouldn't
// be trying to run any replication related commands on it, otherwise they fail.
type vtcomboMysqld struct {
	*mysqlctl.Mysqld
}

// SetReplicationSource implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) SetReplicationSource(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
	return nil
}

// StartReplication implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) StartReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	return nil
}

// RestartReplication implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) RestartReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	return nil
}

// StartReplicationUntilAfter implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) StartReplicationUntilAfter(ctx context.Context, pos replication.Position) error {
	return nil
}

// StopReplication implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) StopReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	return nil
}

// SetSemiSyncEnabled implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) SetSemiSyncEnabled(ctx context.Context, source, replica bool) error {
	return nil
}

// SemiSyncExtensionLoaded implements the MysqlDaemon interface
func (mysqld *vtcomboMysqld) SemiSyncExtensionLoaded(ctx context.Context) (mysql.SemiSyncType, error) {
	return mysql.SemiSyncTypeSource, nil
}
