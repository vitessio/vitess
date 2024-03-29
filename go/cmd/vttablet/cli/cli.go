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

package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/tableacl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/onlineddl"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/yaml2"
	"vitess.io/vitess/resources"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	enforceTableACLConfig        bool
	tableACLConfig               string
	tableACLConfigReloadInterval time.Duration
	tabletPath                   string
	tabletConfig                 string

	tm *tabletmanager.TabletManager

	Main = &cobra.Command{
		Use:   "vttablet",
		Short: "The VTTablet server controls a running MySQL server.",
		Long: `The VTTablet server _controls_ a running MySQL server. VTTablet supports two primary types of deployments:

* Managed MySQL (most common)
* External MySQL

In addition to these deployment types, a partially managed VTTablet is also possible by setting ` + "`--disable_active_reparents`." + `

### Managed MySQL

In this mode, Vitess actively manages MySQL.

### External MySQL.

In this mode, an external MySQL can be used such as AWS RDS, AWS Aurora, Google CloudSQL; or just an existing (vanilla) MySQL installation.

See "Unmanaged Tablet" for the full guide.

Even if a MySQL is external, you can still make vttablet perform some management functions. They are as follows:

` +
			"* `--unmanaged`: This flag indicates that this tablet is running in unmanaged mode. In this mode, any reparent or replica commands are not allowed. These are InitShardPrimary, PlannedReparentShard, EmergencyReparentShard, and ReparentTablet. You should use the TabletExternallyReparented command to inform vitess of the current primary.\n" +
			"* `--replication_connect_retry`: This value is give to mysql when it connects a replica to the primary as the retry duration parameter.\n" +
			"* `--heartbeat_enable` and `--heartbeat_interval duration`: cause vttablet to write heartbeats to the sidecar database. This information is also used by the replication reporter to assess replica lag.\n",
		Example: `
vttablet \
	--topo_implementation etcd2 \
	--topo_global_server_address localhost:2379 \
	--topo_global_root /vitess/ \
	--tablet-path $alias \
	--init_keyspace $keyspace \
	--init_shard $shard \
	--init_tablet_type $tablet_type \
	--port $port \
	--grpc_port $grpc_port \
	--service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream'` + "\n\n`$alias` needs to be of the form: `<cell>-id`, and the cell should match one of the local cells that was created in the topology. The id can be left padded with zeroes: `cell-100` and `cell-000000100` are synonymous.",
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}

	srvTopoCounts *stats.CountersWithSingleLabel
)

func init() {
	srvTopoCounts = stats.NewCountersWithSingleLabel("TabletSrvTopo", "Resilient srvtopo server operations", "type")
}

func run(cmd *cobra.Command, args []string) error {
	servenv.Init()

	tabletAlias, err := topoproto.ParseTabletAlias(tabletPath)
	if err != nil {
		return fmt.Errorf("failed to parse --tablet-path: %w", err)
	}

	mysqlVersion := servenv.MySQLServerVersion()
	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: mysqlVersion,
		TruncateUILen:      servenv.TruncateUILen,
		TruncateErrLen:     servenv.TruncateErrLen,
	})
	if err != nil {
		return fmt.Errorf("cannot initialize vtenv: %w", err)
	}

	// config and mycnf initializations are intertwined.
	config, mycnf, err := initConfig(tabletAlias, env.CollationEnv())
	if err != nil {
		return err
	}

	ts := topo.Open()
	qsc, err := createTabletServer(context.Background(), env, config, ts, tabletAlias, srvTopoCounts)
	if err != nil {
		ts.Close()
		return err
	}

	mysqld := mysqlctl.NewMysqld(config.DB)
	servenv.OnClose(mysqld.Close)

	if err := extractOnlineDDL(); err != nil {
		ts.Close()
		return fmt.Errorf("failed to extract online DDL binaries: %w", err)
	}
	// Initialize and start tm.
	gRPCPort := int32(0)
	if servenv.GRPCPort() != 0 {
		gRPCPort = int32(servenv.GRPCPort())
	}
	tablet, err := tabletmanager.BuildTabletFromInput(tabletAlias, int32(servenv.Port()), gRPCPort, config.DB, env.CollationEnv())
	if err != nil {
		return fmt.Errorf("failed to parse --tablet-path: %w", err)
	}
	tm = &tabletmanager.TabletManager{
		BatchCtx:            context.Background(),
		Env:                 env,
		TopoServer:          ts,
		Cnf:                 mycnf,
		MysqlDaemon:         mysqld,
		DBConfigs:           config.DB.Clone(),
		QueryServiceControl: qsc,
		UpdateStream:        binlog.NewUpdateStream(ts, tablet.Keyspace, tabletAlias.Cell, qsc.SchemaEngine(), env.Parser()),
		VREngine:            vreplication.NewEngine(env, config, ts, tabletAlias.Cell, mysqld, qsc.LagThrottler()),
		VDiffEngine:         vdiff.NewEngine(ts, tablet, env.CollationEnv(), env.Parser()),
	}
	if err := tm.Start(tablet, config); err != nil {
		ts.Close()
		return fmt.Errorf("failed to parse --tablet-path or initialize DB credentials: %w", err)
	}
	servenv.OnClose(func() {
		// Close the tm so that our topo entry gets pruned properly and any
		// background goroutines that use the topo connection are stopped.
		tm.Close()

		// tm uses ts. So, it should be closed after tm.
		ts.Close()
	})

	servenv.RunDefault()

	return nil
}

func initConfig(tabletAlias *topodatapb.TabletAlias, collationEnv *collations.Environment) (*tabletenv.TabletConfig, *mysqlctl.Mycnf, error) {
	tabletenv.Init()
	// Load current config after tabletenv.Init, because it changes it.
	config := tabletenv.NewCurrentConfig()
	if err := config.Verify(); err != nil {
		return nil, nil, fmt.Errorf("invalid config: %w", err)
	}

	if tabletConfig != "" {
		bytes, err := os.ReadFile(tabletConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("error reading config file %s: %w", tabletConfig, err)
		}
		if err := yaml2.Unmarshal(bytes, config); err != nil {
			return nil, nil, fmt.Errorf("error parsing config file %s: %w", bytes, err)
		}
	}
	gotBytes, _ := yaml2.Marshal(config)
	log.Infof("Loaded config file %s successfully:\n%s", tabletConfig, gotBytes)

	var (
		mycnf      *mysqlctl.Mycnf
		socketFile string
	)
	// If no connection parameters were specified, load the mycnf file
	// and use the socket from it. If connection parameters were specified,
	// we assume that the mysql is not local, and we skip loading mycnf.
	// This also means that backup and restore will not be allowed.
	if !config.DB.HasGlobalSettings() {
		var err error
		if mycnf, err = mysqlctl.NewMycnfFromFlags(tabletAlias.Uid); err != nil {
			return nil, nil, fmt.Errorf("mycnf read failed: %w", err)
		}

		socketFile = mycnf.SocketFile
	} else {
		log.Info("connection parameters were specified. Not loading my.cnf.")
	}

	// If connection parameters were specified, socketFile will be empty.
	// Otherwise, the socketFile (read from mycnf) will be used to initialize
	// dbconfigs.
	config.DB.InitWithSocket(socketFile, collationEnv)
	for _, cfg := range config.ExternalConnections {
		cfg.InitWithSocket("", collationEnv)
	}
	return config, mycnf, nil
}

// extractOnlineDDL extracts the gh-ost binary from this executable. gh-ost is appended
// to vttablet executable by `make build` with a go:embed
func extractOnlineDDL() error {
	if binaryFileName, isOverride := onlineddl.GhostBinaryFileName(); !isOverride {
		if err := os.WriteFile(binaryFileName, resources.GhostBinary, 0755); err != nil {
			// One possibility of failure is that gh-ost is up and running. In that case,
			// let's pause and check if the running gh-ost is exact same binary as the one we wish to extract.
			foundBytes, _ := os.ReadFile(binaryFileName)
			if bytes.Equal(resources.GhostBinary, foundBytes) {
				// OK, it's the same binary, there is no need to extract the file anyway
				return nil
			}
			return err
		}
	}

	return nil
}

func createTabletServer(ctx context.Context, env *vtenv.Environment, config *tabletenv.TabletConfig, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, srvTopoCounts *stats.CountersWithSingleLabel) (*tabletserver.TabletServer, error) {
	if tableACLConfig != "" {
		// To override default simpleacl, other ACL plugins must set themselves to be default ACL factory
		tableacl.Register("simpleacl", &simpleacl.Factory{})
	} else if enforceTableACLConfig {
		return nil, fmt.Errorf("table acl config has to be specified with table-acl-config flag because enforce-tableacl-config is set.")
	}

	// creates and registers the query service
	qsc := tabletserver.NewTabletServer(ctx, env, "", config, ts, tabletAlias, srvTopoCounts)
	servenv.OnRun(func() {
		qsc.Register()
		addStatusParts(qsc)
	})
	servenv.OnClose(qsc.StopService)
	qsc.InitACL(tableACLConfig, enforceTableACLConfig, tableACLConfigReloadInterval)
	return qsc, nil
}

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
	Main.Flags().BoolVar(&enforceTableACLConfig, "enforce-tableacl-config", enforceTableACLConfig, "if this flag is true, vttablet will fail to start if a valid tableacl config does not exist")
	Main.Flags().StringVar(&tableACLConfig, "table-acl-config", tableACLConfig, "path to table access checker config file; send SIGHUP to reload this file")
	Main.Flags().DurationVar(&tableACLConfigReloadInterval, "table-acl-config-reload-interval", tableACLConfigReloadInterval, "Ticker to reload ACLs. Duration flag, format e.g.: 30s. Default: do not reload")
	Main.Flags().StringVar(&tabletPath, "tablet-path", tabletPath, "tablet alias")
	Main.Flags().StringVar(&tabletConfig, "tablet_config", tabletConfig, "YAML file config for tablet")
}
