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

// mysqlctld is a daemon that starts or initializes mysqld and provides an RPC
// interface for vttablet to stop and start mysqld from a different container
// without having to restart the container running mysqlctld.
package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	// mysqld is used by the rpc implementation plugin.
	mysqld *mysqlctl.Mysqld
	cnf    *mysqlctl.Mycnf

	mysqlPort    = 3306
	tabletUID    = uint32(41983)
	mysqlSocket  string
	collationEnv *collations.Environment

	// mysqlctl init flags
	waitTime         = 5 * time.Minute
	shutdownWaitTime = 5 * time.Minute
	initDBSQLFile    string

	Main = &cobra.Command{
		Use:   "mysqlctld",
		Short: "mysqlctld is a daemon that starts or initializes mysqld.",
		Long: "`mysqlctld` is a gRPC server that can be used instead of the `mysqlctl` client tool.\n" +
			"If the target directories are empty when it is invoked, it automatically performs initialization operations to bootstrap the `mysqld` instance before starting it.\n" +
			"The `mysqlctld` process can subsequently receive gRPC commands from a `vttablet` to perform housekeeping operations like shutting down and restarting the `mysqld` instance as needed.\n\n" +
			"{{< warning >}}\n" +
			"`mysqld_safe` is not used so the `mysqld` process will not be automatically restarted in case of a failure.\n" +
			"{{</ warning>}}\n\n" +
			"To enable communication with a `vttablet`, the server must be configured to receive gRPC messages on a unix domain socket.",
		Example: `mysqlctld \
	--log_dir=${VTDATAROOT}/logs \
	--tablet_uid=100 \
	--mysql_port=17100 \
	--socket_file=/path/to/socket_file`,
		Args:    cobra.NoArgs,
		Version: servenv.AppVersion.String(),
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}

	timeouts = &servenv.TimeoutFlags{
		LameduckPeriod: 50 * time.Millisecond,
		OnTermTimeout:  shutdownWaitTime + 10*time.Second,
		OnCloseTimeout: 10 * time.Second,
	}
)

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterDefaultSocketFileFlags()
	servenv.RegisterFlagsWithTimeouts(timeouts)
	servenv.RegisterGRPCServerFlags()
	servenv.RegisterGRPCServerAuthFlags()
	servenv.RegisterServiceMapFlag()
	// mysqlctld only starts and stops mysql, only needs dba.
	dbconfigs.RegisterFlags(dbconfigs.Dba)

	servenv.MoveFlagsToCobraCommand(Main)

	Main.Flags().IntVar(&mysqlPort, "mysql_port", mysqlPort, "MySQL port")
	Main.Flags().Uint32Var(&tabletUID, "tablet_uid", tabletUID, "Tablet UID")
	Main.Flags().StringVar(&mysqlSocket, "mysql_socket", mysqlSocket, "Path to the mysqld socket file")
	Main.Flags().DurationVar(&waitTime, "wait_time", waitTime, "How long to wait for mysqld startup")
	Main.Flags().StringVar(&initDBSQLFile, "init_db_sql_file", initDBSQLFile, "Path to .sql file to run after mysqld initialization")
	Main.Flags().DurationVar(&shutdownWaitTime, "shutdown-wait-time", shutdownWaitTime, "How long to wait for mysqld shutdown")

	acl.RegisterFlags(Main.Flags())

	collationEnv = collations.NewEnvironment(servenv.MySQLServerVersion())
}

func run(cmd *cobra.Command, args []string) error {
	defer logutil.Flush()

	// We'll register this OnTerm handler before mysqld starts, so we get notified
	// if mysqld dies on its own without us (or our RPC client) telling it to.
	mysqldTerminated := make(chan struct{})
	onTermFunc := func() {
		close(mysqldTerminated)
	}

	// Start or Init mysqld as needed.
	ctx, cancel := context.WithTimeout(context.Background(), waitTime)
	mycnfFile := mysqlctl.MycnfFile(tabletUID)
	if _, statErr := os.Stat(mycnfFile); os.IsNotExist(statErr) {
		// Generate my.cnf from scratch and use it to find mysqld.
		log.Infof("mycnf file (%s) doesn't exist, initializing", mycnfFile)

		var err error
		mysqld, cnf, err = mysqlctl.CreateMysqldAndMycnf(tabletUID, mysqlSocket, mysqlPort, collationEnv)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to initialize mysql config: %w", err)
		}
		mysqld.OnTerm(onTermFunc)

		if err := mysqld.Init(ctx, cnf, initDBSQLFile); err != nil {
			cancel()
			return fmt.Errorf("failed to initialize mysql data dir and start mysqld: %w", err)
		}
	} else {
		// There ought to be an existing my.cnf, so use it to find mysqld.
		log.Infof("mycnf file (%s) already exists, starting without init", mycnfFile)

		var err error
		mysqld, cnf, err = mysqlctl.OpenMysqldAndMycnf(tabletUID, collationEnv)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to find mysql config: %w", err)
		}
		mysqld.OnTerm(onTermFunc)

		err = mysqld.RefreshConfig(ctx, cnf)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to refresh config: %w", err)
		}

		// check if we were interrupted during a previous restore
		if !mysqlctl.RestoreWasInterrupted(cnf) {
			if err := mysqld.Start(ctx, cnf); err != nil {
				cancel()
				return fmt.Errorf("failed to start mysqld: %w", err)
			}
		} else {
			log.Infof("found interrupted restore, not starting mysqld")
		}
	}
	cancel()

	servenv.Init()

	// Take mysqld down with us on SIGTERM before entering lame duck.
	servenv.OnTermSync(func() {
		log.Infof("mysqlctl received SIGTERM, shutting down mysqld first")
		ctx, cancel := context.WithTimeout(context.Background(), shutdownWaitTime+10*time.Second)
		defer cancel()
		if err := mysqld.Shutdown(ctx, cnf, true, shutdownWaitTime); err != nil {
			log.Errorf("failed to shutdown mysqld: %v", err)
		}
	})

	// Start RPC server and wait for SIGTERM.
	mysqlctldTerminated := make(chan struct{})
	go func() {
		servenv.RunDefault()
		close(mysqlctldTerminated)
	}()

	select {
	case <-mysqldTerminated:
		log.Infof("mysqld shut down on its own, exiting mysqlctld")
	case <-mysqlctldTerminated:
		log.Infof("mysqlctld shut down gracefully")
	}

	return nil
}
