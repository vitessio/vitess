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

// mysqlctl initializes and controls mysqld with Vitess-specific configuration.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
)

// Global flag variables for all commands.
var (
	port        int32
	mysqlPort   int32
	tabletUID   uint32
	mysqlSocket string
	tabletAddr  string // set in Mysqlctl.PersistentPreRun
)

// Commands for the mysqlctl binary.
var (
	// Mysqlctl is the root command for the mysqlctl binary.
	Mysqlctl = &cobra.Command{
		Use:   "mysqlctl [global flags] <command> [command flags]",
		Short: "Controls a local mysqld process and my.cnf config.",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			tmp := os.Args
			os.Args = os.Args[0:1]
			flag.Parse()
			os.Args = tmp

			tabletAddr = netutil.JoinHostPort("localhost", port)
		},
	}

	Init = &cobra.Command{
		Use:                   "init [--wait_time=5m] [--init_db_sql_file=<path>]",
		Short:                 "Initializes the directory structure and starts mysqld.",
		Args:                  cobra.NoArgs,
		DisableFlagsInUseLine: true,
		RunE:                  commandInit,
	}
	InitConfig = &cobra.Command{
		Use:                   "init_config",
		Short:                 "Initializes the directory structure and creates a my.cnf, but does not start mysqld.",
		Args:                  cobra.NoArgs,
		DisableFlagsInUseLine: true,
		RunE:                  commandInitConfig,
	}
	ReinitConfig = &cobra.Command{
		Use:                   "reinit_config",
		Short:                 "Reinitializes my.cnf file with a new server_id.",
		Args:                  cobra.NoArgs,
		DisableFlagsInUseLine: true,
		RunE:                  commandReinitConfig,
	}
	Shutdown = &cobra.Command{
		Use:                   "shutdown [--wait_time=5m]",
		Short:                 "Shuts down mysqld. This does not remove any files.",
		Args:                  cobra.NoArgs,
		DisableFlagsInUseLine: true,
		RunE:                  commandShutdown,
	}
	Start = &cobra.Command{
		Use:                   "start [--wait_time=5m] [--mysqld_args <arg1,arg2,...>]",
		Short:                 "Starts mysqld on an already 'init'-ed directory.",
		Args:                  cobra.NoArgs,
		DisableFlagsInUseLine: true,
		RunE:                  commandStart,
	}
	Teardown = &cobra.Command{
		Use:                   "teardown [--wait_time=5m] [--force]",
		Short:                 "Shuts down mysqld and removes the directory.",
		Args:                  cobra.NoArgs,
		DisableFlagsInUseLine: true,
		RunE:                  commandTeardown,
	}

	Position = &cobra.Command{
		Use:                   "position <operation> <pos1> <pos2 | gtid>",
		Short:                 "Compute operations on replication positions. Valid operations are 'equal', 'at_least', and 'append'.",
		Args:                  cobra.ExactArgs(3),
		DisableFlagsInUseLine: true,
		RunE:                  commandPosition,
	}
)

func commandInitConfig(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, cnf, err := mysqlctl.CreateMysqldAndMycnf(tabletUID, mysqlSocket, mysqlPort)
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	defer mysqld.Close()

	if err := mysqld.InitConfig(cnf); err != nil {
		return fmt.Errorf("failed to init mysql config: %v", err)
	}

	return nil
}

var commandInitOptions = struct {
	WaitTime      time.Duration
	InitDbSQLFile string
}{
	WaitTime: 5 * time.Minute,
}

func commandInit(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, cnf, err := mysqlctl.CreateMysqldAndMycnf(tabletUID, mysqlSocket, mysqlPort)
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), commandInitOptions.WaitTime)
	defer cancel()

	if err := mysqld.Init(ctx, cnf, commandInitOptions.InitDbSQLFile); err != nil {
		return fmt.Errorf("failed to init mysql: %v", err)
	}

	return nil
}

func commandReinitConfig(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(tabletUID)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	if err := mysqld.ReinitConfig(context.TODO(), cnf); err != nil {
		return fmt.Errorf("failed to reinit mysql config: %v", err)
	}

	return nil
}

var commandShutdownOptions = struct {
	WaitTime time.Duration
}{
	WaitTime: 5 * time.Minute,
}

func commandShutdown(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(tabletUID)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), commandShutdownOptions.WaitTime)
	defer cancel()

	if err := mysqld.Shutdown(ctx, cnf, true); err != nil {
		return fmt.Errorf("failed to shutdown mysql: %v", err)
	}

	return nil
}

var commandStartOptions = struct {
	WaitTime   time.Duration
	MySQLdArgs []string
}{
	WaitTime: 5 * time.Minute,
}

func commandStart(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(tabletUID)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), commandStartOptions.WaitTime)
	defer cancel()

	if err := mysqld.Start(ctx, cnf, commandStartOptions.MySQLdArgs...); err != nil {
		return fmt.Errorf("failed to start mysql: %v", err)
	}

	return nil
}

var commandTeardownOptions = struct {
	WaitTime time.Duration
	Force    bool
}{
	WaitTime: 5 * time.Minute,
}

func commandTeardown(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(tabletUID)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), commandTeardownOptions.WaitTime)
	defer cancel()

	if err := mysqld.Teardown(ctx, cnf, commandTeardownOptions.Force); err != nil {
		return fmt.Errorf("failed to teardown mysql (force=%v): %v", commandTeardownOptions.Force, err)
	}

	return nil
}

func commandPosition(cmd *cobra.Command, args []string) error {
	pos1, err := mysql.DecodePosition(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	var resultFn func() any

	switch op := cmd.Flags().Arg(0); op {
	case "equal":
		pos2, err := mysql.DecodePosition(cmd.Flags().Arg(2))
		if err != nil {
			return err
		}

		cli.FinishedParsing(cmd)

		resultFn = func() any { return pos1.Equal(pos2) }
	case "at_least":
		pos2, err := mysql.DecodePosition(cmd.Flags().Arg(2))
		if err != nil {
			return err
		}

		cli.FinishedParsing(cmd)

		resultFn = func() any { return pos1.AtLeast(pos2) }
	case "append":
		gtid, err := mysql.DecodeGTID(cmd.Flags().Arg(2))
		if err != nil {
			return err
		}

		cli.FinishedParsing(cmd)

		resultFn = func() any { return mysql.AppendGTID(pos1, gtid) }
	default:
		return fmt.Errorf("unsupported operation: %v; valid operations are 'equal', 'at_least', and 'append'", op)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%v\n", resultFn())

	return nil
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	Mysqlctl.PersistentFlags().Int32Var(&port, "port", 6612, "vttablet port")
	Mysqlctl.PersistentFlags().Int32Var(&mysqlPort, "mysql_port", 3306, "mysql port")
	Mysqlctl.PersistentFlags().Uint32Var(&tabletUID, "tablet_uid", 41983, "tablet uid")
	Mysqlctl.PersistentFlags().StringVar(&mysqlSocket, "mysql_socket", "", "path to the mysql socket")

	dbconfigs.RegisterFlags(dbconfigs.Dba)

	servenv.OnParseFor("mysqlctl", func(fs *pflag.FlagSet) {
		Mysqlctl.PersistentFlags().AddFlagSet(fs)
	})
	tmp := os.Args
	os.Args = os.Args[0:1]
	servenv.ParseFlags("mysqlctl")
	os.Args = tmp

	Init.Flags().DurationVar(&commandInitOptions.WaitTime, "wait_time", 5*time.Minute, "Time to wait for startup.")
	Init.Flags().StringVar(&commandInitOptions.InitDbSQLFile, "init_db_sql_file", "", "Path to .sql file to run after `mysql_install_db` completes.")
	Mysqlctl.AddCommand(Init)

	Mysqlctl.AddCommand(InitConfig)
	Mysqlctl.AddCommand(ReinitConfig)

	Shutdown.Flags().DurationVar(&commandShutdownOptions.WaitTime, "wait_time", 5*time.Minute, "Time to wait for shutdown.")
	Mysqlctl.AddCommand(Shutdown)

	Start.Flags().DurationVar(&commandStartOptions.WaitTime, "wait_time", 5*time.Minute, "Time to wait for startup.")
	Start.Flags().StringSliceVar(&commandStartOptions.MySQLdArgs, "mysqld_args", nil, "List of comma-separated flags to additionally pass to mysqld.")
	Mysqlctl.AddCommand(Start)

	Teardown.Flags().DurationVar(&commandTeardownOptions.WaitTime, "wait_time", 5*time.Minute, "Time to wait for shutdown.")
	Teardown.Flags().BoolVarP(&commandTeardownOptions.Force, "force", "f", false, "Remove the root directory even if mysqld shutdown fails.")
	Mysqlctl.AddCommand(Teardown)

	Mysqlctl.AddCommand(Position)

	if err := Mysqlctl.Execute(); err != nil {
		exit.Return(1)
	}
}
