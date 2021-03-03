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
	"flag"
	"fmt"
	"os"
	"time"

	"context"

	"vitess.io/vitess/go/cmd"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
)

var (
	port        = flag.Int("port", 6612, "vttablet port")
	mysqlPort   = flag.Int("mysql_port", 3306, "mysql port")
	tabletUID   = flag.Uint("tablet_uid", 41983, "tablet uid")
	mysqlSocket = flag.String("mysql_socket", "", "path to the mysql socket")

	// Reason for nolint : Being used in line 246 (tabletAddr = netutil.JoinHostPort("localhost", int32(*port))
	tabletAddr string //nolint
)

func initConfigCmd(subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)

	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, cnf, err := mysqlctl.CreateMysqldAndMycnf(uint32(*tabletUID), *mysqlSocket, int32(*mysqlPort))
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	defer mysqld.Close()
	if err := mysqld.InitConfig(cnf); err != nil {
		return fmt.Errorf("failed to init mysql config: %v", err)
	}
	return nil
}

func initCmd(subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "how long to wait for startup")
	initDBSQLFile := subFlags.String("init_db_sql_file", "", "path to .sql file to run after mysql_install_db")
	subFlags.Parse(args)

	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, cnf, err := mysqlctl.CreateMysqldAndMycnf(uint32(*tabletUID), *mysqlSocket, int32(*mysqlPort))
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Init(ctx, cnf, *initDBSQLFile); err != nil {
		return fmt.Errorf("failed init mysql: %v", err)
	}
	return nil
}

func reinitConfigCmd(subFlags *flag.FlagSet, args []string) error {
	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(uint32(*tabletUID))
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	if err := mysqld.ReinitConfig(context.TODO(), cnf); err != nil {
		return fmt.Errorf("failed to reinit mysql config: %v", err)
	}
	return nil
}

func shutdownCmd(subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "how long to wait for shutdown")
	subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(uint32(*tabletUID))
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Shutdown(ctx, cnf, true); err != nil {
		return fmt.Errorf("failed shutdown mysql: %v", err)
	}
	return nil
}

func startCmd(subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "how long to wait for startup")
	var mysqldArgs flagutil.StringListValue
	subFlags.Var(&mysqldArgs, "mysqld_args", "List of comma-separated flags to pass additionally to mysqld")
	subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(uint32(*tabletUID))
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Start(ctx, cnf, mysqldArgs...); err != nil {
		return fmt.Errorf("failed start mysql: %v", err)
	}
	return nil
}

func teardownCmd(subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "how long to wait for shutdown")
	force := subFlags.Bool("force", false, "will remove the root directory even if mysqld shutdown fails")
	subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(uint32(*tabletUID))
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Teardown(ctx, cnf, *force); err != nil {
		return fmt.Errorf("failed teardown mysql (forced? %v): %v", *force, err)
	}
	return nil
}

func positionCmd(subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	if len(args) < 3 {
		return fmt.Errorf("not enough arguments for position operation")
	}

	pos1, err := mysql.DecodePosition(args[1])
	if err != nil {
		return err
	}

	switch args[0] {
	case "equal":
		pos2, err := mysql.DecodePosition(args[2])
		if err != nil {
			return err
		}
		fmt.Println(pos1.Equal(pos2))
	case "at_least":
		pos2, err := mysql.DecodePosition(args[2])
		if err != nil {
			return err
		}
		fmt.Println(pos1.AtLeast(pos2))
	case "append":
		gtid, err := mysql.DecodeGTID(args[2])
		if err != nil {
			return err
		}
		fmt.Println(mysql.AppendGTID(pos1, gtid))
	}

	return nil
}

type command struct {
	name   string
	method func(*flag.FlagSet, []string) error
	params string
	help   string
}

var commands = []command{
	{"init", initCmd, "[-wait_time=5m] [-init_db_sql_file=]",
		"Initializes the directory structure and starts mysqld"},
	{"init_config", initConfigCmd, "",
		"Initializes the directory structure, creates my.cnf file, but does not start mysqld"},
	{"reinit_config", reinitConfigCmd, "",
		"Reinitializes my.cnf file with new server_id"},
	{"teardown", teardownCmd, "[-wait_time=5m] [-force]",
		"Shuts mysqld down, and removes the directory"},
	{"start", startCmd, "[-wait_time=5m]",
		"Starts mysqld on an already 'init'-ed directory"},
	{"shutdown", shutdownCmd, "[-wait_time=5m]",
		"Shuts down mysqld, does not remove any file"},

	{"position", positionCmd,
		"<operation> <pos1> <pos2 | gtid>",
		"Compute operations on replication positions"},
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [global parameters] command [command parameters]\n", os.Args[0])

		fmt.Fprintf(os.Stderr, "\nThe global optional parameters are:\n")
		flag.PrintDefaults()

		fmt.Fprintf(os.Stderr, "\nThe commands are listed below. Use '%s <command> -h' for more help.\n\n", os.Args[0])
		for _, cmd := range commands {
			fmt.Fprintf(os.Stderr, "  %s", cmd.name)
			if cmd.params != "" {
				fmt.Fprintf(os.Stderr, " %s", cmd.params)
			}
			fmt.Fprintf(os.Stderr, "\n")
		}
		fmt.Fprintf(os.Stderr, "\n")
	}

	if cmd.IsRunningAsRoot() {
		fmt.Fprintln(os.Stderr, "mysqlctl cannot be ran as root. Please run as a different user")
		exit.Return(1)
	}
	dbconfigs.RegisterFlags(dbconfigs.Dba)
	flag.Parse()

	tabletAddr = netutil.JoinHostPort("localhost", int32(*port))

	action := flag.Arg(0)
	for _, cmd := range commands {
		if cmd.name == action {
			subFlags := flag.NewFlagSet(action, flag.ExitOnError)
			subFlags.Usage = func() {
				fmt.Fprintf(os.Stderr, "Usage: %s %s %s\n\n", os.Args[0], cmd.name, cmd.params)
				fmt.Fprintf(os.Stderr, "%s\n\n", cmd.help)
				subFlags.PrintDefaults()
			}

			if err := cmd.method(subFlags, flag.Args()[1:]); err != nil {
				log.Error(err)
				exit.Return(1)
			}
			return
		}
	}
	log.Errorf("invalid action: %v", action)
	exit.Return(1)
}
