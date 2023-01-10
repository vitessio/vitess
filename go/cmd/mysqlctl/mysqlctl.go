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
	"fmt"
	"os"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/cmd"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	mysqlPort   = 3306
	tabletUID   = uint(41983)
	mysqlSocket string
)

func init() {
	servenv.RegisterDefaultSocketFileFlags()
	servenv.RegisterFlags()
	servenv.RegisterServiceMapFlag()
	// mysqlctl only starts and stops mysql, only needs dba.
	dbconfigs.RegisterFlags(dbconfigs.Dba)
	servenv.OnParse(func(fs *pflag.FlagSet) {
		fs.IntVar(&mysqlPort, "mysql_port", mysqlPort, "MySQL port")
		fs.UintVar(&tabletUID, "tablet_uid", tabletUID, "Tablet UID")
		fs.StringVar(&mysqlSocket, "mysql_socket", mysqlSocket, "Path to the mysqld socket file")

		acl.RegisterFlags(fs)
	})
}

func initConfigCmd(subFlags *pflag.FlagSet, args []string) error {
	_ = subFlags.Parse(args)

	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, cnf, err := mysqlctl.CreateMysqldAndMycnf(uint32(tabletUID), mysqlSocket, int32(mysqlPort))
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	defer mysqld.Close()
	if err := mysqld.InitConfig(cnf); err != nil {
		return fmt.Errorf("failed to init mysql config: %v", err)
	}
	return nil
}

func initCmd(subFlags *pflag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "How long to wait for mysqld startup")
	initDBSQLFile := subFlags.String("init_db_sql_file", "", "Path to .sql file to run after mysqld initiliaztion")
	_ = subFlags.Parse(args)

	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, cnf, err := mysqlctl.CreateMysqldAndMycnf(uint32(tabletUID), mysqlSocket, int32(mysqlPort))
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

func reinitConfigCmd(subFlags *pflag.FlagSet, args []string) error {
	_ = subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(uint32(tabletUID))
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	if err := mysqld.ReinitConfig(context.TODO(), cnf); err != nil {
		return fmt.Errorf("failed to reinit mysql config: %v", err)
	}
	return nil
}

func shutdownCmd(subFlags *pflag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "How long to wait for mysqld shutdown")
	_ = subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(uint32(tabletUID))
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

func startCmd(subFlags *pflag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "How long to wait for mysqld startup")
	var mysqldArgs flagutil.StringListValue
	subFlags.Var(&mysqldArgs, "mysqld_args", "List of comma-separated flags to pass additionally to mysqld")
	_ = subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(uint32(tabletUID))
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

func teardownCmd(subFlags *pflag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "How long to wait for mysqld shutdown")
	force := subFlags.Bool("force", false, "Remove the root directory even if mysqld shutdown fails")
	_ = subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, cnf, err := mysqlctl.OpenMysqldAndMycnf(uint32(tabletUID))
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

func positionCmd(subFlags *pflag.FlagSet, args []string) error {
	_ = subFlags.Parse(args)
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
	method func(*pflag.FlagSet, []string) error
	params string
	help   string
}

var commands = []command{
	{"init", initCmd, "[--wait_time=5m] [--init_db_sql_file=]",
		"Initializes the directory structure and starts mysqld"},
	{"init_config", initConfigCmd, "",
		"Initializes the directory structure, creates my.cnf file, but does not start mysqld"},
	{"reinit_config", reinitConfigCmd, "",
		"Reinitializes my.cnf file with new server_id"},
	{"teardown", teardownCmd, "[--wait_time=5m] [--force]",
		"Shuts mysqld down, and removes the directory"},
	{"start", startCmd, "[--wait_time=5m]",
		"Starts mysqld on an already 'init'-ed directory"},
	{"shutdown", shutdownCmd, "[--wait_time=5m]",
		"Shuts down mysqld, does not remove any file"},

	{"position", positionCmd,
		"<operation> <pos1> <pos2 | gtid>",
		"Compute operations on replication positions"},
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	fs := pflag.NewFlagSet("mysqlctl", pflag.ExitOnError)
	log.RegisterFlags(fs)
	logutil.RegisterFlags(fs)
	pflag.Usage = func() {
		w := os.Stderr
		fmt.Fprintf(w, "Usage: %s [global-flags] <command> -- [command-flags]\n", os.Args[0])
		fmt.Fprintf(w, "\nThe commands are listed below. Use '%s <command> -- {-h, --help}' for command help.\n\n", os.Args[0])
		for _, cmd := range commands {
			fmt.Fprintf(w, "  %s", cmd.name)
			if cmd.params != "" {
				fmt.Fprintf(w, " %s", cmd.params)
			}
			fmt.Fprintf(w, "\n")
		}
		fmt.Fprintf(w, "\nGlobal flags:\n")
		pflag.PrintDefaults()
	}
	args := servenv.ParseFlagsWithArgs("mysqlctl")

	if cmd.IsRunningAsRoot() {
		fmt.Fprintln(os.Stderr, "mysqlctl cannot be ran as root. Please run as a different user")
		exit.Return(1)
	}

	action := args[0]
	for _, cmd := range commands {
		if cmd.name == action {
			subFlags := pflag.NewFlagSet(action, pflag.ExitOnError)
			subFlags.Usage = func() {
				w := os.Stderr
				fmt.Fprintf(w, "Usage: %s %s %s\n\n", os.Args[0], cmd.name, cmd.params)
				fmt.Fprintf(w, cmd.help)
				fmt.Fprintf(w, "\n\n")
				subFlags.PrintDefaults()
			}
			// This is logged and we want sentence capitalization and punctuation.
			pflag.ErrHelp = fmt.Errorf("\nSee %s --help for more information.", os.Args[0]) // nolint:revive
			if err := cmd.method(subFlags, args[1:]); err != nil {
				log.Errorf("%v\n", err)
				subFlags.Usage()
				exit.Return(1)
			}
			return
		}
	}
	log.Errorf("invalid action: %v\n\n", action)
	pflag.Usage()
	exit.Return(1)
}
