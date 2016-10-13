// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// mysqlctl initializes and controls mysqld with Vitess-specific configuration.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/flagutil"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	"golang.org/x/net/context"

	// import mysql to register mysql connection function
	_ "github.com/youtube/vitess/go/mysql"
)

var (
	port        = flag.Int("port", 6612, "vttablet port")
	mysqlPort   = flag.Int("mysql_port", 3306, "mysql port")
	tabletUID   = flag.Uint("tablet_uid", 41983, "tablet uid")
	mysqlSocket = flag.String("mysql_socket", "", "path to the mysql socket")

	tabletAddr string
)

const (
	dbconfigFlags = dbconfigs.AppConfig | dbconfigs.AllPrivsConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
)

func initCmd(subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "how long to wait for startup")
	initDBSQLFile := subFlags.String("init_db_sql_file", "", "path to .sql file to run after mysql_install_db")
	subFlags.Parse(args)

	// Generate my.cnf from scratch and use it to find mysqld.
	mysqld, err := mysqlctl.CreateMysqld(uint32(*tabletUID), *mysqlSocket, int32(*mysqlPort), dbconfigFlags)
	if err != nil {
		return fmt.Errorf("failed to initialize mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Init(ctx, *initDBSQLFile); err != nil {
		return fmt.Errorf("failed init mysql: %v", err)
	}
	return nil
}

func reinitConfigCmd(subFlags *flag.FlagSet, args []string) error {
	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, err := mysqlctl.OpenMysqld(uint32(*tabletUID), dbconfigFlags)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	if err := mysqld.ReinitConfig(context.TODO()); err != nil {
		return fmt.Errorf("failed to reinit mysql config: %v", err)
	}
	return nil
}

func shutdownCmd(subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "how long to wait for shutdown")
	subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, err := mysqlctl.OpenMysqld(uint32(*tabletUID), dbconfigFlags)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Shutdown(ctx, true); err != nil {
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
	mysqld, err := mysqlctl.OpenMysqld(uint32(*tabletUID), dbconfigFlags)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Start(ctx, mysqldArgs...); err != nil {
		return fmt.Errorf("failed start mysql: %v", err)
	}
	return nil
}

func teardownCmd(subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 5*time.Minute, "how long to wait for shutdown")
	force := subFlags.Bool("force", false, "will remove the root directory even if mysqld shutdown fails")
	subFlags.Parse(args)

	// There ought to be an existing my.cnf, so use it to find mysqld.
	mysqld, err := mysqlctl.OpenMysqld(uint32(*tabletUID), dbconfigFlags)
	if err != nil {
		return fmt.Errorf("failed to find mysql config: %v", err)
	}
	defer mysqld.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Teardown(ctx, *force); err != nil {
		return fmt.Errorf("failed teardown mysql (forced? %v): %v", *force, err)
	}
	return nil
}

func positionCmd(subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	if len(args) < 3 {
		return fmt.Errorf("Not enough arguments for position operation.")
	}

	pos1, err := replication.DecodePosition(args[1])
	if err != nil {
		return err
	}

	switch args[0] {
	case "equal":
		pos2, err := replication.DecodePosition(args[2])
		if err != nil {
			return err
		}
		fmt.Println(pos1.Equal(pos2))
	case "at_least":
		pos2, err := replication.DecodePosition(args[2])
		if err != nil {
			return err
		}
		fmt.Println(pos1.AtLeast(pos2))
	case "append":
		gtid, err := replication.DecodeGTID(args[2])
		if err != nil {
			return err
		}
		fmt.Println(replication.AppendGTID(pos1, gtid))
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
		"Initalizes the directory structure and starts mysqld"},
	{"reinit_config", reinitConfigCmd, "",
		"Reinitalizes my.cnf file with new server_id"},
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

	dbconfigs.RegisterFlags(dbconfigFlags)
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
