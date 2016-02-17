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

func initCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 2*time.Minute, "how long to wait for startup")
	initDBSQLFile := subFlags.String("init_db_sql_file", "", "path to .sql file to run after mysql_install_db")
	subFlags.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Init(ctx, *initDBSQLFile); err != nil {
		return fmt.Errorf("failed init mysql: %v", err)
	}
	return nil
}

func shutdownCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 2*time.Minute, "how long to wait for shutdown")
	subFlags.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Shutdown(ctx, true); err != nil {
		return fmt.Errorf("failed shutdown mysql: %v", err)
	}
	return nil
}

func startCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 2*time.Minute, "how long to wait for startup")
	subFlags.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Start(ctx); err != nil {
		return fmt.Errorf("failed start mysql: %v", err)
	}
	return nil
}

func teardownCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", 2*time.Minute, "how long to wait for shutdown")
	force := subFlags.Bool("force", false, "will remove the root directory even if mysqld shutdown fails")
	subFlags.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	defer cancel()
	if err := mysqld.Teardown(ctx, *force); err != nil {
		return fmt.Errorf("failed teardown mysql (forced? %v): %v", *force, err)
	}
	return nil
}

func positionCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
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
	method func(*mysqlctl.Mysqld, *flag.FlagSet, []string) error
	params string
	help   string
}

var commands = []command{
	{"init", initCmd, "[-wait_time=20s] [-init_db_sql_file=]",
		"Initalizes the directory structure and starts mysqld"},
	{"teardown", teardownCmd, "[-force]",
		"Shuts mysqld down, and removes the directory"},
	{"start", startCmd, "[-wait_time=20s]",
		"Starts mysqld on an already 'init'-ed directory"},
	{"shutdown", shutdownCmd, "[-wait_time=20s]",
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

	flags := dbconfigs.AppConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
	dbconfigs.RegisterFlags(flags)
	flag.Parse()

	tabletAddr = netutil.JoinHostPort("localhost", int32(*port))
	mycnf := mysqlctl.NewMycnf(uint32(*tabletUID), int32(*mysqlPort))

	if *mysqlSocket != "" {
		mycnf.SocketFile = *mysqlSocket
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, flags)
	if err != nil {
		log.Errorf("%v", err)
		exit.Return(1)
	}
	mysqld := mysqlctl.NewMysqld("Dba", "App", mycnf, &dbcfgs.Dba, &dbcfgs.App.ConnParams, &dbcfgs.Repl)
	defer mysqld.Close()

	action := flag.Arg(0)
	for _, cmd := range commands {
		if cmd.name == action {
			subFlags := flag.NewFlagSet(action, flag.ExitOnError)
			subFlags.Usage = func() {
				fmt.Fprintf(os.Stderr, "Usage: %s %s %s\n\n", os.Args[0], cmd.name, cmd.params)
				fmt.Fprintf(os.Stderr, "%s\n\n", cmd.help)
				subFlags.PrintDefaults()
			}

			if err := cmd.method(mysqld, subFlags, flag.Args()[1:]); err != nil {
				log.Error(err)
				exit.Return(1)
			}
			return
		}
	}
	log.Errorf("invalid action: %v", action)
	exit.Return(1)
}
