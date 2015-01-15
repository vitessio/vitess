// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// mysqlctl initializes and controls mysqld with Vitess-specific configuration.
package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
)

var (
	port        = flag.Int("port", 6612, "vtocc port")
	mysqlPort   = flag.Int("mysql_port", 3306, "mysql port")
	tabletUID   = flag.Uint("tablet_uid", 41983, "tablet uid")
	mysqlSocket = flag.String("mysql_socket", "", "path to the mysql socket")

	tabletAddr string
)

func initCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", mysqlctl.MysqlWaitTime, "how long to wait for startup")
	bootstrapArchive := subFlags.String("bootstrap_archive", "mysql-db-dir.tbz", "name of bootstrap archive within vitess/data/bootstrap directory")
	skipSchema := subFlags.Bool("skip_schema", false, "don't apply initial schema")
	subFlags.Parse(args)

	if err := mysqld.Init(*waitTime, *bootstrapArchive, *skipSchema); err != nil {
		return fmt.Errorf("failed init mysql: %v", err)
	}
	return nil
}

func restoreCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	dontWaitForSlaveStart := subFlags.Bool("dont_wait_for_slave_start", false, "won't wait for replication to start (useful when restoring from master server)")
	fetchConcurrency := subFlags.Int("fetch_concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch_retry_count", 3, "how many times to retry a failed transfer")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		return fmt.Errorf("Command restore requires <snapshot manifest file>")
	}

	rs, err := mysqlctl.ReadSnapshotManifest(subFlags.Arg(0))
	if err != nil {
		return fmt.Errorf("restore failed: ReadSnapshotManifest: %v", err)
	}
	err = mysqld.RestoreFromSnapshot(logutil.NewConsoleLogger(), rs, *fetchConcurrency, *fetchRetryCount, *dontWaitForSlaveStart, nil)
	if err != nil {
		return fmt.Errorf("restore failed: RestoreFromSnapshot: %v", err)
	}
	return nil
}

func shutdownCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", mysqlctl.MysqlWaitTime, "how long to wait for shutdown")
	subFlags.Parse(args)

	if err := mysqld.Shutdown(true, *waitTime); err != nil {
		return fmt.Errorf("failed shutdown mysql: %v", err)
	}
	return nil
}

func snapshotCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "how many compression jobs to run simultaneously")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		return fmt.Errorf("Command snapshot requires <db name>")
	}

	filename, _, _, err := mysqld.CreateSnapshot(logutil.NewConsoleLogger(), subFlags.Arg(0), tabletAddr, false, *concurrency, false, nil)
	if err != nil {
		return fmt.Errorf("snapshot failed: %v", err)
	}
	log.Infof("manifest location: %v", filename)
	return nil
}

func snapshotSourceStartCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	concurrency := subFlags.Int("concurrency", 4, "how many checksum jobs to run simultaneously")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		return fmt.Errorf("Command snapshotsourcestart requires <db name>")
	}

	filename, slaveStartRequired, readOnly, err := mysqld.CreateSnapshot(logutil.NewConsoleLogger(), subFlags.Arg(0), tabletAddr, false, *concurrency, true, nil)
	if err != nil {
		return fmt.Errorf("snapshot failed: %v", err)
	}
	log.Infof("manifest location: %v", filename)
	log.Infof("slave start required: %v", slaveStartRequired)
	log.Infof("read only: %v", readOnly)
	return nil
}

func snapshotSourceEndCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	slaveStartRequired := subFlags.Bool("slave_start", false, "will restart replication")
	readWrite := subFlags.Bool("read_write", false, "will make the server read-write")
	subFlags.Parse(args)

	err := mysqld.SnapshotSourceEnd(*slaveStartRequired, !(*readWrite), true, map[string]string{})
	if err != nil {
		return fmt.Errorf("snapshotsourceend failed: %v", err)
	}
	return nil
}

func startCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	waitTime := subFlags.Duration("wait_time", mysqlctl.MysqlWaitTime, "how long to wait for startup")
	subFlags.Parse(args)

	if err := mysqld.Start(*waitTime); err != nil {
		return fmt.Errorf("failed start mysql: %v", err)
	}
	return nil
}

func teardownCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "will remove the root directory even if mysqld shutdown fails")
	subFlags.Parse(args)

	if err := mysqld.Teardown(*force); err != nil {
		return fmt.Errorf("failed teardown mysql (forced? %v): %v", *force, err)
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
	command{"init", initCmd, "[-wait_time=20s] [-bootstrap_archive=mysql-db-dir.tbz] [-skip_schema]",
		"Initalizes the directory structure and starts mysqld"},
	command{"teardown", teardownCmd, "[-force]",
		"Shuts mysqld down, and removes the directory"},
	command{"start", startCmd, "[-wait_time=20s]",
		"Starts mysqld on an already 'init'-ed directory"},
	command{"shutdown", shutdownCmd, "[-wait_time=20s]",
		"Shuts down mysqld, does not remove any file"},

	command{"snapshot", snapshotCmd,
		"[-concurrency=4] <db name>",
		"Takes a full snapshot, copying the innodb data files"},
	command{"snapshotsourcestart", snapshotSourceStartCmd,
		"[-concurrency=4] <db name>",
		"Enters snapshot server mode (mysqld stopped, serving innodb data files)"},
	command{"snapshotsourceend", snapshotSourceEndCmd,
		"[-slave_start] [-read_write]",
		"Gets out of snapshot server mode"},
	command{"restore", restoreCmd,
		"[-fetch_concurrency=3] [-fetch_retry_count=3] [-dont_wait_for_slave_start] <snapshot manifest file>",
		"Restores a full snapshot"},
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

	tabletAddr = netutil.JoinHostPort("localhost", *port)
	mycnf := mysqlctl.NewMycnf(uint32(*tabletUID), *mysqlPort)

	if *mysqlSocket != "" {
		mycnf.SocketFile = *mysqlSocket
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, flags)
	if err != nil {
		log.Errorf("%v", err)
		exit.Return(1)
	}
	mysqld := mysqlctl.NewMysqld("Dba", mycnf, &dbcfgs.Dba, &dbcfgs.Repl)
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
