// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
)

var port = flag.Int("port", 6612, "vtocc port")
var mysqlPort = flag.Int("mysql-port", 3306, "mysql port")
var tabletUid = flag.Uint("tablet-uid", 41983, "tablet uid")
var mysqlSocket = flag.String("mysql-socket", "", "path to the mysql socket")
var tabletAddr string

func initCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	waitTime := subFlags.Duration("wait-time", mysqlctl.MysqlWaitTime, "how long to wait for startup")
	subFlags.Parse(args)

	if err := mysqlctl.Init(mysqld, *waitTime); err != nil {
		log.Fatalf("failed init mysql: %v", err)
	}
}

func multisnapshotCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	concurrency := subFlags.Int("concurrency", 8, "how many compression jobs to run simultaneously")
	spec := subFlags.String("spec", "-", "shard specification")
	tablesString := subFlags.String("tables", "", "dump only this comma separated list of tables")
	skipSlaveRestart := subFlags.Bool("skip-slave-restart", false, "after the snapshot is done, do not restart slave replication")
	maximumFilesize := subFlags.Uint64("maximum-file-size", 128*1024*1024, "the maximum size for an uncompressed data file")
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		log.Fatalf("action multisnapshot requires <db name> <key name>")
	}

	shards, err := key.ParseShardingSpec(*spec)
	if err != nil {
		log.Fatalf("multisnapshot failed: %v", err)
	}
	var tables []string
	if *tablesString != "" {
		tables = strings.Split(*tablesString, ",")
	}

	filenames, err := mysqld.CreateMultiSnapshot(shards, subFlags.Arg(0), subFlags.Arg(1), tabletAddr, false, *concurrency, tables, *skipSlaveRestart, *maximumFilesize, nil)
	if err != nil {
		log.Fatalf("multisnapshot failed: %v", err)
	} else {
		log.Infof("manifest locations: %v", filenames)
	}
}

func multiRestoreCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	start := subFlags.String("start", "", "start of the key range")
	end := subFlags.String("end", "", "end of the key range")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	concurrency := subFlags.Int("concurrency", 8, "how many concurrent db inserts to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 4, "how many files to fetch simultaneously")
	insertTableConcurrency := subFlags.Int("insert-table-concurrency", 4, "how many myisam tables to load into a single destination table simultaneously")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, can contain:\n"+
		"    skipAutoIncrement(TTT): we won't add the AUTO_INCREMENT back to that table\n"+
		"    delayPrimaryKey: we won't add the primary key until after the table is populated\n"+
		"    delaySecondaryIndexes: we won't add the secondary indexes until after the table is populated\n"+
		"    useMyIsam: create the table as MyISAM, then convert it to InnoDB after population\n"+
		"    writeBinLogs: write all operations to the binlogs")

	subFlags.Parse(args)

	keyRange, err := key.ParseKeyRangeParts(*start, *end)
	if err != nil {
		log.Fatalf("Invalid start or end: %v", err)
	}

	if subFlags.NArg() < 2 {
		log.Fatalf("multirestore requires <destination_dbname> <source_host>[/<source_dbname>]... %v", args)
	}
	dbName, dbis := subFlags.Arg(0), subFlags.Args()[1:]
	sources := make([]*url.URL, len(dbis))
	for i, dbi := range dbis {
		if !strings.HasPrefix(dbi, "vttp://") && !strings.HasPrefix(dbi, "http://") {
			dbi = "vttp://" + dbi
		}
		dbUrl, err := url.Parse(dbi)
		if err != nil {
			log.Fatalf("incorrect source url: %v", err)
		}
		sources[i] = dbUrl
	}
	if err := mysqld.MultiRestore(dbName, keyRange, sources, *concurrency, *fetchConcurrency, *insertTableConcurrency, *fetchRetryCount, *strategy); err != nil {
		log.Fatalf("multirestore failed: %v", err)
	}
}

func restoreCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	dontWaitForSlaveStart := subFlags.Bool("dont-wait-for-slave-start", false, "won't wait for replication to start (useful when restoring from master server)")
	fetchConcurrency := subFlags.Int("fetch-concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch-retry-count", 3, "how many times to retry a failed transfer")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("Command restore requires <snapshot manifest file>")
	}

	rs, err := mysqlctl.ReadSnapshotManifest(subFlags.Arg(0))
	if err == nil {
		err = mysqld.RestoreFromSnapshot(rs, *fetchConcurrency, *fetchRetryCount, *dontWaitForSlaveStart, nil)
	}
	if err != nil {
		log.Fatalf("restore failed: %v", err)
	}
}

func shutdownCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	waitTime := subFlags.Duration("wait-time", mysqlctl.MysqlWaitTime, "how long to wait for shutdown")
	subFlags.Parse(args)

	if mysqlErr := mysqlctl.Shutdown(mysqld, true, *waitTime); mysqlErr != nil {
		log.Fatalf("failed shutdown mysql: %v", mysqlErr)
	}
}

func snapshotCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	concurrency := subFlags.Int("concurrency", 4, "how many compression jobs to run simultaneously")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("Command snapshot requires <db name>")
	}

	filename, _, _, err := mysqld.CreateSnapshot(subFlags.Arg(0), tabletAddr, false, *concurrency, false, nil)
	if err != nil {
		log.Fatalf("snapshot failed: %v", err)
	} else {
		log.Infof("manifest location: %v", filename)
	}
}

func snapshotSourceStartCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	concurrency := subFlags.Int("concurrency", 4, "how many checksum jobs to run simultaneously")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("Command snapshotsourcestart requires <db name>")
	}

	filename, slaveStartRequired, readOnly, err := mysqld.CreateSnapshot(subFlags.Arg(0), tabletAddr, false, *concurrency, true, nil)
	if err != nil {
		log.Fatalf("snapshot failed: %v", err)
	} else {
		log.Infof("manifest location: %v", filename)
		log.Infof("slave start required: %v", slaveStartRequired)
		log.Infof("read only: %v", readOnly)
	}
}

func snapshotSourceEndCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	slaveStartRequired := subFlags.Bool("slave-start", false, "will restart replication")
	readWrite := subFlags.Bool("read-write", false, "will make the server read-write")
	subFlags.Parse(args)

	err := mysqld.SnapshotSourceEnd(*slaveStartRequired, !(*readWrite), true)
	if err != nil {
		log.Fatalf("snapshotsourceend failed: %v", err)
	}
}

func startCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	waitTime := subFlags.Duration("wait-time", mysqlctl.MysqlWaitTime, "how long to wait for startup")
	subFlags.Parse(args)

	if err := mysqlctl.Start(mysqld, *waitTime); err != nil {
		log.Fatalf("failed start mysql: %v", err)
	}
}

func teardownCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	force := subFlags.Bool("force", false, "will remove the root directory even if mysqld shutdown fails")
	subFlags.Parse(args)

	if err := mysqlctl.Teardown(mysqld, *force); err != nil {
		log.Fatalf("failed teardown mysql (forced? %v): %v", *force, err)
	}
}

type command struct {
	name   string
	method func(*mysqlctl.Mysqld, *flag.FlagSet, []string)
	params string
	help   string
}

var commands = []command{
	command{"init", initCmd, "",
		"Initalizes the directory structure and starts mysqld"},
	command{"teardown", teardownCmd, "[-force]",
		"Shuts mysqld down, and removes the directory"},
	command{"start", startCmd, "[-wait-time=20s]",
		"Starts mysqld on an already 'init'-ed directory"},
	command{"shutdown", shutdownCmd, "[-wait-time=20s]",
		"Shuts down mysqld, does not remove any file"},

	command{"snapshot", snapshotCmd,
		"[-concurrency=4] <db name>",
		"Takes a full snapshot, copying the innodb data files"},
	command{"snapshotsourcestart", snapshotSourceStartCmd,
		"[-concurrency=4] <db name>",
		"Enters snapshot server mode (mysqld stopped, serving innodb data files)"},
	command{"snapshotsourceend", snapshotSourceEndCmd,
		"[-slave-start] [-read-write]",
		"Gets out of snapshot server mode"},
	command{"restore", restoreCmd,
		"[-fetch-concurrency=3] [-fetch-retry-count=3] [-dont-wait-for-slave-start] <snapshot manifest file>",
		"Restores a full snapshot"},
	command{"multirestore", multiRestoreCmd,
		"[-force] [-concurrency=3] [-fetch-concurrency=4] [-insert-table-concurrency=4] [-fetch-retry-count=3] [-start=''] [-end=''] [-strategy=] <destination_dbname> <source_host>[/<source_dbname>]...",
		"Restores a snapshot form multiple hosts"},
	command{"multisnapshot", multisnapshotCmd, "[-concurrency=8] [-spec='-'] [-tables=''] [-skip-slave-restart] [-maximum-file-size=134217728] <db name> <key name>",
		"Makes a complete snapshot using 'select * into' commands."},
}

func main() {
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
	dbConfigsFile, dbCredentialsFile := dbconfigs.RegisterCommonFlags()
	flag.Parse()

	tabletAddr = fmt.Sprintf("%v:%v", "localhost", *port)
	mycnf := mysqlctl.NewMycnf(uint32(*tabletUid), *mysqlPort, mysqlctl.VtReplParams{})

	if *mysqlSocket != "" {
		mycnf.SocketFile = *mysqlSocket
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, *dbConfigsFile, *dbCredentialsFile)
	if err != nil {
		log.Fatalf("%v", err)
	}
	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)

	action := flag.Arg(0)
	for _, cmd := range commands {
		if cmd.name == action {
			subFlags := flag.NewFlagSet(action, flag.ExitOnError)
			subFlags.Usage = func() {
				fmt.Fprintf(os.Stderr, "Usage: %s %s %s\n\n", os.Args[0], cmd.name, cmd.params)
				fmt.Fprintf(os.Stderr, "%s\n\n", cmd.help)
				subFlags.PrintDefaults()
			}

			cmd.method(mysqld, subFlags, flag.Args()[1:])
			return
		}
	}
	log.Fatalf("invalid action: %v", action)
}
