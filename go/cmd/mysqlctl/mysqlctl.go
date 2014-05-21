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
var mysqlPort = flag.Int("mysql_port", 3306, "mysql port")
var tabletUid = flag.Uint("tablet_uid", 41983, "tablet uid")
var mysqlSocket = flag.String("mysql_socket", "", "path to the mysql socket")
var tabletAddr string

func initCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	waitTime := subFlags.Duration("wait_time", mysqlctl.MysqlWaitTime, "how long to wait for startup")
	subFlags.Parse(args)

	if err := mysqld.Init(*waitTime); err != nil {
		log.Fatalf("failed init mysql: %v", err)
	}
}

func multisnapshotCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	concurrency := subFlags.Int("concurrency", 8, "how many compression jobs to run simultaneously")
	spec := subFlags.String("spec", "-", "shard specification")
	tablesString := subFlags.String("tables", "", "dump only this comma separated list of tables")
	skipSlaveRestart := subFlags.Bool("skip_slave_restart", false, "after the snapshot is done, do not restart slave replication")
	maximumFilesize := subFlags.Uint64("maximum_file_size", 128*1024*1024, "the maximum size for an uncompressed data file")
	keyType := subFlags.String("key_type", "uint64", "type of the key column")
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

	kit := key.KeyspaceIdType(*keyType)
	if !key.IsKeyspaceIdTypeInList(kit, key.AllKeyspaceIdTypes) {
		log.Fatalf("invalid key_type")
	}

	filenames, err := mysqld.CreateMultiSnapshot(shards, subFlags.Arg(0), subFlags.Arg(1), kit, tabletAddr, false, *concurrency, tables, *skipSlaveRestart, *maximumFilesize, nil)
	if err != nil {
		log.Fatalf("multisnapshot failed: %v", err)
	} else {
		log.Infof("manifest locations: %v", filenames)
	}
}

func multiRestoreCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	starts := subFlags.String("starts", "", "starts of the key range")
	ends := subFlags.String("ends", "", "ends of the key range")
	fetchRetryCount := subFlags.Int("fetch_retry_count", 3, "how many times to retry a failed transfer")
	concurrency := subFlags.Int("concurrency", 8, "how many concurrent db inserts to run simultaneously")
	fetchConcurrency := subFlags.Int("fetch_concurrency", 4, "how many files to fetch simultaneously")
	insertTableConcurrency := subFlags.Int("insert_table_concurrency", 4, "how many myisam tables to load into a single destination table simultaneously")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, can contain:\n"+
		"    skipAutoIncrement(TTT): we won't add the AUTO_INCREMENT back to that table\n"+
		"    delayPrimaryKey: we won't add the primary key until after the table is populated\n"+
		"    delaySecondaryIndexes: we won't add the secondary indexes until after the table is populated\n"+
		"    useMyIsam: create the table as MyISAM, then convert it to InnoDB after population\n"+
		"    writeBinLogs: write all operations to the binlogs")

	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		log.Fatalf("multirestore requires <destination_dbname> <source_host>[/<source_dbname>]... %v", args)
	}

	startArray := strings.Split(*starts, ",")
	endArray := strings.Split(*ends, ",")
	if len(startArray) != len(endArray) || len(startArray) != subFlags.NArg()-1 {
		log.Fatalf("Need as many starts and ends as source URLs")
	}

	keyRanges := make([]key.KeyRange, len(startArray))
	for i, s := range startArray {
		var err error
		keyRanges[i], err = key.ParseKeyRangeParts(s, endArray[i])
		if err != nil {
			log.Fatalf("Invalid start or end: %v", err)
		}
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
	if err := mysqld.MultiRestore(dbName, keyRanges, sources, nil, *concurrency, *fetchConcurrency, *insertTableConcurrency, *fetchRetryCount, *strategy); err != nil {
		log.Fatalf("multirestore failed: %v", err)
	}
}

func restoreCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	dontWaitForSlaveStart := subFlags.Bool("dont_wait_for_slave_start", false, "won't wait for replication to start (useful when restoring from master server)")
	fetchConcurrency := subFlags.Int("fetch_concurrency", 3, "how many files to fetch simultaneously")
	fetchRetryCount := subFlags.Int("fetch_retry_count", 3, "how many times to retry a failed transfer")
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
	waitTime := subFlags.Duration("wait_time", mysqlctl.MysqlWaitTime, "how long to wait for shutdown")
	subFlags.Parse(args)

	if mysqlErr := mysqld.Shutdown(true, *waitTime); mysqlErr != nil {
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
	slaveStartRequired := subFlags.Bool("slave_start", false, "will restart replication")
	readWrite := subFlags.Bool("read_write", false, "will make the server read-write")
	subFlags.Parse(args)

	err := mysqld.SnapshotSourceEnd(*slaveStartRequired, !(*readWrite), true, map[string]string{})
	if err != nil {
		log.Fatalf("snapshotsourceend failed: %v", err)
	}
}

func startCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	waitTime := subFlags.Duration("wait_time", mysqlctl.MysqlWaitTime, "how long to wait for startup")
	subFlags.Parse(args)

	if err := mysqld.Start(*waitTime); err != nil {
		log.Fatalf("failed start mysql: %v", err)
	}
}

func teardownCmd(mysqld *mysqlctl.Mysqld, subFlags *flag.FlagSet, args []string) {
	force := subFlags.Bool("force", false, "will remove the root directory even if mysqld shutdown fails")
	subFlags.Parse(args)

	if err := mysqld.Teardown(*force); err != nil {
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
	command{"multirestore", multiRestoreCmd,
		"[-force] [-concurrency=3] [-fetch_concurrency=4] [-insert_table_concurrency=4] [-fetch_retry_count=3] [-starts=start1,start2,...] [-ends=end1,end2,...] [-strategy=] <destination_dbname> <source_host>[/<source_dbname>]...",
		"Restores a snapshot form multiple hosts"},
	command{"multisnapshot", multisnapshotCmd, "[-concurrency=8] [-spec='-'] [-tables=''] [-skip_slave_restart] [-maximum_file_size=134217728] <db name> <key name>",
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
	dbconfigs.RegisterFlags()
	flag.Parse()

	tabletAddr = fmt.Sprintf("%v:%v", "localhost", *port)
	mycnf := mysqlctl.NewMycnf(uint32(*tabletUid), *mysqlPort)

	if *mysqlSocket != "" {
		mycnf.SocketFile = *mysqlSocket
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile)
	if err != nil {
		log.Fatalf("%v", err)
	}
	mysqld := mysqlctl.NewMysqld(mycnf, &dbcfgs.Dba, &dbcfgs.Repl)

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
