// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/dbconfigs"
	"code.google.com/p/vitess/go/vt/key"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"flag"
	"fmt"
	"log"
	"os"
)

var port = flag.Int("port", 6612, "vtocc port")
var force = flag.Bool("force", false, "force action")
var mysqlPort = flag.Int("mysql-port", 3306, "mysql port")
var tabletUid = flag.Uint("tablet-uid", 41983, "tablet uid")
var logLevel = flag.String("log.level", "WARNING", "set log level")

func main() {
	flag.Parse()
	logger := relog.New(os.Stderr, "",
		log.Ldate|log.Lmicroseconds|log.Lshortfile,
		relog.LogNameToLogLevel(*logLevel))
	relog.SetLogger(logger)

	tabletAddr := fmt.Sprintf("%v:%v", "localhost", *port)
	mycnf := mysqlctl.NewMycnf(uint32(*tabletUid), *mysqlPort, mysqlctl.VtReplParams{})
	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile)
	if err != nil {
		relog.Fatal("%v", err)
	}
	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)

	action := flag.Arg(0)
	switch action {
	case "init":
		if mysqlErr := mysqlctl.Init(mysqld); mysqlErr != nil {
			relog.Fatal("failed init mysql: %v", mysqlErr)
		}
	case "partialrestore":
		rs, err := mysqlctl.ReadSplitSnapshotManifest(flag.Arg(1))
		if err == nil {
			err = mysqld.RestoreFromPartialSnapshot(rs)
		}
		if err != nil {
			relog.Fatal("partialrestore failed: %v", err)
		}
	case "partialsnapshot":
		subFlags := flag.NewFlagSet("partialsnapshot", flag.ExitOnError)
		start := subFlags.String("start", "", "start of the key range")
		end := subFlags.String("end", "", "end of the key range")

		if err := subFlags.Parse(flag.Args()[1:]); err != nil {
			flag.Usage()
			os.Exit(1)
		}

		if len(subFlags.Args()) != 2 {
			relog.Fatal("action %v requires <db name> <key name>", flag.Arg(0))
		}

		filename, err := mysqld.CreateSplitSnapshot(subFlags.Arg(0), subFlags.Arg(1), key.HexKeyspaceId(*start), key.HexKeyspaceId(*end), tabletAddr, false)
		if err != nil {
			relog.Fatal("partialsnapshot failed: %v", err)
		} else {
			relog.Info("manifest location: %v", filename)
		}
	case "restore":
		rs, err := mysqlctl.ReadSnapshotManifest(flag.Arg(1))
		if err == nil {
			err = mysqld.RestoreFromSnapshot(rs)
		}
		if err != nil {
			relog.Fatal("restore failed: %v", err)
		}
	case "shutdown":
		if mysqlErr := mysqlctl.Shutdown(mysqld, true); mysqlErr != nil {
			relog.Fatal("failed shutdown mysql: %v", mysqlErr)
		}
	case "snapshot":
		filename, err := mysqld.CreateSnapshot(flag.Arg(1), tabletAddr, false)
		if err != nil {
			relog.Fatal("snapshot failed: %v", err)
		} else {
			relog.Info("manifest location: %v", filename)
		}
	case "start":
		if mysqlErr := mysqlctl.Start(mysqld); mysqlErr != nil {
			relog.Fatal("failed start mysql: %v", mysqlErr)
		}
	case "teardown":
		if mysqlErr := mysqlctl.Teardown(mysqld, *force); mysqlErr != nil {
			relog.Fatal("failed teardown mysql (forced? %v): %v", *force, mysqlErr)
		}
	default:
		relog.Fatal("invalid action: %v", action)
	}
}
