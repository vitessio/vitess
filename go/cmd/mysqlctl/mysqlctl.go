// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/dbconfigs"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"flag"
	"log"
	"os"
)

var port = flag.Int("port", 6612, "vtocc port")
var force = flag.Bool("force", false, "force action")
var mysqlPort = flag.Int("mysql-port", 3306, "mysql port")
var tabletUid = flag.Int("tablet-uid", 41983, "tablet uid")
var logLevel = flag.String("log.level", "WARNING", "set log level")

func main() {
	flag.Parse()
	logger := relog.New(os.Stderr, "",
		log.Ldate|log.Lmicroseconds|log.Lshortfile,
		relog.LogNameToLogLevel(*logLevel))
	relog.SetLogger(logger)

	vtRepl := mysqlctl.VtReplParams{TabletHost: "localhost", TabletPort: *port}
	mycnf := mysqlctl.NewMycnf(uint(*tabletUid), *mysqlPort, vtRepl)
	dbcfgs, err := dbconfigs.Init(mycnf)
	if err != nil {
		relog.Fatal("%s", err)
	}
	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)

	action := flag.Arg(0)
	switch action {
	case "init":
		if mysqlErr := mysqlctl.Init(mysqld); mysqlErr != nil {
			log.Fatalf("failed init mysql: %v", mysqlErr)
		}
	case "partialrestore":
		rs, err := mysqlctl.ReadSplitReplicaSource(flag.Arg(1))
		if err == nil {
			err = mysqld.RestoreFromPartialSnapshot(rs)
		}
		if err != nil {
			log.Fatalf("partialrestore failed: %v", err)
		}
	case "partialsnapshot":
		_, err := mysqld.CreateSplitReplicaSource(flag.Arg(1), flag.Arg(2), flag.Arg(3), flag.Arg(4), vtRepl.TabletAddr(), false)
		if err != nil {
			log.Fatalf("partialsnapshot failed: %v", err)
		}
	case "restore":
		rs, err := mysqlctl.ReadReplicaSource(flag.Arg(1))
		if err == nil {
			err = mysqld.RestoreFromSnapshot(rs)
		}
		if err != nil {
			log.Fatalf("restore failed: %v", err)
		}
	case "shutdown":
		if mysqlErr := mysqlctl.Shutdown(mysqld, true); mysqlErr != nil {
			log.Fatalf("failed shutdown mysql: %v", mysqlErr)
		}
	case "snapshot":
		_, err := mysqld.CreateSnapshot(flag.Arg(1), vtRepl.TabletAddr(), false)
		if err != nil {
			log.Fatalf("snapshot failed: %v", err)
		}
	case "start":
		if mysqlErr := mysqlctl.Start(mysqld); mysqlErr != nil {
			log.Fatalf("failed start mysql: %v", mysqlErr)
		}
	case "teardown":
		if mysqlErr := mysqlctl.Teardown(mysqld, *force); mysqlErr != nil {
			log.Fatalf("failed teardown mysql (forced? %v): %v", *force, mysqlErr)
		}
	default:
		log.Fatalf("invalid action: %v", action)
	}
}
