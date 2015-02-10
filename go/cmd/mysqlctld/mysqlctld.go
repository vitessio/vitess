// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// mysqlctld is a daemon that starts or initializes mysqld and provides an RPC
// interface for vttablet to stop and start mysqld from a different container
// without having to restart the container running mysqlctld.
package main

import (
	"flag"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
)

var (
	// mysqld is used by the rpc implementation plugin.
	mysqld *mysqlctl.Mysqld

	mysqlPort   = flag.Int("mysql_port", 3306, "mysql port")
	tabletUID   = flag.Uint("tablet_uid", 41983, "tablet uid")
	mysqlSocket = flag.String("mysql_socket", "", "path to the mysql socket")

	// mysqlctl init flags
	waitTime         = flag.Duration("wait_time", mysqlctl.MysqlWaitTime, "how long to wait for mysqld startup or shutdown")
	bootstrapArchive = flag.String("bootstrap_archive", "mysql-db-dir.tbz", "name of bootstrap archive within vitess/data/bootstrap directory")
	skipSchema       = flag.Bool("skip_schema", false, "don't apply initial schema")
)

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterDefaultSocketFileFlags()

	// Enable only for unix socket by default, can be changed on the command-line.
	servenv.ServiceMap["bsonrpc-unix-mysqlctl"] = true
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	flags := dbconfigs.AppConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
	dbconfigs.RegisterFlags(flags)
	flag.Parse()

	mycnf := mysqlctl.NewMycnf(uint32(*tabletUID), *mysqlPort)
	if *mysqlSocket != "" {
		mycnf.SocketFile = *mysqlSocket
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, flags)
	if err != nil {
		log.Errorf("%v", err)
		exit.Return(255)
	}
	mysqld = mysqlctl.NewMysqld("Dba", "App", mycnf, &dbcfgs.Dba, &dbcfgs.App.ConnectionParams, &dbcfgs.Repl)

	// Register OnTerm handler before mysqld starts, so we get notified if mysqld
	// dies on its own without us (or our RPC client) telling it to.
	mysqldTerminated := make(chan struct{})
	mysqld.OnTerm(func() {
		close(mysqldTerminated)
	})

	// Start or Init mysqld as needed.
	if _, err = os.Stat(mycnf.DataDir); os.IsNotExist(err) {
		log.Infof("mysql data dir (%s) doesn't exist, initializing", mycnf.DataDir)
		mysqld.Init(*waitTime, *bootstrapArchive, *skipSchema)
	} else {
		log.Infof("mysql data dir (%s) already exists, starting without init", mycnf.DataDir)
		mysqld.Start(*waitTime)
	}

	servenv.Init()
	defer servenv.Close()

	// Take mysqld down with us on SIGTERM before entering lame duck.
	servenv.OnTerm(func() {
		log.Infof("mysqlctl received SIGTERM, shutting down mysqld first")
		mysqld.Shutdown(false, 0)
	})

	// Start RPC server and wait for SIGTERM.
	mysqlctldTerminated := make(chan struct{})
	go func() {
		servenv.RunDefault()
		close(mysqlctldTerminated)
	}()

	select {
	case <-mysqldTerminated:
		log.Infof("mysqld shut down on its own, exiting mysqlctld")
	case <-mysqlctldTerminated:
		log.Infof("mysqlctld shut down gracefully")
	}
}
