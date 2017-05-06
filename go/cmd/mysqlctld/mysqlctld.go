/*
Copyright 2017 Google Inc.

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

// mysqlctld is a daemon that starts or initializes mysqld and provides an RPC
// interface for vttablet to stop and start mysqld from a different container
// without having to restart the container running mysqlctld.
package main

import (
	"flag"
	"os"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"golang.org/x/net/context"
)

var (
	// mysqld is used by the rpc implementation plugin.
	mysqld *mysqlctl.Mysqld

	mysqlPort   = flag.Int("mysql_port", 3306, "mysql port")
	tabletUID   = flag.Uint("tablet_uid", 41983, "tablet uid")
	mysqlSocket = flag.String("mysql_socket", "", "path to the mysql socket")

	// mysqlctl init flags
	waitTime      = flag.Duration("wait_time", 5*time.Minute, "how long to wait for mysqld startup or shutdown")
	initDBSQLFile = flag.String("init_db_sql_file", "", "path to .sql file to run after mysql_install_db")
)

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterDefaultSocketFileFlags()
}

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	// mysqlctld only starts and stops mysql, only needs dba.
	dbconfigFlags := dbconfigs.DbaConfig
	dbconfigs.RegisterFlags(dbconfigFlags)
	flag.Parse()

	// We'll register this OnTerm handler before mysqld starts, so we get notified
	// if mysqld dies on its own without us (or our RPC client) telling it to.
	mysqldTerminated := make(chan struct{})
	onTermFunc := func() {
		close(mysqldTerminated)
	}

	// Start or Init mysqld as needed.
	ctx, cancel := context.WithTimeout(context.Background(), *waitTime)
	mycnfFile := mysqlctl.MycnfFile(uint32(*tabletUID))
	if _, statErr := os.Stat(mycnfFile); os.IsNotExist(statErr) {
		// Generate my.cnf from scratch and use it to find mysqld.
		log.Infof("mycnf file (%s) doesn't exist, initializing", mycnfFile)

		var err error
		mysqld, err = mysqlctl.CreateMysqld(uint32(*tabletUID), *mysqlSocket, int32(*mysqlPort), dbconfigFlags)
		if err != nil {
			log.Errorf("failed to initialize mysql config: %v", err)
			exit.Return(1)
		}
		mysqld.OnTerm(onTermFunc)

		if err := mysqld.Init(ctx, *initDBSQLFile); err != nil {
			log.Errorf("failed to initialize mysql data dir and start mysqld: %v", err)
			exit.Return(1)
		}
	} else {
		// There ought to be an existing my.cnf, so use it to find mysqld.
		log.Infof("mycnf file (%s) already exists, starting without init", mycnfFile)

		var err error
		mysqld, err = mysqlctl.OpenMysqld(uint32(*tabletUID), dbconfigFlags)
		if err != nil {
			log.Errorf("failed to find mysql config: %v", err)
			exit.Return(1)
		}
		mysqld.OnTerm(onTermFunc)

		err = mysqld.RefreshConfig()
		if err != nil {
			log.Errorf("failed to refresh config: %v", err)
			exit.Return(1)
		}

		if err := mysqld.Start(ctx); err != nil {
			log.Errorf("failed to start mysqld: %v", err)
			exit.Return(1)
		}
	}
	cancel()

	servenv.Init()
	defer servenv.Close()

	// Take mysqld down with us on SIGTERM before entering lame duck.
	servenv.OnTermSync(func() {
		log.Infof("mysqlctl received SIGTERM, shutting down mysqld first")
		ctx := context.Background()
		mysqld.Shutdown(ctx, true)
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
