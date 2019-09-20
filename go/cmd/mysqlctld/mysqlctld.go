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

// mysqlctld is a daemon that starts or initializes mysqld and provides an RPC
// interface for vttablet to stop and start mysqld from a different container
// without having to restart the container running mysqlctld.
package main

import (
	"flag"
	"os"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	// mysqld is used by the rpc implementation plugin.
	mysqld *mysqlctl.Mysqld
	cnf    *mysqlctl.Mycnf

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
	dbconfigs.RegisterFlags(dbconfigs.Dba)
	servenv.ParseFlags("mysqlctld")

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
		mysqld, cnf, err = mysqlctl.CreateMysqldAndMycnf(uint32(*tabletUID), *mysqlSocket, int32(*mysqlPort))
		if err != nil {
			log.Errorf("failed to initialize mysql config: %v", err)
			exit.Return(1)
		}
		mysqld.OnTerm(onTermFunc)

		if err := mysqld.Init(ctx, cnf, *initDBSQLFile); err != nil {
			log.Errorf("failed to initialize mysql data dir and start mysqld: %v", err)
			exit.Return(1)
		}
	} else {
		// There ought to be an existing my.cnf, so use it to find mysqld.
		log.Infof("mycnf file (%s) already exists, starting without init", mycnfFile)

		var err error
		mysqld, cnf, err = mysqlctl.OpenMysqldAndMycnf(uint32(*tabletUID))
		if err != nil {
			log.Errorf("failed to find mysql config: %v", err)
			exit.Return(1)
		}
		mysqld.OnTerm(onTermFunc)

		err = mysqld.RefreshConfig(ctx, cnf)
		if err != nil {
			log.Errorf("failed to refresh config: %v", err)
			exit.Return(1)
		}

		// check if we were interrupted during a previous restore
		if !mysqlctl.RestoreWasInterrupted(cnf) {
			if err := mysqld.Start(ctx, cnf); err != nil {
				log.Errorf("failed to start mysqld: %v", err)
				exit.Return(1)
			}
		} else {
			log.Infof("found interrupted restore, not starting mysqld")
		}
	}
	cancel()

	servenv.Init()
	defer servenv.Close()

	// Take mysqld down with us on SIGTERM before entering lame duck.
	servenv.OnTermSync(func() {
		log.Infof("mysqlctl received SIGTERM, shutting down mysqld first")
		ctx := context.Background()
		mysqld.Shutdown(ctx, cnf, true)
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
