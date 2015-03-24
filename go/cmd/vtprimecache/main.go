// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vtprimecache is a standalone version of primecache
package main

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/primecache"
)

var (
	mysqlSocketFile = flag.String("mysql_socket_file", "", "location of the mysql socket file")
	relayLogsPath   = flag.String("relay_logs_path", "", "location of the relay logs for the mysql instance")
	sleepDuration   = flag.Duration("sleep_duration", 1*time.Second, "how long to sleep in between each run")
	workerCount     = flag.Int("worker_count", 4, "number of connections to use to talk to mysql")
)

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	flags := dbconfigs.AppConfig | dbconfigs.DbaConfig
	dbconfigs.RegisterFlags(flags)
	flag.Parse()

	dbcfgs, err := dbconfigs.Init(*mysqlSocketFile, flags)
	if err != nil {
		log.Errorf("Failed to init dbconfigs: %v", err)
		exit.Return(1)
	}

	pc := primecache.NewPrimeCache(dbcfgs, *relayLogsPath, mysql.Connect)
	pc.WorkerCount = *workerCount
	pc.SleepDuration = *sleepDuration

	pc.Loop()
}
