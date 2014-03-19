// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vtprimecache is a standalone version of primecache
package main

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/primecache"
)

var (
	mysqlSocketFile = flag.String("mysql_socket_file", "", "location of the mysql socket file")
	relayLogsPath   = flag.String("relay_logs_path", "", "location of the relay logs for the mysql instance")
	sleepDuration   = flag.Duration("sleep_duration", 1*time.Second, "how long to sleep in between each run")
	workerCount     = flag.Int("worker_count", 4, "number of connections to use to talk to mysql")
)

func main() {
	dbconfigs.RegisterFlags()
	flag.Parse()

	dbcfgs, err := dbconfigs.Init(*mysqlSocketFile)
	if err != nil {
		log.Fatalf("Failed to init dbconfigs: %v", err)
	}

	pc := primecache.NewPrimeCache(dbcfgs, *relayLogsPath)
	pc.WorkerCount = *workerCount
	pc.SleepDuration = *sleepDuration

	pc.Loop()
}
