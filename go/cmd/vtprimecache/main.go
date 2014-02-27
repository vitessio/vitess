// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vtprimecache is a standalone version of primecache
package main

import (
	"flag"
	"path"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/primecache"
)

var (
	mycnfFile   = flag.String("mycnf_file", "", "my.cnf file")
	workerCount = flag.Int("worker_count", 4, "number of connections to use to talk to mysql")
)

func main() {
	dbconfigs.RegisterFlags()
	flag.Parse()

	mycnf, err := mysqlctl.ReadMycnf(*mycnfFile)
	if err != nil {
		log.Fatalf("mycnf read failed: %v", err)
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile)
	if err != nil {
		log.Warning(err)
	}

	pc := primecache.NewPrimeCache(dbcfgs, path.Dir(mycnf.RelayLogPath))
	pc.WorkerCount = *workerCount

	pc.Loop()
}
