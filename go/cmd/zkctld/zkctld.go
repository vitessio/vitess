// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// zkctld is a daemon that starts or initializes ZooKeeper with Vitess-specific
// configuration. It will stay running as long as the underlying ZooKeeper
// server, and will pass along SIGTERM.
package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/zk/zkctl"
)

var (
	zkCfg = flag.String("zk.cfg", "6@<hostname>:3801:3802:3803",
		"zkid@server1:leaderPort1:electionPort1:clientPort1,...)")
	myId = flag.Uint("zk.myid", 0,
		"which server do you want to be? only needed when running multiple instance on one box, otherwise myid is implied by hostname")
)

func main() {
	defer exit.Recover()
	defer logutil.Flush()

	flag.Parse()

	zkConfig := zkctl.MakeZkConfigFromString(*zkCfg, uint32(*myId))
	zkd := zkctl.NewZkd(zkConfig)

	if zkd.Inited() {
		log.Infof("already initialized, starting without init...")
		if err := zkd.Start(); err != nil {
			log.Errorf("failed start: %v", err)
			exit.Return(255)
		}
	} else {
		log.Infof("initializing...")
		if err := zkd.Init(); err != nil {
			log.Errorf("failed init: %v", err)
			exit.Return(255)
		}
	}

	log.Infof("waiting for signal or server shutdown...")
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-zkd.Done():
		log.Infof("server shut down on its own")
	case <-sig:
		log.Infof("signal received, shutting down server")
		zkd.Shutdown()
	}
}
