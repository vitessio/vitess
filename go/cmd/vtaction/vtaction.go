// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	_ "github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager/actor"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	action     = flag.String("action", "", "management action to perform")
	actionNode = flag.String("action-node", "", "path to zk node representing the action")
	actionGuid = flag.String("action-guid", "", "a label to help track processes")
	force      = flag.Bool("force", false, "force an action to rerun")
)

func init() {
	servenv.RegisterDefaultFlags()
	stats.NewString("BinaryName").Set("vtaction")
}

func main() {
	dbconfigs.RegisterFlags()
	mysqlctl.RegisterFlags()
	flag.Parse()
	servenv.Init()
	defer servenv.Close()

	log.Infof("started vtaction %v", os.Args)

	servenv.ServeRPC()

	mycnf, mycnfErr := mysqlctl.NewMycnfFromFlags(0)
	if mycnfErr != nil {
		log.Fatalf("mycnf read failed: %v", mycnfErr)
	}

	log.V(6).Infof("mycnf: %v", jscfg.ToJson(mycnf))

	dbcfgs, cfErr := dbconfigs.Init(mycnf.SocketFile)
	if cfErr != nil {
		log.Fatalf("%s", cfErr)
	}
	mysqld := mysqlctl.NewMysqld("Dba", mycnf, &dbcfgs.Dba, &dbcfgs.Repl)
	defer mysqld.Close()

	topoServer := topo.GetServer()
	defer topo.CloseServers()

	actor := actor.NewTabletActor(mysqld, mysqld, topoServer, topo.TabletAlias{})

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	bindAddr := fmt.Sprintf(":%v", *servenv.Port)
	httpServer := &http.Server{Addr: bindAddr}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Errorf("httpServer.ListenAndServe err: %v", err)
		}
	}()

	actionErr := actor.HandleAction(*actionNode, *action, *actionGuid, *force)
	if actionErr != nil {
		log.Fatalf("action error: %v", actionErr)
	}

	log.Infof("finished vtaction %v", os.Args)
}
