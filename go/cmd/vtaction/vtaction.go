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
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	port       = flag.Int("port", 0, "port for debug http server")
	action     = flag.String("action", "", "management action to perform")
	actionNode = flag.String("action-node", "", "path to zk node representing the action")
	actionGuid = flag.String("action-guid", "", "a label to help track processes")
	force      = flag.Bool("force", false, "force an action to rerun")

	mycnfFile = flag.String("mycnf-file", "/etc/my.cnf", "path to my.cnf")
)

func init() {
	stats.NewString("BinaryName").Set("vtaction")
}

func main() {
	dbCredentialsFile := dbconfigs.RegisterCommonFlags()
	flag.Parse()
	servenv.Init()
	defer servenv.Close()

	log.Infof("started vtaction %v", os.Args)

	servenv.ServeRPC()

	mycnf, mycnfErr := mysqlctl.ReadMycnf(*mycnfFile)
	if mycnfErr != nil {
		log.Fatalf("mycnf read failed: %v", mycnfErr)
	}

	log.V(6).Infof("mycnf: %v", jscfg.ToJson(mycnf))

	dbcfgs, cfErr := dbconfigs.Init(mycnf.SocketFile, *dbCredentialsFile)
	if cfErr != nil {
		log.Fatalf("%s", cfErr)
	}
	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)

	topoServer := topo.GetServer()
	defer topo.CloseServers()

	actor := tabletmanager.NewTabletActor(mysqld, mysqld, topoServer, topo.TabletAlias{})

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	bindAddr := fmt.Sprintf(":%v", *port)
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
