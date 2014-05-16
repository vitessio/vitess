// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	tabletPath     = flag.String("tablet-path", "", "tablet alias or path to zk node representing the tablet")
	enableRowcache = flag.Bool("enable-rowcache", false, "enable rowcacche")
	overridesFile  = flag.String("schema-override", "", "schema overrides file")

	agent *tabletmanager.ActionAgent
)

// tabletParamToTabletAlias takes either an old style ZK tablet path or a
// new style tablet alias as a string, and returns a TabletAlias.
func tabletParamToTabletAlias(param string) topo.TabletAlias {
	if param[0] == '/' {
		// old zookeeper path, convert to new-style string tablet alias
		zkPathParts := strings.Split(param, "/")
		if len(zkPathParts) != 6 || zkPathParts[0] != "" || zkPathParts[1] != "zk" || zkPathParts[3] != "vt" || zkPathParts[4] != "tablets" {
			log.Fatalf("Invalid tablet path: %v", param)
		}
		param = zkPathParts[2] + "-" + zkPathParts[5]
	}
	result, err := topo.ParseTabletAliasString(param)
	if err != nil {
		log.Fatalf("Invalid tablet alias %v: %v", param, err)
	}
	return result
}

func main() {
	dbconfigs.RegisterFlags()
	mysqlctl.RegisterFlags()
	flag.Parse()
	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Fatalf("vttablet doesn't take any positional arguments")
	}

	servenv.Init()

	if *tabletPath == "" {
		log.Fatalf("tabletPath required")
	}
	tabletAlias := tabletParamToTabletAlias(*tabletPath)

	mycnf, err := mysqlctl.NewMycnfFromFlags(tabletAlias.Uid)
	if err != nil {
		log.Fatalf("mycnf read failed: %v", err)
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile)
	if err != nil {
		log.Warning(err)
	}
	dbcfgs.App.EnableRowcache = *enableRowcache

	tabletserver.InitQueryService()
	binlog.RegisterUpdateStreamService(mycnf)

	// Depends on both query and updateStream.
	agent, err = tabletmanager.NewActionAgent(tabletAlias, dbcfgs, mycnf, *servenv.Port, *servenv.SecurePort, *overridesFile)
	if err != nil {
		log.Fatal(err)
	}

	tabletmanager.HttpHandleSnapshots(mycnf, tabletAlias.Uid)
	servenv.OnTerm(func() {
		tabletserver.DisallowQueries()
		binlog.DisableUpdateStreamService()
		agent.Stop()
	})
	servenv.OnClose(func() {
		// We will still use the topo server during lameduck period
		// to update our state, so closing it in OnClose()
		topo.CloseServers()
	})
	servenv.Run()
}
