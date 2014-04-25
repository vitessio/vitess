// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/binlog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	ts "github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet"
)

var (
	tabletPath     = flag.String("tablet-path", "", "tablet alias or path to zk node representing the tablet")
	mycnfFile      = flag.String("mycnf-file", "", "my.cnf file")
	enableRowcache = flag.Bool("enable-rowcache", false, "enable rowcacche")
	overridesFile  = flag.String("schema-override", "", "schema overrides file")

	agent *tabletmanager.ActionAgent
)

func main() {
	dbconfigs.RegisterFlags()
	flag.Parse()
	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Fatalf("vttablet doesn't take any positional arguments")
	}

	servenv.Init()

	if *tabletPath == "" {
		log.Fatalf("tabletPath required")
	}
	tabletAlias := vttablet.TabletParamToTabletAlias(*tabletPath)

	if *mycnfFile == "" {
		*mycnfFile = mysqlctl.MycnfFile(tabletAlias.Uid)
	}

	mycnf, err := mysqlctl.ReadMycnf(*mycnfFile)
	if err != nil {
		log.Fatalf("mycnf read failed: %v", err)
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile)
	if err != nil {
		log.Warning(err)
	}
	dbcfgs.App.EnableRowcache = *enableRowcache

	ts.InitQueryService()
	binlog.RegisterUpdateStreamService(mycnf)

	// Depends on both query and updateStream.
	agent, err = vttablet.InitAgent(tabletAlias, dbcfgs, mycnf, *servenv.Port, *servenv.SecurePort, *overridesFile)
	if err != nil {
		log.Fatal(err)
	}

	vttablet.HttpHandleSnapshots(mycnf, tabletAlias.Uid)
	servenv.OnTerm(func() {
		ts.DisallowQueries()
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
