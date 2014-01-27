// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"
	"time"

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
	port          = flag.Int("port", 6509, "port for the server")
	tabletPath    = flag.String("tablet-path", "", "tablet alias or path to zk node representing the tablet")
	mycnfFile     = flag.String("mycnf-file", "", "my.cnf file")
	overridesFile = flag.String("schema-override", "", "schema overrides file")

	securePort = flag.Int("secure-port", 0, "port for the secure server")
	cert       = flag.String("cert", "", "cert file")
	key        = flag.String("key", "", "key file")
	caCert     = flag.String("ca-cert", "", "ca-cert file")

	agent *tabletmanager.ActionAgent
)

func main() {
	dbCredentialsFile := dbconfigs.RegisterCommonFlags()
	flag.Parse()

	servenv.Init()

	tabletAlias := vttablet.TabletParamToTabletAlias(*tabletPath)

	if *mycnfFile == "" {
		*mycnfFile = mysqlctl.MycnfFile(tabletAlias.Uid)
	}

	mycnf, err := mysqlctl.ReadMycnf(*mycnfFile)
	if err != nil {
		log.Fatalf("mycnf read failed: %v", err)
	}

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, *dbCredentialsFile)
	if err != nil {
		log.Warning(err)
	}

	ts.InitQueryService()
	binlog.RegisterUpdateStreamService(mycnf)

	// Depends on both query and updateStream.
	agent, err = vttablet.InitAgent(tabletAlias, dbcfgs, mycnf, *dbCredentialsFile, *port, *securePort, *overridesFile)
	if err != nil {
		log.Fatal(err)
	}

	vttablet.HttpHandleSnapshots(mycnf, tabletAlias.Uid)
	servenv.OnClose(func() {
		time.Sleep(5 * time.Millisecond)
		ts.DisallowQueries()
		binlog.DisableUpdateStreamService()
		topo.CloseServers()
		agent.Stop()
	})
	servenv.RunSecure(*port, *securePort, *cert, *key, *caCert)
}
