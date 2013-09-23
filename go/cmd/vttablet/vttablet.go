// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
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
)

func main() {
	dbConfigsFile, dbCredentialsFile := dbconfigs.RegisterCommonFlags()
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

	dbcfgs, err := dbconfigs.Init(mycnf.SocketFile, *dbConfigsFile, *dbCredentialsFile)
	if err != nil {
		log.Warning(err)
	}

	ts.InitQueryService()
	mysqlctl.RegisterUpdateStreamService(mycnf)

	// Depends on both query and updateStream.
	ts.RegisterCacheInvalidator()

	// Depends on both query and updateStream.
	if err := vttablet.InitAgent(tabletAlias, dbcfgs, mycnf, *dbConfigsFile, *dbCredentialsFile, *port, *securePort, *mycnfFile, *overridesFile); err != nil {
		log.Fatal(err)
	}

	vttablet.HttpHandleSnapshots(mycnf, tabletAlias.Uid)
	servenv.OnClose(func() {
		time.Sleep(5 * time.Millisecond)
		ts.DisallowQueries()
		mysqlctl.DisableUpdateStreamService()
		topo.CloseServers()
		vttablet.CloseAgent()
	})
	servenv.RunSecure(*port, *securePort, *cert, *key, *caCert)
}
