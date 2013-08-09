// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/proc"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/auth"
	_ "github.com/youtube/vitess/go/snitch"
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
	authConfig    = flag.String("auth-credentials", "", "name of file containing auth credentials")
	customrules   = flag.String("customrules", "", "custom query rules file")
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
	defer servenv.Close()

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

	vttablet.InitQueryService(dbcfgs)
	mysqlctl.RegisterUpdateStreamService(mycnf)

	// Depends on both query and updateStream.
	ts.RegisterCacheInvalidator()

	// Depends on both query and updateStream.
	if err := vttablet.InitAgent(tabletAlias, dbcfgs, mycnf, *dbConfigsFile, *dbCredentialsFile, *port, *securePort, *mycnfFile, *customrules, *overridesFile); err != nil {
		log.Fatal(err)
	}

	rpc.HandleHTTP()

	// NOTE(szopa): Changing credentials requires a server
	// restart.
	if *authConfig != "" {
		if err := auth.LoadCredentials(*authConfig); err != nil {
			log.Errorf("could not load authentication credentials, not starting rpc servers: %v", err)
		}
		vttablet.ServeAuthRPC()
	}

	vttablet.ServeRPC()

	vttablet.HttpHandleSnapshots(mycnf, tabletAlias.Uid)

	l, err := proc.Listen(fmt.Sprintf("%v", *port))
	if err != nil {
		log.Fatal(err)
	}
	go http.Serve(l, nil)

	if *securePort != 0 {
		log.Infof("listening on secure port %v", *securePort)
		vttablet.SecureServe(fmt.Sprintf(":%d", *securePort), *cert, *key, *caCert)
	}

	log.Infof("started vttablet %v", *port)
	s := proc.Wait()

	// A SIGUSR1 means that we're restarting
	if s == syscall.SIGUSR1 {
		// Give some time for the other process
		// to pick up the listeners
		log.Info("Exiting on SIGUSR1")
		time.Sleep(5 * time.Millisecond)
		ts.DisallowQueries(true)
	} else {
		log.Info("Exiting on SIGTERM")
		ts.DisallowQueries(false)
	}
	mysqlctl.DisableUpdateStreamService()
	topo.CloseServers()
	vttablet.CloseAgent()
}
