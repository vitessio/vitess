// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver"
)

var (
	// BaseConfig is the base config of the server.
	BaseConfig tabletserver.Config
	// Target is the target info for the server.
	Target querypb.Target
	// Server is the TabletServer for the framework.
	Server *tabletserver.TabletServer
	// ServerAddress is the http URL for the server.
	ServerAddress string
)

// StartServer starts the server and initializes
// all the global variables. This function should only be called
// once at the beginning of the test.
func StartServer(connParams sqldb.ConnParams) error {
	dbcfgs := dbconfigs.DBConfigs{
		App: connParams,
	}

	mysqld := mysqlctl.NewMysqld(
		"Dba",
		"App",
		&mysqlctl.Mycnf{},
		&dbcfgs.Dba,
		&dbcfgs.App,
		&dbcfgs.Repl)

	BaseConfig = tabletserver.DefaultQsConfig
	BaseConfig.EnableAutoCommit = true
	BaseConfig.StrictTableAcl = true

	Target = querypb.Target{
		Keyspace:   "vttest",
		Shard:      "0",
		TabletType: topodatapb.TabletType_MASTER,
	}

	Server = tabletserver.NewTabletServer(BaseConfig)
	Server.Register()
	err := Server.StartService(Target, dbcfgs, mysqld)
	if err != nil {
		return fmt.Errorf("could not start service: %v\n", err)
	}

	// Start http service.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return fmt.Errorf("could not start listener: %v\n", err)
	}
	ServerAddress = fmt.Sprintf("http://%s", ln.Addr().String())
	go http.Serve(ln, nil)
	for {
		time.Sleep(10 * time.Millisecond)
		response, err := http.Get(fmt.Sprintf("%s/debug/vars", ServerAddress))
		if err == nil {
			response.Body.Close()
			break
		}
	}
	return nil
}

// StopServer must be called once all the tests are done.
func StopServer() {
	Server.StopService()
}
