// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"time"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/vttest"
)

var (
	// BaseConfig is the base config of the default server.
	BaseConfig tabletserver.Config
	// Target is the target info for the default server.
	Target query.Target
	// DefaultServer is the default server.
	DefaultServer *tabletserver.TabletServer
	// ServerAddress is the http URL for the default server.
	ServerAddress string
)

// StartDefaultServer starts the default server and initializes
// all the global variables. This function should only be called
// once at the beginning of the test.
func StartDefaultServer(connParams sqldb.ConnParams) error {
	dbcfgs := dbconfigs.DBConfigs{
		App: dbconfigs.DBConfig{
			ConnParams:        connParams,
			Keyspace:          "vttest",
			Shard:             "0",
			EnableRowcache:    true,
			EnableInvalidator: false,
		},
	}

	mysqld := mysqlctl.NewMysqld(
		"Dba",
		"App",
		&mysqlctl.Mycnf{},
		&dbcfgs.Dba,
		&dbcfgs.App.ConnParams,
		&dbcfgs.Repl)

	BaseConfig = tabletserver.DefaultQsConfig
	BaseConfig.RowCache.Binary = vttest.MemcachedPath()
	BaseConfig.RowCache.Socket = path.Join(os.TempDir(), "memcache.sock")
	BaseConfig.RowCache.Connections = 100
	BaseConfig.EnableAutoCommit = true

	Target = query.Target{
		Keyspace:   "vttest",
		Shard:      "0",
		TabletType: topodata.TabletType_MASTER,
	}

	DefaultServer = tabletserver.NewTabletServer(BaseConfig)
	DefaultServer.Register()
	err := DefaultServer.StartService(&Target, &dbcfgs, nil, mysqld)
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

// StopDefaultServer must be called once all the tests are done.
func StopDefaultServer() {
	DefaultServer.StopService()
}

// NewServer starts a new unregistered TabletServer.
// You have to end it with StopService once testing is done.
func NewServer(config tabletserver.Config, connParams sqldb.ConnParams, enableRowcache bool) (*tabletserver.TabletServer, error) {
	dbcfgs := dbconfigs.DBConfigs{
		App: dbconfigs.DBConfig{
			ConnParams:        connParams,
			Keyspace:          "vttest",
			Shard:             "0",
			EnableRowcache:    enableRowcache,
			EnableInvalidator: false,
		},
	}

	mysqld := mysqlctl.NewMysqld(
		"Dba",
		"App",
		&mysqlctl.Mycnf{},
		&dbcfgs.Dba,
		&dbcfgs.App.ConnParams,
		&dbcfgs.Repl)

	Target = query.Target{
		Keyspace:   "vttest",
		Shard:      "0",
		TabletType: topodata.TabletType_MASTER,
	}

	server := tabletserver.NewTabletServer(config)
	err := server.StartService(&Target, &dbcfgs, nil, mysqld)
	if err != nil {
		return nil, err
	}
	return server, nil
}
