/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vtgate/fakerpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// Target is the target info for the server.
	Target querypb.Target
	// Server is the TabletServer for the framework.
	Server *tabletserver.TabletServer
	// ServerAddress is the http URL for the server.
	ServerAddress string
	// ResolveChan is the channel that sends dtids that are to be resolved.
	ResolveChan = make(chan string, 1)
	// Mysqld is for sending direct commands to mysql
	Mysqld *mysqlctl.Mysqld
)

// StartServer starts the server with configs set for endtoend testing.
func StartServer(connParams, connAppDebugParams mysql.ConnParams, dbName string) error {
	config := tabletenv.DefaultQsConfig
	config.EnableAutoCommit = true
	config.StrictTableACL = true
	config.TwoPCEnable = true
	config.TwoPCAbandonAge = 1
	config.TwoPCCoordinatorAddress = "fake"
	config.EnableHotRowProtection = true

	return startServer(nil, config, connParams, connAppDebugParams, dbName, "", "")
}

// StartFullServer is like StartServer, but with more configurable options.
func StartFullServer(ts *topo.Server, config tabletenv.TabletConfig, connParams, connAppDebugParams mysql.ConnParams, dbName, keyspaceName, alias string) error {
	return startServer(ts, config, connParams, connAppDebugParams, dbName, keyspaceName, alias)
}

func startServer(ts *topo.Server, config tabletenv.TabletConfig, connParams, connAppDebugParams mysql.ConnParams, dbName, keyspaceName, alias string) error {
	// Setup a fake vtgate server.
	protocol := "resolveTest"
	*vtgateconn.VtgateProtocol = protocol
	vtgateconn.RegisterDialer(protocol, func(context.Context, string) (vtgateconn.Impl, error) {
		return &txResolver{
			FakeVTGateConn: fakerpcvtgateconn.FakeVTGateConn{},
		}, nil
	})

	dbcfgs := dbconfigs.NewTestDBConfigs(connParams, connAppDebugParams, dbName)
	Mysqld = mysqlctl.NewMysqld(dbcfgs)

	Target = querypb.Target{
		Keyspace:   keyspaceName,
		Shard:      "0",
		TabletType: topodatapb.TabletType_MASTER,
	}

	if ts == nil {
		Server = tabletserver.NewTabletServerWithNilTopoServer(config)
	} else {
		talias, err := topoproto.ParseTabletAlias(alias)
		if err != nil {
			return err
		}
		Server = tabletserver.NewTabletServer(config, ts, *talias)
	}

	Server.Register()
	if err := Server.StartService(Target, dbcfgs); err != nil {
		return vterrors.Wrap(err, "could not start service")
	}

	// Start http service.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return vterrors.Wrap(err, "could not start listener")
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
	Mysqld.Close()
	Server.StopService()
}

// txReolver transmits dtids to be resolved through ResolveChan.
type txResolver struct {
	fakerpcvtgateconn.FakeVTGateConn
}

func (conn *txResolver) ResolveTransaction(ctx context.Context, dtid string) error {
	select {
	case ResolveChan <- dtid:
	default:
	}
	return nil
}
