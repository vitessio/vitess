/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/yaml2"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"

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
	Target *querypb.Target
	// Server is the TabletServer for the framework.
	Server *tabletserver.TabletServer
	// ServerAddress is the http URL for the server.
	ServerAddress string
	// ResolveChan is the channel that sends dtids that are to be resolved.
	ResolveChan = make(chan string, 1)
	// TopoServer is the topology for the server
	TopoServer *topo.Server
)

// StartCustomServer starts the server and initializes
// all the global variables. This function should only be called
// once at the beginning of the test.
func StartCustomServer(connParams, connAppDebugParams mysql.ConnParams, dbName string, config *tabletenv.TabletConfig) error {
	// Setup a fake vtgate server.
	protocol := "resolveTest"
	vtgateconn.SetVTGateProtocol(protocol)
	vtgateconn.RegisterDialer(protocol, func(context.Context, string) (vtgateconn.Impl, error) {
		return &txResolver{
			FakeVTGateConn: fakerpcvtgateconn.FakeVTGateConn{},
		}, nil
	})

	dbcfgs := dbconfigs.NewTestDBConfigs(connParams, connAppDebugParams, dbName)

	Target = &querypb.Target{
		Keyspace:   "vttest",
		Shard:      "0",
		TabletType: topodatapb.TabletType_PRIMARY,
	}
	TopoServer = memorytopo.NewServer("")

	Server = tabletserver.NewTabletServer("", config, TopoServer, &topodatapb.TabletAlias{})
	Server.Register()
	err := Server.StartService(Target, dbcfgs, nil /* mysqld */)
	if err != nil {
		return vterrors.Wrap(err, "could not start service")
	}

	// Start http service.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return vterrors.Wrap(err, "could not start listener")
	}
	ServerAddress = fmt.Sprintf("http://%s", ln.Addr().String())
	go func() {
		err := servenv.HTTPServe(ln)
		if err != nil {
			log.Errorf("HTTPServe failed: %v", err)
		}
	}()
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

// StartServer starts the server and initializes
// all the global variables. This function should only be called
// once at the beginning of the test.
func StartServer(connParams, connAppDebugParams mysql.ConnParams, dbName string) error {
	config := tabletenv.NewDefaultConfig()
	config.StrictTableACL = true
	config.TwoPCEnable = true
	config.TwoPCAbandonAge = 1
	config.TwoPCCoordinatorAddress = "fake"
	config.HotRowProtection.Mode = tabletenv.Enable
	config.TrackSchemaVersions = true
	_ = config.GracePeriods.ShutdownSeconds.Set("2s")
	config.SignalWhenSchemaChange = true
	_ = config.Healthcheck.IntervalSeconds.Set("100ms")
	_ = config.Oltp.TxTimeoutSeconds.Set("5s")
	_ = config.Olap.TxTimeoutSeconds.Set("5s")
	config.EnableViews = true
	gotBytes, _ := yaml2.Marshal(config)
	log.Infof("Config:\n%s", gotBytes)
	return StartCustomServer(connParams, connAppDebugParams, dbName, config)
}

// StopServer must be called once all the tests are done.
func StopServer() {
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
