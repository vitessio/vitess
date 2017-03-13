// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package framework

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vtgate/fakerpcvtgateconn"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
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
)

// StartServer starts the server and initializes
// all the global variables. This function should only be called
// once at the beginning of the test.
func StartServer(connParams sqldb.ConnParams) error {
	// Setup a fake vtgate server.
	protocol := "resolveTest"
	*vtgateconn.VtgateProtocol = protocol
	vtgateconn.RegisterDialer(protocol, func(context.Context, string, time.Duration) (vtgateconn.Impl, error) {
		return &txResolver{
			FakeVTGateConn: fakerpcvtgateconn.FakeVTGateConn{},
		}, nil
	})

	dbcfgs := dbconfigs.DBConfigs{
		App:           connParams,
		SidecarDBName: "_vt",
	}

	mysqld := mysqlctl.NewMysqld(
		&mysqlctl.Mycnf{},
		&dbcfgs,
		dbconfigs.AppConfig,
	)

	config := tabletenv.DefaultQsConfig
	config.EnableAutoCommit = true
	config.StrictTableACL = true
	config.TwoPCEnable = true
	config.TwoPCAbandonAge = 1
	config.TwoPCCoordinatorAddress = "fake"

	Target = querypb.Target{
		Keyspace:   "vttest",
		Shard:      "0",
		TabletType: topodatapb.TabletType_MASTER,
	}

	Server = tabletserver.NewTabletServerWithNilTopoServer(config)
	Server.Register()
	err := Server.StartService(Target, dbcfgs, mysqld)
	if err != nil {
		return fmt.Errorf("could not start service: %v", err)
	}

	// Start http service.
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return fmt.Errorf("could not start listener: %v", err)
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
