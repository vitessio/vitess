// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"net/rpc"
	"syscall"

	"code.google.com/p/vitess.x/go/vt/mysqlctl"
	"code.google.com/p/vitess.x/go/vt/tabletmanager"
	"code.google.com/p/vitess.x/go/zk"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/rpcwrap/jsonrpc"
	"code.google.com/p/vitess/go/sighandler"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/vt/servenv"
	//ts "code.google.com/p/vitess/go/vt/tabletserver"
	"code.google.com/p/vitess/go/umgmt"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.0
)

var (
	port           = flag.Int("port", 0, "port for the http server")
	lameDuckPeriod = flag.Float64("lame-duck-period", DefaultLameDuckPeriod,
		"how long to give in-flight transactions to finish")
	rebindDelay = flag.Float64("rebind-delay", DefaultRebindDelay,
		"artificial delay before rebinding a hijacked listener")
	tabletPath = flag.String("tablet-path", "",
		"path to zk node representing the tablet")
)

func main() {
	flag.Parse()
	if *port == 0 {
		panic("no -port supplied")
	}

	env.Init("vttablet")

	// FIXME: (sougou) Integrate tabletserver initialization with zk
	//config, dbconfig := ts.Init()
	//ts.StartQueryService(config)
	//ts.AllowQueries(dbconfig)

	rpc.HandleHTTP()
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()

	zconn := zk.NewMetaConn(5e9)
	defer zconn.Close()

	bindAddr := fmt.Sprintf(":%v", *port)

	// Action agent listens to changes in zookeeper and makes modifcations to this
	// tablet.
	mysqlAddr := "fixme:3306"
	agent := tabletmanager.NewActionAgent(zconn, *tabletPath)
	agent.Start(bindAddr, mysqlAddr)

	relog.Info("mycnf: %v", agent.MycnfPath)
	mycnf, mycnfErr := mysqlctl.ReadMycnf(agent.MycnfPath)
	if mycnfErr != nil {
		relog.Error("mycnf read failed: %v", mycnfErr)
		return
	}

	dbaconfig := map[string]interface{}{
		"uname":       "vt_dba",
		"unix_socket": mycnf.SocketPath,
		"pass":        "",
		"dbname":      "",
		"charset":     "utf8",
		"host":        "",
		"port":        0,
	}
	mysqld := mysqlctl.NewMysqld(mycnf, dbaconfig)

	// The TabletManager rpc service allow other processes to query for management
	// related data. It might be co-registered with the query server.
	tm := tabletmanager.NewTabletManager(bindAddr, nil, mysqld)
	rpc.Register(tm)

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	/*usefulLameDuckPeriod := float64(config.QueryTimeout + 1)
	if usefulLameDuckPeriod > *lameDuckPeriod {
		*lameDuckPeriod = usefulLameDuckPeriod
		relog.Info("readjusted -lame-duck-period to %f", *lameDuckPeriod)
	}*/
	umgmt.SetLameDuckPeriod(float32(*lameDuckPeriod))
	umgmt.SetRebindDelay(float32(*rebindDelay))
	umgmt.AddStartupCallback(func() {
		umgmt.StartHttpServer(fmt.Sprintf(":%v", *port))
	})
	umgmt.AddStartupCallback(func() {
		sighandler.SetSignalHandler(syscall.SIGTERM, umgmt.SigTermHandler)
	})
	/*umgmt.AddCloseCallback(func() {
		ts.DisallowQueries()
	})*/

	relog.Info("started vttablet %v", *port)
	umgmtSocket := fmt.Sprintf("/tmp/vttablet-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}
