// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"net/rpc"
	"syscall"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap"
	"code.google.com/p/vitess/go/rpcwrap/auth"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/rpcwrap/jsonrpc"
	"code.google.com/p/vitess/go/sighandler"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/umgmt"
	"code.google.com/p/vitess/go/vt/servenv"
	ts "code.google.com/p/vitess/go/vt/tabletserver"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.0
)

var (
	port           = flag.Int("port", 6510, "tcp port to serve on")
	lameDuckPeriod = flag.Float64("lame-duck-period", DefaultLameDuckPeriod, "how long to give in-flight transactions to finish")
	rebindDelay    = flag.Float64("rebind-delay", DefaultRebindDelay, "artificial delay before rebinding a hijacked listener")
	authConfig     = flag.String("auth-credentials", "", "name of file containing auth credentials")
)

func serveAuthRPC() {
	bsonrpc.ServeAuthRPC()
	jsonrpc.ServeAuthRPC()
}

func serveRPC() {
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()
}

func main() {
	flag.Parse()
	env.Init("vtocc")

	config, dbconfig := ts.Init()
	qm := &OccManager{config, dbconfig}
	rpcwrap.RegisterAuthenticated(qm)
	ts.StartQueryService(config)
	ts.AllowQueries(dbconfig)

	rpc.HandleHTTP()

	// NOTE(szopa): Changing credentials requires a server
	// restart.
	if *authConfig != "" {
		if err := auth.LoadCredentials(*authConfig); err != nil {
			relog.Error("could not load authentication credentials, not starting rpc servers: %v", err)
		}
		serveAuthRPC()
	}
	serveRPC()

	relog.Info("started vtocc %v", *port)

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	usefulLameDuckPeriod := float64(config.QueryTimeout + 1)
	if usefulLameDuckPeriod > *lameDuckPeriod {
		*lameDuckPeriod = usefulLameDuckPeriod
		relog.Info("readjusted -lame-duck-period to %f", *lameDuckPeriod)
	}
	umgmt.SetLameDuckPeriod(float32(*lameDuckPeriod))
	umgmt.SetRebindDelay(float32(*rebindDelay))
	umgmt.AddStartupCallback(func() {
		umgmt.StartHttpServer(fmt.Sprintf(":%v", *port))
	})
	umgmt.AddStartupCallback(func() {
		sighandler.SetSignalHandler(syscall.SIGTERM, umgmt.SigTermHandler)
	})
	umgmt.AddCloseCallback(func() {
		ts.DisallowQueries()
	})

	umgmtSocket := fmt.Sprintf("/tmp/vtocc-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}

// OccManager is deprecated. Use SqlQuery.GetSessionId instead.
type OccManager struct {
	config   ts.Config
	dbconfig map[string]interface{}
}

func (m *OccManager) GetSessionId(dbname *string, sessionId *int64) error {
	if *dbname != m.dbconfig["dbname"].(string) {
		return errors.New(fmt.Sprintf("db name mismatch, expecting %v, received %v",
			m.dbconfig["dbname"].(string), *dbname))
	}
	*sessionId = ts.GetSessionId()
	return nil
}
