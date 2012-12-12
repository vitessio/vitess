// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap"
	"code.google.com/p/vitess/go/rpcwrap/auth"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/rpcwrap/jsonrpc"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/umgmt"
	"code.google.com/p/vitess/go/vt/dbconfigs"
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
	configFile     = flag.String("config", "", "config file name")
	dbConfigFile   = flag.String("dbconfig", "", "db config file name")
	queryLog       = flag.String("querylog", "", "for testing: log all queries to this file")
)

var config = ts.Config{
	CachePoolCap:       1000,
	PoolSize:           16,
	StreamPoolSize:     750,
	TransactionCap:     20,
	TransactionTimeout: 30,
	MaxResultSize:      10000,
	QueryCacheSize:     5000,
	SchemaReloadTime:   30 * 60,
	QueryTimeout:       0,
	IdleTimeout:        30 * 60,
	StreamBufferSize:   32 * 1024,
}

var dbconfig = dbconfigs.DBConfig{
	Host:    "localhost",
	Uname:   "vt_app",
	Charset: "utf8",
}

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

	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", log.Ldate|log.Lmicroseconds, relog.DEBUG)
		} else {
			relog.Fatal("Error opening file %v: %v", *queryLog, err)
		}
	}
	unmarshalFile(*configFile, &config)
	data, _ := json.MarshalIndent(config, "", "  ")
	relog.Info("config: %s\n", data)

	unmarshalFile(*dbConfigFile, &dbconfig)
	relog.Info("dbconfig: %s\n", dbconfig)

	qm := &OccManager{config, dbconfig}
	rpcwrap.RegisterAuthenticated(qm)
	ts.RegisterQueryService(config)
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
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM)
		go func() {
			for sig := range c {
				umgmt.SigTermHandler(sig)
			}
		}()
	})
	umgmt.AddCloseCallback(func() {
		ts.DisallowQueries(true)
	})

	umgmtSocket := fmt.Sprintf("/tmp/vtocc-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}

func unmarshalFile(name string, val interface{}) {
	if name != "" {
		data, err := ioutil.ReadFile(name)
		if err != nil {
			relog.Fatal("could not read %v: %v", val, err)
		}
		if err = json.Unmarshal(data, val); err != nil {
			relog.Fatal("could not read %s: %v", val, err)
		}
	}
}

// OccManager is deprecated. Use SqlQuery.GetSessionId instead.
type OccManager struct {
	config   ts.Config
	dbconfig dbconfigs.DBConfig
}

func (m *OccManager) GetSessionId(dbname *string, sessionId *int64) error {
	if *dbname != m.dbconfig.Dbname {
		return errors.New(fmt.Sprintf("db name mismatch, expecting %v, received %v",
			m.dbconfig.Dbname, *dbname))
	}
	*sessionId = ts.GetSessionId()
	return nil
}
