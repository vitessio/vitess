// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
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
	DefaultRebindDelay    = 0.01
)

var (
	port           = flag.Int("port", 6510, "tcp port to serve on")
	lameDuckPeriod = flag.Float64("lame-duck-period", DefaultLameDuckPeriod, "how long to give in-flight transactions to finish")
	rebindDelay    = flag.Float64("rebind-delay", DefaultRebindDelay, "artificial delay before rebinding a hijacked listener")
	authConfig     = flag.String("auth-credentials", "", "name of file containing auth credentials")
	configFile     = flag.String("config", "", "config file name")
	dbConfigFile   = flag.String("dbconfig", "", "db config file name")
	rowcache       = flag.String("rowcache", "", "rowcache connection, host:port or /path/to/socket")
	queryLog       = flag.String("querylog", "", "for testing: log all queries to this file")
	customrules    = flag.String("customrules", "", "custom query rules file")
	overridesFile  = flag.String("schema-override", "", "schema overrides file")
)

var config = ts.Config{
	CachePoolCap:       400,
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

var schemaOverrides []ts.SchemaOverride

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
	if err := servenv.Init("vtocc"); err != nil {
		relog.Fatal("Error in servenv.Init: %v", err)
	}
	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", relog.DEBUG)
		} else {
			relog.Fatal("Error opening file %v: %v", *queryLog, err)
		}
	}
	ts.SqlQueryLogger.ServeLogs("/debug/querylog")
	ts.TxLogger.ServeLogs("/debug/txlog")
	unmarshalFile(*configFile, &config)
	data, _ := json.MarshalIndent(config, "", "  ")
	relog.Info("config: %s\n", data)

	unmarshalFile(*dbConfigFile, &dbconfig)
	dbconfig.Memcache = *rowcache
	relog.Info("dbconfig: %s\n", dbconfig)

	unmarshalFile(*overridesFile, &schemaOverrides)
	data, _ = json.MarshalIndent(schemaOverrides, "", "  ")
	relog.Info("schemaOverrides: %s\n", data)

	ts.RegisterQueryService(config)
	qrs := loadCustomRules()
	ts.AllowQueries(dbconfig, schemaOverrides, qrs)

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

func loadCustomRules() (qrs *ts.QueryRules) {
	if *customrules == "" {
		return ts.NewQueryRules()
	}

	data, err := ioutil.ReadFile(*customrules)
	if err != nil {
		relog.Fatal("Error reading file %v: %v", *customrules, err)
	}

	qrs = ts.NewQueryRules()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		relog.Fatal("Error unmarshaling query rules %v", err)
	}
	return qrs
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
