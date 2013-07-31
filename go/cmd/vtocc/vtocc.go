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

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/relog"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/auth"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/rpcwrap/jsonrpc"
	_ "github.com/youtube/vitess/go/snitch"
	"github.com/youtube/vitess/go/umgmt"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/servenv"
	ts "github.com/youtube/vitess/go/vt/tabletserver"
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
	queryLog       = flag.String("querylog", "", "for testing: log all queries to this file")
	customrules    = flag.String("customrules", "", "custom query rules file")
	overridesFile  = flag.String("schema-override", "", "schema overrides file")
)

var config = ts.Config{
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
	RowCache:           nil,
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
	servenv.Init()
	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", relog.DEBUG)
		} else {
			log.Fatalf("Error opening file %v: %v", *queryLog, err)
		}
	}
	ts.SqlQueryLogger.ServeLogs("/debug/querylog")
	ts.TxLogger.ServeLogs("/debug/txlog")
	unmarshalFile(*configFile, &config)
	data, _ := json.MarshalIndent(config, "", "  ")
	log.Infof("config: %s\n", data)

	unmarshalFile(*dbConfigFile, &dbconfig)
	log.Infof("dbconfig: %s\n", dbconfig)

	unmarshalFile(*overridesFile, &schemaOverrides)
	data, _ = json.MarshalIndent(schemaOverrides, "", "  ")
	log.Infof("schemaOverrides: %s\n", data)

	ts.RegisterQueryService(config)
	qrs := loadCustomRules()
	ts.AllowQueries(dbconfig, schemaOverrides, qrs)

	rpc.HandleHTTP()

	// NOTE(szopa): Changing credentials requires a server
	// restart.
	if *authConfig != "" {
		if err := auth.LoadCredentials(*authConfig); err != nil {
			log.Errorf("could not load authentication credentials, not starting rpc servers: %v", err)
		}
		serveAuthRPC()
	}
	serveRPC()

	log.Infof("started vtocc %v", *port)

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	usefulLameDuckPeriod := float64(config.QueryTimeout + 1)
	if usefulLameDuckPeriod > *lameDuckPeriod {
		*lameDuckPeriod = usefulLameDuckPeriod
		log.Infof("readjusted -lame-duck-period to %f", *lameDuckPeriod)
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
		log.Errorf("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	log.Infof("done")
}

func loadCustomRules() (qrs *ts.QueryRules) {
	if *customrules == "" {
		return ts.NewQueryRules()
	}

	data, err := ioutil.ReadFile(*customrules)
	if err != nil {
		log.Fatalf("Error reading file %v: %v", *customrules, err)
	}

	qrs = ts.NewQueryRules()
	err = qrs.UnmarshalJSON(data)
	if err != nil {
		log.Fatalf("Error unmarshaling query rules %v", err)
	}
	return qrs
}

func unmarshalFile(name string, val interface{}) {
	if name != "" {
		data, err := ioutil.ReadFile(name)
		if err != nil {
			log.Fatalf("could not read %v: %v", val, err)
		}
		if err = json.Unmarshal(data, val); err != nil {
			log.Fatalf("could not read %s: %v", val, err)
		}
	}
}
