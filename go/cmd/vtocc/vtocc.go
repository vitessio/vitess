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
	"syscall"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/proc"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/auth"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/rpcwrap/jsonrpc"
	_ "github.com/youtube/vitess/go/snitch"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/servenv"
	ts "github.com/youtube/vitess/go/vt/tabletserver"
)

var (
	port          = flag.Int("port", 6510, "tcp port to serve on")
	authConfig    = flag.String("auth-credentials", "", "name of file containing auth credentials")
	configFile    = flag.String("config", "", "config file name")
	dbConfigFile  = flag.String("dbconfig", "", "db config file name")
	queryLog      = flag.String("querylog", "", "for testing: log all queries to this file")
	customrules   = flag.String("customrules", "", "custom query rules file")
	overridesFile = flag.String("schema-override", "", "schema overrides file")
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
	if err := servenv.Init("vtocc"); err != nil {
		log.Fatalf("Error in servenv.Init: %v", err)
	}
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

	log.Infof("starting vtocc %v", *port)
	s := proc.ListenAndServe(fmt.Sprintf("%v", *port))

	// A SIGUSR1 means that we're restarting
	if s == syscall.SIGUSR1 {
		// Give some time for the other process
		// to pick up the listeners
		time.Sleep(5 * time.Millisecond)
		ts.DisallowQueries(true)
	} else {
		ts.DisallowQueries(false)
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
