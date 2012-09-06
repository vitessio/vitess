// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"path"
	"syscall"

	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/rpcwrap/jsonrpc"
	"code.google.com/p/vitess/go/sighandler"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/umgmt"
	"code.google.com/p/vitess/go/vt/dbconfigs"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/servenv"
	"code.google.com/p/vitess/go/vt/tabletmanager"
	ts "code.google.com/p/vitess/go/vt/tabletserver"
	"code.google.com/p/vitess/go/zk"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.01
)

var (
	port           = flag.Int("port", 6509, "port for the server")
	lameDuckPeriod = flag.Float64("lame-duck-period", DefaultLameDuckPeriod, "how long to give in-flight transactions to finish")
	rebindDelay    = flag.Float64("rebind-delay", DefaultRebindDelay, "artificial delay before rebinding a hijacked listener")
	tabletPath     = flag.String("tablet-path", "", "path to zk node representing the tablet")
	qsConfigFile   = flag.String("queryserver-config-file", "", "config file name for the query service")
	mycnfFile      = flag.String("mycnf-file", "", "my.cnf file")
	queryLog       = flag.String("debug-querylog-file", "", "for testing: log all queries to this file")
)

// Default values for the config
//
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great.  (the overhead makes the final packets on the wire about
// twice bigger than this).
var qsConfig ts.Config = ts.Config{
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

func main() {
	flag.Parse()

	env.Init("vttablet")
	mycnf := readMycnf()
	dbcfgs, err := dbconfigs.Init(mycnf)
	if err != nil {
		relog.Fatal("%s", err)
	}

	initAgent(dbcfgs, mycnf)
	initQueryService(dbcfgs)

	rpc.HandleHTTP()
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	umgmt.SetLameDuckPeriod(float32(*lameDuckPeriod))
	umgmt.SetRebindDelay(float32(*rebindDelay))
	umgmt.AddStartupCallback(func() {
		umgmt.StartHttpServer(fmt.Sprintf(":%v", *port))
	})
	umgmt.AddStartupCallback(func() {
		sighandler.SetSignalHandler(syscall.SIGTERM, umgmt.SigTermHandler)
	})

	relog.Info("started vttablet %v", *port)
	umgmtSocket := fmt.Sprintf("/tmp/vttablet-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		relog.Error("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	relog.Info("done")
}

func readMycnf() *mysqlctl.Mycnf {
	if *mycnfFile == "" {
		_, tabletid := path.Split(*tabletPath)
		*mycnfFile = fmt.Sprintf("%s/vt_%s/my.cnf", mysqlctl.VtDataRoot, tabletid)
	}
	mycnf, mycnfErr := mysqlctl.ReadMycnf(*mycnfFile)
	if mycnfErr != nil {
		relog.Fatal("mycnf read failed: %v", mycnfErr)
	}
	return mycnf
}

func initAgent(dbcfgs dbconfigs.DBConfigs, mycnf *mysqlctl.Mycnf) {
	zconn := zk.NewMetaConn(5e9)
	umgmt.AddCloseCallback(func() {
		zconn.Close()
	})

	bindAddr := fmt.Sprintf(":%v", *port)

	// Action agent listens to changes in zookeeper and makes modifcations to this
	// tablet.
	agent := tabletmanager.NewActionAgent(zconn, *tabletPath, *mycnfFile, *dbconfigs.DBConfigsFile)
	agent.Start(bindAddr, mycnf.MysqlAddr())
	umgmt.AddCloseCallback(func() {
		agent.Stop()
	})

	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)

	// The TabletManager rpc service allow other processes to query for management
	// related data. It might be co-registered with the query server.
	tm := tabletmanager.NewTabletManager(bindAddr, nil, mysqld)
	rpc.Register(tm)
}

func initQueryService(dbcfgs dbconfigs.DBConfigs) {
	if err := dbconfigs.ReadJson(*qsConfigFile, &qsConfig); err != nil {
		relog.Fatal("%s", err)
	}
	ts.StartQueryService(qsConfig)
	usefulLameDuckPeriod := float64(qsConfig.QueryTimeout + 1)
	if usefulLameDuckPeriod > *lameDuckPeriod {
		*lameDuckPeriod = usefulLameDuckPeriod
		relog.Info("readjusted -lame-duck-period to %f", *lameDuckPeriod)
	}

	if dbcfgs.App.Dbname == "" {
		relog.Info("db-configs-file has no dbname. Skipping start of query service")
		return
	}
	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", log.Ldate|log.Lmicroseconds, relog.DEBUG)
		} else {
			relog.Fatal("Error opening file %v: %v", *queryLog, err)
		}
	}
	ts.AllowQueries(dbcfgs.App)
	umgmt.AddCloseCallback(func() {
		ts.DisallowQueries()
	})
}
