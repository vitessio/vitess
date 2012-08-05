// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt tablet server: Serves queries and performs housekeeping jobs.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	_ "net/http/pprof"
	"net/rpc"
	"os"
	"path"
	"syscall"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/rpcwrap/jsonrpc"
	"code.google.com/p/vitess/go/sighandler"
	_ "code.google.com/p/vitess/go/snitch"
	"code.google.com/p/vitess/go/umgmt"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/servenv"
	"code.google.com/p/vitess/go/vt/tabletmanager"
	ts "code.google.com/p/vitess/go/vt/tabletserver"
	"code.google.com/p/vitess/go/zk"
)

const (
	DefaultLameDuckPeriod = 30.0
	DefaultRebindDelay    = 0.0
)

var (
	port           = flag.Int("port", 6509, "port for the server")
	lameDuckPeriod = flag.Float64("lame-duck-period", DefaultLameDuckPeriod, "how long to give in-flight transactions to finish")
	rebindDelay    = flag.Float64("rebind-delay", DefaultRebindDelay, "artificial delay before rebinding a hijacked listener")
	tabletPath     = flag.String("tablet-path", "", "path to zk node representing the tablet")
	qsConfigFile   = flag.String("qsconfig", "", "config file name for the query service")
	dbCredsFile    = flag.String("dbcreds", "", "db connection credentials file")
	mycnfFile      = flag.String("mycnf", "", "my.cnf file")
	queryLog       = flag.String("querylog", "", "for testing: log all queries to this file")
)

var qsConfig ts.Config = ts.Config{
	1000,
	16,
	20,
	30,
	10000,
	5000,
	30 * 60,
	0,
	30 * 60,
}

func main() {
	flag.Parse()

	env.Init("vttablet")
	mycnf := readMycnf()

	dbcreds := make(map[string]interface{})
	unmarshalFile(*dbCredsFile, &dbcreds)

	initAgent(dbcreds, mycnf)
	initQueryService(dbcreds, mycnf)

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
		*mycnfFile = fmt.Sprintf("/vt/vt_%s/my.cnf", tabletid)
	}
	mycnf, mycnfErr := mysqlctl.ReadMycnf(*mycnfFile)
	if mycnfErr != nil {
		relog.Fatal("mycnf read failed: %v", mycnfErr)
	}
	return mycnf
}

func initAgent(dbcreds map[string]interface{}, mycnf *mysqlctl.Mycnf) {
	zconn := zk.NewMetaConn(5e9)
	umgmt.AddCloseCallback(func() {
		zconn.Close()
	})

	bindAddr := fmt.Sprintf(":%v", *port)

	// Action agent listens to changes in zookeeper and makes modifcations to this
	// tablet.
	agent := tabletmanager.NewActionAgent(zconn, *tabletPath, *mycnfFile)
	agent.Start(bindAddr, mycnf.MysqlAddr())

	dbaconfig := map[string]interface{}{
		"uname":   "vt_dba",
		"charset": "utf8",
	}
	if _, ok := dbcreds["dba"]; ok {
		dbaconfig = dbcreds["dba"].(map[string]interface{})
	}
	dbaconfig["unix_socket"] = mycnf.SocketPath
	mysqld := mysqlctl.NewMysqld(mycnf, dbaconfig)

	// The TabletManager rpc service allow other processes to query for management
	// related data. It might be co-registered with the query server.
	tm := tabletmanager.NewTabletManager(bindAddr, nil, mysqld)
	rpc.Register(tm)
}

func initQueryService(dbcreds map[string]interface{}, mycnf *mysqlctl.Mycnf) {
	unmarshalFile(*qsConfigFile, &qsConfig)
	ts.StartQueryService(qsConfig)
	usefulLameDuckPeriod := float64(qsConfig.QueryTimeout + 1)
	if usefulLameDuckPeriod > *lameDuckPeriod {
		*lameDuckPeriod = usefulLameDuckPeriod
		relog.Info("readjusted -lame-duck-period to %f", *lameDuckPeriod)
	}

	if _, ok := dbcreds["app"]; !ok {
		relog.Info("dbcreds has no app credentials. Skipping start of query service")
		return
	}
	appconfig := dbcreds["app"].(map[string]interface{})
	appconfig["unix_socket"] = mycnf.SocketPath
	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			ts.QueryLogger = relog.New(f, "", log.Ldate|log.Lmicroseconds, relog.DEBUG)
		} else {
			relog.Fatal("Error opening file %v: %v", *queryLog, err)
		}
	}
	ts.AllowQueries(appconfig)
	umgmt.AddCloseCallback(func() {
		ts.DisallowQueries()
	})
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
	data, _ := json.MarshalIndent(val, "", "  ")
	relog.Info("config: %s\n", data)
}
