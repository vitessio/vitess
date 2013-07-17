// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"expvar"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/relog"
	rpc "code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/rpcwrap/jsonrpc"
	_ "code.google.com/p/vitess/go/snitch"

	"code.google.com/p/vitess/go/vt/dbconfigs"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/tabletmanager"
	"code.google.com/p/vitess/go/vt/topo"
)

var port = flag.Int("port", 0, "port for debug http server")
var action = flag.String("action", "", "management action to perform")
var actionNode = flag.String("action-node", "",
	"path to zk node representing the action")
var actionGuid = flag.String("action-guid", "",
	"a label to help track processes")
var logLevel = flag.String("log.level", "debug", "set log level")
var logFilename = flag.String("logfile", "/dev/stderr", "log path")
var force = flag.Bool("force", false, "force an action to rerun")

// FIXME(msolomon) temporary, until we are starting mysql ourselves
var mycnfFile = flag.String("mycnf-file", "/etc/my.cnf", "path to my.cnf")

func init() {
	expvar.NewString("binary-name").Set("vtaction")
}

func main() {
	dbConfigsFile, dbCredentialsFile := dbconfigs.RegisterCommonFlags()
	flag.Parse()

	relog.Info("started vtaction %v", os.Args)

	rpc.HandleHTTP()
	jsonrpc.ServeHTTP()
	jsonrpc.ServeRPC()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()

	logFile, err := os.OpenFile(*logFilename,
		os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		relog.Fatal("Can't open log file: %v", err)
	}
	relog.SetOutput(logFile)
	relog.SetPrefix(fmt.Sprintf("vtaction [%v] ", os.Getpid()))
	if err := relog.SetLevelByName(*logLevel); err != nil {
		relog.Fatal("%v", err)
	}
	relog.HijackLog(nil)
	relog.HijackStdio(logFile, logFile)

	mycnf, mycnfErr := mysqlctl.ReadMycnf(*mycnfFile)
	if mycnfErr != nil {
		relog.Fatal("mycnf read failed: %v", mycnfErr)
	}

	relog.Debug("mycnf: %v", jscfg.ToJson(mycnf))

	dbcfgs, cfErr := dbconfigs.Init(mycnf.SocketFile, *dbConfigsFile, *dbCredentialsFile)
	if err != nil {
		relog.Fatal("%s", cfErr)
	}
	mysqld := mysqlctl.NewMysqld(mycnf, dbcfgs.Dba, dbcfgs.Repl)

	topoServer := topo.GetServer()
	defer topo.CloseServers()

	actor := tabletmanager.NewTabletActor(mysqld, topoServer)

	// we delegate out startup to the micromanagement server so these actions
	// will occur after we have obtained our socket.
	bindAddr := fmt.Sprintf(":%v", *port)
	httpServer := &http.Server{Addr: bindAddr}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			relog.Error("httpServer.ListenAndServe err: %v", err)
		}
	}()

	actionErr := actor.HandleAction(*actionNode, *action, *actionGuid, *force)
	if actionErr != nil {
		relog.Fatal("action error: %v", actionErr)
	}

	relog.Info("finished vtaction %v", os.Args)
}
