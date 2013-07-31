// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// vt binlog server: Serves binlog for out of band replication.
package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	log "github.com/golang/glog"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	_ "github.com/youtube/vitess/go/snitch"
	"github.com/youtube/vitess/go/umgmt"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/servenv"
)

var (
	port      = flag.Int("port", 6614, "port for the server")
	dbname    = flag.String("dbname", "", "database name")
	mycnfFile = flag.String("mycnf-file", "", "path of mycnf file")
)

func main() {
	flag.Parse()
	servenv.Init()

	if *mycnfFile == "" {
		log.Fatalf("Please specify the path for mycnf file.")
	}
	mycnf, err := mysqlctl.ReadMycnf(*mycnfFile)
	if err != nil {
		log.Fatalf("Error reading mycnf file %v", *mycnfFile)
	}

	binlogServer := mysqlctl.NewBinlogServer(mycnf, *dbname)

	proto.RegisterBinlogServer(binlogServer)
	rpcwrap.RegisterAuthenticated(binlogServer)
	//bsonrpc.ServeAuthRPC()

	rpc.HandleHTTP()
	bsonrpc.ServeHTTP()
	bsonrpc.ServeRPC()

	umgmt.SetLameDuckPeriod(30.0)
	umgmt.SetRebindDelay(0.01)
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

	log.Infof("vt_binlog_server registered at port %v", *port)
	umgmtSocket := fmt.Sprintf("/tmp/vt_binlog_server-%08x-umgmt.sock", *port)
	if umgmtErr := umgmt.ListenAndServe(umgmtSocket); umgmtErr != nil {
		log.Errorf("umgmt.ListenAndServe err: %v", umgmtErr)
	}
	log.Infof("done")
}
