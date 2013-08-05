// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
The vt_binlog_player reads data from the a remote host via vt_binlog_server.
This is mostly intended for online data migrations.
*/
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
)

const (
	TXN_BATCH        = 10
	MAX_TXN_INTERVAL = 30
)

var (
	keyrangeStart  = flag.String("start", "", "keyrange start to use in hex")
	keyrangeEnd    = flag.String("end", "", "keyrange end to use in hex")
	port           = flag.Int("port", 0, "port for the server")
	txnBatch       = flag.Int("txn-batch", TXN_BATCH, "transaction batch size")
	maxTxnInterval = flag.Int("max-txn-interval", MAX_TXN_INTERVAL, "max txn interval")
	dbConfigFile   = flag.String("db-config-file", "", "json file for db credentials")
	debug          = flag.Bool("debug", true, "run a debug version - prints the sql statements rather than executing them")
	tables         = flag.String("tables", "", "tables to play back")
	execDdl        = flag.Bool("exec-ddl", false, "execute ddl")
)

func readDbConfig(dbConfigFile string) (*mysql.ConnectionParams, error) {
	dbConfigData, err := ioutil.ReadFile(dbConfigFile)
	if err != nil {
		return nil, fmt.Errorf("Error %s in reading db-config-file %s", err, dbConfigFile)
	}
	relog.Info("dbConfigData %v", string(dbConfigData))

	dbConfig := new(mysql.ConnectionParams)
	err = json.Unmarshal(dbConfigData, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshaling dbconfig data, err '%v'", err)
	}
	return dbConfig, nil
}

func main() {
	flag.Parse()
	servenv.Init("vt_binlog_player")

	if *dbConfigFile == "" {
		relog.Fatal("Cannot start without db-config-file")
	}
	dbConfig, err := readDbConfig(*dbConfigFile)
	if err != nil {
		relog.Fatal("Cannot read db config file: %v", err)
	}

	var t []string
	if *tables != "" {
		t = strings.Split(*tables, ",")
		for i, table := range t {
			t[i] = strings.TrimSpace(table)
		}
	}

	interrupted := make(chan struct{})
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		for _ = range c {
			close(interrupted)
		}
	}()

	var vtClient mysqlctl.VtClient
	vtClient = mysqlctl.NewDbClient(dbConfig)
	err = vtClient.Connect()
	if err != nil {
		relog.Fatal("error in initializing dbClient: %v", err)
	}
	brs, err := mysqlctl.ReadStartPosition(vtClient, *keyrangeStart, *keyrangeEnd)
	if err != nil {
		relog.Fatal("Cannot read start position from db: %v", err)
	}
	if *debug {
		vtClient = mysqlctl.NewDummyVtClient()
	}
	blp, err := mysqlctl.NewBinlogPlayer(vtClient, brs, t, *txnBatch, time.Duration(*maxTxnInterval)*time.Second, *execDdl)
	if err != nil {
		relog.Fatal("error in initializing binlog player: %v", err)
	}
	err = blp.ApplyBinlogEvents(interrupted)
	if err != nil {
		relog.Error("Error in applying binlog events, err %v", err)
	}
	relog.Info("vt_binlog_player done")
}
