// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	ts "github.com/youtube/vitess/go/vt/tabletserver"
)

var (
	overridesFile     = flag.String("schema-override", "", "schema overrides file")
	enableRowcache    = flag.Bool("enable-rowcache", false, "enable rowcacche")
	enableInvalidator = flag.Bool("enable-invalidator", false, "enable rowcache invalidator")
	binlogPath        = flag.String("binlog-path", "", "binlog path used by rowcache invalidator")
)

var schemaOverrides []ts.SchemaOverride

func main() {
	dbconfigs.RegisterFlags()
	flag.Parse()
	servenv.Init()

	dbConfigs, err := dbconfigs.Init("")
	if err != nil {
		log.Fatalf("Cannot initialize App dbconfig: %v", err)
	}
	if *enableRowcache {
		dbConfigs.App.EnableRowcache = true
		if *enableInvalidator {
			dbConfigs.App.EnableInvalidator = true
		}
	}
	mycnf := &mysqlctl.Mycnf{BinLogPath: *binlogPath}
	mysqld := mysqlctl.NewMysqld(mycnf, &dbConfigs.Dba, &dbConfigs.Repl)

	unmarshalFile(*overridesFile, &schemaOverrides)
	data, _ := json.MarshalIndent(schemaOverrides, "", "  ")
	log.Infof("schemaOverrides: %s\n", data)

	ts.InitQueryService()

	ts.AllowQueries(&dbConfigs.App, schemaOverrides, ts.LoadCustomRules(), mysqld)

	log.Infof("starting vtocc %v", *servenv.Port)
	servenv.OnClose(func() {
		time.Sleep(5 * time.Millisecond)
		ts.DisallowQueries()
	})
	servenv.Run()
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
