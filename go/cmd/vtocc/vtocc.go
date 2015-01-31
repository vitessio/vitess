// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tabletserver"
)

var (
	overridesFile     = flag.String("schema-override", "", "schema overrides file")
	enableRowcache    = flag.Bool("enable-rowcache", false, "enable rowcacche")
	enableInvalidator = flag.Bool("enable-invalidator", false, "enable rowcache invalidator")
	binlogPath        = flag.String("binlog-path", "", "binlog path used by rowcache invalidator")
	tableAclConfig    = flag.String("table-acl-config", "", "path to table access checker config file")
)

var schemaOverrides []tabletserver.SchemaOverride

func init() {
	servenv.RegisterDefaultFlags()
	servenv.InitServiceMapForBsonRpcService("queryservice")
}

func main() {
	defer exit.Recover()

	flags := dbconfigs.AppConfig | dbconfigs.DbaConfig |
		dbconfigs.FilteredConfig | dbconfigs.ReplConfig
	dbconfigs.RegisterFlags(flags)
	flag.Parse()
	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Errorf("vtocc doesn't take any positional arguments")
		exit.Return(1)
	}
	servenv.Init()

	dbConfigs, err := dbconfigs.Init("", flags)
	if err != nil {
		log.Errorf("Cannot initialize App dbconfig: %v", err)
		exit.Return(1)
	}
	if *enableRowcache {
		dbConfigs.App.EnableRowcache = true
		if *enableInvalidator {
			dbConfigs.App.EnableInvalidator = true
		}
	}
	mycnf := &mysqlctl.Mycnf{BinLogPath: *binlogPath}
	mysqld := mysqlctl.NewMysqld("Dba", mycnf, &dbConfigs.Dba, &dbConfigs.Repl)

	if err := unmarshalFile(*overridesFile, &schemaOverrides); err != nil {
		log.Error(err)
		exit.Return(1)
	}
	data, _ := json.MarshalIndent(schemaOverrides, "", "  ")
	log.Infof("schemaOverrides: %s\n", data)

	if *tableAclConfig != "" {
		tableacl.Init(*tableAclConfig)
	}
	tabletserver.InitQueryService()

	// Query service can go into NOT_SERVING state if mysql goes down.
	// So, continuously retry starting the service. So, it tries to come
	// back up if it went down.
	go func() {
		for {
			_ = tabletserver.AllowQueries(dbConfigs, schemaOverrides, mysqld)
			time.Sleep(30 * time.Second)
		}
	}()

	log.Infof("starting vtocc %v", *servenv.Port)
	servenv.OnTerm(func() {
		tabletserver.DisallowQueries()
		mysqld.Close()
	})
	servenv.RunDefault()
}

func unmarshalFile(name string, val interface{}) error {
	if name != "" {
		data, err := ioutil.ReadFile(name)
		if err != nil {
			return fmt.Errorf("unmarshalFile: could not read %v: %v", val, err)
		}
		if err = json.Unmarshal(data, val); err != nil {
			return fmt.Errorf("unmarshalFile: could not read %s: %v", val, err)
		}
	}
	return nil
}
