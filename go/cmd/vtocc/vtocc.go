// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tableacl"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/customrule"
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
	dbconfigs.RegisterFlags()
	flag.Parse()
	if len(flag.Args()) > 0 {
		flag.Usage()
		log.Fatalf("vtocc doesn't take any positional arguments")
	}
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
	mysqld := mysqlctl.NewMysqld("Dba", mycnf, &dbConfigs.Dba, &dbConfigs.Repl)

	unmarshalFile(*overridesFile, &schemaOverrides)
	data, _ := json.MarshalIndent(schemaOverrides, "", "  ")
	log.Infof("schemaOverrides: %s\n", data)

	if *tableAclConfig != "" {
		tableacl.Init(*tableAclConfig)
	}
	tabletserver.InitQueryService()

	err = customrule.InitializeCustomRuleManager(*tabletserver.CustomRules, tabletserver.SqlQueryRpcService)
	if err != nil {
		log.Warningf("Fail to initialize custom rule manager, Error message: %s", err)
	}

	err = tabletserver.AllowQueries(dbConfigs, schemaOverrides, mysqld, true)
	if err != nil {
		return
	}

	log.Infof("starting vtocc %v", *servenv.Port)
	servenv.OnTerm(func() {
		tabletserver.DisallowQueries()
		mysqld.Close()
	})
	servenv.RunDefault()
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
