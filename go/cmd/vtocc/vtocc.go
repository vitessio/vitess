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
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/servenv"
	ts "github.com/youtube/vitess/go/vt/tabletserver"
)

var (
	port          = flag.Int("port", 6510, "tcp port to serve on")
	overridesFile = flag.String("schema-override", "", "schema overrides file")
)

var DefaultDBConfig = dbconfigs.DBConfig{
	ConnectionParams: mysql.ConnectionParams{
		Host:    "localhost",
		Uname:   "vt_app",
		Charset: "utf8",
	},
}

var schemaOverrides []ts.SchemaOverride

func main() {
	dbCredentialsFile := dbconfigs.RegisterAppFlags(DefaultDBConfig)
	flag.Parse()
	servenv.Init()

	dbConfigs, _ := dbconfigs.Init("", *dbCredentialsFile)

	unmarshalFile(*overridesFile, &schemaOverrides)
	data, _ := json.MarshalIndent(schemaOverrides, "", "  ")
	log.Infof("schemaOverrides: %s\n", data)

	ts.InitQueryService()

	ts.AllowQueries(dbConfigs.App, schemaOverrides, ts.LoadCustomRules())

	log.Infof("starting vtocc %v", *port)
	servenv.OnClose(func() {
		time.Sleep(5 * time.Millisecond)
		ts.DisallowQueries()
	})
	servenv.Run(*port)
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
