
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"code.google.com/p/vitess/go/relog"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	CachePoolCap       int
	PoolSize           int
	TransactionCap     int
	TransactionTimeout float64
	MaxResultSize      int
	QueryCacheSize     int
	SchemaReloadTime   float64
	QueryTimeout       float64
	IdleTimeout        float64
}

var config Config = Config {
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

var dbconfig map[string]interface{} = map[string]interface{}{
	"host":        "localhost",
	"port":        0,
	"unix_socket": "",
	"uname":       "vt_app",
	"pass":        "",
	"dbname":      "",
	"charset":     "utf8",
}

var configFile = flag.String("config", "", "config file name")
var dbConfigFile = flag.String("dbconfig", "", "db config file name")
var queryLog = flag.String("querylog", "", "for testing: log all queries to this file")

// Init should be called after the main function calls flag.Parse.
func Init() (Config, map[string]interface{}) {
	if *queryLog != "" {
		if f, err := os.OpenFile(*queryLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err == nil {
			QueryLogger = relog.New(f, "", log.Ldate|log.Lmicroseconds, relog.DEBUG)
		} else {
			relog.Fatal("Error opening file %v: %v", *queryLog, err)
		}
	}
	unmarshalFile(*configFile, &config)
	unmarshalFile(*dbConfigFile, &dbconfig)
	// work-around for jsonism
	if v, ok := dbconfig["port"].(float64); ok {
		dbconfig["port"] = int(v)
	}
	return config, dbconfig
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
