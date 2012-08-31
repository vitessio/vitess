// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package dbconfigs is reusable by vt tools to load
// the db configs file.
package dbconfigs

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/tabletserver"
)

// Offer a sample config - probably should load this when file isn't set.
const defaultConfig = `{
  "app": {
    "uname": "vt_app",
    "charset": "utf8"
  },
  "dba": {
    "uname": "vt_dba",
    "charset": "utf8"
  },
  "repl": {
    "uname": "vt_repl",
    "charset": "utf8"
  }
}`

// FIXME(msolomon) the usage of this string seems odd. If this
// is truly universal to all apps this variable should be private.
// It's unclear why we need both Init and ReadJson - there should
// only be one public entry point.
var DBConfigsFile = flag.String("db-configs-file", "", "db connection configs file")

type DBConfigs struct {
	App      tabletserver.DBConfig  `json:"app"`
	Dba      mysql.ConnectionParams `json:"dba"`
	Repl     mysql.ConnectionParams `json:"repl"`
	Memcache string                 `json:"memcache"`
}

func Init(mycnf *mysqlctl.Mycnf) (dbcfgs DBConfigs, err error) {
	dbcfgs.App = tabletserver.DBConfig{
		Uname:   "vt_app",
		Charset: "utf8",
	}
	dbcfgs.Dba = mysql.ConnectionParams{
		Uname:   "vt_dba",
		Charset: "utf8",
	}
	dbcfgs.Repl = mysql.ConnectionParams{
		Uname:   "vt_repl",
		Charset: "utf8",
	}
	err = ReadJson(*DBConfigsFile, &dbcfgs)
	dbcfgs.App.UnixSocket = mycnf.SocketFile
	dbcfgs.Dba.UnixSocket = mycnf.SocketFile
	return
}

func ReadJson(name string, val interface{}) error {
	if name != "" {
		data, err := ioutil.ReadFile(name)
		if err != nil {
			return fmt.Errorf("could not read %v: %v", val, err)
		}
		if err = json.Unmarshal(data, val); err != nil {
			return fmt.Errorf("could not read %s: %v", val, err)
		}
	}
	data, _ := json.MarshalIndent(val, "", "  ")
	relog.Info("%s: %s\n", name, data)
	return nil
}
