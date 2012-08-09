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

var DBConfigsFile = flag.String("db-configs-file", "", "db connection configs file")

type DBConfigs struct {
	App      tabletserver.DBConfig  `json:"app"`
	Dba      mysql.ConnectionParams `json:"dba"`
	Repl     mysql.ConnectionParams `json:"repl"`
	Memcache string                 `json:"memcache"`
}

func Init(mycnf *mysqlctl.Mycnf) (dbcfgs DBConfigs, err error) {
	dbcfgs.Dba = mysql.ConnectionParams{
		Uname:   "vt_dba",
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
