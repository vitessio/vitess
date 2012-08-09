// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package dbcredentials is reusable by vt tools to load
// the db credentials file.
package dbcredentials

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

var DBCredsFile = flag.String("db-credentials-file", "", "db connection credentials file")

type DBCredentials struct {
	App      tabletserver.DBConfig  `json:"app"`
	Dba      mysql.ConnectionParams `json:"dba"`
	Repl     mysql.ConnectionParams `json:"repl"`
	Memcache string                 `json:"memcache"`
}

func Init(mycnf *mysqlctl.Mycnf) (dbcreds DBCredentials, err error) {
	dbcreds.Dba = mysql.ConnectionParams{
		Uname:   "vt_dba",
		Charset: "utf8",
	}
	err = ReadJson(*DBCredsFile, &dbcreds)
	dbcreds.App.UnixSocket = mycnf.SocketFile
	dbcreds.Dba.UnixSocket = mycnf.SocketFile
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
