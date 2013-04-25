// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package dbconfigs is reusable by vt tools to load
// the db configs file.
package dbconfigs

import (
	"encoding/json"
	"flag"

	"code.google.com/p/vitess/go/jscfg"
	"code.google.com/p/vitess/go/mysql"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/key"
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

func RegisterCommonFlags() (*string, *string) {
	dbConfigsFile := flag.String("db-configs-file", "", "db connection configs file")
	dbCredentialsFile := flag.String("db-credentials-file", "", "db credentials file")
	return dbConfigsFile, dbCredentialsFile
}

type DBConfig struct {
	Host       string       `json:"host"`
	Port       int          `json:"port"`
	Uname      string       `json:"uname"`
	Pass       string       `json:"pass"`
	Dbname     string       `json:"dbname"`
	UnixSocket string       `json:"unix_socket"`
	Charset    string       `json:"charset"`
	Memcache   string       `json:"memcache"`
	KeyRange   key.KeyRange `json:"keyrange"`
	Keyspace   string       `json:"keyspace"`
	Shard      string       `json:"shard"`
}

func (d DBConfig) String() string {
	data, err := json.MarshalIndent(d, "", " ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (d DBConfig) Redacted() interface{} {
	d.Pass = relog.Redact(d.Pass)
	return d
}

func (d DBConfig) MysqlParams() mysql.ConnectionParams {
	return mysql.ConnectionParams{
		Host:       d.Host,
		Port:       d.Port,
		Uname:      d.Uname,
		Pass:       d.Pass,
		Dbname:     d.Dbname,
		UnixSocket: d.UnixSocket,
		Charset:    d.Charset,
	}
}

type DBConfigs struct {
	App      DBConfig               `json:"app"`
	Dba      mysql.ConnectionParams `json:"dba"`
	Repl     mysql.ConnectionParams `json:"repl"`
	Memcache string                 `json:"memcache"`
}

func (dbcfgs DBConfigs) String() string {
	data, err := json.MarshalIndent(dbcfgs, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (dbcfgs DBConfigs) Redacted() interface{} {
	dbcfgs.App = dbcfgs.App.Redacted().(DBConfig)
	dbcfgs.Dba = dbcfgs.Dba.Redacted().(mysql.ConnectionParams)
	dbcfgs.Repl = dbcfgs.Repl.Redacted().(mysql.ConnectionParams)
	return dbcfgs
}

// Map user to a list of passwords. Right now we only use the first.
type dbCredentials map[string][]string

func Init(socketFile, dbConfigsFile, dbCredentialsFile string) (dbcfgs DBConfigs, err error) {
	dbcfgs.App = DBConfig{
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
	if dbConfigsFile != "" {
		if err = jscfg.ReadJson(dbConfigsFile, &dbcfgs); err != nil {
			return
		}
	}

	if dbCredentialsFile != "" {
		dbCreds := make(dbCredentials)
		if err = jscfg.ReadJson(dbCredentialsFile, &dbCreds); err != nil {
			return
		}
		if passwd, ok := dbCreds[dbcfgs.App.Uname]; ok {
			dbcfgs.App.Pass = passwd[0]
		}
		if passwd, ok := dbCreds[dbcfgs.Dba.Uname]; ok {
			dbcfgs.Dba.Pass = passwd[0]
		}
		if passwd, ok := dbCreds[dbcfgs.Repl.Uname]; ok {
			dbcfgs.Repl.Pass = passwd[0]
		}
	}
	dbcfgs.App.UnixSocket = socketFile
	dbcfgs.Dba.UnixSocket = socketFile
	dbcfgs.Repl.UnixSocket = socketFile
	relog.Info("%s: %s\n", dbConfigsFile, dbcfgs)
	return
}
