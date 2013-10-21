// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package dbconfigs is reusable by vt tools to load
// the db configs file.
package dbconfigs

import (
	"encoding/json"
	"flag"
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/mysql"
)

// Offer a default config.
var DefaultDBConfigs = DBConfigs{
	App:  DBConfig{ConnectionParams: mysql.ConnectionParams{Uname: "vt_app", Charset: "utf8"}},
	Dba:  mysql.ConnectionParams{Uname: "vt_dba", Charset: "utf8"},
	Repl: mysql.ConnectionParams{Uname: "vt_repl", Charset: "utf8"},
}

var dbConfigs DBConfigs

func registerConnFlags(connParams *mysql.ConnectionParams, name string, defaultParams mysql.ConnectionParams) {
	flag.StringVar(&connParams.Host, "db-config-"+name+"-host", defaultParams.Host, "db "+name+" connection host")
	flag.IntVar(&connParams.Port, "db-config-"+name+"-port", defaultParams.Port, "db "+name+" connection port")
	flag.StringVar(&connParams.Uname, "db-config-"+name+"-uname", defaultParams.Uname, "db "+name+" connection uname")
	flag.StringVar(&connParams.Pass, "db-config-"+name+"-pass", defaultParams.Pass, "db "+name+" connection pass")
	flag.StringVar(&connParams.DbName, "db-config-"+name+"-dbname", defaultParams.DbName, "db "+name+" connection dbname")
	flag.StringVar(&connParams.UnixSocket, "db-config-"+name+"-unixsocket", defaultParams.UnixSocket, "db "+name+" connection unix socket")
	flag.StringVar(&connParams.Charset, "db-config-"+name+"-charset", defaultParams.Charset, "db "+name+" connection charset")
	flag.Uint64Var(&connParams.Flags, "db-config-"+name+"-flags", defaultParams.Flags, "db "+name+" connection flags")
	flag.StringVar(&connParams.SslCa, "db-config-"+name+"-ssl-ca", defaultParams.SslCa, "db "+name+" connection ssl ca")
	flag.StringVar(&connParams.SslCaPath, "db-config-"+name+"-ssl-ca-path", defaultParams.SslCaPath, "db "+name+" connection ssl ca path")
	flag.StringVar(&connParams.SslCert, "db-config-"+name+"-ssl-cert", defaultParams.SslCert, "db "+name+" connection ssl certificate")
	flag.StringVar(&connParams.SslKey, "db-config-"+name+"-ssl-key", defaultParams.SslKey, "db "+name+" connection ssl key")

}

func RegisterAppFlags(defaultDBConfig DBConfig) *string {
	registerConnFlags(&dbConfigs.App.ConnectionParams, "app", defaultDBConfig.MysqlParams())
	flag.StringVar(&dbConfigs.App.Keyspace, "db-config-app-keyspace", defaultDBConfig.Keyspace, "db app connection keyspace")
	flag.StringVar(&dbConfigs.App.Shard, "db-config-app-shard", defaultDBConfig.Shard, "db app connection shard")
	return flag.String("db-credentials-file", "", "db credentials file")
}

func RegisterCommonFlags() *string {
	registerConnFlags(&dbConfigs.Dba, "dba", DefaultDBConfigs.Dba)
	registerConnFlags(&dbConfigs.Repl, "repl", DefaultDBConfigs.Repl)
	return RegisterAppFlags(DefaultDBConfigs.App)
}

type DBConfig struct {
	mysql.ConnectionParams
	Keyspace string `json:"keyspace"`
	Shard    string `json:"shard"`
}

func (d DBConfig) String() string {
	data, err := json.MarshalIndent(d, "", " ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (d DBConfig) Redacted() DBConfig {
	d.Pass = "****"
	return d
}

func (d DBConfig) MysqlParams() mysql.ConnectionParams {
	return d.ConnectionParams
}

type DBConfigs struct {
	App  DBConfig               `json:"app"`
	Dba  mysql.ConnectionParams `json:"dba"`
	Repl mysql.ConnectionParams `json:"repl"`
}

func (dbcfgs DBConfigs) String() string {
	data, err := json.MarshalIndent(dbcfgs, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (dbcfgs DBConfigs) Redacted() DBConfigs {
	dbcfgs.App = dbcfgs.App.Redacted()
	dbcfgs.Dba = dbcfgs.Dba.Redacted()
	dbcfgs.Repl = dbcfgs.Repl.Redacted()
	return dbcfgs
}

// Map user to a list of passwords. Right now we only use the first.
type dbCredentials map[string][]string

func Init(socketFile, dbCredentialsFile string) (DBConfigs, error) {
	var err error
	if dbCredentialsFile != "" {
		dbCreds := make(dbCredentials)
		if err = jscfg.ReadJson(dbCredentialsFile, &dbCreds); err != nil {
			return dbConfigs, err
		}
		if passwd, ok := dbCreds[dbConfigs.App.Uname]; ok {
			dbConfigs.App.Pass = passwd[0]
		}
		if passwd, ok := dbCreds[dbConfigs.Dba.Uname]; ok {
			dbConfigs.Dba.Pass = passwd[0]
		}
		if passwd, ok := dbCreds[dbConfigs.Repl.Uname]; ok {
			dbConfigs.Repl.Pass = passwd[0]
		}
	}
	if socketFile != "" {
		dbConfigs.App.UnixSocket = socketFile
		dbConfigs.Dba.UnixSocket = socketFile
		dbConfigs.Repl.UnixSocket = socketFile
	}
	log.Infof("DBConfigs: %s\n", dbConfigs.Redacted())
	return dbConfigs, err
}

func GetSubprocessFlags() []string {
	cmd := []string{}
	f := func(connParams *mysql.ConnectionParams, name string) {
		if connParams.Host != "" {
			cmd = append(cmd, "-db-config-"+name+"-host", connParams.Host)
		}
		if connParams.Port > 0 {
			cmd = append(cmd, "-db-config-"+name+"-port", strconv.Itoa(connParams.Port))
		}
		if connParams.Uname != "" {
			cmd = append(cmd, "-db-config-"+name+"-uname", connParams.Uname)
		}
		if connParams.DbName != "" {
			cmd = append(cmd, "-db-config-"+name+"-dbname", connParams.DbName)
		}
		if connParams.UnixSocket != "" {
			cmd = append(cmd, "-db-config-"+name+"-unixsocket", connParams.UnixSocket)
		}
		if connParams.Charset != "" {
			cmd = append(cmd, "-db-config-"+name+"-charset", connParams.Charset)
		}
		if connParams.Flags > 0 {
			cmd = append(cmd, "-db-config-"+name+"-flags", strconv.FormatUint(connParams.Flags, 10))
		}
		if connParams.SslCa != "" {
			cmd = append(cmd, "-db-config-"+name+"-ssl-ca", connParams.SslCa)
		}
		if connParams.SslCaPath != "" {
			cmd = append(cmd, "-db-config-"+name+"-ssl-ca-path", connParams.SslCaPath)
		}
		if connParams.SslCert != "" {
			cmd = append(cmd, "-db-config-"+name+"-ssl-cert", connParams.SslCert)
		}
		if connParams.SslKey != "" {
			cmd = append(cmd, "-db-config-"+name+"-ssl-key", connParams.SslKey)
		}
	}
	f(&dbConfigs.App.ConnectionParams, "app")
	if dbConfigs.App.Keyspace != "" {
		cmd = append(cmd, "-db-config-app-keyspace", dbConfigs.App.Keyspace)
	}
	if dbConfigs.App.Shard != "" {
		cmd = append(cmd, "-db-config-app-shard", dbConfigs.App.Shard)
	}
	f(&dbConfigs.Dba, "dba")
	f(&dbConfigs.Repl, "repl")
	return cmd
}
