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
	"github.com/youtube/vitess/go/mysql"
)

// Offer a default config.
var DefaultDBConfigs = DBConfigs{
	App: DBConfig{
		ConnectionParams: mysql.ConnectionParams{
			Uname:   "vt_app",
			Charset: "utf8",
		},
	},
	Dba: mysql.ConnectionParams{
		Uname:   "vt_dba",
		Charset: "utf8",
	},
	Repl: mysql.ConnectionParams{
		Uname:   "vt_repl",
		Charset: "utf8",
	},
}

// We keep a global singleton for the db configs, and that's the one
// the flags will change
var dbConfigs DBConfigs

// The flags will change the global singleton
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

// vtocc will only register the app flags, it doesn't do any dba or repl
// access.
func RegisterAppFlags(defaultDBConfig DBConfig) {
	registerConnFlags(&dbConfigs.App.ConnectionParams, "app", defaultDBConfig.ConnectionParams)
	flag.StringVar(&dbConfigs.App.Keyspace, "db-config-app-keyspace", defaultDBConfig.Keyspace, "db app connection keyspace")
	flag.StringVar(&dbConfigs.App.Shard, "db-config-app-shard", defaultDBConfig.Shard, "db app connection shard")
}

// vttablet will register client, dba and repl.
func RegisterCommonFlags() {
	registerConnFlags(&dbConfigs.Dba, "dba", DefaultDBConfigs.Dba)
	registerConnFlags(&dbConfigs.Repl, "repl", DefaultDBConfigs.Repl)
	RegisterAppFlags(DefaultDBConfigs.App)
}

// InitConnectionParams may overwrite the socket file,
// and refresh the password to check that works.
func InitConnectionParams(cp *mysql.ConnectionParams, socketFile string) error {
	if socketFile != "" {
		cp.UnixSocket = socketFile
	}
	params := *cp
	return refreshPassword(&params)
}

// refreshPassword uses the CredentialServer to refresh the password
// to use.
func refreshPassword(params *mysql.ConnectionParams) error {
	user, passwd, err := GetCredentialsServer().GetUserAndPassword(params.Uname)
	switch err {
	case nil:
		params.Uname = user
		params.Pass = passwd
	case ErrUnknownUser:
	default:
		return err
	}
	return nil
}

// returns a copy of our ConnectionParams that we can use to connect,
// after going through the CredentialsServer.
func MysqlParams(cp *mysql.ConnectionParams) (mysql.ConnectionParams, error) {
	params := *cp
	err := refreshPassword(&params)
	return params, err
}

// DBConfig encapsulates a ConnectionParams object and adds a keyspace and a
// shard.
type DBConfig struct {
	mysql.ConnectionParams
	Keyspace string `json:"keyspace"`
	Shard    string `json:"shard"`
}

func (d *DBConfig) String() string {
	data, err := json.MarshalIndent(d, "", " ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// DBConfigs is all we need for a smart tablet server:
// - DBConfig for the query engine, running for the specified keyspace / shard
// - Dba access for any dba-type operation (db creation, replication, ...)
// - Replication access to change master
type DBConfigs struct {
	App  DBConfig               `json:"app"`
	Dba  mysql.ConnectionParams `json:"dba"`
	Repl mysql.ConnectionParams `json:"repl"`
}

func (dbcfgs *DBConfigs) String() string {
	if dbcfgs.App.ConnectionParams.Pass != mysql.REDACTED_PASSWORD {
		panic("Cannot log a non-redacted DBConfig")
	}
	data, err := json.MarshalIndent(dbcfgs, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// This will remove the password, so the object can be logged
func (dbcfgs *DBConfigs) Redact() {
	dbcfgs.App.ConnectionParams.Redact()
	dbcfgs.Dba.Redact()
	dbcfgs.Repl.Redact()
}

// Initialize only the app side of the db configs (for vtocc)
func InitApp(socketFile string) (*DBConfig, error) {
	if err := InitConnectionParams(&dbConfigs.App.ConnectionParams, socketFile); err != nil {
		return nil, err
	}
	return &dbConfigs.App, nil
}

// Initialize app, dba and repl configs
func Init(socketFile string) (*DBConfigs, error) {
	if _, err := InitApp(socketFile); err != nil {
		return nil, err
	}

	// init configs
	if err := InitConnectionParams(&dbConfigs.Dba, socketFile); err != nil {
		return nil, err
	}
	if err := InitConnectionParams(&dbConfigs.Repl, socketFile); err != nil {
		return nil, err
	}

	// the Dba connection is not linked to a specific database
	// (allows us to create them)
	dbConfigs.Dba.DbName = ""

	toLog := dbConfigs
	toLog.Redact()
	log.Infof("DBConfigs: %s\n", toLog)
	return &dbConfigs, nil
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
	cmd = append(cmd, getCredentialsServerSubprocessFlags()...)
	return cmd
}
