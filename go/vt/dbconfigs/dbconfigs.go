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

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqldbconn"
)

// Offer a default config.
var DefaultDBConfigs = DBConfigs{}

// We keep a global singleton for the db configs, and that's the one
// the flags will change
var dbConfigs DBConfigs

// DBConfigFlag describes which flags we need
type DBConfigFlag int

// config flags
const (
	EmptyConfig DBConfigFlag = 0
	AppConfig   DBConfigFlag = 1 << iota
	DbaConfig
	FilteredConfig
	ReplConfig
)

// DbConfigName describes which DB config we should use
type DbConfigName string

// DBA config
const DbaConfigName DbConfigName = "dba"

// Regular app config
const AppConfigName DbConfigName = "app"

// Config for filtered replication
const FilteredConfigName DbConfigName = "filtered"

// Config for replication
const ReplConfigName DbConfigName = "repl"

// The flags will change the global singleton
func registerConnFlags(connParams *sqldbconn.ConnectionParams, cnfName DbConfigName, defaultParams sqldbconn.ConnectionParams) {
	name := string(cnfName)
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

// RegisterFlags registers the flags for the given DBConfigFlag.
// For instance, vttablet will register client, dba and repl.
// Returns all registered flags.
func RegisterFlags(flags DBConfigFlag) DBConfigFlag {
	if flags == EmptyConfig {
		panic("No DB config is provided.")
	}
	registeredFlags := EmptyConfig
	if AppConfig&flags != 0 {
		registerConnFlags(&dbConfigs.App.ConnectionParams, AppConfigName, DefaultDBConfigs.App.ConnectionParams)
		registeredFlags |= AppConfig
	}
	if DbaConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Dba, DbaConfigName, DefaultDBConfigs.Dba)
		registeredFlags |= DbaConfig
	}
	if FilteredConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Filtered, FilteredConfigName, DefaultDBConfigs.Filtered)
		registeredFlags |= FilteredConfig
	}
	if ReplConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Repl, ReplConfigName, DefaultDBConfigs.Repl)
		registeredFlags |= ReplConfig
	}
	flag.StringVar(&dbConfigs.App.Keyspace, "db-config-app-keyspace", DefaultDBConfigs.App.Keyspace, "db app connection keyspace")
	flag.StringVar(&dbConfigs.App.Shard, "db-config-app-shard", DefaultDBConfigs.App.Shard, "db app connection shard")
	return registeredFlags
}

// initConnectionParams may overwrite the socket file,
// and refresh the password to check that works.
func initConnectionParams(cp *sqldbconn.ConnectionParams, socketFile string) error {
	if socketFile != "" {
		cp.UnixSocket = socketFile
	}
	_, err := MysqlParams(cp)
	return err
}

// MysqlParams returns a copy of our ConnectionParams that we can use
// to connect, after going through the CredentialsServer.
func MysqlParams(cp *sqldbconn.ConnectionParams) (sqldbconn.ConnectionParams, error) {
	result := *cp
	user, passwd, err := GetCredentialsServer().GetUserAndPassword(cp.Uname)
	switch err {
	case nil:
		result.Uname = user
		result.Pass = passwd
	case ErrUnknownUser:
		// we just use what we have, and will fail later anyway
		err = nil
	}
	return result, err
}

// DBConfig encapsulates a ConnectionParams object and adds a keyspace and a
// shard.
type DBConfig struct {
	sqldbconn.ConnectionParams
	Keyspace          string
	Shard             string
	EnableRowcache    bool
	EnableInvalidator bool
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
// - Filtered access for filtered replication
// - Replication access to change master
type DBConfigs struct {
	App      DBConfig
	Dba      sqldbconn.ConnectionParams
	Filtered sqldbconn.ConnectionParams
	Repl     sqldbconn.ConnectionParams
}

func (dbcfgs *DBConfigs) String() string {
	if dbcfgs.App.ConnectionParams.Pass != mysql.RedactedPassword {
		panic("Cannot log a non-redacted DBConfig")
	}
	data, err := json.MarshalIndent(dbcfgs, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// Redact will remove the password, so the object can be logged
func (dbcfgs *DBConfigs) Redact() {
	dbcfgs.App.ConnectionParams.Pass = mysql.RedactedPassword
	dbcfgs.Dba.Pass = mysql.RedactedPassword
	dbcfgs.Filtered.Pass = mysql.RedactedPassword
	dbcfgs.Repl.Pass = mysql.RedactedPassword
}

// Init will initialize app, dba, filterec and repl configs
func Init(socketFile string, flags DBConfigFlag) (*DBConfigs, error) {
	if flags == EmptyConfig {
		panic("No DB config is provided.")
	}
	if AppConfig&flags != 0 {
		if err := initConnectionParams(&dbConfigs.App.ConnectionParams, socketFile); err != nil {
			return nil, fmt.Errorf("app dbconfig cannot be initialized: %v", err)
		}
	}
	if DbaConfig&flags != 0 {
		if err := initConnectionParams(&dbConfigs.Dba, socketFile); err != nil {
			return nil, fmt.Errorf("dba dbconfig cannot be initialized: %v", err)
		}
	}
	if FilteredConfig&flags != 0 {
		if err := initConnectionParams(&dbConfigs.Filtered, socketFile); err != nil {
			return nil, fmt.Errorf("filtered dbconfig cannot be initialized: %v", err)
		}
	}
	if ReplConfig&flags != 0 {
		if err := initConnectionParams(&dbConfigs.Repl, socketFile); err != nil {
			return nil, fmt.Errorf("repl dbconfig cannot be initialized: %v", err)
		}
	}
	// the Dba connection is not linked to a specific database
	// (allows us to create them)
	if dbConfigs.Dba.DbName != "" {
		log.Warningf("dba dbname is set to '%v', ignoring the value", dbConfigs.Dba.DbName)
		dbConfigs.Dba.DbName = ""
	}

	toLog := dbConfigs
	toLog.Redact()
	log.Infof("DBConfigs: %v\n", toLog.String())
	return &dbConfigs, nil
}
