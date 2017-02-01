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
	"github.com/youtube/vitess/go/sqldb"

	// Include both current implementations.
	_ "github.com/youtube/vitess/go/mysql"
	_ "github.com/youtube/vitess/go/mysqlconn"
)

// We keep a global singleton for the db configs, and that's the one
// the flags will change
var dbConfigs = DBConfigs{
	SidecarDBName: "_vt",
}

// DBConfigFlag describes which flags we need
type DBConfigFlag int

// config flags
const (
	EmptyConfig DBConfigFlag = 0
	AppConfig   DBConfigFlag = 1 << iota
	// AllPrivs user should have more privileges than App (should include possibility to do
	// schema changes and write to internal Vitess tables), but it shouldn't have SUPER
	// privilege like Dba has.
	AllPrivsConfig
	DbaConfig
	FilteredConfig
	ReplConfig
)

// redactedPassword is used in redacted configs so it's not in logs.
const redactedPassword = "****"

// The flags will change the global singleton
func registerConnFlags(connParams *sqldb.ConnParams, name string) {
	flag.StringVar(&connParams.Engine, "db-config-"+name+"-engine", "libmysqlclient", "db "+name+" engine to use (empty for default, libmysqlclient or mysqlconn)")
	flag.StringVar(&connParams.Host, "db-config-"+name+"-host", "", "db "+name+" connection host")
	flag.IntVar(&connParams.Port, "db-config-"+name+"-port", 0, "db "+name+" connection port")
	flag.StringVar(&connParams.Uname, "db-config-"+name+"-uname", "", "db "+name+" connection uname")
	flag.StringVar(&connParams.Pass, "db-config-"+name+"-pass", "", "db "+name+" connection pass")
	flag.StringVar(&connParams.DbName, "db-config-"+name+"-dbname", "", "db "+name+" connection dbname")
	flag.StringVar(&connParams.UnixSocket, "db-config-"+name+"-unixsocket", "", "db "+name+" connection unix socket")
	flag.StringVar(&connParams.Charset, "db-config-"+name+"-charset", "", "db "+name+" connection charset")
	flag.Uint64Var(&connParams.Flags, "db-config-"+name+"-flags", 0, "db "+name+" connection flags")
	flag.StringVar(&connParams.SslCa, "db-config-"+name+"-ssl-ca", "", "db "+name+" connection ssl ca")
	flag.StringVar(&connParams.SslCaPath, "db-config-"+name+"-ssl-ca-path", "", "db "+name+" connection ssl ca path")
	flag.StringVar(&connParams.SslCert, "db-config-"+name+"-ssl-cert", "", "db "+name+" connection ssl certificate")
	flag.StringVar(&connParams.SslKey, "db-config-"+name+"-ssl-key", "", "db "+name+" connection ssl key")
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
		registerConnFlags(&dbConfigs.App, "app")
		registeredFlags |= AppConfig
	}
	if AllPrivsConfig&flags != 0 {
		registerConnFlags(&dbConfigs.AllPrivs, "allprivs")
		registeredFlags |= AllPrivsConfig
	}
	if DbaConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Dba, "dba")
		registeredFlags |= DbaConfig
	}
	if FilteredConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Filtered, "filtered")
		registeredFlags |= FilteredConfig
	}
	if ReplConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Repl, "repl")
		registeredFlags |= ReplConfig
	}
	return registeredFlags
}

// initConnParams may overwrite the socket file,
// and refresh the password to check that works.
func initConnParams(cp *sqldb.ConnParams, socketFile string) error {
	// Always try to connect with the socket if provided.
	if socketFile != "" {
		cp.UnixSocket = socketFile
	}

	// See if the CredentialsServer is working. We do not use the
	// result for anything, this is just a check.
	_, err := WithCredentials(cp)
	return err
}

// DBConfigs is all we need for a smart tablet server:
// - App access with db name for serving app queries
// - AllPrivs access for administrative actions (like schema changes)
//   that should be done without SUPER privilege
// - Dba access for any dba-type operation (db creation, replication, ...)
// - Filtered access for filtered replication
// - Replication access to change master
// - SidecarDBName for storing operational metadata
type DBConfigs struct {
	App           sqldb.ConnParams
	AllPrivs      sqldb.ConnParams
	Dba           sqldb.ConnParams
	Filtered      sqldb.ConnParams
	Repl          sqldb.ConnParams
	SidecarDBName string
}

func (dbcfgs *DBConfigs) String() string {
	if dbcfgs.App.Pass != redactedPassword {
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
	dbcfgs.App.Pass = redactedPassword
	dbcfgs.AllPrivs.Pass = redactedPassword
	dbcfgs.Dba.Pass = redactedPassword
	dbcfgs.Filtered.Pass = redactedPassword
	dbcfgs.Repl.Pass = redactedPassword
}

// IsZero returns true if DBConfigs was uninitialized.
func (dbcfgs *DBConfigs) IsZero() bool {
	return dbcfgs.App.Uname == ""
}

// Init will initialize app, allprivs, dba, filtered and repl configs.
func Init(socketFile string, flags DBConfigFlag) (*DBConfigs, error) {
	if flags == EmptyConfig {
		panic("No DB config is provided.")
	}
	if AppConfig&flags != 0 {
		if err := initConnParams(&dbConfigs.App, socketFile); err != nil {
			return nil, fmt.Errorf("app dbconfig cannot be initialized: %v", err)
		}
	}
	if AllPrivsConfig&flags != 0 {
		if err := initConnParams(&dbConfigs.AllPrivs, socketFile); err != nil {
			return nil, fmt.Errorf("allprivs dbconfig cannot be initialized: %v", err)
		}
	}
	if DbaConfig&flags != 0 {
		if err := initConnParams(&dbConfigs.Dba, socketFile); err != nil {
			return nil, fmt.Errorf("dba dbconfig cannot be initialized: %v", err)
		}
	}
	if FilteredConfig&flags != 0 {
		if err := initConnParams(&dbConfigs.Filtered, socketFile); err != nil {
			return nil, fmt.Errorf("filtered dbconfig cannot be initialized: %v", err)
		}
	}
	if ReplConfig&flags != 0 {
		if err := initConnParams(&dbConfigs.Repl, socketFile); err != nil {
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
