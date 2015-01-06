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

// RegisterFlags registers the flags for the given DBConfigFlag.
// For instance, vttablet will register client, dba and repl.
// Returns all registered flags.
func RegisterFlags(flags DBConfigFlag) DBConfigFlag {
	if flags == EmptyConfig {
		panic("No DB config is provided.")
	}
	registeredFlags := EmptyConfig
	if AppConfig&flags != 0 {
		registerConnFlags(&dbConfigs.App.ConnectionParams, "app", DefaultDBConfigs.App.ConnectionParams)
		registeredFlags |= AppConfig
	}
	if DbaConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Dba, "dba", DefaultDBConfigs.Dba)
		registeredFlags |= DbaConfig
	}
	if FilteredConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Filtered, "filtered", DefaultDBConfigs.Filtered)
		registeredFlags |= FilteredConfig
	}
	if ReplConfig&flags != 0 {
		registerConnFlags(&dbConfigs.Repl, "repl", DefaultDBConfigs.Repl)
		registeredFlags |= ReplConfig
	}
	flag.StringVar(&dbConfigs.App.Keyspace, "db-config-app-keyspace", DefaultDBConfigs.App.Keyspace, "db app connection keyspace")
	flag.StringVar(&dbConfigs.App.Shard, "db-config-app-shard", DefaultDBConfigs.App.Shard, "db app connection shard")
	return registeredFlags
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

// MysqlParams returns a copy of our ConnectionParams that we can use
// to connect, after going through the CredentialsServer.
func MysqlParams(cp *mysql.ConnectionParams) (mysql.ConnectionParams, error) {
	params := *cp
	err := refreshPassword(&params)
	return params, err
}

// DBConfig encapsulates a ConnectionParams object and adds a keyspace and a
// shard.
type DBConfig struct {
	mysql.ConnectionParams
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
	Dba      mysql.ConnectionParams
	Filtered mysql.ConnectionParams
	Repl     mysql.ConnectionParams
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
	dbcfgs.App.ConnectionParams.Redact()
	dbcfgs.Dba.Redact()
	dbcfgs.Filtered.Redact()
	dbcfgs.Repl.Redact()
}

// Init will initialize app, dba, filterec and repl configs
func Init(socketFile string, flags DBConfigFlag) (*DBConfigs, error) {
	if flags == EmptyConfig {
		panic("No DB config is provided.")
	}
	if AppConfig&flags != 0 {
		if err := InitConnectionParams(&dbConfigs.App.ConnectionParams, socketFile); err != nil {
			return nil, err
		}
	}
	if DbaConfig&flags != 0 {
		if err := InitConnectionParams(&dbConfigs.Dba, socketFile); err != nil {
			return nil, err
		}
	}
	if FilteredConfig&flags != 0 {
		if err := InitConnectionParams(&dbConfigs.Filtered, socketFile); err != nil {
			return nil, err
		}
	}
	if ReplConfig&flags != 0 {
		if err := InitConnectionParams(&dbConfigs.Repl, socketFile); err != nil {
			return nil, err
		}
	}
	// the Dba connection is not linked to a specific database
	// (allows us to create them)
	dbConfigs.Dba.DbName = ""

	toLog := dbConfigs
	toLog.Redact()
	log.Infof("DBConfigs: %v\n", toLog.String())
	return &dbConfigs, nil
}

// GetSubprocessFlags returns the flags to send to a subprocess so it has the
// same config as us.
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
	f(&dbConfigs.Filtered, "filtered")
	f(&dbConfigs.Repl, "repl")
	cmd = append(cmd, getCredentialsServerSubprocessFlags()...)
	return cmd
}
