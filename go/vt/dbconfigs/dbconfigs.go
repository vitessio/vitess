/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package dbconfigs is reusable by vt tools to load
// the db configs file.
package dbconfigs

import (
	"encoding/json"
	"flag"
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
)

var (
	dbFlags       DBConfigFlag
	dbConfigs     = DBConfigs{SidecarDBName: "_vt"}
	baseConfig    = mysql.ConnParams{}
	allConnParams = []*mysql.ConnParams{
		&dbConfigs.App,
		&dbConfigs.AppDebug,
		&dbConfigs.AllPrivs,
		&dbConfigs.Dba,
		&dbConfigs.Filtered,
		&dbConfigs.Repl,
	}
)

// DBConfigs is all we need for a smart tablet server:
// - App access with db name for serving app queries
// - AllPrivs access for administrative actions (like schema changes)
//   that should be done without SUPER privilege
// - Dba access for any dba-type operation (db creation, replication, ...)
// - Filtered access for filtered replication
// - Replication access to change master
// - SidecarDBName for storing operational metadata
type DBConfigs struct {
	App           mysql.ConnParams
	AppDebug      mysql.ConnParams
	AllPrivs      mysql.ConnParams
	Dba           mysql.ConnParams
	Filtered      mysql.ConnParams
	Repl          mysql.ConnParams
	SidecarDBName string
}

// DBConfigFlag describes which flags we need
type DBConfigFlag int

// config flags
const (
	EmptyConfig DBConfigFlag = 0
	AppConfig   DBConfigFlag = 1 << iota
	AppDebugConfig
	// AllPrivs user should have more privileges than App (should include possibility to do
	// schema changes and write to internal Vitess tables), but it shouldn't have SUPER
	// privilege like Dba has.
	AllPrivsConfig
	DbaConfig
	FilteredConfig
	ReplConfig
)

// AllConfig represents all connection configurations.
const AllConfig = (AppConfig | AppDebugConfig | AllPrivsConfig | DbaConfig | FilteredConfig | ReplConfig)

// AllButDbaConfig represents all connection configurations except Dba.
const AllButDbaConfig = (AppConfig | AppDebugConfig | AllPrivsConfig | FilteredConfig | ReplConfig)

// redactedPassword is used in redacted configs so it's not in logs.
const redactedPassword = "****"

func registerBaseFlags() {
	flag.StringVar(&baseConfig.Host, "db_host", "", "connection host")
	flag.IntVar(&baseConfig.Port, "db_port", 0, "connection port")
	flag.StringVar(&baseConfig.UnixSocket, "db_socket", "", "connection unix socket")
	flag.StringVar(&baseConfig.Charset, "db_charset", "utf8", "connection charset")
	flag.Uint64Var(&baseConfig.Flags, "db_flags", 0, "connection flags")
	flag.StringVar(&baseConfig.SslCa, "db_ssl_ca", "", "connection ssl ca")
	flag.StringVar(&baseConfig.SslCaPath, "db_ssl_ca_path", "", "connection ssl ca path")
	flag.StringVar(&baseConfig.SslCert, "db_ssl_cert", "", "connection ssl certificate")
	flag.StringVar(&baseConfig.SslKey, "db_ssl_key", "", "connection ssl key")
}

// The flags will change the global singleton
// TODO(sougou): deprecate the legacy flags.
func registerUserFlags(connParams *mysql.ConnParams, name string) {
	newUserFlag := "db_" + name + "_user"
	flag.StringVar(&connParams.Uname, "db-config-"+name+"-uname", "vt_"+name, "deprecated: use "+newUserFlag)
	flag.StringVar(&connParams.Uname, newUserFlag, "vt_"+name, "db "+name+" user name")

	newPasswordFlag := "db_" + name + "_password"
	flag.StringVar(&connParams.Pass, "db-config-"+name+"-pass", "", "db "+name+" deprecated: use "+newPasswordFlag)
	flag.StringVar(&connParams.Pass, newPasswordFlag, "", "db "+name+" password")

	flag.StringVar(&connParams.Host, "db-config-"+name+"-host", "", "deprecated: use db_host")
	flag.IntVar(&connParams.Port, "db-config-"+name+"-port", 0, "deprecated: use db_port")
	flag.StringVar(&connParams.UnixSocket, "db-config-"+name+"-unixsocket", "", "deprecated: use db_socket")
	flag.StringVar(&connParams.Charset, "db-config-"+name+"-charset", "utf8", "deprecated: use db_charset")
	flag.Uint64Var(&connParams.Flags, "db-config-"+name+"-flags", 0, "deprecated: use db_flags")
	flag.StringVar(&connParams.SslCa, "db-config-"+name+"-ssl-ca", "", "deprecated: use db_ssl_ca")
	flag.StringVar(&connParams.SslCaPath, "db-config-"+name+"-ssl-ca-path", "", "deprecated: use db_ssl_ca_path")
	flag.StringVar(&connParams.SslCert, "db-config-"+name+"-ssl-cert", "", "deprecated: use db_ssl_cert")
	flag.StringVar(&connParams.SslKey, "db-config-"+name+"-ssl-key", "", "deprecated: use db_ssl_key")
}

// RegisterFlags registers the flags for the given DBConfigFlag.
// For instance, vttablet will register client, dba and repl.
// Returns all registered flags.
func RegisterFlags(flags DBConfigFlag) {
	if flags == EmptyConfig {
		panic("No DB config is provided.")
	}
	registerBaseFlags()
	if AppConfig&flags != 0 {
		registerUserFlags(&dbConfigs.App, "app")
		dbFlags |= AppConfig
	}
	if AppDebugConfig&flags != 0 {
		registerUserFlags(&dbConfigs.AppDebug, "appdebug")
		dbFlags |= AppDebugConfig
	}
	if AllPrivsConfig&flags != 0 {
		registerUserFlags(&dbConfigs.AllPrivs, "allprivs")
		dbFlags |= AllPrivsConfig
	}
	if DbaConfig&flags != 0 {
		registerUserFlags(&dbConfigs.Dba, "dba")
		dbFlags |= DbaConfig
	}
	if FilteredConfig&flags != 0 {
		registerUserFlags(&dbConfigs.Filtered, "filtered")
		dbFlags |= FilteredConfig
	}
	if ReplConfig&flags != 0 {
		registerUserFlags(&dbConfigs.Repl, "repl")
		dbFlags |= ReplConfig
	}
}

func (dbcfgs *DBConfigs) String() string {
	if dbcfgs.App.Pass != redactedPassword {
		panic("Cannot log a non-redacted DBConfig")
	}
	if dbcfgs.AppDebug.Pass != redactedPassword {
		panic("Cannot log a non-redacted DBConfig")
	}
	data, err := json.MarshalIndent(dbcfgs, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// SetDBName sets the dbname for the specified connections.
func (dbcfgs *DBConfigs) SetDBName(dbName string, flags DBConfigFlag) {
	if AppConfig&flags != 0 {
		dbcfgs.App.DbName = dbName
	}
	if AppDebugConfig&flags != 0 {
		dbcfgs.AppDebug.DbName = dbName
	}
	if AllPrivsConfig&flags != 0 {
		dbcfgs.AllPrivs.DbName = dbName
	}
	if DbaConfig&flags != 0 {
		dbcfgs.Dba.DbName = dbName
	}
	if FilteredConfig&flags != 0 {
		dbcfgs.Filtered.DbName = dbName
	}
	if ReplConfig&flags != 0 {
		dbcfgs.Repl.DbName = dbName
	}
}

// Redact will remove the password, so the object can be logged
func (dbcfgs *DBConfigs) Redact() {
	dbcfgs.App.Pass = redactedPassword
	dbcfgs.AppDebug.Pass = redactedPassword
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
func Init(defaultSocketFile string) (DBConfigs, error) {
	if dbFlags == EmptyConfig {
		panic("No DB config is provided.")
	}

	// This is to support legacy behavior: use supplied socket value
	// if conn parameters are not specified.
	// TODO(sougou): deprecate.
	for _, param := range allConnParams {
		if param.UnixSocket == "" && param.Host == "" {
			param.UnixSocket = defaultSocketFile
		}
	}

	// The new base configs, if set, supersede legacy settings.
	if baseConfig.Host != "" || baseConfig.UnixSocket != "" {
		for _, param := range allConnParams {
			tmpconfig := baseConfig
			tmpconfig.Uname = param.Uname
			tmpconfig.Pass = param.Pass
			*param = tmpconfig
		}
	}

	// See if the CredentialsServer is working. We do not use the
	// result for anything, this is just a check.
	if _, err := WithCredentials(&dbConfigs.App); err != nil {
		return DBConfigs{}, fmt.Errorf("dbconfig cannot be initialized: %v", err)
	}

	toLog := dbConfigs
	toLog.Redact()
	log.Infof("DBConfigs: %v\n", toLog.String())
	return dbConfigs, nil
}
