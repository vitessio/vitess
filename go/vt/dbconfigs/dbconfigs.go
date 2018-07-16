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
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
)

var (
	dbFlags       DBConfigFlag
	dbConfigs     DBConfigs
	baseConfig    = mysql.ConnParams{}
	allConnParams = []*mysql.ConnParams{
		&dbConfigs.app,
		&dbConfigs.appDebug,
		&dbConfigs.allPrivs,
		&dbConfigs.dba,
		&dbConfigs.filtered,
		&dbConfigs.repl,
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
	app           mysql.ConnParams
	appDebug      mysql.ConnParams
	allPrivs      mysql.ConnParams
	dba           mysql.ConnParams
	filtered      mysql.ConnParams
	repl          mysql.ConnParams
	DBName        sync2.AtomicString
	SidecarDBName sync2.AtomicString
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

// RegisterFlags registers the flags for the given DBConfigFlag.
// For instance, vttablet will register client, dba and repl.
// Returns all registered flags.
func RegisterFlags(flags DBConfigFlag) {
	if flags == EmptyConfig {
		panic("No DB config is provided.")
	}
	registerBaseFlags()
	if AppConfig&flags != 0 {
		registerUserFlags(&dbConfigs.app, "app")
		dbFlags |= AppConfig
	}
	if AppDebugConfig&flags != 0 {
		registerUserFlags(&dbConfigs.appDebug, "appdebug")
		dbFlags |= AppDebugConfig
	}
	if AllPrivsConfig&flags != 0 {
		registerUserFlags(&dbConfigs.allPrivs, "allprivs")
		dbFlags |= AllPrivsConfig
	}
	if DbaConfig&flags != 0 {
		registerUserFlags(&dbConfigs.dba, "dba")
		dbFlags |= DbaConfig
	}
	if FilteredConfig&flags != 0 {
		registerUserFlags(&dbConfigs.filtered, "filtered")
		dbFlags |= FilteredConfig
	}
	if ReplConfig&flags != 0 {
		registerUserFlags(&dbConfigs.repl, "repl")
		dbFlags |= ReplConfig
	}
}

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

// AppWithDB returns connection parameters for app with dbname set.
func (dbcfgs *DBConfigs) AppWithDB() *mysql.ConnParams {
	result := dbcfgs.app
	result.DbName = dbcfgs.DBName.Get()
	return &result
}

// AppDebugWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) AppDebugWithDB() *mysql.ConnParams {
	result := dbcfgs.appDebug
	result.DbName = dbcfgs.DBName.Get()
	return &result
}

// AllPrivsWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) AllPrivsWithDB() *mysql.ConnParams {
	result := dbcfgs.allPrivs
	result.DbName = dbcfgs.DBName.Get()
	return &result
}

// Dba returns connection parameters for dba with no dbname set.
func (dbcfgs *DBConfigs) Dba() *mysql.ConnParams {
	result := dbcfgs.dba
	return &result
}

// DbaWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) DbaWithDB() *mysql.ConnParams {
	result := dbcfgs.dba
	result.DbName = dbcfgs.DBName.Get()
	return &result
}

// FilteredWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) FilteredWithDB() *mysql.ConnParams {
	result := dbcfgs.filtered
	result.DbName = dbcfgs.DBName.Get()
	return &result
}

// Repl returns connection parameters for appdebug with no dbname set.
func (dbcfgs *DBConfigs) Repl() *mysql.ConnParams {
	result := dbcfgs.repl
	return &result
}

// IsZero returns true if DBConfigs was uninitialized.
func (dbcfgs *DBConfigs) IsZero() bool {
	return dbcfgs.app.Uname == ""
}

func (dbcfgs *DBConfigs) String() string {
	out := struct {
		App           mysql.ConnParams
		AppDebug      string
		AllPrivs      string
		Dba           string
		Filtered      string
		Repl          string
		DBName        string
		SidecarDBName string
	}{
		App:           dbcfgs.app,
		AppDebug:      dbcfgs.appDebug.Uname,
		AllPrivs:      dbcfgs.allPrivs.Uname,
		Dba:           dbcfgs.dba.Uname,
		Filtered:      dbcfgs.filtered.Uname,
		Repl:          dbcfgs.repl.Uname,
		DBName:        dbcfgs.DBName.Get(),
		SidecarDBName: dbcfgs.SidecarDBName.Get(),
	}
	out.App.Pass = "****"
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// Copy returns a copy of the DBConfig.
func (dbcfgs *DBConfigs) Copy() *DBConfigs {
	result := &DBConfigs{
		app:      dbcfgs.app,
		appDebug: dbcfgs.appDebug,
		allPrivs: dbcfgs.allPrivs,
		dba:      dbcfgs.dba,
		filtered: dbcfgs.filtered,
		repl:     dbcfgs.repl,
	}
	result.DBName.Set(dbcfgs.DBName.Get())
	result.SidecarDBName.Set(dbcfgs.SidecarDBName.Get())
	return result
}

// Init will initialize app, allprivs, dba, filtered and repl configs.
func Init(defaultSocketFile string) (*DBConfigs, error) {
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
	if _, err := WithCredentials(&dbConfigs.app); err != nil {
		return nil, fmt.Errorf("dbconfig cannot be initialized: %v", err)
	}
	dbConfigs.SidecarDBName.Set("_vt")

	log.Infof("DBConfigs: %v\n", dbConfigs.String())
	return &dbConfigs, nil
}

// NewTestDBConfigs returns a DBConfigs meant for testing.
func NewTestDBConfigs(genParams, appDebugParams mysql.ConnParams, dbName string) *DBConfigs {
	dbcfgs := &DBConfigs{
		app:      genParams,
		appDebug: appDebugParams,
		allPrivs: genParams,
		dba:      genParams,
		filtered: genParams,
		repl:     genParams,
	}
	dbcfgs.DBName.Set(dbName)
	dbcfgs.SidecarDBName.Set("_vt")
	return dbcfgs
}
