/*
Copyright 2019 The Vitess Authors.

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

// Package dbconfigs provides the registration for command line options
// to collect db connection parameters. Once registered and collected,
// it provides variables and functions to build connection parameters
// for connecting to the database.
package dbconfigs

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
)

var (
	dbConfigs  = DBConfigs{userConfigs: make(map[string]*userConfig)}
	baseConfig = mysql.ConnParams{}
)

// DBConfigs stores all the data needed to build various connection
// parameters for the db. It stores credentials for app, appdebug,
// allprivs, dba, filtered and repl users.
// It contains other connection parameters like socket, charset, etc.
// It also stores the default db name, which it can combine with the
// rest of the data to build db-sepcific connection parameters.
// It also supplies the SidecarDBName. This is currently hardcoded
// to "_vt", but will soon become customizable.
// The life-cycle of this package is as follows:
// App must call RegisterFlags to request the types of connections
// it wants support for. This must be done before involing flags.Parse.
// After flag parsing, app invokes the Init function, which will return
// a DBConfigs object.
// The app must store the DBConfigs object internally, and use it to
// build connection parameters as needed.
// The DBName is initially empty and may later be set or changed by the app.
type DBConfigs struct {
	userConfigs   map[string]*userConfig
	DBName        sync2.AtomicString
	SidecarDBName sync2.AtomicString
}

type userConfig struct {
	useSSL bool
	param  mysql.ConnParams
}

// config flags
const (
	App      = "app"
	AppDebug = "appdebug"
	// AllPrivs user should have more privileges than App (should include possibility to do
	// schema changes and write to internal Vitess tables), but it shouldn't have SUPER
	// privilege like Dba has.
	AllPrivs     = "allprivs"
	Dba          = "dba"
	Filtered     = "filtered"
	Repl         = "repl"
	ExternalRepl = "erepl"
)

// All can be used to register all flags: RegisterFlags(All...)
var All = []string{App, AppDebug, AllPrivs, Dba, Filtered, Repl, ExternalRepl}

// RegisterFlags registers the flags for the given DBConfigFlag.
// For instance, vttablet will register client, dba and repl.
// Returns all registered flags.
func RegisterFlags(userKeys ...string) {
	registerBaseFlags()
	for _, userKey := range userKeys {
		uc := &userConfig{}
		dbConfigs.userConfigs[userKey] = uc
		registerPerUserFlags(uc, userKey)
	}
}

func registerBaseFlags() {
	flag.StringVar(&baseConfig.UnixSocket, "db_socket", "", "The unix socket to connect on. If this is specified, host and port will not be used.")
	flag.StringVar(&baseConfig.Host, "db_host", "", "The host name for the tcp connection.")
	flag.IntVar(&baseConfig.Port, "db_port", 0, "tcp port")
	flag.StringVar(&baseConfig.Charset, "db_charset", "", "Character set. Only utf8 or latin1 based character sets are supported.")
	flag.Uint64Var(&baseConfig.Flags, "db_flags", 0, "Flag values as defined by MySQL.")
	flag.StringVar(&baseConfig.Flavor, "db_flavor", "", "Flavor overrid. Valid value is FilePos.")
	flag.StringVar(&baseConfig.SslCa, "db_ssl_ca", "", "connection ssl ca")
	flag.StringVar(&baseConfig.SslCaPath, "db_ssl_ca_path", "", "connection ssl ca path")
	flag.StringVar(&baseConfig.SslCert, "db_ssl_cert", "", "connection ssl certificate")
	flag.StringVar(&baseConfig.SslKey, "db_ssl_key", "", "connection ssl key")
	flag.StringVar(&baseConfig.ServerName, "db_server_name", "", "server name of the DB we are connecting to.")
	flag.Uint64Var(&baseConfig.ConnectTimeoutMs, "db_connect_timeout_ms", 0, "connection timeout to mysqld in milliseconds (0 for no timeout)")
}

// The flags will change the global singleton
// TODO(sougou): deprecate the legacy flags.
func registerPerUserFlags(dbc *userConfig, userKey string) {
	newUserFlag := "db_" + userKey + "_user"
	flag.StringVar(&dbc.param.Uname, "db-config-"+userKey+"-uname", "vt_"+userKey, "deprecated: use "+newUserFlag)
	flag.StringVar(&dbc.param.Uname, newUserFlag, "vt_"+userKey, "db "+userKey+" user userKey")

	newPasswordFlag := "db_" + userKey + "_password"
	flag.StringVar(&dbc.param.Pass, "db-config-"+userKey+"-pass", "", "db "+userKey+" deprecated: use "+newPasswordFlag)
	flag.StringVar(&dbc.param.Pass, newPasswordFlag, "", "db "+userKey+" password")

	flag.BoolVar(&dbc.useSSL, "db_"+userKey+"_use_ssl", true, "Set this flag to false to make the "+userKey+" connection to not use ssl")

	flag.StringVar(&dbc.param.Host, "db-config-"+userKey+"-host", "", "deprecated: use db_host")
	flag.IntVar(&dbc.param.Port, "db-config-"+userKey+"-port", 0, "deprecated: use db_port")
	flag.StringVar(&dbc.param.UnixSocket, "db-config-"+userKey+"-unixsocket", "", "deprecated: use db_socket")
	flag.StringVar(&dbc.param.Charset, "db-config-"+userKey+"-charset", "utf8", "deprecated: use db_charset")
	flag.Uint64Var(&dbc.param.Flags, "db-config-"+userKey+"-flags", 0, "deprecated: use db_flags")
	flag.StringVar(&dbc.param.SslCa, "db-config-"+userKey+"-ssl-ca", "", "deprecated: use db_ssl_ca")
	flag.StringVar(&dbc.param.SslCaPath, "db-config-"+userKey+"-ssl-ca-path", "", "deprecated: use db_ssl_ca_path")
	flag.StringVar(&dbc.param.SslCert, "db-config-"+userKey+"-ssl-cert", "", "deprecated: use db_ssl_cert")
	flag.StringVar(&dbc.param.SslKey, "db-config-"+userKey+"-ssl-key", "", "deprecated: use db_ssl_key")
	flag.StringVar(&dbc.param.ServerName, "db-config-"+userKey+"-server_name", "", "deprecated: use db_server_name")
	flag.StringVar(&dbc.param.Flavor, "db-config-"+userKey+"-flavor", "", "deprecated: use db_flavor")

	flag.StringVar(&dbc.param.DeprecatedDBName, "db-config-"+userKey+"-dbname", "", "deprecated: dbname does not need to be explicitly configured")

}

// Connector contains Connection Parameters for mysql connection
type Connector struct {
	connParams *mysql.ConnParams
	dbName     string
	host       string
}

// New initializes a ConnParams from mysql connection parameters
func New(mcp *mysql.ConnParams) Connector {
	return Connector{
		connParams: mcp,
	}
}

// Connect will invoke the mysql.connect method and return a connection
func (c Connector) Connect(ctx context.Context) (*mysql.Conn, error) {
	params, err := c.MysqlParams()
	if err != nil {
		return nil, err
	}
	conn, err := mysql.Connect(ctx, params)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// MysqlParams returns the connections params
func (c Connector) MysqlParams() (*mysql.ConnParams, error) {
	params, err := withCredentials(c.connParams)
	if err != nil {
		return nil, err
	}
	return params, nil
}

// DBName gets the dbname from mysql.ConnParams
func (c Connector) DBName() string {
	params, _ := c.MysqlParams()
	return params.DbName
}

// Host gets the host from mysql.ConnParams
func (c Connector) Host() string {
	params, _ := c.MysqlParams()
	return params.Host
}

// AppWithDB returns connection parameters for app with dbname set.
func (dbcfgs *DBConfigs) AppWithDB() Connector {
	return dbcfgs.makeParams(App, true)
}

// AppDebugWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) AppDebugWithDB() Connector {
	return dbcfgs.makeParams(AppDebug, true)
}

// AllPrivsWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) AllPrivsWithDB() Connector {
	return dbcfgs.makeParams(AllPrivs, true)
}

// Dba returns connection parameters for dba with no dbname set.
func (dbcfgs *DBConfigs) Dba() Connector {
	return dbcfgs.makeParams(Dba, false)
}

// DbaWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) DbaWithDB() Connector {
	return dbcfgs.makeParams(Dba, true)
}

// FilteredWithDB returns connection parameters for filtered with dbname set.
func (dbcfgs *DBConfigs) FilteredWithDB() Connector {
	return dbcfgs.makeParams(Filtered, true)
}

// Repl returns connection parameters for repl with no dbname set.
func (dbcfgs *DBConfigs) Repl() Connector {
	return dbcfgs.makeParams(Repl, false)
}

// ExternalRepl returns connection parameters for repl with no dbname set.
func (dbcfgs *DBConfigs) ExternalRepl() Connector {
	return dbcfgs.makeParams(ExternalRepl, true)
}

// ExternalReplWithDB returns connection parameters for repl with dbname set.
func (dbcfgs *DBConfigs) ExternalReplWithDB() Connector {
	params := dbcfgs.makeParams(ExternalRepl, true)
	// TODO @rafael: This is a hack to allows to configure external databases by providing
	// db-config-erepl-dbname.
	if params.connParams.DeprecatedDBName != "" {
		params.connParams.DbName = params.connParams.DeprecatedDBName
		return params
	}
	return params
}

// AppWithDB returns connection parameters for app with dbname set.
func (dbcfgs *DBConfigs) makeParams(userKey string, withDB bool) Connector {
	orig := dbcfgs.userConfigs[userKey]
	if orig == nil {
		return Connector{
			connParams: &mysql.ConnParams{},
		}
	}
	result := orig.param
	if withDB {
		result.DbName = dbcfgs.DBName.Get()
	}
	return Connector{
		connParams: &result,
	}
}

// IsZero returns true if DBConfigs was uninitialized.
func (dbcfgs *DBConfigs) IsZero() bool {
	return len(dbcfgs.userConfigs) == 0
}

func (dbcfgs *DBConfigs) String() string {
	out := struct {
		Conn  mysql.ConnParams
		Users map[string]string
	}{
		Users: make(map[string]string),
	}
	if conn := dbcfgs.userConfigs[App]; conn != nil {
		out.Conn = conn.param
	} else if conn := dbcfgs.userConfigs[Dba]; conn != nil {
		out.Conn = conn.param
	}
	out.Conn.Pass = "****"
	for k, uc := range dbcfgs.userConfigs {
		out.Users[k] = uc.param.Uname
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// Copy returns a copy of the DBConfig.
func (dbcfgs *DBConfigs) Copy() *DBConfigs {
	result := &DBConfigs{userConfigs: make(map[string]*userConfig)}
	for k, u := range dbcfgs.userConfigs {
		newu := *u
		result.userConfigs[k] = &newu
	}
	result.DBName.Set(dbcfgs.DBName.Get())
	result.SidecarDBName.Set(dbcfgs.SidecarDBName.Get())
	return result
}

// HasConnectionParams returns true if connection parameters were
// specified in the command-line. This will allow the caller to
// search for alternate ways to connect, like looking in the my.cnf
// file.
func HasConnectionParams() bool {
	return baseConfig.Host != "" || baseConfig.UnixSocket != ""
}

// Init will initialize all the necessary connection parameters.
// Precedence is as follows: if baseConfig command line options are
// set, they supersede all other settings.
// If baseConfig is not set, the next priority is with per-user connection
// parameters. This is only for legacy support.
// If no per-user parameters are supplied, then the defaultSocketFile
// is used to initialize the per-user conn params.
func Init(defaultSocketFile string) (*DBConfigs, error) {
	// The new base configs, if set, supersede legacy settings.
	for user, uc := range dbConfigs.userConfigs {
		// TODO @rafael: For ExternalRepl we need to respect the provided host / port
		// At the moment this is an snowflake user connection type that it used by
		// vreplication to connect to external mysql hosts that are not part of a vitess
		// cluster. In the future we need to refactor all dbconfig to support custom users
		// in a more flexible way.
		if HasConnectionParams() && user != ExternalRepl {
			uc.param.Host = baseConfig.Host
			uc.param.Port = baseConfig.Port
			uc.param.UnixSocket = baseConfig.UnixSocket
		} else if uc.param.UnixSocket == "" && uc.param.Host == "" {
			uc.param.UnixSocket = defaultSocketFile
		}

		if baseConfig.Charset != "" {
			uc.param.Charset = baseConfig.Charset
		}
		if baseConfig.Flags != 0 {
			uc.param.Flags = baseConfig.Flags
		}
		if user != ExternalRepl {
			uc.param.Flavor = baseConfig.Flavor
		}
		if uc.useSSL {
			uc.param.SslCa = baseConfig.SslCa
			uc.param.SslCaPath = baseConfig.SslCaPath
			uc.param.SslCert = baseConfig.SslCert
			uc.param.SslKey = baseConfig.SslKey
			uc.param.ServerName = baseConfig.ServerName
		}
		uc.param.ConnectTimeoutMs = baseConfig.ConnectTimeoutMs
	}

	// See if the CredentialsServer is working. We do not use the
	// result for anything, this is just a check.
	for _, uc := range dbConfigs.userConfigs {
		if _, err := withCredentials(&uc.param); err != nil {
			return nil, fmt.Errorf("dbconfig cannot be initialized: %v", err)
		}
		// Check for only one.
		break
	}
	dbConfigs.SidecarDBName.Set("_vt")

	log.Infof("DBConfigs: %v\n", dbConfigs.String())
	return &dbConfigs, nil
}

// NewTestDBConfigs returns a DBConfigs meant for testing.
func NewTestDBConfigs(genParams, appDebugParams mysql.ConnParams, dbName string) *DBConfigs {
	dbcfgs := &DBConfigs{
		userConfigs: map[string]*userConfig{
			App:          {param: genParams},
			AppDebug:     {param: appDebugParams},
			AllPrivs:     {param: genParams},
			Dba:          {param: genParams},
			Filtered:     {param: genParams},
			Repl:         {param: genParams},
			ExternalRepl: {param: genParams},
		},
	}
	dbcfgs.DBName.Set(dbName)
	dbcfgs.SidecarDBName.Set("_vt")
	return dbcfgs
}
