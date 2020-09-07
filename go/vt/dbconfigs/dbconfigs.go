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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/yaml2"
)

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

var (
	// GlobalDBConfigs contains the initial values of dbconfgis from flags.
	GlobalDBConfigs DBConfigs

	// All can be used to register all flags: RegisterFlags(All...)
	All = []string{App, AppDebug, AllPrivs, Dba, Filtered, Repl, ExternalRepl}
)

// DBConfigs stores all the data needed to build various connection
// parameters for the db. It stores credentials for app, appdebug,
// allprivs, dba, filtered and repl users.
// It contains other connection parameters like socket, charset, etc.
// It also stores the default db name, which it can combine with the
// rest of the data to build db-sepcific connection parameters.
//
// The legacy way of initializing is as follows:
// App must call RegisterFlags to request the types of connections
// it wants support for. This must be done before invoking flags.Parse.
// After flag parsing, app invokes the Init function, which will return
// a DBConfigs object.
// The app must store the DBConfigs object internally, and use it to
// build connection parameters as needed.
type DBConfigs struct {
	Socket                     string `json:"socket,omitempty"`
	Host                       string `json:"host,omitempty"`
	Port                       int    `json:"port,omitempty"`
	Charset                    string `json:"charset,omitempty"`
	Flags                      uint64 `json:"flags,omitempty"`
	Flavor                     string `json:"flavor,omitempty"`
	SslCa                      string `json:"sslCa,omitempty"`
	SslCaPath                  string `json:"sslCaPath,omitempty"`
	SslCert                    string `json:"sslCert,omitempty"`
	SslKey                     string `json:"sslKey,omitempty"`
	ServerName                 string `json:"serverName,omitempty"`
	ConnectTimeoutMilliseconds int    `json:"connectTimeoutMilliseconds,omitempty"`
	DBName                     string `json:"dbName,omitempty"`

	App          UserConfig `json:"app,omitempty"`
	Dba          UserConfig `json:"dba,omitempty"`
	Filtered     UserConfig `json:"filtered,omitempty"`
	Repl         UserConfig `json:"repl,omitempty"`
	Appdebug     UserConfig `json:"appdebug,omitempty"`
	Allprivs     UserConfig `json:"allprivs,omitempty"`
	externalRepl UserConfig

	appParams          mysql.ConnParams
	dbaParams          mysql.ConnParams
	filteredParams     mysql.ConnParams
	replParams         mysql.ConnParams
	appdebugParams     mysql.ConnParams
	allprivsParams     mysql.ConnParams
	externalReplParams mysql.ConnParams
}

// UserConfig contains user-specific configs.
type UserConfig struct {
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
	UseSSL   bool   `json:"useSsl,omitempty"`
	UseTCP   bool   `json:"useTcp,omitempty"`
}

// RegisterFlags registers the flags for the given DBConfigFlag.
// For instance, vttablet will register client, dba and repl.
// Returns all registered flags.
func RegisterFlags(userKeys ...string) {
	registerBaseFlags()
	for _, userKey := range userKeys {
		uc, cp := GlobalDBConfigs.getParams(userKey, &GlobalDBConfigs)
		registerPerUserFlags(userKey, uc, cp)
	}
}

func registerBaseFlags() {
	flag.StringVar(&GlobalDBConfigs.Socket, "db_socket", "", "The unix socket to connect on. If this is specified, host and port will not be used.")
	flag.StringVar(&GlobalDBConfigs.Host, "db_host", "", "The host name for the tcp connection.")
	flag.IntVar(&GlobalDBConfigs.Port, "db_port", 0, "tcp port")
	flag.StringVar(&GlobalDBConfigs.Charset, "db_charset", "", "Character set. Only utf8 or latin1 based character sets are supported.")
	flag.Uint64Var(&GlobalDBConfigs.Flags, "db_flags", 0, "Flag values as defined by MySQL.")
	flag.StringVar(&GlobalDBConfigs.Flavor, "db_flavor", "", "Flavor overrid. Valid value is FilePos.")
	flag.StringVar(&GlobalDBConfigs.SslCa, "db_ssl_ca", "", "connection ssl ca")
	flag.StringVar(&GlobalDBConfigs.SslCaPath, "db_ssl_ca_path", "", "connection ssl ca path")
	flag.StringVar(&GlobalDBConfigs.SslCert, "db_ssl_cert", "", "connection ssl certificate")
	flag.StringVar(&GlobalDBConfigs.SslKey, "db_ssl_key", "", "connection ssl key")
	flag.StringVar(&GlobalDBConfigs.ServerName, "db_server_name", "", "server name of the DB we are connecting to.")
	flag.IntVar(&GlobalDBConfigs.ConnectTimeoutMilliseconds, "db_connect_timeout_ms", 0, "connection timeout to mysqld in milliseconds (0 for no timeout)")
}

// The flags will change the global singleton
// TODO(sougou): deprecate the legacy flags.
func registerPerUserFlags(userKey string, uc *UserConfig, cp *mysql.ConnParams) {
	newUserFlag := "db_" + userKey + "_user"
	flag.StringVar(&uc.User, "db-config-"+userKey+"-uname", "vt_"+userKey, "deprecated: use "+newUserFlag)
	flag.StringVar(&uc.User, newUserFlag, "vt_"+userKey, "db "+userKey+" user userKey")

	newPasswordFlag := "db_" + userKey + "_password"
	flag.StringVar(&uc.Password, "db-config-"+userKey+"-pass", "", "db "+userKey+" deprecated: use "+newPasswordFlag)
	flag.StringVar(&uc.Password, newPasswordFlag, "", "db "+userKey+" password")

	flag.BoolVar(&uc.UseSSL, "db_"+userKey+"_use_ssl", true, "Set this flag to false to make the "+userKey+" connection to not use ssl")

	flag.StringVar(&cp.Host, "db-config-"+userKey+"-host", "", "deprecated: use db_host")
	flag.IntVar(&cp.Port, "db-config-"+userKey+"-port", 0, "deprecated: use db_port")
	flag.StringVar(&cp.UnixSocket, "db-config-"+userKey+"-unixsocket", "", "deprecated: use db_socket")
	flag.StringVar(&cp.Charset, "db-config-"+userKey+"-charset", "utf8", "deprecated: use db_charset")
	flag.Uint64Var(&cp.Flags, "db-config-"+userKey+"-flags", 0, "deprecated: use db_flags")
	flag.StringVar(&cp.SslCa, "db-config-"+userKey+"-ssl-ca", "", "deprecated: use db_ssl_ca")
	flag.StringVar(&cp.SslCaPath, "db-config-"+userKey+"-ssl-ca-path", "", "deprecated: use db_ssl_ca_path")
	flag.StringVar(&cp.SslCert, "db-config-"+userKey+"-ssl-cert", "", "deprecated: use db_ssl_cert")
	flag.StringVar(&cp.SslKey, "db-config-"+userKey+"-ssl-key", "", "deprecated: use db_ssl_key")
	flag.StringVar(&cp.ServerName, "db-config-"+userKey+"-server_name", "", "deprecated: use db_server_name")
	flag.StringVar(&cp.Flavor, "db-config-"+userKey+"-flavor", "", "deprecated: use db_flavor")

	if userKey == ExternalRepl {
		flag.StringVar(&cp.DeprecatedDBName, "db-config-"+userKey+"-dbname", "", "deprecated: dbname does not need to be explicitly configured")
	}

}

// Connector contains Connection Parameters for mysql connection
type Connector struct {
	connParams *mysql.ConnParams
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
	if c.connParams == nil {
		// This is only possible during tests.
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "parameters are empty")
	}
	params, err := withCredentials(c.connParams)
	if err != nil {
		return nil, err
	}
	return params, nil
}

// DBName gets the dbname from mysql.ConnParams
func (c Connector) DBName() string {
	return c.connParams.DbName
}

// Host gets the host from mysql.ConnParams
func (c Connector) Host() string {
	return c.connParams.Host
}

// AppWithDB returns connection parameters for app with dbname set.
func (dbcfgs *DBConfigs) AppWithDB() Connector {
	return dbcfgs.makeParams(&dbcfgs.appParams, true)
}

// AppDebugWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) AppDebugWithDB() Connector {
	return dbcfgs.makeParams(&dbcfgs.appdebugParams, true)
}

// AllPrivsConnector returns connection parameters for appdebug with no dbname set.
func (dbcfgs *DBConfigs) AllPrivsConnector() Connector {
	return dbcfgs.makeParams(&dbcfgs.allprivsParams, false)
}

// AllPrivsWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) AllPrivsWithDB() Connector {
	return dbcfgs.makeParams(&dbcfgs.allprivsParams, true)
}

// DbaConnector returns connection parameters for dba with no dbname set.
func (dbcfgs *DBConfigs) DbaConnector() Connector {
	return dbcfgs.makeParams(&dbcfgs.dbaParams, false)
}

// DbaWithDB returns connection parameters for appdebug with dbname set.
func (dbcfgs *DBConfigs) DbaWithDB() Connector {
	return dbcfgs.makeParams(&dbcfgs.dbaParams, true)
}

// FilteredWithDB returns connection parameters for filtered with dbname set.
func (dbcfgs *DBConfigs) FilteredWithDB() Connector {
	return dbcfgs.makeParams(&dbcfgs.filteredParams, true)
}

// ReplConnector returns connection parameters for repl with no dbname set.
func (dbcfgs *DBConfigs) ReplConnector() Connector {
	return dbcfgs.makeParams(&dbcfgs.replParams, false)
}

// ExternalRepl returns connection parameters for repl with no dbname set.
func (dbcfgs *DBConfigs) ExternalRepl() Connector {
	return dbcfgs.makeParams(&dbcfgs.externalReplParams, true)
}

// ExternalReplWithDB returns connection parameters for repl with dbname set.
func (dbcfgs *DBConfigs) ExternalReplWithDB() Connector {
	params := dbcfgs.makeParams(&dbcfgs.externalReplParams, true)
	// TODO @rafael: This is a hack to allows to configure external databases by providing
	// db-config-erepl-dbname.
	if params.connParams.DeprecatedDBName != "" {
		params.connParams.DbName = params.connParams.DeprecatedDBName
		return params
	}
	return params
}

// AppWithDB returns connection parameters for app with dbname set.
func (dbcfgs *DBConfigs) makeParams(cp *mysql.ConnParams, withDB bool) Connector {
	result := *cp
	if withDB {
		result.DbName = dbcfgs.DBName
	}
	return Connector{
		connParams: &result,
	}
}

// IsZero returns true if DBConfigs was uninitialized.
func (dbcfgs *DBConfigs) IsZero() bool {
	return *dbcfgs == DBConfigs{}
}

// HasGlobalSettings returns true if DBConfigs contains values
// for gloabl configs.
func (dbcfgs *DBConfigs) HasGlobalSettings() bool {
	return dbcfgs.Host != "" || dbcfgs.Socket != ""
}

func (dbcfgs *DBConfigs) String() string {
	out, err := yaml2.Marshal(dbcfgs.Redacted())
	if err != nil {
		return err.Error()
	}
	return string(out)
}

// MarshalJSON marshals after redacting passwords.
func (dbcfgs *DBConfigs) MarshalJSON() ([]byte, error) {
	type nonCustom DBConfigs
	return json.Marshal((*nonCustom)(dbcfgs.Redacted()))
}

// Redacted redacts passwords from DBConfigs.
func (dbcfgs *DBConfigs) Redacted() *DBConfigs {
	dbcfgs = dbcfgs.Clone()
	dbcfgs.App.Password = "****"
	dbcfgs.Dba.Password = "****"
	dbcfgs.Filtered.Password = "****"
	dbcfgs.Repl.Password = "****"
	dbcfgs.Appdebug.Password = "****"
	dbcfgs.Allprivs.Password = "****"
	return dbcfgs
}

// Clone returns a clone of the DBConfig.
func (dbcfgs *DBConfigs) Clone() *DBConfigs {
	result := *dbcfgs
	return &result
}

// InitWithSocket will initialize all the necessary connection parameters.
// Precedence is as follows: if UserConfig settings are set,
// they supersede all other settings.
// The next priority is with per-user connection
// parameters. This is only for legacy support.
// If no per-user parameters are supplied, then the defaultSocketFile
// is used to initialize the per-user conn params.
func (dbcfgs *DBConfigs) InitWithSocket(defaultSocketFile string) {
	for _, userKey := range All {
		uc, cp := dbcfgs.getParams(userKey, dbcfgs)
		// TODO @rafael: For ExternalRepl we need to respect the provided host / port
		// At the moment this is an snowflake user connection type that it used by
		// vreplication to connect to external mysql hosts that are not part of a vitess
		// cluster. In the future we need to refactor all dbconfig to support custom users
		// in a more flexible way.
		if dbcfgs.HasGlobalSettings() && userKey != ExternalRepl {
			cp.Host = dbcfgs.Host
			cp.Port = dbcfgs.Port
			if !uc.UseTCP {
				cp.UnixSocket = dbcfgs.Socket
			}
		} else if cp.UnixSocket == "" && cp.Host == "" {
			cp.UnixSocket = defaultSocketFile
		}

		if dbcfgs.Charset != "" {
			cp.Charset = dbcfgs.Charset
		}
		if dbcfgs.Flags != 0 {
			cp.Flags = dbcfgs.Flags
		}
		if userKey != ExternalRepl {
			cp.Flavor = dbcfgs.Flavor
		}
		cp.ConnectTimeoutMs = uint64(dbcfgs.ConnectTimeoutMilliseconds)

		cp.Uname = uc.User
		cp.Pass = uc.Password
		if uc.UseSSL {
			cp.SslCa = dbcfgs.SslCa
			cp.SslCaPath = dbcfgs.SslCaPath
			cp.SslCert = dbcfgs.SslCert
			cp.SslKey = dbcfgs.SslKey
			cp.ServerName = dbcfgs.ServerName
		}
	}

	log.Infof("DBConfigs: %v\n", dbcfgs.String())
}

func (dbcfgs *DBConfigs) getParams(userKey string, dbc *DBConfigs) (*UserConfig, *mysql.ConnParams) {
	var uc *UserConfig
	var cp *mysql.ConnParams
	switch userKey {
	case App:
		uc = &dbcfgs.App
		cp = &dbcfgs.appParams
	case AppDebug:
		uc = &dbcfgs.Appdebug
		cp = &dbcfgs.appdebugParams
	case AllPrivs:
		uc = &dbcfgs.Allprivs
		cp = &dbcfgs.allprivsParams
	case Dba:
		uc = &dbcfgs.Dba
		cp = &dbcfgs.dbaParams
	case Filtered:
		uc = &dbcfgs.Filtered
		cp = &dbcfgs.filteredParams
	case Repl:
		uc = &dbcfgs.Repl
		cp = &dbcfgs.replParams
	case ExternalRepl:
		uc = &dbcfgs.externalRepl
		cp = &dbcfgs.externalReplParams
	default:
		log.Exitf("Invalid db user key requested: %s", userKey)
	}
	return uc, cp
}

// SetDbParams sets the dba and app params
func (dbcfgs *DBConfigs) SetDbParams(dbaParams, appParams mysql.ConnParams) {
	dbcfgs.dbaParams = dbaParams
	dbcfgs.appParams = appParams
}

// NewTestDBConfigs returns a DBConfigs meant for testing.
func NewTestDBConfigs(genParams, appDebugParams mysql.ConnParams, dbname string) *DBConfigs {
	return &DBConfigs{
		appParams:          genParams,
		appdebugParams:     appDebugParams,
		allprivsParams:     genParams,
		dbaParams:          genParams,
		filteredParams:     genParams,
		replParams:         genParams,
		externalReplParams: genParams,
		DBName:             dbname,
	}
}
