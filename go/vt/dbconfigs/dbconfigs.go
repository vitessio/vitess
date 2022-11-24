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

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttls"

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
	// GlobalDBConfigs contains the initial values of dbconfigs from flags.
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
	Socket                     string        `json:"socket,omitempty"`
	Host                       string        `json:"host,omitempty"`
	Port                       int           `json:"port,omitempty"`
	Charset                    string        `json:"charset,omitempty"`
	Flags                      uint64        `json:"flags,omitempty"`
	Flavor                     string        `json:"flavor,omitempty"`
	SslMode                    vttls.SslMode `json:"sslMode,omitempty"`
	SslCa                      string        `json:"sslCa,omitempty"`
	SslCaPath                  string        `json:"sslCaPath,omitempty"`
	SslCert                    string        `json:"sslCert,omitempty"`
	SslKey                     string        `json:"sslKey,omitempty"`
	TLSMinVersion              string        `json:"tlsMinVersion,omitempty"`
	ServerName                 string        `json:"serverName,omitempty"`
	ConnectTimeoutMilliseconds int           `json:"connectTimeoutMilliseconds,omitempty"`
	DBName                     string        `json:"dbName,omitempty"`
	EnableQueryInfo            bool          `json:"enableQueryInfo,omitempty"`

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

// RegisterFlags registers the base DBFlags, credentials flags, and the user
// specific ones for the specified system users for the requesting command.
// For instance, the vttablet command will register flags for all users
// as defined in the dbconfigs.All variable.
func RegisterFlags(userKeys ...string) {
	servenv.OnParse(func(fs *pflag.FlagSet) {
		registerBaseFlags(fs)
		for _, userKey := range userKeys {
			uc, cp := GlobalDBConfigs.getParams(userKey, &GlobalDBConfigs)
			registerPerUserFlags(fs, userKey, uc, cp)
		}
	})
}

func registerBaseFlags(fs *pflag.FlagSet) {
	fs.StringVar(&GlobalDBConfigs.Socket, "db_socket", "", "The unix socket to connect on. If this is specified, host and port will not be used.")
	fs.StringVar(&GlobalDBConfigs.Host, "db_host", "", "The host name for the tcp connection.")
	fs.IntVar(&GlobalDBConfigs.Port, "db_port", 0, "tcp port")
	fs.StringVar(&GlobalDBConfigs.Charset, "db_charset", "utf8mb4", "Character set used for this tablet.")
	fs.Uint64Var(&GlobalDBConfigs.Flags, "db_flags", 0, "Flag values as defined by MySQL.")
	fs.StringVar(&GlobalDBConfigs.Flavor, "db_flavor", "", "Flavor overrid. Valid value is FilePos.")
	fs.Var(&GlobalDBConfigs.SslMode, "db_ssl_mode", "SSL mode to connect with. One of disabled, preferred, required, verify_ca & verify_identity.")
	fs.StringVar(&GlobalDBConfigs.SslCa, "db_ssl_ca", "", "connection ssl ca")
	fs.StringVar(&GlobalDBConfigs.SslCaPath, "db_ssl_ca_path", "", "connection ssl ca path")
	fs.StringVar(&GlobalDBConfigs.SslCert, "db_ssl_cert", "", "connection ssl certificate")
	fs.StringVar(&GlobalDBConfigs.SslKey, "db_ssl_key", "", "connection ssl key")
	fs.StringVar(&GlobalDBConfigs.TLSMinVersion, "db_tls_min_version", "", "Configures the minimal TLS version negotiated when SSL is enabled. Defaults to TLSv1.2. Options: TLSv1.0, TLSv1.1, TLSv1.2, TLSv1.3.")
	fs.StringVar(&GlobalDBConfigs.ServerName, "db_server_name", "", "server name of the DB we are connecting to.")
	fs.IntVar(&GlobalDBConfigs.ConnectTimeoutMilliseconds, "db_connect_timeout_ms", 0, "connection timeout to mysqld in milliseconds (0 for no timeout)")
	fs.BoolVar(&GlobalDBConfigs.EnableQueryInfo, "db_conn_query_info", false, "enable parsing and processing of QUERY_OK info fields")
}

// The flags will change the global singleton
func registerPerUserFlags(fs *pflag.FlagSet, userKey string, uc *UserConfig, cp *mysql.ConnParams) {
	newUserFlag := "db_" + userKey + "_user"
	fs.StringVar(&uc.User, newUserFlag, "vt_"+userKey, "db "+userKey+" user userKey")

	newPasswordFlag := "db_" + userKey + "_password"
	fs.StringVar(&uc.Password, newPasswordFlag, "", "db "+userKey+" password")

	fs.BoolVar(&uc.UseSSL, "db_"+userKey+"_use_ssl", true, "Set this flag to false to make the "+userKey+" connection to not use ssl")
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
func (c *Connector) Connect(ctx context.Context) (*mysql.Conn, error) {
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

// AllPrivsConnector returns connection parameters for all privileges with no dbname set.
func (dbcfgs *DBConfigs) AllPrivsConnector() Connector {
	return dbcfgs.makeParams(&dbcfgs.allprivsParams, false)
}

// AllPrivsWithDB returns connection parameters for all privileges with dbname set.
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

		// If the connection params has a charset defined, it will not be overridden by the
		// global configuration.
		if dbcfgs.Charset != "" && cp.Charset == "" {
			cp.Charset = dbcfgs.Charset
		}

		if dbcfgs.Flags != 0 {
			cp.Flags = dbcfgs.Flags
		}
		if userKey != ExternalRepl {
			cp.Flavor = dbcfgs.Flavor
		}
		cp.ConnectTimeoutMs = uint64(dbcfgs.ConnectTimeoutMilliseconds)
		cp.EnableQueryInfo = dbcfgs.EnableQueryInfo

		cp.Uname = uc.User
		cp.Pass = uc.Password
		if uc.UseSSL {
			cp.SslMode = dbcfgs.SslMode
			cp.SslCa = dbcfgs.SslCa
			cp.SslCaPath = dbcfgs.SslCaPath
			cp.SslCert = dbcfgs.SslCert
			cp.SslKey = dbcfgs.SslKey
			cp.TLSMinVersion = dbcfgs.TLSMinVersion
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
func (dbcfgs *DBConfigs) SetDbParams(dbaParams, appParams, filteredParams mysql.ConnParams) {
	dbcfgs.dbaParams = dbaParams
	dbcfgs.appParams = appParams
	dbcfgs.filteredParams = filteredParams
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
		Charset:            "utf8mb4_general_ci",
	}
}
