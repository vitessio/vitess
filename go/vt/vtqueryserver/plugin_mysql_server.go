/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtqueryserver

import (
	"flag"
	"fmt"
	"net"
	"os"
	"syscall"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlproxy"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttls"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	mysqlServerPort               = flag.Int("mysqlproxy_server_port", -1, "If set, also listen for MySQL binary protocol connections on this port.")
	mysqlServerBindAddress        = flag.String("mysqlproxy_server_bind_address", "", "Binds on this address when listening to MySQL binary protocol. Useful to restrict listening to 'localhost' only for instance.")
	mysqlServerSocketPath         = flag.String("mysqlproxy_server_socket_path", "", "This option specifies the Unix socket file to use when listening for local connections. By default it will be empty and it won't listen to a unix socket")
	mysqlAuthServerImpl           = flag.String("mysql_auth_server_impl", "static", "Which auth server implementation to use.")
	mysqlAllowClearTextWithoutTLS = flag.Bool("mysql_allow_clear_text_without_tls", false, "If set, the server will allow the use of a clear text password over non-SSL connections.")

	mysqlSslCert = flag.String("mysqlproxy_server_ssl_cert", "", "Path to the ssl cert for mysql server plugin SSL")
	mysqlSslKey  = flag.String("mysqlproxy_server_ssl_key", "", "Path to ssl key for mysql server plugin SSL")
	mysqlSslCa   = flag.String("mysqlproxy_server_ssl_ca", "", "Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs.")

	mysqlSlowConnectWarnThreshold = flag.Duration("mysqlproxy_slow_connect_warn_threshold", 0, "Warn if it takes more than the given threshold for a mysql connection to establish")

	mysqlConnReadTimeout  = flag.Duration("mysql_server_read_timeout", 0, "connection read timeout")
	mysqlConnWriteTimeout = flag.Duration("mysql_server_write_timeout", 0, "connection write timeout")
	mysqlQueryTimeout     = flag.Duration("mysql_server_query_timeout", 0, "mysql query timeout")
)

// proxyHandler implements the Listener interface.
// It stores the Session in the ClientData of a Connection, if a transaction
// is in progress.
type proxyHandler struct {
	mp *mysqlproxy.Proxy
}

func newProxyHandler(mp *mysqlproxy.Proxy) *proxyHandler {
	return &proxyHandler{
		mp: mp,
	}
}

func (mh *proxyHandler) NewConnection(c *mysql.Conn) {
}

func (mh *proxyHandler) ConnectionClosed(c *mysql.Conn) {
	// Rollback if there is an ongoing transaction. Ignore error.
	var ctx context.Context
	var cancel context.CancelFunc
	if *mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	session, _ := c.ClientData.(*mysqlproxy.ProxySession)
	if session != nil && session.TransactionID != 0 {
		_ = mh.mp.Rollback(ctx, session)
	}
}

func (mh *proxyHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	var ctx context.Context
	var cancel context.CancelFunc
	if *mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"mysqlproxy MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session, _ := c.ClientData.(*mysqlproxy.ProxySession)
	if session == nil {
		session = &mysqlproxy.ProxySession{
			Options: &querypb.ExecuteOptions{
				IncludedFields: querypb.ExecuteOptions_ALL,
			},
			Autocommit: true,
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
	}
	if session.TargetString == "" && c.SchemaName != "" {
		session.TargetString = c.SchemaName
	}
	session, result, err := mh.mp.Execute(ctx, session, query, make(map[string]*querypb.BindVariable))
	c.ClientData = session
	err = mysql.NewSQLErrorFromError(err)
	if err != nil {
		return err
	}

	return callback(result)
}

func (mh *proxyHandler) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func (mh *proxyHandler) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, error) {
	return nil, nil
}

func (mh *proxyHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

func (mh *proxyHandler) ComResetConnection(c *mysql.Conn) {

}

var mysqlListener *mysql.Listener
var mysqlUnixListener *mysql.Listener

// initiMySQLProtocol starts the mysql protocol.
// It should be called only once in a process.
func initMySQLProtocol() {
	log.Infof("initializing mysql protocol")

	// Flag is not set, just return.
	if *mysqlServerPort < 0 && *mysqlServerSocketPath == "" {
		return
	}

	// If no mysqlproxy was created, just return.
	if mysqlProxy == nil {
		log.Fatalf("mysqlProxy not initialized")
		return
	}

	// Initialize registered AuthServer implementations (or other plugins)
	for _, initFn := range pluginInitializers {
		initFn()
	}
	authServer := mysql.GetAuthServer(*mysqlAuthServerImpl)

	// Create a Listener.
	var err error
	mh := newProxyHandler(mysqlProxy)
	if *mysqlServerPort >= 0 {
		mysqlListener, err = mysql.NewListener("tcp", net.JoinHostPort(*mysqlServerBindAddress, fmt.Sprintf("%v", *mysqlServerPort)), authServer, mh, *mysqlConnReadTimeout, *mysqlConnWriteTimeout)
		if err != nil {
			log.Exitf("mysql.NewListener failed: %v", err)
		}
		if *mysqlSslCert != "" && *mysqlSslKey != "" {
			mysqlListener.TLSConfig, err = vttls.ServerConfig(*mysqlSslCert, *mysqlSslKey, *mysqlSslCa)
			if err != nil {
				log.Exitf("grpcutils.TLSServerConfig failed: %v", err)
				return
			}
		}
		mysqlListener.AllowClearTextWithoutTLS = *mysqlAllowClearTextWithoutTLS

		// Check for the connection threshold
		if *mysqlSlowConnectWarnThreshold != 0 {
			log.Infof("setting mysql slow connection threshold to %v", mysqlSlowConnectWarnThreshold)
			mysqlListener.SlowConnectWarnThreshold = *mysqlSlowConnectWarnThreshold
		}
		// Start listening for tcp
		go mysqlListener.Accept()
		log.Infof("listening on %s:%d", *mysqlServerBindAddress, *mysqlServerPort)
	}

	if *mysqlServerSocketPath != "" {
		// Let's create this unix socket with permissions to all users. In this way,
		// clients can connect to mysqlproxy mysql server without being mysqlproxy user
		oldMask := syscall.Umask(000)
		mysqlUnixListener, err = newMysqlUnixSocket(*mysqlServerSocketPath, authServer, mh)
		_ = syscall.Umask(oldMask)
		if err != nil {
			log.Exitf("mysql.NewListener failed: %v", err)
			return
		}
		// Listen for unix socket
		go mysqlUnixListener.Accept()
	}
}

// newMysqlUnixSocket creates a new unix socket mysql listener. If a socket file already exists, attempts
// to clean it up.
func newMysqlUnixSocket(address string, authServer mysql.AuthServer, handler mysql.Handler) (*mysql.Listener, error) {
	listener, err := mysql.NewListener("unix", address, authServer, handler, *mysqlConnReadTimeout, *mysqlConnWriteTimeout)
	switch err := err.(type) {
	case nil:
		return listener, nil
	case *net.OpError:
		log.Warningf("Found existent socket when trying to create new unix mysql listener: %s, attempting to clean up", address)
		// err.Op should never be different from listen, just being extra careful
		// in case in the future other errors are returned here
		if err.Op != "listen" {
			return nil, err
		}
		_, dialErr := net.Dial("unix", address)
		if dialErr == nil {
			log.Errorf("Existent socket '%s' is still accepting connections, aborting", address)
			return nil, err
		}
		removeFileErr := os.Remove(address)
		if removeFileErr != nil {
			log.Errorf("Couldn't remove existent socket file: %s", address)
			return nil, err
		}
		listener, listenerErr := mysql.NewListener("unix", address, authServer, handler, *mysqlConnReadTimeout, *mysqlConnWriteTimeout)
		return listener, listenerErr
	default:
		return nil, err
	}
}

func shutdownMySQLProtocol() {
	log.Infof("shutting down mysql protocol")
	if mysqlListener != nil {
		mysqlListener.Close()
		mysqlListener = nil
	}

	if mysqlUnixListener != nil {
		mysqlUnixListener.Close()
		mysqlUnixListener = nil
	}
}

func init() {
	servenv.OnRun(initMySQLProtocol)
	servenv.OnTerm(shutdownMySQLProtocol)
}

var pluginInitializers []func()

// RegisterPluginInitializer lets plugins register themselves to be init'ed at servenv.OnRun-time
func RegisterPluginInitializer(initializer func()) {
	pluginInitializers = append(pluginInitializers, initializer)
}
