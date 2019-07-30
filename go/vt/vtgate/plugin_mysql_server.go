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

package vtgate

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/trace"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttls"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var (
	mysqlServerPort               = flag.Int("mysql_server_port", -1, "If set, also listen for MySQL binary protocol connections on this port.")
	mysqlServerBindAddress        = flag.String("mysql_server_bind_address", "", "Binds on this address when listening to MySQL binary protocol. Useful to restrict listening to 'localhost' only for instance.")
	mysqlServerSocketPath         = flag.String("mysql_server_socket_path", "", "This option specifies the Unix socket file to use when listening for local connections. By default it will be empty and it won't listen to a unix socket")
	mysqlTCPVersion               = flag.String("mysql_tcp_version", "tcp", "Select tcp, tcp4, or tcp6 to control the socket type.")
	mysqlAuthServerImpl           = flag.String("mysql_auth_server_impl", "static", "Which auth server implementation to use.")
	mysqlAllowClearTextWithoutTLS = flag.Bool("mysql_allow_clear_text_without_tls", false, "If set, the server will allow the use of a clear text password over non-SSL connections.")
	mysqlServerVersion            = flag.String("mysql_server_version", mysql.DefaultServerVersion, "MySQL server version to advertise.")

	mysqlServerRequireSecureTransport = flag.Bool("mysql_server_require_secure_transport", false, "Reject insecure connections but only if mysql_server_ssl_cert and mysql_server_ssl_key are provided")

	mysqlSslCert = flag.String("mysql_server_ssl_cert", "", "Path to the ssl cert for mysql server plugin SSL")
	mysqlSslKey  = flag.String("mysql_server_ssl_key", "", "Path to ssl key for mysql server plugin SSL")
	mysqlSslCa   = flag.String("mysql_server_ssl_ca", "", "Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs.")

	mysqlSlowConnectWarnThreshold = flag.Duration("mysql_slow_connect_warn_threshold", 0, "Warn if it takes more than the given threshold for a mysql connection to establish")

	mysqlConnReadTimeout  = flag.Duration("mysql_server_read_timeout", 0, "connection read timeout")
	mysqlConnWriteTimeout = flag.Duration("mysql_server_write_timeout", 0, "connection write timeout")
	mysqlQueryTimeout     = flag.Duration("mysql_server_query_timeout", 0, "mysql query timeout")

	busyConnections int32
)

// vtgateHandler implements the Listener interface.
// It stores the Session in the ClientData of a Connection, if a transaction
// is in progress.
type vtgateHandler struct {
	vtg *VTGate
}

func newVtgateHandler(vtg *VTGate) *vtgateHandler {
	return &vtgateHandler{
		vtg: vtg,
	}
}

func (vh *vtgateHandler) NewConnection(c *mysql.Conn) {
}

func (vh *vtgateHandler) ConnectionClosed(c *mysql.Conn) {
	// Rollback if there is an ongoing transaction. Ignore error.
	var ctx context.Context
	var cancel context.CancelFunc
	if *mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	session, _ := c.ClientData.(*vtgatepb.Session)
	if session != nil {
		if session.InTransaction {
			defer atomic.AddInt32(&busyConnections, -1)
		}
		_, _, _ = vh.vtg.Execute(ctx, session, "rollback", make(map[string]*querypb.BindVariable))
	}
}

func (vh *vtgateHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	ctx := context.Background()
	var cancel context.CancelFunc
	if *mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, *mysqlQueryTimeout)
		defer cancel()
	}
	span, ctx := trace.NewSpan(ctx, "vtgateHandler.ComQuery")
	trace.AnnotateSQL(span, query)
	defer span.Finish()

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session, _ := c.ClientData.(*vtgatepb.Session)
	if session == nil {
		session = &vtgatepb.Session{
			Options: &querypb.ExecuteOptions{
				IncludedFields: querypb.ExecuteOptions_ALL,
			},
			Autocommit: true,
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
	}

	if !session.InTransaction {
		atomic.AddInt32(&busyConnections, 1)
	}
	defer func() {
		if !session.InTransaction {
			atomic.AddInt32(&busyConnections, -1)
		}
	}()

	if c.SchemaName != "" {
		session.TargetString = c.SchemaName
	}
	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		err := vh.vtg.StreamExecute(ctx, session, query, make(map[string]*querypb.BindVariable), callback)
		return mysql.NewSQLErrorFromError(err)
	}
	session, result, err := vh.vtg.Execute(ctx, session, query, make(map[string]*querypb.BindVariable))
	c.ClientData = session
	err = mysql.NewSQLErrorFromError(err)
	if err != nil {
		return err
	}
	return callback(result)
}

// ComPrepare is the handler for command prepare.
func (vh *vtgateHandler) ComPrepare(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	var ctx context.Context
	var cancel context.CancelFunc
	if *mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session, _ := c.ClientData.(*vtgatepb.Session)
	if session == nil {
		session = &vtgatepb.Session{
			Options: &querypb.ExecuteOptions{
				IncludedFields: querypb.ExecuteOptions_ALL,
			},
			Autocommit: true,
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
	}

	if !session.InTransaction {
		atomic.AddInt32(&busyConnections, 1)
	}
	defer func() {
		if !session.InTransaction {
			atomic.AddInt32(&busyConnections, -1)
		}
	}()

	if c.SchemaName != "" {
		session.TargetString = c.SchemaName
	}

	statement, err := sqlparser.ParseStrictDDL(query)
	if err != nil {
		err = mysql.NewSQLErrorFromError(err)
		return err
	}

	paramsCount := uint16(0)
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.SQLVal:
			if strings.HasPrefix(string(node.Val), ":v") {
				paramsCount++
			}
		}
		return true, nil
	}, statement)

	prepare := c.PrepareData[c.StatementID]

	if paramsCount > 0 {
		prepare.ParamsCount = paramsCount
		prepare.ParamsType = make([]int32, paramsCount)
		prepare.BindVars = make(map[string]*querypb.BindVariable, paramsCount)
	}

	session, result, err := vh.vtg.Prepare(ctx, session, query, make(map[string]*querypb.BindVariable))
	c.ClientData = session
	err = mysql.NewSQLErrorFromError(err)
	if err != nil {
		return err
	}
	return callback(result)
}

func (vh *vtgateHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	var ctx context.Context
	var cancel context.CancelFunc
	if *mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session, _ := c.ClientData.(*vtgatepb.Session)
	if session == nil {
		session = &vtgatepb.Session{
			Options: &querypb.ExecuteOptions{
				IncludedFields: querypb.ExecuteOptions_ALL,
			},
			Autocommit: true,
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
	}

	if !session.InTransaction {
		atomic.AddInt32(&busyConnections, 1)
	}
	defer func() {
		if !session.InTransaction {
			atomic.AddInt32(&busyConnections, -1)
		}
	}()

	if c.SchemaName != "" {
		session.TargetString = c.SchemaName
	}
	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		err := vh.vtg.StreamExecute(ctx, session, prepare.PrepareStmt, prepare.BindVars, callback)
		return mysql.NewSQLErrorFromError(err)
	}
	_, qr, err := vh.vtg.Execute(ctx, session, prepare.PrepareStmt, prepare.BindVars)
	if err != nil {
		err = mysql.NewSQLErrorFromError(err)
		return err
	}

	return callback(qr)
}

func (vh *vtgateHandler) WarningCount(c *mysql.Conn) uint16 {
	session, _ := c.ClientData.(*vtgatepb.Session)
	if session != nil {
		return uint16(len(session.GetWarnings()))
	}
	return 0
}

var mysqlListener *mysql.Listener
var mysqlUnixListener *mysql.Listener

// initiMySQLProtocol starts the mysql protocol.
// It should be called only once in a process.
func initMySQLProtocol() {
	// Flag is not set, just return.
	if *mysqlServerPort < 0 && *mysqlServerSocketPath == "" {
		return
	}

	// If no VTGate was created, just return.
	if rpcVTGate == nil {
		return
	}

	// Initialize registered AuthServer implementations (or other plugins)
	for _, initFn := range pluginInitializers {
		initFn()
	}
	authServer := mysql.GetAuthServer(*mysqlAuthServerImpl)

	switch *mysqlTCPVersion {
	case "tcp", "tcp4", "tcp6":
		// Valid flag value.
	default:
		log.Exitf("-mysql_tcp_version must be one of [tcp, tcp4, tcp6]")
	}

	// Create a Listener.
	var err error
	vh := newVtgateHandler(rpcVTGate)
	if *mysqlServerPort >= 0 {
		mysqlListener, err = mysql.NewListener(*mysqlTCPVersion, net.JoinHostPort(*mysqlServerBindAddress, fmt.Sprintf("%v", *mysqlServerPort)), authServer, vh, *mysqlConnReadTimeout, *mysqlConnWriteTimeout)
		if err != nil {
			log.Exitf("mysql.NewListener failed: %v", err)
		}
		if *mysqlServerVersion != "" {
			mysqlListener.ServerVersion = *mysqlServerVersion
		}
		if *mysqlSslCert != "" && *mysqlSslKey != "" {
			mysqlListener.TLSConfig, err = vttls.ServerConfig(*mysqlSslCert, *mysqlSslKey, *mysqlSslCa)
			if err != nil {
				log.Exitf("grpcutils.TLSServerConfig failed: %v", err)
				return
			}
			mysqlListener.RequireSecureTransport = *mysqlServerRequireSecureTransport
		}
		mysqlListener.AllowClearTextWithoutTLS = *mysqlAllowClearTextWithoutTLS
		// Check for the connection threshold
		if *mysqlSlowConnectWarnThreshold != 0 {
			log.Infof("setting mysql slow connection threshold to %v", mysqlSlowConnectWarnThreshold)
			mysqlListener.SlowConnectWarnThreshold = *mysqlSlowConnectWarnThreshold
		}
		// Start listening for tcp
		go mysqlListener.Accept()
	}

	if *mysqlServerSocketPath != "" {
		// Let's create this unix socket with permissions to all users. In this way,
		// clients can connect to vtgate mysql server without being vtgate user
		oldMask := syscall.Umask(000)
		mysqlUnixListener, err = newMysqlUnixSocket(*mysqlServerSocketPath, authServer, vh)
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

func shutdownMysqlProtocolAndDrain() {
	if mysqlListener != nil {
		mysqlListener.Close()
		mysqlListener = nil
	}
	if mysqlUnixListener != nil {
		mysqlUnixListener.Close()
		mysqlUnixListener = nil
	}

	if atomic.LoadInt32(&busyConnections) > 0 {
		log.Infof("Waiting for all client connections to be idle (%d active)...", atomic.LoadInt32(&busyConnections))
		start := time.Now()
		reported := start
		for atomic.LoadInt32(&busyConnections) != 0 {
			if time.Since(reported) > 2*time.Second {
				log.Infof("Still waiting for client connections to be idle (%d active)...", atomic.LoadInt32(&busyConnections))
				reported = time.Now()
			}

			time.Sleep(1 * time.Millisecond)
		}
	}
}

func init() {
	servenv.OnRun(initMySQLProtocol)
	servenv.OnTermSync(shutdownMysqlProtocolAndDrain)
}

var pluginInitializers []func()

// RegisterPluginInitializer lets plugins register themselves to be init'ed at servenv.OnRun-time
func RegisterPluginInitializer(initializer func()) {
	pluginInitializers = append(pluginInitializers, initializer)
}
