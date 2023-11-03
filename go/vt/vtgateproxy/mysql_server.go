/*
Copyright 2023 The Vitess Authors.

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

package vtgateproxy

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttls"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	mysqlServerPort               = flag.Int("mysql_server_port", -1, "If set, also listen for MySQL binary protocol connections on this port.")
	mysqlServerBindAddress        = flag.String("mysql_server_bind_address", "", "Binds on this address when listening to MySQL binary protocol. Useful to restrict listening to 'localhost' only for instance.")
	mysqlServerSocketPath         = flag.String("mysql_server_socket_path", "", "This option specifies the Unix socket file to use when listening for local connections. By default it will be empty and it won't listen to a unix socket")
	mysqlTCPVersion               = flag.String("mysql_tcp_version", "tcp", "Select tcp, tcp4, or tcp6 to control the socket type.")
	mysqlAuthServerImpl           = flag.String("mysql_auth_server_impl", "static", "Which auth server implementation to use. Options: none, ldap, clientcert, static, vault.")
	mysqlAllowClearTextWithoutTLS = flag.Bool("mysql_allow_clear_text_without_tls", false, "If set, the server will allow the use of a clear text password over non-SSL connections.")
	mysqlProxyProtocol            = flag.Bool("proxy_protocol", false, "Enable HAProxy PROXY protocol on MySQL listener socket")

	mysqlServerRequireSecureTransport = flag.Bool("mysql_server_require_secure_transport", false, "Reject insecure connections but only if mysql_server_ssl_cert and mysql_server_ssl_key are provided")

	mysqlSslCert = flag.String("mysql_server_ssl_cert", "", "Path to the ssl cert for mysql server plugin SSL")
	mysqlSslKey  = flag.String("mysql_server_ssl_key", "", "Path to ssl key for mysql server plugin SSL")
	mysqlSslCa   = flag.String("mysql_server_ssl_ca", "", "Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs.")
	mysqlSslCrl  = flag.String("mysql_server_ssl_crl", "", "Path to ssl CRL for mysql server plugin SSL")

	mysqlTLSMinVersion = flag.String("mysql_server_tls_min_version", "", "Configures the minimal TLS version negotiated when SSL is enabled. Defaults to TLSv1.2. Options: TLSv1.0, TLSv1.1, TLSv1.2, TLSv1.3.")

	mysqlSslServerCA = flag.String("mysql_server_ssl_server_ca", "", "path to server CA in PEM format, which will be combine with server cert, return full certificate chain to clients")

	mysqlSlowConnectWarnThreshold = flag.Duration("mysql_slow_connect_warn_threshold", 0, "Warn if it takes more than the given threshold for a mysql connection to establish")

	mysqlConnReadTimeout  = flag.Duration("mysql_server_read_timeout", 0, "connection read timeout")
	mysqlConnWriteTimeout = flag.Duration("mysql_server_write_timeout", 0, "connection write timeout")
	mysqlQueryTimeout     = flag.Duration("mysql_server_query_timeout", 0, "mysql query timeout")

	mysqlDefaultWorkloadName = flag.String("mysql_default_workload", "OLTP", "Default session workload (OLTP, OLAP, DBA)")
	mysqlDefaultWorkload     int32

	busyConnections int32
)

// proxyHandler implements the Listener interface.
// It stores the Session in the ClientData of a Connection.
type proxyHandler struct {
	mysql.UnimplementedHandler
	mu sync.Mutex

	proxy *VTGateProxy
}

func newProxyHandler(proxy *VTGateProxy) *proxyHandler {
	return &proxyHandler{
		proxy: proxy,
	}
}

func (ph *proxyHandler) NewConnection(c *mysql.Conn) {
}

func (ph *proxyHandler) ComResetConnection(c *mysql.Conn) {
	ctx := context.Background()
	session, err := ph.getSession(ctx, c)
	if err != nil {
		return
	}
	if session.SessionPb().InTransaction {
		defer atomic.AddInt32(&busyConnections, -1)
	}
	err = ph.proxy.CloseSession(ctx, session)
	if err != nil {
		log.Errorf("Error happened in transaction rollback: %v", err)
	}
}

func (ph *proxyHandler) ConnectionClosed(c *mysql.Conn) {
	// Rollback if there is an ongoing transaction. Ignore error.
	defer func() {
		ph.mu.Lock()
		defer ph.mu.Unlock()
	}()

	var ctx context.Context
	var cancel context.CancelFunc
	if *mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	session, err := ph.getSession(ctx, c)
	if err != nil {
		return
	}
	if session.SessionPb().InTransaction {
		defer atomic.AddInt32(&busyConnections, -1)
	}
	_ = ph.proxy.CloseSession(ctx, session)
}

// Regexp to extract parent span id over the sql query
var r = regexp.MustCompile(`/\*VT_SPAN_CONTEXT=(.*)\*/`)

// this function is here to make this logic easy to test by decoupling the logic from the `trace.NewSpan` and `trace.NewFromString` functions
func startSpanTestable(ctx context.Context, query, label string,
	newSpan func(context.Context, string) (trace.Span, context.Context),
	newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error)) (trace.Span, context.Context, error) {
	_, comments := sqlparser.SplitMarginComments(query)
	match := r.FindStringSubmatch(comments.Leading)
	span, ctx := getSpan(ctx, match, newSpan, label, newSpanFromString)

	trace.AnnotateSQL(span, sqlparser.Preview(query))

	return span, ctx, nil
}

func getSpan(ctx context.Context, match []string, newSpan func(context.Context, string) (trace.Span, context.Context), label string, newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error)) (trace.Span, context.Context) {
	var span trace.Span
	if len(match) != 0 {
		var err error
		span, ctx, err = newSpanFromString(ctx, match[1], label)
		if err == nil {
			return span, ctx
		}
		log.Warningf("Unable to parse VT_SPAN_CONTEXT: %s", err.Error())
	}
	span, ctx = newSpan(ctx, label)
	return span, ctx
}

func startSpan(ctx context.Context, query, label string) (trace.Span, context.Context, error) {
	return startSpanTestable(ctx, query, label, trace.NewSpan, trace.NewFromString)
}

func (ph *proxyHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	ctx := context.Background()
	var cancel context.CancelFunc
	if *mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, *mysqlQueryTimeout)
		defer cancel()
	}

	span, ctx, err := startSpan(ctx, query, "proxyHandler.ComQuery")
	if err != nil {
		return vterrors.Wrap(err, "failed to extract span")
	}
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

	session, err := ph.getSession(ctx, c)
	if err != nil {
		return err
	}
	if !session.SessionPb().InTransaction {
		atomic.AddInt32(&busyConnections, 1)
	}
	defer func() {
		if session == nil || !session.SessionPb().InTransaction {
			atomic.AddInt32(&busyConnections, -1)
		}
	}()

	if session.SessionPb().Options.Workload == querypb.ExecuteOptions_OLAP {
		err := ph.proxy.StreamExecute(ctx, session, query, make(map[string]*querypb.BindVariable), callback)
		return mysql.NewSQLErrorFromError(err)
	}

	result, err := ph.proxy.Execute(ctx, session, query, make(map[string]*querypb.BindVariable))

	if err := mysql.NewSQLErrorFromError(err); err != nil {
		return err
	}
	fillInTxStatusFlags(c, session)
	return callback(result)
}

func fillInTxStatusFlags(c *mysql.Conn, session *vtgateconn.VTGateSession) {
	if session.SessionPb().InTransaction {
		c.StatusFlags |= mysql.ServerStatusInTrans
	} else {
		c.StatusFlags &= mysql.NoServerStatusInTrans
	}
	if session.SessionPb().Autocommit {
		c.StatusFlags |= mysql.ServerStatusAutocommit
	} else {
		c.StatusFlags &= mysql.NoServerStatusAutocommit
	}
}

// ComPrepare is the handler for command prepare.
func (ph *proxyHandler) ComPrepare(c *mysql.Conn, query string, bindVars map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
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
		"VTGateProxy MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session, err := ph.getSession(ctx, c)
	if err != nil {
		return nil, err
	}
	if !session.SessionPb().InTransaction {
		atomic.AddInt32(&busyConnections, 1)
	}
	defer func() {
		if !session.SessionPb().InTransaction {
			atomic.AddInt32(&busyConnections, -1)
		}
	}()

	session, fld, err := ph.proxy.Prepare(ctx, session, query, bindVars)
	err = mysql.NewSQLErrorFromError(err)
	if err != nil {
		return nil, err
	}
	return fld, nil
}

func (ph *proxyHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
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
		"VTGateProxy MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	session, err := ph.getSession(ctx, c)
	if err != nil {
		return err
	}
	if !session.SessionPb().InTransaction {
		atomic.AddInt32(&busyConnections, 1)
	}
	defer func() {
		if !session.SessionPb().InTransaction {
			atomic.AddInt32(&busyConnections, -1)
		}
	}()

	if session.SessionPb().Options.Workload == querypb.ExecuteOptions_OLAP {
		err := ph.proxy.StreamExecute(ctx, session, prepare.PrepareStmt, prepare.BindVars, callback)
		return mysql.NewSQLErrorFromError(err)
	}

	qr, err := ph.proxy.Execute(ctx, session, prepare.PrepareStmt, prepare.BindVars)
	if err != nil {
		return mysql.NewSQLErrorFromError(err)
	}
	fillInTxStatusFlags(c, session)

	return callback(qr)
}

func (ph *proxyHandler) WarningCount(c *mysql.Conn) uint16 {
	session, _ := c.ClientData.(*vtgateconn.VTGateSession)
	if session == nil {
		return 0
	}

	return uint16(len(session.SessionPb().GetWarnings()))
}

// ComBinlogDumpGTID is part of the mysql.Handler interface.
func (ph *proxyHandler) ComBinlogDumpGTID(c *mysql.Conn, gtidSet mysql.GTIDSet) error {
	return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "ComBinlogDumpGTID")
}

func (ph *proxyHandler) getSession(ctx context.Context, c *mysql.Conn) (*vtgateconn.VTGateSession, error) {
	session, _ := c.ClientData.(*vtgateconn.VTGateSession)
	if session == nil {
		options := &querypb.ExecuteOptions{
			IncludedFields: querypb.ExecuteOptions_ALL,
			Workload:       querypb.ExecuteOptions_Workload(mysqlDefaultWorkload),
		}

		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			options.ClientFoundRows = true
		}

		var err error
		session, err = ph.proxy.NewSession(ctx, options, c.Attributes)
		if err != nil {
			log.Errorf("error creating new session for %s: %v", c.GetRawConn().RemoteAddr().String(), err)
			return nil, err
		}

		if session != nil {
			c.ClientData = session
		}
	}

	return session, nil
}

var mysqlListener *mysql.Listener
var mysqlUnixListener *mysql.Listener
var sigChan chan os.Signal
var proxyHandle *proxyHandler

// initTLSConfig inits tls config for the given mysql listener
func initTLSConfig(mysqlListener *mysql.Listener, mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA string, mysqlServerRequireSecureTransport bool, mysqlMinTLSVersion uint16) error {
	serverConfig, err := vttls.ServerConfig(mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlMinTLSVersion)
	if err != nil {
		log.Exitf("grpcutils.TLSServerConfig failed: %v", err)
		return err
	}
	mysqlListener.TLSConfig.Store(serverConfig)
	mysqlListener.RequireSecureTransport = mysqlServerRequireSecureTransport
	sigChan = make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	go func() {
		for range sigChan {
			serverConfig, err := vttls.ServerConfig(mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlMinTLSVersion)
			if err != nil {
				log.Errorf("grpcutils.TLSServerConfig failed: %v", err)
			} else {
				log.Info("grpcutils.TLSServerConfig updated")
				mysqlListener.TLSConfig.Store(serverConfig)
			}
		}
	}()
	return nil
}

// initiMySQLProtocol starts the mysql protocol.
// It should be called only once in a process.
func initMySQLProtocol() {
	// Flag is not set, just return.
	if *mysqlServerPort < 0 && *mysqlServerSocketPath == "" {
		return
	}

	// Initialize registered AuthServer implementations (or other plugins)
	for _, initFn := range pluginInitializers {
		initFn()
	}
	authServer := mysql.GetAuthServer(*mysqlAuthServerImpl)

	// Check mysql_default_workload
	var ok bool
	if mysqlDefaultWorkload, ok = querypb.ExecuteOptions_Workload_value[strings.ToUpper(*mysqlDefaultWorkloadName)]; !ok {
		log.Exitf("-mysql_default_workload must be one of [OLTP, OLAP, DBA, UNSPECIFIED]")
	}

	switch *mysqlTCPVersion {
	case "tcp", "tcp4", "tcp6":
		// Valid flag value.
	default:
		log.Exitf("-mysql_tcp_version must be one of [tcp, tcp4, tcp6]")
	}

	// Create a Listener.
	var err error
	proxyHandle = newProxyHandler(vtGateProxy)
	if *mysqlServerPort >= 0 {
		log.Infof("Mysql Server listening on Port %d", *mysqlServerPort)
		mysqlListener, err = mysql.NewListener(*mysqlTCPVersion, net.JoinHostPort(*mysqlServerBindAddress, fmt.Sprintf("%v", *mysqlServerPort)), authServer, proxyHandle, *mysqlConnReadTimeout, *mysqlConnWriteTimeout, *mysqlProxyProtocol)
		if err != nil {
			log.Exitf("mysql.NewListener failed: %v", err)
		}
		if *servenv.MySQLServerVersion != "" {
			mysqlListener.ServerVersion = *servenv.MySQLServerVersion
		}
		if *mysqlSslCert != "" && *mysqlSslKey != "" {
			tlsVersion, err := vttls.TLSVersionToNumber(*mysqlTLSMinVersion)
			if err != nil {
				log.Exitf("mysql.NewListener failed: %v", err)
			}

			_ = initTLSConfig(mysqlListener, *mysqlSslCert, *mysqlSslKey, *mysqlSslCa, *mysqlSslCrl, *mysqlSslServerCA, *mysqlServerRequireSecureTransport, tlsVersion)
		}
		mysqlListener.AllowClearTextWithoutTLS.Set(*mysqlAllowClearTextWithoutTLS)
		// Check for the connection threshold
		if *mysqlSlowConnectWarnThreshold != 0 {
			log.Infof("setting mysql slow connection threshold to %v", mysqlSlowConnectWarnThreshold)
			mysqlListener.SlowConnectWarnThreshold.Set(*mysqlSlowConnectWarnThreshold)
		}
		// Start listening for tcp
		go mysqlListener.Accept()
	}

	if *mysqlServerSocketPath != "" {
		// Let's create this unix socket with permissions to all users. In this way,
		// clients can connect to vtgate mysql server without being vtgate user
		oldMask := syscall.Umask(000)
		mysqlUnixListener, err = newMysqlUnixSocket(*mysqlServerSocketPath, authServer, proxyHandle)
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
	listener, err := mysql.NewListener("unix", address, authServer, handler, *mysqlConnReadTimeout, *mysqlConnWriteTimeout, false)
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
		listener, listenerErr := mysql.NewListener("unix", address, authServer, handler, *mysqlConnReadTimeout, *mysqlConnWriteTimeout, false)
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
	if sigChan != nil {
		signal.Stop(sigChan)
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

func rollbackAtShutdown() {
	defer log.Flush()

	// XXX/demmer figure out numConnections and rollback
	/*

		// Close all open connections. If they're waiting for reads, this will cause
		// them to error out, which will automatically rollback open transactions.
		func() {
			if proxyHandle != nil {
				proxyHandle.mu.Lock()
				defer proxyHandle.mu.Unlock()
				for c := range proxyHandle.connections {
					if c != nil {
						log.Infof("Rolling back transactions associated with connection ID: %v", c.ConnectionID)
						c.Close()
					}
				}
			}
		}()


		// If vtgate is instead busy executing a query, the number of open conns
		// will be non-zero. Give another second for those queries to finish.
		   for i := 0; i < 100; i++ {
		   		if proxyHandle.numConnections() == 0 {
		   			log.Infof("All connections have been rolled back.")
		   			return
		   		}
		   		time.Sleep(10 * time.Millisecond)
		   	}
	*/
	log.Errorf("All connections did not go idle. Shutting down anyway.")
}

func mysqlSocketPath() string {
	if mysqlServerSocketPath == nil {
		return ""
	}
	return *mysqlServerSocketPath
}

func init() {
	servenv.OnRun(initMySQLProtocol)
	servenv.OnTermSync(shutdownMysqlProtocolAndDrain)
	servenv.OnClose(rollbackAtShutdown)
}

var pluginInitializers []func()

// RegisterPluginInitializer lets plugins register themselves to be init'ed at servenv.OnRun-time
func RegisterPluginInitializer(initializer func()) {
	pluginInitializers = append(pluginInitializers, initializer)
}
