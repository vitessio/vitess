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

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/servenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
)

var (
	mysqlServerPort               = flag.Int("mysql_server_port", -1, "If set, also listen for MySQL binary protocol connections on this port.")
	mysqlServerBindAddress        = flag.String("mysql_server_bind_address", "", "Binds on this address when listening to MySQL binary protocol. Useful to restrict listening to 'localhost' only for instance.")
	mysqlAuthServerImpl           = flag.String("mysql_auth_server_impl", "static", "Which auth server implementation to use.")
	mysqlAllowClearTextWithoutTLS = flag.Bool("mysql_allow_clear_text_without_tls", false, "If set, the server will allow the use of a clear text password over non-SSL connections.")

	mysqlSslCert = flag.String("mysql_server_ssl_cert", "", "Path to the ssl cert for mysql server plugin SSL")
	mysqlSslKey  = flag.String("mysql_server_ssl_key", "", "Path to ssl key for mysql server plugin SSL")
	mysqlSslCa   = flag.String("mysql_server_ssl_ca", "", "Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs.")

	mysqlSlowConnectWarnThreshold = flag.Duration("mysql_slow_connect_warn_threshold", 0, "Warn if it takes more than the given threshold for a mysql connection to establish")
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
	ctx := context.Background()
	session, _ := c.ClientData.(*vtgatepb.Session)
	if session != nil {
		_, _, _ = vh.vtg.Execute(ctx, session, "rollback", make(map[string]interface{}))
	}
}

func (vh *vtgateHandler) ComQuery(c *mysql.Conn, query []byte, callback func(*sqltypes.Result) error) error {
	// FIXME(alainjobart): Add some kind of timeout to the context.
	ctx := context.Background()

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
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
	}
	if c.SchemaName != "" {
		session.TargetString = c.SchemaName
	}
	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		err := vh.vtg.StreamExecute(ctx, session, string(query), make(map[string]interface{}), callback)
		return mysql.NewSQLErrorFromError(err)
	}
	session, result, err := vh.vtg.Execute(ctx, session, string(query), make(map[string]interface{}))
	c.ClientData = session
	err = mysql.NewSQLErrorFromError(err)
	if err != nil {
		return err
	}
	return callback(result)
}

var mysqlListener *mysql.Listener

// initiMySQLProtocol starts the mysql protocol.
// It should be called only once in a process.
func initMySQLProtocol() {
	// Flag is not set, just return.
	if *mysqlServerPort < 0 {
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

	// Create a Listener.
	var err error
	vh := newVtgateHandler(rpcVTGate)
	mysqlListener, err = mysql.NewListener("tcp", net.JoinHostPort(*mysqlServerBindAddress, fmt.Sprintf("%v", *mysqlServerPort)), authServer, vh)
	if err != nil {
		log.Fatalf("mysql.NewListener failed: %v", err)
	}
	if *mysqlSslCert != "" && *mysqlSslKey != "" {
		mysqlListener.TLSConfig, err = grpcutils.TLSServerConfig(*mysqlSslCert, *mysqlSslKey, *mysqlSslCa)
		if err != nil {
			log.Fatalf("grpcutils.TLSServerConfig failed: %v", err)
			return
		}
	}
	mysqlListener.AllowClearTextWithoutTLS = *mysqlAllowClearTextWithoutTLS

	// Check for the connection threshold
	if *mysqlSlowConnectWarnThreshold != 0 {
		log.Infof("setting mysql slow connection threshold to %v", mysqlSlowConnectWarnThreshold)
		mysqlListener.SlowConnectWarnThreshold = *mysqlSlowConnectWarnThreshold
	}

	// And starts listening.
	go func() {
		mysqlListener.Accept()
	}()
}

func init() {
	servenv.OnRun(initMySQLProtocol)

	servenv.OnTerm(func() {
		if mysqlListener != nil {
			mysqlListener.Close()
			mysqlListener = nil
		}
	})
}

var pluginInitializers []func()

// RegisterPluginInitializer lets plugins register themselves to be init'ed at servenv.OnRun-time
func RegisterPluginInitializer(initializer func()) {
	pluginInitializers = append(pluginInitializers, initializer)
}
