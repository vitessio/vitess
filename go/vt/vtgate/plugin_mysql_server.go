package vtgate

import (
	"flag"
	"fmt"
	"net"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/servenv"

	"strings"

	"sync"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

var (
	mysqlServerPort               = flag.Int("mysql_server_port", 0, "If set, also listen for MySQL binary protocol connections on this port.")
	mysqlAuthServerImpl           = flag.String("mysql_auth_server_impl", "static", "Which auth server implementation to use.")
	mysqlAllowClearTextWithoutTLS = flag.Bool("mysql_allow_clear_text_without_tls", false, "If set, the server will allow the use of a clear text password over non-SSL connections.")
	assumeSingleDbConnections     = flag.Bool("mysql_server_assume_single_db", false, "If set, when someone connects to a keyspace we will force routing to keyspace/0. User can hit all shards by doing keyspace/*")

	mysqlSslCert = flag.String("mysql_server_ssl_cert", "", "Path to the ssl cert for mysql server plugin SSL")
	mysqlSslKey  = flag.String("mysql_server_ssl_key", "", "Path to ssl key for mysql server plugin SSL")
	mysqlSslCa   = flag.String("mysql_server_ssl_ca", "", "Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs.")
)

// vtgateHandler implements the Listener interface.
// It stores the Session in the ClientData of a Connection, if a transaction
// is in progress.
type vtgateHandler struct {
	vtg *VTGate

	mu                    sync.Mutex
	keyspaceSingleDbCache map[string]string
}

func newVtgateHandler(vtg *VTGate) *vtgateHandler {
	return &vtgateHandler{
		vtg: vtg,
		keyspaceSingleDbCache: make(map[string]string),
	}
}

func (vh *vtgateHandler) NewConnection(c *mysqlconn.Conn) {
}

func (vh *vtgateHandler) ConnectionClosed(c *mysqlconn.Conn) {
	// Rollback if there is an ongoing transaction. Ignore error.
	ctx := context.Background()
	session, _ := c.ClientData.(*vtgatepb.Session)
	if session == nil || !session.InTransaction {
		return
	}
	_, _, _ = vh.vtg.Execute(ctx, "rollback", make(map[string]interface{}), "", topodatapb.TabletType_MASTER, session, false, &querypb.ExecuteOptions{})
}

func (vh *vtgateHandler) ComQuery(c *mysqlconn.Conn, query string) (*sqltypes.Result, error) {
	// FIXME(alainjobart): Add some kind of timeout to the context.
	ctx := context.Background()

	switch {
	case sqlparser.HasPrefix(query, "use"):
		schema := strings.TrimSpace(query[3:])
		// the schema name can be quoted, remove them if necessary
		if schema[0] == schema[len(schema)-1] && schema[0] == '`' || schema[0] == '"' || schema[0] == '\'' {
			schema = schema[1 : len(schema)-1]
		}
		if schema == "" {
			return nil, fmt.Errorf("Unable to parse schema name from USE statement: %v", schema)
		}
		c.SchemaName = schema
		return &sqltypes.Result{}, nil
	default:
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
		session, result, err := vh.vtg.Execute(ctx, query, make(map[string]interface{}), vh.ensureKeyspaceTarget(ctx, c.SchemaName), topodatapb.TabletType_MASTER, session, false /* notInTransaction */, &querypb.ExecuteOptions{
			IncludedFields: querypb.ExecuteOptions_ALL,
		})
		c.ClientData = session
		return result, sqldb.NewSQLErrorFromError(err)
	}
}

// ensureKeyspaceTarget is a temporary measure to improve the usability of the mysql protocol
// for situations when most keyspaces are unsharded. It will be removed once V3 is improved to handle
// more adhoc, DBA, DDL, etc queries for unsharded keyspaces.
// converts `keyspace` to `keyspace/0` if the keyspace has only one shard.
func (vh *vtgateHandler) ensureKeyspaceTarget(ctx context.Context, schema string) string {
	if schema == "" {
		return schema
	}
	keyspace, shard := parseKeyspaceOptionalShard(schema)
	// allow `keyspace/*` to connect to multiple dbs
	if shard == "*" {
		return keyspace
	}
	if shard != "" || !*assumeSingleDbConnections {
		return schema
	}

	vh.mu.Lock()
	if val, ok := vh.keyspaceSingleDbCache[schema]; ok {
		vh.mu.Unlock()
		return val
	}
	vh.mu.Unlock()

	// Count the shards for the keyspace according to SrvKeyspace
	srvKeyspace, err := vh.vtg.router.serv.GetSrvKeyspace(ctx, vh.vtg.resolver.cell, keyspace)
	if err != nil {
		log.Warningf("error getting SrvKeyspace for %v: %v", keyspace, err)
		return schema
	}
	vh.mu.Lock()
	defer vh.mu.Unlock()
	shardCount := 0
	for _, partition := range srvKeyspace.Partitions {
		if sc := len(partition.ShardReferences); sc > shardCount {
			shardCount = sc
		}
	}
	if shardCount != 1 {
		vh.keyspaceSingleDbCache[schema] = schema
		return schema
	}

	res := fmt.Sprintf("%s/0", keyspace)
	vh.keyspaceSingleDbCache[schema] = res

	return res
}

func init() {
	var listener *mysqlconn.Listener

	servenv.OnRun(func() {
		// Flag is not set, just return.
		if *mysqlServerPort == 0 {
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
		authServer := mysqlconn.GetAuthServer(*mysqlAuthServerImpl)

		// Create a Listener.
		var err error
		vh := newVtgateHandler(rpcVTGate)
		listener, err = mysqlconn.NewListener("tcp", net.JoinHostPort("", fmt.Sprintf("%v", *mysqlServerPort)), authServer, vh)
		if err != nil {
			log.Fatalf("mysqlconn.NewListener failed: %v", err)
		}
		if *mysqlSslCert != "" && *mysqlSslKey != "" {
			listener.TLSConfig, err = grpcutils.TLSServerConfig(*mysqlSslCert, *mysqlSslKey, *mysqlSslCa)
			if err != nil {
				log.Fatalf("grpcutils.TLSServerConfig failed: %v", err)
				return
			}
		}
		listener.AllowClearTextWithoutTLS = *mysqlAllowClearTextWithoutTLS

		// And starts listening.
		go func() {
			listener.Accept()
		}()
	})

	servenv.OnTerm(func() {
		if listener != nil {
			listener.Close()
		}
	})
}

var pluginInitializers []func()

// RegisterPluginInitializer lets plugins register themselves to be init'ed at servenv.OnRun-time
func RegisterPluginInitializer(initializer func()) {
	pluginInitializers = append(pluginInitializers, initializer)
}
