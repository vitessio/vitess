package vtgate

import (
	"flag"
	"fmt"
	"net"
	"strings"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/servenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var (
	mysqlServerPort = flag.Int("mysql_server_port", 0, "If set, also listen for MySQL binary protocol connections on this port.")
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

func (vh *vtgateHandler) NewConnection(c *mysqlconn.Conn) {
}

func (vh *vtgateHandler) ConnectionClosed(c *mysqlconn.Conn) {
	// Rollback if there is an ongoing transaction. Ignore error.
	ctx := context.Background()
	vh.rollback(ctx, c)
}

func (vh *vtgateHandler) begin(ctx context.Context, c *mysqlconn.Conn) (*sqltypes.Result, error) {
	// Check we're not inside a transaction already.
	if c.ClientData != nil {
		return nil, sqldb.NewSQLError(mysqlconn.ERCantDoThisDuringAnTransaction, mysqlconn.SSCantDoThisDuringAnTransaction, "already in a transaction")
	}

	// Do the begin.
	session, err := vh.vtg.Begin(ctx, false /* singledb */)
	if err != nil {
		return nil, sqldb.NewSQLError(mysqlconn.ERUnknownError, mysqlconn.SSUnknownSQLState, "vtgate.Begin failed: %v", err)
	}

	// Save the session.
	c.ClientData = session
	return &sqltypes.Result{}, nil
}

func (vh *vtgateHandler) commit(ctx context.Context, c *mysqlconn.Conn) (*sqltypes.Result, error) {
	// Check we're inside a transaction already.
	if c.ClientData == nil {
		return nil, sqldb.NewSQLError(mysqlconn.ERUnknownError, mysqlconn.SSUnknownSQLState, "not in a transaction")
	}
	session, ok := c.ClientData.(*vtgatepb.Session)
	if !ok || session == nil {
		return nil, sqldb.NewSQLError(mysqlconn.ERUnknownError, mysqlconn.SSUnknownSQLState, "internal error: got a weird ClientData of type %T: %v %v", c.ClientData, session, ok)
	}

	// Commit using vtgate's transaction mode.
	if err := vh.vtg.Commit(ctx, vh.vtg.transactionMode == TxTwoPC, session); err != nil {
		return nil, sqldb.NewSQLError(mysqlconn.ERUnknownError, mysqlconn.SSUnknownSQLState, "vtgate.Commit failed: %v", err)
	}

	// Clear the Session.
	c.ClientData = nil
	return &sqltypes.Result{}, nil
}

func (vh *vtgateHandler) rollback(ctx context.Context, c *mysqlconn.Conn) (*sqltypes.Result, error) {
	// Check we're inside a transaction already.
	if c.ClientData == nil {
		return nil, sqldb.NewSQLError(mysqlconn.ERUnknownError, mysqlconn.SSUnknownSQLState, "not in a transaction")
	}
	session, ok := c.ClientData.(*vtgatepb.Session)
	if !ok || session == nil {
		return nil, sqldb.NewSQLError(mysqlconn.ERUnknownError, mysqlconn.SSUnknownSQLState, "internal error: got a weird ClientData of type %T: %v %v", c.ClientData, session, ok)
	}

	// Rollback.
	if err := vh.vtg.Rollback(ctx, session); err != nil {
		return nil, sqldb.NewSQLError(mysqlconn.ERUnknownError, mysqlconn.SSUnknownSQLState, "vtgate.Rollback failed: %v", err)
	}

	// Clear the Session.
	c.ClientData = nil
	return &sqltypes.Result{}, nil
}

func (vh *vtgateHandler) ComQuery(c *mysqlconn.Conn, query string) (*sqltypes.Result, error) {
	// FIXME(alainjobart): do something better for context.
	// Include some kind of callerid reference, using the
	// authenticated user.
	// Add some kind of timeout too.
	ctx := context.Background()

	// FIXME(alainjobart) would be good to have the parser understand this.
	switch {
	case strings.EqualFold(query, "begin"):
		return vh.begin(ctx, c)
	case strings.EqualFold(query, "commit"):
		return vh.commit(ctx, c)
	case strings.EqualFold(query, "rollback"):
		return vh.rollback(ctx, c)
	default:
		// Grab the current session, if any.
		var session *vtgatepb.Session
		if c.ClientData != nil {
			session, _ = c.ClientData.(*vtgatepb.Session)
		}

		// And just go to v3.
		result, err := vh.vtg.Execute(ctx, query, make(map[string]interface{}), c.SchemaName, topodatapb.TabletType_MASTER, session, false /* notInTransaction */, &querypb.ExecuteOptions{
			IncludedFields: querypb.ExecuteOptions_ALL,
		})
		return result, sqldb.NewSQLErrorFromError(err)
	}
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

		// Create a Listener.
		var err error
		vh := newVtgateHandler(rpcVTGate)
		listener, err = mysqlconn.NewListener("tcp", net.JoinHostPort("", fmt.Sprintf("%v", *mysqlServerPort)), vh)
		if err != nil {
			log.Fatalf("mysqlconn.NewListener failed: %v", err)
		}

		// Add fake users for now.
		// FIXME(alainjobart): add a config file with users
		// and passwords.
		listener.PasswordMap["mysql_user"] = "mysql_password"

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
