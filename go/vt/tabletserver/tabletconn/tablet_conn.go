// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletconn

import (
	"flag"
	"io"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const (
	// ConnClosed is returned when the underlying connection was closed.
	ConnClosed = OperationalError("vttablet: Connection Closed")
)

var (
	// TabletProtocol is exported for unit tests
	TabletProtocol = flag.String("tablet_protocol", "grpc", "how to talk to the vttablets")
)

// ServerError represents an error that was returned from
// a vttablet server. it implements vterrors.VtError.
type ServerError struct {
	Err string
	// ServerCode is the error code that we got from the server.
	ServerCode vtrpcpb.ErrorCode
}

func (e *ServerError) Error() string { return e.Err }

// VtErrorCode returns the underlying Vitess error code.
// This makes ServerError implement vterrors.VtError.
func (e *ServerError) VtErrorCode() vtrpcpb.ErrorCode { return e.ServerCode }

// OperationalError represents an error due to a failure to
// communicate with vttablet.
type OperationalError string

func (e OperationalError) Error() string { return string(e) }

// In all the following calls, context is an opaque structure that may
// carry data related to the call. For instance, if an incoming RPC
// call is responsible for these outgoing calls, and the incoming
// protocol and outgoing protocols support forwarding information, use
// context.

// TabletDialer represents a function that will return a TabletConn
// object that can communicate with a tablet. Only the tablet's
// HostName and PortMap should be used (and maybe the alias for debug
// messages).
//
// When using this TabletDialer to talk to a l2vtgate, only the Hostname
// will be set to the full address to dial. Implementations should detect
// this use case as the portmap will then be empty.
//
// timeout represents the connection timeout. If set to 0, this
// connection should be established in the background and the
// TabletDialer should return right away.
type TabletDialer func(tablet *topodatapb.Tablet, timeout time.Duration) (TabletConn, error)

// TabletConn defines the interface for a vttablet client. It should
// be thread-safe, so it can be used concurrently used across goroutines.
//
// Most RPC functions can return:
// - tabletconn.ConnClosed if the underlying connection was closed.
// - context.Canceled if the query was canceled by the user.
type TabletConn interface {
	// Execute executes a non-streaming query on vttablet.
	Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error)

	// ExecuteBatch executes a group of queries.
	ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error)

	// StreamExecute executes a streaming query on vttablet.
	StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error

	// Transaction support
	Begin(ctx context.Context, target *querypb.Target) (transactionID int64, err error)
	Commit(ctx context.Context, target *querypb.Target, transactionID int64) error
	Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error
	Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error
	CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) error
	RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) error
	CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) error
	StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error
	SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) error
	ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) error
	ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error)

	// Combo RPC calls: they execute both a Begin and another call.
	// Note even if error is set, transactionID may be returned
	// and different than zero, if the Begin part worked.
	BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (result *sqltypes.Result, transactionID int64, err error)
	BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) (results []sqltypes.Result, transactionID int64, err error)

	// Messaging methods.
	MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error
	MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error)

	// SplitQuery splits a query into equally sized smaller queries by
	// appending primary key range clauses to the original query
	SplitQuery(
		ctx context.Context,
		target *querypb.Target,
		query querytypes.BoundQuery,
		splitColumns []string,
		splitCount int64,
		numRowsPerQueryPart int64,
		algorithm querypb.SplitQueryRequest_Algorithm) (queries []querytypes.QuerySplit, err error)

	// StreamHealth starts a streaming RPC for VTTablet health status updates.
	StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error

	// UpdateStream streams updates from the provided position or timestamp.
	UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error

	// Close must be called for releasing resources.
	Close(ctx context.Context) error
}

type resultStreamer struct {
	done chan struct{}
	ch   chan *sqltypes.Result
	err  error
}

func (rs *resultStreamer) Recv() (*sqltypes.Result, error) {
	select {
	case <-rs.done:
		return nil, rs.err
	case qr := <-rs.ch:
		return qr, nil
	}
}

// ExecuteWithStreamer performs a StreamExecute, but returns a *sqltypes.ResultStream to iterate on.
// This function should only be used for legacy code. New usage should directly use StreamExecute.
func ExecuteWithStreamer(ctx context.Context, conn TabletConn, target *querypb.Target, sql string, bindVariables map[string]interface{}, options *querypb.ExecuteOptions) sqltypes.ResultStream {
	rs := &resultStreamer{
		done: make(chan struct{}),
		ch:   make(chan *sqltypes.Result),
	}
	go func() {
		defer close(rs.done)
		rs.err = conn.StreamExecute(ctx, target, sql, bindVariables, options, func(qr *sqltypes.Result) error {
			select {
			case <-ctx.Done():
				return io.EOF
			case rs.ch <- qr:
			}
			return nil
		})
		if rs.err == nil {
			rs.err = io.EOF
		}
	}()
	return rs
}

var dialers = make(map[string]TabletDialer)

// RegisterDialer is meant to be used by TabletDialer implementations
// to self register.
func RegisterDialer(name string, dialer TabletDialer) {
	if _, ok := dialers[name]; ok {
		log.Fatalf("Dialer %s already exists", name)
	}
	dialers[name] = dialer
}

// GetDialer returns the dialer to use, described by the command line flag
func GetDialer() TabletDialer {
	td, ok := dialers[*TabletProtocol]
	if !ok {
		log.Fatalf("No dialer registered for tablet protocol %s", *TabletProtocol)
	}
	return td
}
