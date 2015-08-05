// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletconn

import (
	"flag"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	ERR_NORMAL = iota
	ERR_RETRY
	ERR_FATAL
	ERR_TX_POOL_FULL
	ERR_NOT_IN_TX
)

const (
	ConnClosed = OperationalError("vttablet: Connection Closed")
	Cancelled  = OperationalError("vttablet: Context Cancelled")
)

var (
	// TabletProtocol is exported for unit tests
	TabletProtocol = flag.String("tablet_protocol", "gorpc", "how to talk to the vttablets")
)

// ServerError represents an error that was returned from
// a vttablet server.
type ServerError struct {
	Code int
	Err  string
}

func (e *ServerError) Error() string { return e.Err }

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
// object that can communicate with a tablet.
//
// We support two modes of operation:
// 1 - using GetSessionId (right after dialing) to get a sessionId.
// 2 - using Target with each call (and never calling GetSessionId).
// If tabletType is set to UNKNOWN, we'll use mode 1.
// Mode 1 is being deprecated.
type TabletDialer func(ctx context.Context, endPoint *pbt.EndPoint, keyspace, shard string, tabletType pbt.TabletType, timeout time.Duration) (TabletConn, error)

// TabletConn defines the interface for a vttablet client. It should
// not be concurrently used across goroutines.
type TabletConn interface {
	// Execute executes a non-streaming query on vttablet.
	Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionId int64) (*mproto.QueryResult, error)

	// ExecuteBatch executes a group of queries.
	ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionId int64) (*tproto.QueryResultList, error)

	// StreamExecute executes a streaming query on vttablet. It returns a channel, ErrFunc and error.
	// If error is non-nil, it means that the StreamExecute failed to send the request. Otherwise,
	// you can pull values from the channel till it's closed. Following this, you can call ErrFunc
	// to see if the stream ended normally or due to a failure.
	StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionId int64) (<-chan *mproto.QueryResult, ErrFunc, error)

	// Transaction support
	Begin(ctx context.Context) (transactionId int64, err error)
	Commit(ctx context.Context, transactionId int64) error
	Rollback(ctx context.Context, transactionId int64) error

	// These should not be used for anything except tests for now; they will eventually
	// replace the existing methods.
	Execute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionId int64) (*mproto.QueryResult, error)
	ExecuteBatch2(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionId int64) (*tproto.QueryResultList, error)
	Begin2(ctx context.Context) (transactionId int64, err error)
	Commit2(ctx context.Context, transactionId int64) error
	Rollback2(ctx context.Context, transactionId int64) error
	StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionId int64) (<-chan *mproto.QueryResult, ErrFunc, error)

	// Close must be called for releasing resources.
	Close()

	// SetTarget can be called to change the target used for
	// subsequent calls. Can only be called if tabletType was not
	// set to UNKNOWN in TabletDialer.
	SetTarget(keyspace, shard string, tabletType pbt.TabletType) error

	// GetEndPoint returns the end point info.
	EndPoint() *pbt.EndPoint

	// SplitQuery splits a query into equally sized smaller queries by
	// appending primary key range clauses to the original query
	SplitQuery(ctx context.Context, query tproto.BoundQuery, splitColumn string, splitCount int) ([]tproto.QuerySplit, error)

	// StreamHealth streams StreamHealthResponse to the client
	StreamHealth(ctx context.Context) (<-chan *pb.StreamHealthResponse, ErrFunc, error)
}

type ErrFunc func() error

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
