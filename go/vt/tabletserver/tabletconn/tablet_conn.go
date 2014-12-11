// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletconn

import (
	"flag"
	"time"

	"code.google.com/p/go.net/context"
	log "github.com/golang/glog"
	mproto "github.com/henryanand/vitess/go/mysql/proto"
	tproto "github.com/henryanand/vitess/go/vt/tabletserver/proto"
	"github.com/henryanand/vitess/go/vt/topo"
)

const (
	ERR_NORMAL = iota
	ERR_RETRY
	ERR_FATAL
	ERR_TX_POOL_FULL
	ERR_NOT_IN_TX
)

const (
	CONN_CLOSED = OperationalError("vttablet: Connection Closed")
)

var (
	tabletProtocol = flag.String("tablet_protocol", "gorpc", "how to talk to the vttablets")
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

// TabletDialer represents a function that will return a TabletConn object that can communicate with a tablet.
type TabletDialer func(context context.Context, endPoint topo.EndPoint, keyspace, shard string, timeout time.Duration) (TabletConn, error)

// TabletConn defines the interface for a vttablet client. It should
// not be concurrently used across goroutines.
type TabletConn interface {
	// Execute executes a non-streaming query on vttablet.
	Execute(context context.Context, query string, bindVars map[string]interface{}, transactionId int64) (*mproto.QueryResult, error)

	// ExecuteBatch executes a group of queries.
	ExecuteBatch(context context.Context, queries []tproto.BoundQuery, transactionId int64) (*tproto.QueryResultList, error)

	// StreamExecute executes a streaming query on vttablet. It returns a channel, ErrFunc and error.
	// If error is non-nil, it means that the StreamExecute failed to send the request. Otherwise,
	// you can pull values from the channel till it's closed. Following this, you can call ErrFunc
	// to see if the stream ended normally or due to a failure.
	StreamExecute(context context.Context, query string, bindVars map[string]interface{}, transactionId int64) (<-chan *mproto.QueryResult, ErrFunc, error)

	// Transaction support
	Begin(context context.Context) (transactionId int64, err error)
	Commit(context context.Context, transactionId int64) error
	Rollback(context context.Context, transactionId int64) error

	// Close must be called for releasing resources.
	Close()

	// GetEndPoint returns the end point info.
	EndPoint() topo.EndPoint

	// SplitQuery splits a query into equally sized smaller queries by
	// appending primary key range clauses to the original query
	SplitQuery(context context.Context, query tproto.BoundQuery, splitCount int) ([]tproto.QuerySplit, error)
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
	td, ok := dialers[*tabletProtocol]
	if !ok {
		log.Fatalf("No dialer registered for tablet protocol %s", *tabletProtocol)
	}
	return td
}
