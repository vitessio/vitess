// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletconn

import (
	"flag"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
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

// TabletDialer represents a function that will return a TabletConn object that can communicate with a tablet.
type TabletDialer func(endPoint topo.EndPoint, keyspace, shard string) (TabletConn, error)

// TabletConn defines the interface for a vttablet client. It should
// not be concurrently used across goroutines.
type TabletConn interface {
	// Execute executes a non-streaming query on vttablet.
	Execute(query string, bindVars map[string]interface{}, transactionId int64) (*mproto.QueryResult, error)

	// ExecuteBatch executes a group of queries.
	ExecuteBatch(queries []tproto.BoundQuery, transactionId int64) (*tproto.QueryResultList, error)

	// StreamExecute exectutes a streaming query on vttablet. It returns a channel that will stream results.
	// It also returns an ErrFunc that can be called to check if there were any errors. ErrFunc can be called
	// immediately after StreamExecute returns to check if there were errors sending the call. It should also
	// be called after finishing the iteration over the channel to see if there were other errors.
	StreamExecute(query string, bindVars map[string]interface{}, transactionId int64) (<-chan *mproto.QueryResult, ErrFunc)

	// Transaction support
	Begin() (transactionId int64, err error)
	Commit(transactionId int64) error
	Rollback(transactionId int64) error

	// Close must be called for releasing resources.
	Close()

	// GetEndPoint returns the end point info.
	EndPoint() topo.EndPoint
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
	return dialers[*tabletProtocol]
}
