// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
)

const (
	ERR_NORMAL = iota
	ERR_RETRY
	ERR_FATAL
	ERR_TX_POOL_FULL
	ERR_NOT_IN_TX
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
type TabletDialer func(addr, keyspace, shard, username, password string, encrypted bool) (TabletConn, error)

// TabletConn defines the interface for a vttablet client. It should
// not be concurrently used across goroutines.
type TabletConn interface {
	// Execute executes a non-streaming query on vttablet.
	Execute(query string, bindVars map[string]interface{}) (*mproto.QueryResult, error)

	// ExecuteBatch executes a group of queries.
	ExecuteBatch(queries []tproto.BoundQuery) (*tproto.QueryResultList, error)

	// StreamExecute exectutes a streaming query on vttablet. It returns a channel that will stream results.
	// It also returns an ErrFunc that can be called to check if there were any errors. ErrFunc can be called
	// immediately after StreamExecute returns to check if there were errors sending the call. It should also
	// be called after finishing the iteration over the channel to see if there were other errors.
	StreamExecute(query string, bindVars map[string]interface{}) (<-chan *mproto.QueryResult, ErrFunc)

	// Transaction support
	Begin() error
	Commit() error
	Rollback() error
	// TransactionId returns 0 if there is no transaction.
	TransactionId() int64

	// Close must be called for releasing resources.
	Close() error
}

type ErrFunc func() error

var dialers = make(map[string]TabletDialer)

func RegisterDialer(name string, dialer TabletDialer) {
	if _, ok := dialers[name]; ok {
		log.Fatalf("Dialer %s already exists", name)
	}
	dialers[name] = dialer
}

func GetDialer(name string) TabletDialer {
	return dialers[name]
}
