// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgateconn

import (
	"flag"
	"fmt"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"
)

const (
	// GoRPCProtocol is a vtgate protocol based on go rpc
	GoRPCProtocol = "gorpc"
)

var (
	vtgateProtocol = flag.String("vtgate_protocol", GoRPCProtocol, "how to talk to vtgate")
)

// ServerError represents an error that was returned from
// a vtgate server.
type ServerError struct {
	Code int
	Err  string
}

func (e *ServerError) Error() string { return e.Err }

// OperationalError represents an error due to a failure to
// communicate with vtgate.
type OperationalError string

func (e OperationalError) Error() string { return string(e) }

// VTGateConn defines the interface for a vtgate client.
// It can be used concurrently across goroutines.
type VTGateConn struct {
	impl Impl
}

// Execute executes a non-streaming query on vtgate.
func (conn *VTGateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	res, _, err := conn.impl.Execute(ctx, query, bindVars, tabletType, nil)
	return res, err
}

// ExecuteShard executes a non-streaming query for multiple shards on vtgate.
func (conn *VTGateConn) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	res, _, err := conn.impl.ExecuteShard(ctx, query, keyspace, shards, bindVars, tabletType, nil)
	return res, err
}

// StreamExecute executes a streaming query on vtgate. It returns a channel, ErrFunc and error.
// If error is non-nil, it means that the StreamExecute failed to send the request. Otherwise,
// you can pull values from the channel till it's closed. Following this, you can call ErrFunc
// to see if the stream ended normally or due to a failure.
func (conn *VTGateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, ErrFunc) {
	return conn.impl.StreamExecute(ctx, query, bindVars, tabletType)
}

// Begin starts a transaction and returns a VTGateTX.
func (conn *VTGateConn) Begin(ctx context.Context) (*VTGateTx, error) {
	session, err := conn.impl.Begin(ctx)
	if err != nil {
		return nil, err
	}

	return &VTGateTx{
		impl:    conn.impl,
		session: session,
	}, nil
}

// Close must be called for releasing resources.
func (conn *VTGateConn) Close() {
	conn.impl.Close()
	conn.impl = nil
}

// SplitQuery splits a query into equally sized smaller queries by
// appending primary key range clauses to the original query
func (conn *VTGateConn) SplitQuery(ctx context.Context, keyspace string, query tproto.BoundQuery, splitCount int) ([]proto.SplitQueryPart, error) {
	return conn.impl.SplitQuery(ctx, keyspace, query, splitCount)
}

// VTGateTx defines an ongoing transaction.
// It should not be concurrently used across goroutines.
type VTGateTx struct {
	impl    Impl
	session interface{}
}

// Execute executes a query on vtgate within the current transaction.
func (tx *VTGateTx) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("execute: not in transaction")
	}
	res, session, err := tx.impl.Execute(ctx, query, bindVars, tabletType, tx.session)
	tx.session = session
	return res, err
}

// ExecuteShard executes a query for multiple shards on vtgate within the current transaction.
func (tx *VTGateTx) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeShard: not in transaction")
	}
	res, session, err := tx.impl.ExecuteShard(ctx, query, keyspace, shards, bindVars, tabletType, tx.session)
	tx.session = session
	return res, err
}

// Commit commits the current transaction.
func (tx *VTGateTx) Commit(ctx context.Context) error {
	if tx.session == nil {
		return fmt.Errorf("commit: not in transaction")
	}
	err := tx.impl.Commit(ctx, tx.session)
	tx.session = nil
	return err
}

// Rollback rolls back the current transaction.
func (tx *VTGateTx) Rollback(ctx context.Context) error {
	if tx.session == nil {
		return nil
	}
	err := tx.impl.Rollback(ctx, tx.session)
	tx.session = nil
	return err
}

// ErrFunc is used to check for streaming errors.
type ErrFunc func() error

//
// The rest of this file is for the protocol implementations.
//

// Impl defines the interface for a vtgate client protocol
// implementation. It can be used concurrently across goroutines.
type Impl interface {
	// Execute executes a non-streaming query on vtgate.
	Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType, session interface{}) (*mproto.QueryResult, interface{}, error)

	// ExecuteShard executes a non-streaming query for multiple shards on vtgate.
	ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType, session interface{}) (*mproto.QueryResult, interface{}, error)

	// StreamExecute executes a streaming query on vtgate. It returns a channel, ErrFunc and error.
	// If error is non-nil, it means that the StreamExecute failed to send the request. Otherwise,
	// you can pull values from the channel till it's closed. Following this, you can call ErrFunc
	// to see if the stream ended normally or due to a failure.
	StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, ErrFunc)

	// Begin starts a transaction and returns a VTGateTX.
	Begin(ctx context.Context) (interface{}, error)

	// Commit commits the current transaction.
	Commit(ctx context.Context, session interface{}) error

	// Rollback rolls back the current transaction.
	Rollback(ctx context.Context, session interface{}) error

	// SplitQuery splits a query into equally sized smaller queries by
	// appending primary key range clauses to the original query
	SplitQuery(ctx context.Context, keyspace string, query tproto.BoundQuery, splitCount int) ([]proto.SplitQueryPart, error)

	// Close must be called for releasing resources.
	Close()
}

// DialerFunc represents a function that will return a VTGateConn object that can communicate with a VTGate.
type DialerFunc func(ctx context.Context, address string, timeout time.Duration) (Impl, error)

var dialers = make(map[string]DialerFunc)

// RegisterDialer is meant to be used by Dialer implementations
// to self register.
func RegisterDialer(name string, dialer DialerFunc) {
	if _, ok := dialers[name]; ok {
		log.Warningf("Dialer %s already exists", name)
		return
	}
	dialers[name] = dialer
}

// DialProtocol dials a specific protocol, and returns the *VTGateConn
func DialProtocol(ctx context.Context, protocol string, address string, timeout time.Duration) (*VTGateConn, error) {
	dialer, ok := dialers[protocol]
	if !ok {
		return nil, fmt.Errorf("no dialer registered for VTGate protocol %s", protocol)
	}
	impl, err := dialer(ctx, address, timeout)
	if err != nil {
		return nil, err
	}
	return &VTGateConn{
		impl: impl,
	}, nil
}

// Dial dials using the command-line specified protocol, and returns
// the *VTGateConn.
func Dial(ctx context.Context, address string, timeout time.Duration) (*VTGateConn, error) {
	return DialProtocol(ctx, *vtgateProtocol, address, timeout)
}
