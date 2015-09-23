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
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

const (
	// GoRPCProtocol is a vtgate protocol based on go rpc
	GoRPCProtocol = "gorpc"
)

var (
	vtgateProtocol = flag.String("vtgate_protocol", GoRPCProtocol, "how to talk to vtgate")
)

// VTGateConn is the client API object to talk to vtgate.
// It is constructed using the Dial method.
// It can be used concurrently across goroutines.
type VTGateConn struct {
	impl Impl
}

// Execute executes a non-streaming query on vtgate.
// This is using v3 API.
func (conn *VTGateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType) (*mproto.QueryResult, error) {
	res, _, err := conn.impl.Execute(ctx, query, bindVars, tabletType, false, nil)
	return res, err
}

// ExecuteShards executes a non-streaming query for multiple shards on vtgate.
func (conn *VTGateConn) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType) (*mproto.QueryResult, error) {
	res, _, err := conn.impl.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, false, nil)
	return res, err
}

// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids.
func (conn *VTGateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType) (*mproto.QueryResult, error) {
	res, _, err := conn.impl.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, false, nil)
	return res, err
}

// ExecuteKeyRanges executes a non-streaming query on a key range.
func (conn *VTGateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType) (*mproto.QueryResult, error) {
	res, _, err := conn.impl.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, false, nil)
	return res, err
}

// ExecuteEntityIds executes a non-streaming query for multiple entities.
func (conn *VTGateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType pb.TabletType) (*mproto.QueryResult, error) {
	res, _, err := conn.impl.ExecuteEntityIds(ctx, query, keyspace, entityColumnName, entityKeyspaceIDs, bindVars, tabletType, false, nil)
	return res, err
}

// ExecuteBatchShards executes a set of non-streaming queries for multiple shards.
func (conn *VTGateConn) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool) ([]mproto.QueryResult, error) {
	res, _, err := conn.impl.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, nil)
	return res, err
}

// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids.
func (conn *VTGateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool) ([]mproto.QueryResult, error) {
	res, _, err := conn.impl.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, nil)
	return res, err
}

// StreamExecute executes a streaming query on vtgate. It returns a
// channel, an ErrFunc, and error. First check the error. Then you can
// pull values from the channel till it's closed. Following this, you
// can call ErrFunc to see if the stream ended normally or due to a
// failure.
func (conn *VTGateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error) {
	return conn.impl.StreamExecute(ctx, query, bindVars, tabletType)
}

// StreamExecute2 executes a streaming query on vtgate. It returns a
// channel, an ErrFunc, and error. First check the error. Then you can
// pull values from the channel till it's closed. Following this, you
// can call ErrFunc to see if the stream ended normally or due to a
// failure.
func (conn *VTGateConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error) {
	return conn.impl.StreamExecute2(ctx, query, bindVars, tabletType)
}

// StreamExecuteShards executes a streaming query on vtgate, on a set
// of shards.  It returns a channel, an ErrFunc, and error. First
// check the error. Then you can pull values from the channel till
// it's closed. Following this, you can call ErrFunc to see if the
// stream ended normally or due to a failure.
func (conn *VTGateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error) {
	return conn.impl.StreamExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType)
}

// StreamExecuteShards2 executes a streaming query on vtgate, on a set
// of shards.  It returns a channel, an ErrFunc, and error. First
// check the error. Then you can pull values from the channel till
// it's closed. Following this, you can call ErrFunc to see if the
// stream ended normally or due to a failure.
func (conn *VTGateConn) StreamExecuteShards2(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error) {
	return conn.impl.StreamExecuteShards2(ctx, query, keyspace, shards, bindVars, tabletType)
}

// StreamExecuteKeyRanges executes a streaming query on vtgate, on a
// set of keyranges.  It returns a channel, an ErrFunc, and
// error. First check the error. Then you can pull values from the
// channel till it's closed. Following this, you can call ErrFunc to
// see if the stream ended normally or due to a failure.
func (conn *VTGateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error) {
	return conn.impl.StreamExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType)
}

// StreamExecuteKeyRanges2 executes a streaming query on vtgate, on a
// set of keyranges.  It returns a channel, an ErrFunc, and
// error. First check the error. Then you can pull values from the
// channel till it's closed. Following this, you can call ErrFunc to
// see if the stream ended normally or due to a failure.
func (conn *VTGateConn) StreamExecuteKeyRanges2(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error) {
	return conn.impl.StreamExecuteKeyRanges2(ctx, query, keyspace, keyRanges, bindVars, tabletType)
}

// StreamExecuteKeyspaceIds executes a streaming query on vtgate, for
// the given keyspaceIds.  It returns a channel, an ErrFunc, and
// error. First check the error. Then you can pull values from the
// channel till it's closed. Following this, you can call ErrFunc to
// see if the stream ended normally or due to a failure.
func (conn *VTGateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error) {
	return conn.impl.StreamExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType)
}

// StreamExecuteKeyspaceIds2 executes a streaming query on vtgate, for
// the given keyspaceIds.  It returns a channel, an ErrFunc, and
// error. First check the error. Then you can pull values from the
// channel till it's closed. Following this, you can call ErrFunc to
// see if the stream ended normally or due to a failure.
func (conn *VTGateConn) StreamExecuteKeyspaceIds2(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error) {
	return conn.impl.StreamExecuteKeyspaceIds2(ctx, query, keyspace, keyspaceIds, bindVars, tabletType)
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

// Begin2 starts a transaction and returns a VTGateTX.
func (conn *VTGateConn) Begin2(ctx context.Context) (*VTGateTx, error) {
	session, err := conn.impl.Begin2(ctx)
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
func (conn *VTGateConn) SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]interface{}, splitColumn string, splitCount int) ([]*pbg.SplitQueryResponse_Part, error) {
	return conn.impl.SplitQuery(ctx, keyspace, query, bindVars, splitColumn, splitCount)
}

// GetSrvKeyspace returns a topo.SrvKeyspace object.
func (conn *VTGateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*pb.SrvKeyspace, error) {
	return conn.impl.GetSrvKeyspace(ctx, keyspace)
}

// VTGateTx defines an ongoing transaction.
// It should not be concurrently used across goroutines.
type VTGateTx struct {
	impl    Impl
	session interface{}
}

// Execute executes a query on vtgate within the current transaction.
func (tx *VTGateTx) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("execute: not in transaction")
	}
	res, session, err := tx.impl.Execute(ctx, query, bindVars, tabletType, notInTransaction, tx.session)
	tx.session = session
	return res, err
}

// ExecuteShards executes a query for multiple shards on vtgate within the current transaction.
func (tx *VTGateTx) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeShards: not in transaction")
	}
	res, session, err := tx.impl.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, notInTransaction, tx.session)
	tx.session = session
	return res, err
}

// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids.
func (tx *VTGateTx) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeKeyspaceIds: not in transaction")
	}
	res, session, err := tx.impl.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, notInTransaction, tx.session)
	tx.session = session
	return res, err
}

// ExecuteKeyRanges executes a non-streaming query on a key range.
func (tx *VTGateTx) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeKeyRanges: not in transaction")
	}
	res, session, err := tx.impl.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, notInTransaction, tx.session)
	tx.session = session
	return res, err
}

// ExecuteEntityIds executes a non-streaming query for multiple entities.
func (tx *VTGateTx) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeEntityIds: not in transaction")
	}
	res, session, err := tx.impl.ExecuteEntityIds(ctx, query, keyspace, entityColumnName, entityKeyspaceIDs, bindVars, tabletType, notInTransaction, tx.session)
	tx.session = session
	return res, err
}

// ExecuteBatchShards executes a set of non-streaming queries for multiple shards.
func (tx *VTGateTx) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool) ([]mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeBatchShards: not in transaction")
	}
	res, session, err := tx.impl.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, tx.session)
	tx.session = session
	return res, err
}

// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids.
func (tx *VTGateTx) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool) ([]mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeBatchKeyspaceIds: not in transaction")
	}
	res, session, err := tx.impl.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, tx.session)
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

// Commit2 commits the current transaction.
func (tx *VTGateTx) Commit2(ctx context.Context) error {
	if tx.session == nil {
		return fmt.Errorf("commit: not in transaction")
	}
	err := tx.impl.Commit2(ctx, tx.session)
	tx.session = nil
	return err
}

// Rollback2 rolls back the current transaction.
func (tx *VTGateTx) Rollback2(ctx context.Context) error {
	if tx.session == nil {
		return nil
	}
	err := tx.impl.Rollback2(ctx, tx.session)
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
	Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error)

	// ExecuteShards executes a non-streaming query for multiple shards on vtgate.
	ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error)

	// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids.
	ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error)

	// ExecuteKeyRanges executes a non-streaming query on a key range.
	ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error)

	// ExecuteEntityIds executes a non-streaming query for multiple entities.
	ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error)

	// ExecuteBatchShards executes a set of non-streaming queries for multiple shards.
	ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error)

	// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids.
	ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error)

	// StreamExecute executes a streaming query on vtgate.
	StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error)

	// StreamExecute2 executes a streaming query on vtgate.
	StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error)

	// StreamExecuteShards executes a streaming query on vtgate, on a set of shards.
	StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error)

	// StreamExecuteShards2 executes a streaming query on vtgate, on a set of shards.
	StreamExecuteShards2(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error)

	// StreamExecuteKeyRanges executes a streaming query on vtgate, on a set of keyranges.
	StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error)

	// StreamExecuteKeyRanges2 executes a streaming query on vtgate, on a set of keyranges.
	StreamExecuteKeyRanges2(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error)

	// StreamExecuteKeyspaceIds executes a streaming query on vtgate, for the given keyspaceIds.
	StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error)

	// StreamExecuteKeyspaceIds2 executes a streaming query on vtgate, for the given keyspaceIds.
	StreamExecuteKeyspaceIds2(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, ErrFunc, error)

	// Begin starts a transaction and returns a VTGateTX.
	Begin(ctx context.Context) (interface{}, error)

	// Commit commits the current transaction.
	Commit(ctx context.Context, session interface{}) error

	// Rollback rolls back the current transaction.
	Rollback(ctx context.Context, session interface{}) error

	// New methods (that don't quite work yet) which will eventually replace the existing ones:

	// Begin starts a transaction and returns a VTGateTX.
	Begin2(ctx context.Context) (interface{}, error)
	// Commit commits the current transaction.
	Commit2(ctx context.Context, session interface{}) error
	// Rollback rolls back the current transaction.
	Rollback2(ctx context.Context, session interface{}) error

	// SplitQuery splits a query into equally sized smaller queries by
	// appending primary key range clauses to the original query.
	SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]interface{}, splitColumn string, splitCount int) ([]*pbg.SplitQueryResponse_Part, error)

	// GetSrvKeyspace returns a topo.SrvKeyspace.
	GetSrvKeyspace(ctx context.Context, keyspace string) (*pb.SrvKeyspace, error)

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
		log.Warningf("Dialer %s already exists, overwriting it", name)
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
