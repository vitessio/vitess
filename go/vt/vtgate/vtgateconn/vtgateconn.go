// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgateconn

import (
	"flag"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var (
	// VtgateProtocol defines the RPC implementation used for connecting to vtgate.
	VtgateProtocol = flag.String("vtgate_protocol", "grpc", "how to talk to vtgate")
)

// Atomicity specifies atomicity level of a transaction.
type Atomicity int

const (
	// AtomicityMulti is the default level. It allows distributed transactions
	// with best effort commits. Partial commits are possible.
	AtomicityMulti = Atomicity(iota)
	// AtomicitySingle prevents a transaction from crossing the boundary of
	// a single database.
	AtomicitySingle
	// Atomicity2PC allows distributed transactions, and performs 2PC commits.
	Atomicity2PC
)

// WithAtomicity returns a context with the atomicity level set.
func WithAtomicity(ctx context.Context, level Atomicity) context.Context {
	return context.WithValue(ctx, Atomicity(0), level)
}

// AtomicityFromContext returns the atomicity of the context.
func AtomicityFromContext(ctx context.Context) Atomicity {
	v, _ := ctx.Value(Atomicity(0)).(Atomicity)
	return v
}

// VTGateConn is the client API object to talk to vtgate.
// It is constructed using the Dial method.
// It can be used concurrently across goroutines.
type VTGateConn struct {
	// keyspace is set at Dial time, and used as a default
	// keyspace for Execute / StreamExecute.
	keyspace string
	impl     Impl
}

// Execute executes a non-streaming query on vtgate.
// This is using v3 API.
func (conn *VTGateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	res, _, err := conn.impl.Execute(ctx, query, bindVars, conn.keyspace, tabletType, nil, options)
	return res, err
}

// ExecuteShards executes a non-streaming query for multiple shards on vtgate.
func (conn *VTGateConn) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	res, _, err := conn.impl.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, nil, options)
	return res, err
}

// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids.
func (conn *VTGateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	res, _, err := conn.impl.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, nil, options)
	return res, err
}

// ExecuteKeyRanges executes a non-streaming query on a key range.
func (conn *VTGateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	res, _, err := conn.impl.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, nil, options)
	return res, err
}

// ExecuteEntityIds executes a non-streaming query for multiple entities.
func (conn *VTGateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	res, _, err := conn.impl.ExecuteEntityIds(ctx, query, keyspace, entityColumnName, entityKeyspaceIDs, bindVars, tabletType, nil, options)
	return res, err
}

// ExecuteBatch executes a non-streaming list of queries on vtgate.
// This is using v3 API.
func (conn *VTGateConn) ExecuteBatch(ctx context.Context, queryList []string, bindVarsList []map[string]interface{}, tabletType topodatapb.TabletType, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.QueryResponse, error) {
	res, _, err := conn.impl.ExecuteBatch(ctx, queryList, bindVarsList, conn.keyspace, tabletType, asTransaction, nil, options)
	return res, err
}

// ExecuteBatchShards executes a set of non-streaming queries for multiple shards.
// If "asTransaction" is true, vtgate will automatically create a transaction
// (per shard) that encloses all the batch queries.
func (conn *VTGateConn) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	res, _, err := conn.impl.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, nil, options)
	return res, err
}

// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids.
// If "asTransaction" is true, vtgate will automatically create a transaction
// (per shard) that encloses all the batch queries.
func (conn *VTGateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	res, _, err := conn.impl.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, nil, options)
	return res, err
}

// StreamExecute executes a streaming query on vtgate. It returns a
// ResultStream and an error. First check the error. Then you can
// pull values from the ResultStream until io.EOF, or another error.
func (conn *VTGateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	return conn.impl.StreamExecute(ctx, query, bindVars, conn.keyspace, tabletType, options)
}

// StreamExecuteShards executes a streaming query on vtgate, on a set
// of shards. It returns a ResultStream and an error. First check the
// error. Then you can pull values from the ResultStream until io.EOF,
// or another error.
func (conn *VTGateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	return conn.impl.StreamExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, options)
}

// StreamExecuteKeyRanges executes a streaming query on vtgate, on a
// set of keyranges. It returns a ResultStream and an error. First check the
// error. Then you can pull values from the ResultStream until io.EOF,
// or another error.
func (conn *VTGateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	return conn.impl.StreamExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, options)
}

// StreamExecuteKeyspaceIds executes a streaming query on vtgate, for
// the given keyspaceIds.  It returns a ResultStream and an error. First check the
// error. Then you can pull values from the ResultStream until io.EOF,
// or another error.
func (conn *VTGateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	return conn.impl.StreamExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, options)
}

// ResolveTransaction resolves the 2pc transaction.
func (conn *VTGateConn) ResolveTransaction(ctx context.Context, dtid string) error {
	return conn.impl.ResolveTransaction(ctx, dtid)
}

// MessageStream streams messages.
func (conn *VTGateConn) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	return conn.impl.MessageStream(ctx, keyspace, shard, keyRange, name, callback)
}

// MessageAck acks messages.
func (conn *VTGateConn) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	return conn.impl.MessageAck(ctx, keyspace, name, ids)
}

// Begin starts a transaction and returns a VTGateTX.
func (conn *VTGateConn) Begin(ctx context.Context) (*VTGateTx, error) {
	atomicity := AtomicityFromContext(ctx)
	session, err := conn.impl.Begin(ctx, atomicity == AtomicitySingle)
	if err != nil {
		return nil, err
	}

	return &VTGateTx{
		conn:      conn,
		session:   session,
		atomicity: atomicity,
	}, nil
}

// Close must be called for releasing resources.
func (conn *VTGateConn) Close() {
	conn.impl.Close()
	conn.impl = nil
}

// SplitQuery splits a query into smaller queries. It is mostly used by batch job frameworks
// such as MapReduce. See the documentation for the vtgate.SplitQueryRequest protocol buffer message
// in 'proto/vtgate.proto'.
func (conn *VTGateConn) SplitQuery(
	ctx context.Context,
	keyspace string,
	query string,
	bindVars map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]*vtgatepb.SplitQueryResponse_Part, error) {

	return conn.impl.SplitQuery(
		ctx, keyspace, query, bindVars, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
}

// GetSrvKeyspace returns a topo.SrvKeyspace object.
func (conn *VTGateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return conn.impl.GetSrvKeyspace(ctx, keyspace)
}

// UpdateStreamReader is returned by UpdateStream.
type UpdateStreamReader interface {
	// Recv returns the next result on the stream.
	// It will return io.EOF if the stream ended.
	Recv() (*querypb.StreamEvent, int64, error)
}

// UpdateStream executes a streaming query on vtgate. It returns an
// UpdateStreamReader and an error. First check the error. Then you
// can pull values from the UpdateStreamReader until io.EOF, or
// another error.
func (conn *VTGateConn) UpdateStream(ctx context.Context, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken) (UpdateStreamReader, error) {
	return conn.impl.UpdateStream(ctx, conn.keyspace, shard, keyRange, tabletType, timestamp, event)
}

// VTGateTx defines an ongoing transaction.
// It should not be concurrently used across goroutines.
type VTGateTx struct {
	conn      *VTGateConn
	session   interface{}
	atomicity Atomicity
}

// Execute executes a query on vtgate within the current transaction.
func (tx *VTGateTx) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("execute: not in transaction")
	}
	res, session, err := tx.conn.impl.Execute(ctx, query, bindVars, tx.conn.keyspace, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteShards executes a query for multiple shards on vtgate within the current transaction.
func (tx *VTGateTx) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeShards: not in transaction")
	}
	res, session, err := tx.conn.impl.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids.
func (tx *VTGateTx) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeKeyspaceIds: not in transaction")
	}
	res, session, err := tx.conn.impl.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteKeyRanges executes a non-streaming query on a key range.
func (tx *VTGateTx) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeKeyRanges: not in transaction")
	}
	res, session, err := tx.conn.impl.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteEntityIds executes a non-streaming query for multiple entities.
func (tx *VTGateTx) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeEntityIds: not in transaction")
	}
	res, session, err := tx.conn.impl.ExecuteEntityIds(ctx, query, keyspace, entityColumnName, entityKeyspaceIDs, bindVars, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteBatch executes a list of queries on vtgate within the current transaction.
func (tx *VTGateTx) ExecuteBatch(ctx context.Context, query []string, bindVars []map[string]interface{}, tabletType topodatapb.TabletType, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.QueryResponse, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("execute: not in transaction")
	}
	res, session, errs := tx.conn.impl.ExecuteBatch(ctx, query, bindVars, tx.conn.keyspace, tabletType, asTransaction, tx.session, options)
	tx.session = session
	return res, errs
}

// ExecuteBatchShards executes a set of non-streaming queries for multiple shards.
func (tx *VTGateTx) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeBatchShards: not in transaction")
	}
	res, session, err := tx.conn.impl.ExecuteBatchShards(ctx, queries, tabletType, false /* asTransaction */, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids.
func (tx *VTGateTx) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeBatchKeyspaceIds: not in transaction")
	}
	res, session, err := tx.conn.impl.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, false /* asTransaction */, tx.session, options)
	tx.session = session
	return res, err
}

// Commit commits the current transaction.
func (tx *VTGateTx) Commit(ctx context.Context) error {
	if tx.session == nil {
		return fmt.Errorf("commit: not in transaction")
	}
	err := tx.conn.impl.Commit(ctx, tx.session, tx.atomicity == Atomicity2PC)
	tx.session = nil
	return err
}

// Rollback rolls back the current transaction.
func (tx *VTGateTx) Rollback(ctx context.Context) error {
	if tx.session == nil {
		return nil
	}
	err := tx.conn.impl.Rollback(ctx, tx.session)
	tx.session = nil
	return err
}

//
// The rest of this file is for the protocol implementations.
//

// Impl defines the interface for a vtgate client protocol
// implementation. It can be used concurrently across goroutines.
type Impl interface {
	// Execute executes a non-streaming query on vtgate.
	Execute(ctx context.Context, query string, bindVars map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error)

	// ExecuteShards executes a non-streaming query for multiple shards on vtgate.
	ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error)

	// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids.
	ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error)

	// ExecuteKeyRanges executes a non-streaming query on a key range.
	ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error)

	// ExecuteEntityIds executes a non-streaming query for multiple entities.
	ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session interface{}, options *querypb.ExecuteOptions) (*sqltypes.Result, interface{}, error)

	// ExecuteBatch executes a non-streaming queries on vtgate.
	ExecuteBatch(ctx context.Context, queryList []string, bindVarsList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, asTransaction bool, session interface{}, options *querypb.ExecuteOptions) ([]sqltypes.QueryResponse, interface{}, error)

	// ExecuteBatchShards executes a set of non-streaming queries for multiple shards.
	ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session interface{}, options *querypb.ExecuteOptions) ([]sqltypes.Result, interface{}, error)

	// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids.
	ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session interface{}, options *querypb.ExecuteOptions) ([]sqltypes.Result, interface{}, error)

	// StreamExecute executes a streaming query on vtgate.
	StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error)

	// StreamExecuteShards executes a streaming query on vtgate, on a set of shards.
	StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error)

	// StreamExecuteKeyRanges executes a streaming query on vtgate, on a set of keyranges.
	StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error)

	// StreamExecuteKeyspaceIds executes a streaming query on vtgate, for the given keyspaceIds.
	StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error)

	// Begin starts a transaction and returns a VTGateTX.
	Begin(ctx context.Context, singledb bool) (interface{}, error)
	// Commit commits the current transaction.
	Commit(ctx context.Context, session interface{}, twopc bool) error
	// Rollback rolls back the current transaction.
	Rollback(ctx context.Context, session interface{}) error
	// ResolveTransaction resolves the specified 2pc transaction.
	ResolveTransaction(ctx context.Context, dtid string) error

	// Messaging functions.
	MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error
	MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error)

	// SplitQuery splits a query into smaller queries. It is mostly used by batch job frameworks
	// such as MapReduce. See the documentation for the vtgate.SplitQueryRequest protocol buffer
	// message in 'proto/vtgate.proto'.
	SplitQuery(
		ctx context.Context,
		keyspace string,
		query string,
		bindVars map[string]interface{},
		splitColumns []string,
		splitCount int64,
		numRowsPerQueryPart int64,
		algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error)

	// GetSrvKeyspace returns a topo.SrvKeyspace.
	GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error)

	// UpdateStream asks for a stream of StreamEvent.
	UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken) (UpdateStreamReader, error)

	// Close must be called for releasing resources.
	Close()
}

// DialerFunc represents a function that will return an Impl
// object that can communicate with a VTGate.
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
func DialProtocol(ctx context.Context, protocol string, address string, timeout time.Duration, keyspace string) (*VTGateConn, error) {
	dialer, ok := dialers[protocol]
	if !ok {
		return nil, fmt.Errorf("no dialer registered for VTGate protocol %s", protocol)
	}
	impl, err := dialer(ctx, address, timeout)
	if err != nil {
		return nil, err
	}
	return &VTGateConn{
		keyspace: keyspace,
		impl:     impl,
	}, nil
}

// Dial dials using the command-line specified protocol, and returns
// the *VTGateConn.
func Dial(ctx context.Context, address string, timeout time.Duration, keyspace string) (*VTGateConn, error) {
	return DialProtocol(ctx, *VtgateProtocol, address, timeout, keyspace)
}
