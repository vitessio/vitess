/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgateconn

import (
	"flag"
	"fmt"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

var (
	// VtgateProtocol defines the RPC implementation used for connecting to vtgate.
	VtgateProtocol = flag.String("vtgate_protocol", "grpc", "how to talk to vtgate")
)

// VTGateConn is the client API object to talk to vtgate.
// It is constructed using the Dial method. It supports
// legacy V2 APIs. It can be used concurrently. To access
// V3 functionality, use the Session function to create a
// VTGateSession objects.
type VTGateConn struct {
	impl Impl
}

// Session returns a VTGateSession that can be used to access V3 functions.
func (conn *VTGateConn) Session(targetString string, options *querypb.ExecuteOptions) *VTGateSession {
	return &VTGateSession{
		session: &vtgatepb.Session{
			TargetString: targetString,
			Options:      options,
			Autocommit:   true,
		},
		impl: conn.impl,
	}
}

// ExecuteShards executes a non-streaming query for multiple shards on vtgate.
func (conn *VTGateConn) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	_, res, err := conn.impl.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, nil, options)
	return res, err
}

// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids.
func (conn *VTGateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	_, res, err := conn.impl.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, nil, options)
	return res, err
}

// ExecuteKeyRanges executes a non-streaming query on a key range.
func (conn *VTGateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	_, res, err := conn.impl.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, nil, options)
	return res, err
}

// ExecuteEntityIds executes a non-streaming query for multiple entities.
func (conn *VTGateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	_, res, err := conn.impl.ExecuteEntityIds(ctx, query, keyspace, entityColumnName, entityKeyspaceIDs, bindVars, tabletType, nil, options)
	return res, err
}

// ExecuteBatchShards executes a set of non-streaming queries for multiple shards.
// If "asTransaction" is true, vtgate will automatically create a transaction
// (per shard) that encloses all the batch queries.
func (conn *VTGateConn) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	_, res, err := conn.impl.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, nil, options)
	return res, err
}

// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids.
// If "asTransaction" is true, vtgate will automatically create a transaction
// (per shard) that encloses all the batch queries.
func (conn *VTGateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	_, res, err := conn.impl.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, nil, options)
	return res, err
}

// StreamExecuteShards executes a streaming query on vtgate, on a set
// of shards. It returns a ResultStream and an error. First check the
// error. Then you can pull values from the ResultStream until io.EOF,
// or another error.
func (conn *VTGateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	return conn.impl.StreamExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, options)
}

// StreamExecuteKeyRanges executes a streaming query on vtgate, on a
// set of keyranges. It returns a ResultStream and an error. First check the
// error. Then you can pull values from the ResultStream until io.EOF,
// or another error.
func (conn *VTGateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	return conn.impl.StreamExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, options)
}

// StreamExecuteKeyspaceIds executes a streaming query on vtgate, for
// the given keyspaceIds.  It returns a ResultStream and an error. First check the
// error. Then you can pull values from the ResultStream until io.EOF,
// or another error.
func (conn *VTGateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
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

// MessageAckKeyspaceIds is part of the vtgate service API. It routes
// message acks based on the associated keyspace ids.
func (conn *VTGateConn) MessageAckKeyspaceIds(ctx context.Context, keyspace string, name string, idKeyspaceIDs []*vtgatepb.IdKeyspaceId) (int64, error) {
	return conn.impl.MessageAckKeyspaceIds(ctx, keyspace, name, idKeyspaceIDs)
}

// Begin starts a transaction and returns a VTGateTX.
func (conn *VTGateConn) Begin(ctx context.Context) (*VTGateTx, error) {
	session, err := conn.impl.Begin(ctx, false /* singledb */)
	if err != nil {
		return nil, err
	}

	return &VTGateTx{
		conn:    conn,
		session: session,
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
func (conn *VTGateConn) SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]*querypb.BindVariable, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	return conn.impl.SplitQuery(ctx, keyspace, query, bindVars, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
}

// GetSrvKeyspace returns a topo.SrvKeyspace object.
func (conn *VTGateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return conn.impl.GetSrvKeyspace(ctx, keyspace)
}

// VStreamReader is returned by VStream.
type VStreamReader interface {
	// Recv returns the next result on the stream.
	// It will return io.EOF if the stream ended.
	Recv() ([]*binlogdatapb.VEvent, error)
}

// VStream streams binlog events.
func (conn *VTGateConn) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter) (VStreamReader, error) {
	return conn.impl.VStream(ctx, tabletType, vgtid, filter)
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
func (conn *VTGateConn) UpdateStream(ctx context.Context, keyspace, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken) (UpdateStreamReader, error) {
	return conn.impl.UpdateStream(ctx, keyspace, shard, keyRange, tabletType, timestamp, event)
}

// VTGateSession exposes the V3 API to the clients.
// The object maintains client-side state and is comparable to a native MySQL connection.
// For example, if you enable autocommit on a Session object, all subsequent calls will respect this.
// Functions within an object must not be called concurrently.
// You can create as many objects as you want.
// All of them will share the underlying connection to vtgate ("VTGateConn" object).
type VTGateSession struct {
	session *vtgatepb.Session
	impl    Impl
}

// Execute performs a VTGate Execute.
func (sn *VTGateSession) Execute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	session, res, err := sn.impl.Execute(ctx, sn.session, query, bindVars)
	sn.session = session
	return res, err
}

// ExecuteBatch executes a list of queries on vtgate within the current transaction.
func (sn *VTGateSession) ExecuteBatch(ctx context.Context, query []string, bindVars []map[string]*querypb.BindVariable) ([]sqltypes.QueryResponse, error) {
	session, res, errs := sn.impl.ExecuteBatch(ctx, sn.session, query, bindVars)
	sn.session = session
	return res, errs
}

// StreamExecute executes a streaming query on vtgate.
// It returns a ResultStream and an error. First check the
// error. Then you can pull values from the ResultStream until io.EOF,
// or another error.
func (sn *VTGateSession) StreamExecute(ctx context.Context, query string, bindVars map[string]*querypb.BindVariable) (sqltypes.ResultStream, error) {
	// StreamExecute is only used for SELECT queries that don't change
	// the session. So, the protocol doesn't return an updated session.
	// This may change in the future.
	return sn.impl.StreamExecute(ctx, sn.session, query, bindVars)
}

// VTGateTx defines an ongoing transaction.
// It should not be concurrently used across goroutines.
type VTGateTx struct {
	conn    *VTGateConn
	session *vtgatepb.Session
}

// ExecuteShards executes a query for multiple shards on vtgate within the current transaction.
func (tx *VTGateTx) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeShards: not in transaction")
	}
	session, res, err := tx.conn.impl.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids.
func (tx *VTGateTx) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeKeyspaceIds: not in transaction")
	}
	session, res, err := tx.conn.impl.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteKeyRanges executes a non-streaming query on a key range.
func (tx *VTGateTx) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeKeyRanges: not in transaction")
	}
	session, res, err := tx.conn.impl.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteEntityIds executes a non-streaming query for multiple entities.
func (tx *VTGateTx) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeEntityIds: not in transaction")
	}
	session, res, err := tx.conn.impl.ExecuteEntityIds(ctx, query, keyspace, entityColumnName, entityKeyspaceIDs, bindVars, tabletType, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteBatchShards executes a set of non-streaming queries for multiple shards.
func (tx *VTGateTx) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeBatchShards: not in transaction")
	}
	session, res, err := tx.conn.impl.ExecuteBatchShards(ctx, queries, tabletType, false /* asTransaction */, tx.session, options)
	tx.session = session
	return res, err
}

// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids.
func (tx *VTGateTx) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if tx.session == nil {
		return nil, fmt.Errorf("executeBatchKeyspaceIds: not in transaction")
	}
	session, res, err := tx.conn.impl.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, false /* asTransaction */, tx.session, options)
	tx.session = session
	return res, err
}

// Commit commits the current transaction.
func (tx *VTGateTx) Commit(ctx context.Context) error {
	if tx.session == nil {
		return fmt.Errorf("commit: not in transaction")
	}
	err := tx.conn.impl.Commit(ctx, tx.session, false /* twopc */)
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
	// Execute executes a non-streaming query on vtgate. This is a V3 function.
	Execute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error)

	// ExecuteBatch executes a non-streaming queries on vtgate. This is a V3 function.
	ExecuteBatch(ctx context.Context, session *vtgatepb.Session, queryList []string, bindVarsList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error)

	// StreamExecute executes a streaming query on vtgate. This is a V3 function.
	StreamExecute(ctx context.Context, session *vtgatepb.Session, query string, bindVars map[string]*querypb.BindVariable) (sqltypes.ResultStream, error)

	// ExecuteShards executes a non-streaming query for multiple shards on vtgate. This is a legacy function.
	ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error)

	// ExecuteKeyspaceIds executes a non-streaming query for multiple keyspace_ids. This is a legacy function.
	ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error)

	// ExecuteKeyRanges executes a non-streaming query on a key range. This is a legacy function.
	ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error)

	// ExecuteEntityIds executes a non-streaming query for multiple entities. This is a legacy function.
	ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error)

	// ExecuteBatchShards executes a set of non-streaming queries for multiple shards. This is a legacy function.
	ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.Result, error)

	// ExecuteBatchKeyspaceIds executes a set of non-streaming queries for multiple keyspace ids. This is a legacy function.
	ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.Result, error)

	// StreamExecuteShards executes a streaming query on vtgate, on a set of shards. This is a legacy function.
	StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error)

	// StreamExecuteKeyRanges executes a streaming query on vtgate, on a set of keyranges. This is a legacy function.
	StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error)

	// StreamExecuteKeyspaceIds executes a streaming query on vtgate, for the given keyspaceIds. This is a legacy function.
	StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error)

	// Begin starts a transaction and returns a VTGateTX. This is a legacy function.
	Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error)
	// Commit commits the current transaction. This is a legacy function.
	Commit(ctx context.Context, session *vtgatepb.Session, twopc bool) error
	// Rollback rolls back the current transaction. This is a legacy function.
	Rollback(ctx context.Context, session *vtgatepb.Session) error

	// ResolveTransaction resolves the specified 2pc transaction.
	ResolveTransaction(ctx context.Context, dtid string) error

	// Messaging functions.
	MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error
	MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error)
	MessageAckKeyspaceIds(ctx context.Context, keyspace string, name string, idKeyspaceIDs []*vtgatepb.IdKeyspaceId) (int64, error)

	// SplitQuery splits a query into smaller queries. It is mostly used by batch job frameworks
	// such as MapReduce. See the documentation for the vtgate.SplitQueryRequest protocol buffer
	// message in 'proto/vtgate.proto'.
	SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]*querypb.BindVariable, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error)

	// GetSrvKeyspace returns a topo.SrvKeyspace.
	GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error)

	// VStream streams binlogevents
	VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter) (VStreamReader, error)

	// UpdateStream asks for a stream of StreamEvent.
	UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken) (UpdateStreamReader, error)

	// Close must be called for releasing resources.
	Close()
}

// DialerFunc represents a function that will return an Impl
// object that can communicate with a VTGate.
type DialerFunc func(ctx context.Context, address string) (Impl, error)

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
func DialProtocol(ctx context.Context, protocol string, address string) (*VTGateConn, error) {
	dialer, ok := dialers[protocol]
	if !ok {
		return nil, fmt.Errorf("no dialer registered for VTGate protocol %s", protocol)
	}
	impl, err := dialer(ctx, address)
	if err != nil {
		return nil, err
	}
	return &VTGateConn{
		impl: impl,
	}, nil
}

// Dial dials using the command-line specified protocol, and returns
// the *VTGateConn.
func Dial(ctx context.Context, address string) (*VTGateConn, error) {
	return DialProtocol(ctx, *VtgateProtocol, address)
}
