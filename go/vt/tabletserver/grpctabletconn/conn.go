// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctabletconn

import (
	"flag"
	"io"
	"sync"
	"time"

	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	queryservicepb "github.com/youtube/vitess/go/vt/proto/queryservice"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const protocolName = "grpc"

var (
	cert = flag.String("tablet_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("tablet_grpc_key", "", "the key to use to connect")
	ca   = flag.String("tablet_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("tablet_grpc_server_name", "", "the server name to use to validate server certificate")
)

func init() {
	tabletconn.RegisterDialer(protocolName, DialTablet)
}

// gRPCQueryClient implements a gRPC implementation for TabletConn
type gRPCQueryClient struct {
	// tablet is set at construction time, and never changed
	tablet *topodatapb.Tablet

	// mu protects the next fields
	mu sync.RWMutex
	cc *grpc.ClientConn
	c  queryservicepb.QueryClient
}

// DialTablet creates and initializes gRPCQueryClient.
func DialTablet(tablet *topodatapb.Tablet, timeout time.Duration) (tabletconn.TabletConn, error) {
	// create the RPC client
	addr := ""
	if grpcPort, ok := tablet.PortMap["grpc"]; ok {
		addr = netutil.JoinHostPort(tablet.Hostname, grpcPort)
	} else {
		addr = tablet.Hostname
	}
	opt, err := grpcutils.ClientSecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{opt}
	if timeout > 0 {
		opts = append(opts, grpc.WithBlock(), grpc.WithTimeout(timeout))
	}
	cc, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	c := queryservicepb.NewQueryClient(cc)

	result := &gRPCQueryClient{
		tablet: tablet,
		cc:     cc,
		c:      c,
	}

	return result, nil
}

// Execute sends the query to VTTablet.
func (conn *gRPCQueryClient) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, err
	}

	req := &querypb.ExecuteRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             q,
		TransactionId:     transactionID,
		Options:           options,
	}
	er, err := conn.c.Execute(ctx, req)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	return sqltypes.Proto3ToResult(er.Result), nil
}

// ExecuteBatch sends a batch query to VTTablet.
func (conn *gRPCQueryClient) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &querypb.ExecuteBatchRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Queries:           make([]*querypb.BoundQuery, len(queries)),
		AsTransaction:     asTransaction,
		TransactionId:     transactionID,
		Options:           options,
	}
	for i, q := range queries {
		qq, err := querytypes.BoundQueryToProto3(q.Sql, q.BindVariables)
		if err != nil {
			return nil, err
		}
		req.Queries[i] = qq
	}
	ebr, err := conn.c.ExecuteBatch(ctx, req)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	return sqltypes.Proto3ToResults(ebr.Results), nil
}

type streamExecuteAdapter struct {
	stream queryservicepb.Query_StreamExecuteClient
	fields []*querypb.Field
}

func (a *streamExecuteAdapter) Recv() (*sqltypes.Result, error) {
	ser, err := a.stream.Recv()
	switch err {
	case nil:
		if a.fields == nil {
			a.fields = ser.Result.Fields
		}
		return sqltypes.CustomProto3ToResult(a.fields, ser.Result), nil
	case io.EOF:
		return nil, err
	default:
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
}

// StreamExecute starts a streaming query to VTTablet.
func (conn *gRPCQueryClient) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, err
	}
	req := &querypb.StreamExecuteRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             q,
		Options:           options,
	}
	stream, err := conn.c.StreamExecute(ctx, req)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	return &streamExecuteAdapter{stream: stream}, err
}

// Begin starts a transaction.
func (conn *gRPCQueryClient) Begin(ctx context.Context, target *querypb.Target) (transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}

	req := &querypb.BeginRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
	}
	br, err := conn.c.Begin(ctx, req)
	if err != nil {
		return 0, tabletconn.TabletErrorFromGRPC(err)
	}
	return br.TransactionId, nil
}

// Commit commits the ongoing transaction.
func (conn *gRPCQueryClient) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.CommitRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
	}
	_, err := conn.c.Commit(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// Rollback rolls back the ongoing transaction.
func (conn *gRPCQueryClient) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.RollbackRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
	}
	_, err := conn.c.Rollback(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// Prepare executes a Prepare on the ongoing transaction.
func (conn *gRPCQueryClient) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.PrepareRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	_, err := conn.c.Prepare(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// CommitPrepared commits the prepared transaction.
func (conn *gRPCQueryClient) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.CommitPreparedRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	_, err := conn.c.CommitPrepared(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// RollbackPrepared rolls back the prepared transaction.
func (conn *gRPCQueryClient) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.RollbackPreparedRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     originalID,
		Dtid:              dtid,
	}
	_, err := conn.c.RollbackPrepared(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (conn *gRPCQueryClient) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.CreateTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
		Participants:      participants,
	}
	_, err := conn.c.CreateTransaction(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (conn *gRPCQueryClient) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.StartCommitRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	_, err := conn.c.StartCommit(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (conn *gRPCQueryClient) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.SetRollbackRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		Dtid:              dtid,
	}
	_, err := conn.c.SetRollback(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// ConcludeTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (conn *gRPCQueryClient) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.ConcludeTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	_, err := conn.c.ConcludeTransaction(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// ReadTransaction returns the metadata for the sepcified dtid.
func (conn *gRPCQueryClient) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (*querypb.TransactionMetadata, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &querypb.ReadTransactionRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Dtid:              dtid,
	}
	response, err := conn.c.ReadTransaction(ctx, req)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	return response.Metadata, nil
}

// BeginExecute starts a transaction and runs an Execute.
func (conn *gRPCQueryClient) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, options *querypb.ExecuteOptions) (result *sqltypes.Result, transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, tabletconn.ConnClosed
	}

	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, 0, err
	}

	req := &querypb.BeginExecuteRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             q,
		Options:           options,
	}
	reply, err := conn.c.BeginExecute(ctx, req)
	if err != nil {
		return nil, 0, tabletconn.TabletErrorFromGRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.TransactionId, tabletconn.TabletErrorFromRPCError(reply.Error)
	}
	return sqltypes.Proto3ToResult(reply.Result), reply.TransactionId, nil
}

// BeginExecuteBatch starts a transaction and runs an ExecuteBatch.
func (conn *gRPCQueryClient) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) (results []sqltypes.Result, transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, tabletconn.ConnClosed
	}

	req := &querypb.BeginExecuteBatchRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Queries:           make([]*querypb.BoundQuery, len(queries)),
		AsTransaction:     asTransaction,
		Options:           options,
	}
	for i, q := range queries {
		qq, err := querytypes.BoundQueryToProto3(q.Sql, q.BindVariables)
		if err != nil {
			return nil, transactionID, err
		}
		req.Queries[i] = qq
	}

	reply, err := conn.c.BeginExecuteBatch(ctx, req)
	if err != nil {
		return nil, 0, tabletconn.TabletErrorFromGRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.TransactionId, tabletconn.TabletErrorFromRPCError(reply.Error)
	}
	return sqltypes.Proto3ToResults(reply.Results), reply.TransactionId, nil
}

// MessageStream streams messages.
func (conn *gRPCQueryClient) MessageStream(ctx context.Context, target *querypb.Target, name string, sendReply func(*sqltypes.Result) error) error {
	stream, err := func() (queryservicepb.Query_MessageStreamClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &querypb.MessageStreamRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Name:              name,
		}
		stream, err := conn.c.MessageStream(ctx, req)
		if err != nil {
			return nil, tabletconn.TabletErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	var fields []*querypb.Field
	for {
		msr, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return tabletconn.TabletErrorFromGRPC(err)
		}
		if fields == nil {
			fields = msr.Result.Fields
		}
		if err := sendReply(sqltypes.CustomProto3ToResult(fields, msr.Result)); err != nil {
			return tabletconn.TabletErrorFromGRPC(err)
		}
	}
}

// MessageAck acks messages.
func (conn *gRPCQueryClient) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}
	req := &querypb.MessageAckRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Name:              name,
		Ids:               ids,
	}
	reply, err := conn.c.MessageAck(ctx, req)
	if err != nil {
		return 0, tabletconn.TabletErrorFromGRPC(err)
	}
	return int64(reply.Result.RowsAffected), nil
}

// SplitQuery is the stub for TabletServer.SplitQuery RPC
func (conn *gRPCQueryClient) SplitQuery(
	ctx context.Context,
	target *querypb.Target,
	query querytypes.BoundQuery,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) (queries []querytypes.QuerySplit, err error) {

	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		err = tabletconn.ConnClosed
		return
	}

	q, err := querytypes.BoundQueryToProto3(query.Sql, query.BindVariables)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	req := &querypb.SplitQueryRequest{
		Target:              target,
		EffectiveCallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId:   callerid.ImmediateCallerIDFromContext(ctx),
		Query:               q,
		SplitColumn:         splitColumns,
		SplitCount:          splitCount,
		NumRowsPerQueryPart: numRowsPerQueryPart,
		Algorithm:           algorithm,
	}
	sqr, err := conn.c.SplitQuery(ctx, req)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	split, err := querytypes.Proto3ToQuerySplits(sqr.Queries)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	return split, nil
}

// StreamHealth starts a streaming RPC for VTTablet health status updates.
func (conn *gRPCQueryClient) StreamHealth(ctx context.Context) (tabletconn.StreamHealthReader, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	return conn.c.StreamHealth(ctx, &querypb.StreamHealthRequest{})
}

type updateStreamAdapter struct {
	stream queryservicepb.Query_UpdateStreamClient
}

func (a *updateStreamAdapter) Recv() (*querypb.StreamEvent, error) {
	r, err := a.stream.Recv()
	switch err {
	case nil:
		return r.Event, nil
	case io.EOF:
		return nil, err
	default:
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
}

// UpdateStream starts a streaming query to VTTablet.
func (conn *gRPCQueryClient) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64) (tabletconn.StreamEventReader, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &querypb.UpdateStreamRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Position:          position,
		Timestamp:         timestamp,
	}
	stream, err := conn.c.UpdateStream(ctx, req)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	return &updateStreamAdapter{stream: stream}, err
}

// Close closes underlying gRPC channel.
func (conn *gRPCQueryClient) Close(ctx context.Context) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.cc == nil {
		return nil
	}

	cc := conn.cc
	conn.cc = nil
	return cc.Close()
}

// Tablet returns the rpc end point.
func (conn *gRPCQueryClient) Tablet() *topodatapb.Tablet {
	return conn.tablet
}
