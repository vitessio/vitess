/*
Copyright 2019 The Vitess Authors.

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

package grpctabletconn

import (
	"context"
	"flag"
	"io"
	"sync"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	queryservicepb "vitess.io/vitess/go/vt/proto/queryservice"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const protocolName = "grpc"

var (
	cert = flag.String("tablet_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("tablet_grpc_key", "", "the key to use to connect")
	ca   = flag.String("tablet_grpc_ca", "", "the server ca to use to validate servers when connecting")
	crl  = flag.String("tablet_grpc_crl", "", "the server crl to use to validate server certificates when connecting")
	name = flag.String("tablet_grpc_server_name", "", "the server name to use to validate server certificate")
)

func init() {
	tabletconn.RegisterDialer(protocolName, DialTablet)
}

// gRPCQueryClient implements a gRPC implementation for QueryService
type gRPCQueryClient struct {
	// tablet is set at construction time, and never changed
	tablet *topodatapb.Tablet

	// mu protects the next fields
	mu sync.RWMutex
	cc *grpc.ClientConn
	c  queryservicepb.QueryClient
}

var _ queryservice.QueryService = (*gRPCQueryClient)(nil)

// DialTablet creates and initializes gRPCQueryClient.
func DialTablet(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
	// create the RPC client
	addr := ""
	if grpcPort, ok := tablet.PortMap["grpc"]; ok {
		addr = netutil.JoinHostPort(tablet.Hostname, grpcPort)
	} else {
		addr = tablet.Hostname
	}
	opt, err := grpcclient.SecureDialOption(*cert, *key, *ca, *crl, *name)
	if err != nil {
		return nil, err
	}
	cc, err := grpcclient.Dial(addr, failFast, opt)
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
func (conn *gRPCQueryClient) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &querypb.ExecuteRequest{
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Target:            target,
		Query: &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
		TransactionId: transactionID,
		Options:       options,
		ReservedId:    reservedID,
	}
	er, err := conn.c.Execute(ctx, req)
	if err != nil {
		return nil, tabletconn.ErrorFromGRPC(err)
	}
	return sqltypes.Proto3ToResult(er.Result), nil
}

// StreamExecute executes the query and streams results back through callback.
func (conn *gRPCQueryClient) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	// All streaming clients should follow the code pattern below.
	// The first part of the function starts the stream while holding
	// a lock on conn.mu. The second part receives the data and calls
	// callback.
	// A new cancelable context is needed because there's currently
	// no direct API to end a stream from the client side. If callback
	// returns an error, we return from the function. The deferred
	// cancel will then cause the stream to be terminated.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := func() (queryservicepb.Query_StreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &querypb.StreamExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query: &querypb.BoundQuery{
				Sql:           query,
				BindVariables: bindVars,
			},
			Options:       options,
			TransactionId: transactionID,
			ReservedId:    reservedID,
		}
		stream, err := conn.c.StreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	var fields []*querypb.Field
	for {
		ser, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == nil || err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// Begin starts a transaction.
func (conn *gRPCQueryClient) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (transactionID int64, alias *topodatapb.TabletAlias, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, nil, tabletconn.ConnClosed
	}

	req := &querypb.BeginRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Options:           options,
	}
	br, err := conn.c.Begin(ctx, req)
	if err != nil {
		return 0, nil, tabletconn.ErrorFromGRPC(err)
	}
	return br.TransactionId, br.TabletAlias, nil
}

// Commit commits the ongoing transaction.
func (conn *gRPCQueryClient) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}

	req := &querypb.CommitRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
	}
	resp, err := conn.c.Commit(ctx, req)
	if err != nil {
		return 0, tabletconn.ErrorFromGRPC(err)
	}
	return resp.ReservedId, nil
}

// Rollback rolls back the ongoing transaction.
func (conn *gRPCQueryClient) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}

	req := &querypb.RollbackRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
	}
	resp, err := conn.c.Rollback(ctx, req)
	if err != nil {
		return 0, tabletconn.ErrorFromGRPC(err)
	}
	return resp.ReservedId, nil
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
		return tabletconn.ErrorFromGRPC(err)
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
		return tabletconn.ErrorFromGRPC(err)
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
		return tabletconn.ErrorFromGRPC(err)
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
		return tabletconn.ErrorFromGRPC(err)
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
		return tabletconn.ErrorFromGRPC(err)
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
		return tabletconn.ErrorFromGRPC(err)
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
		return tabletconn.ErrorFromGRPC(err)
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
		return nil, tabletconn.ErrorFromGRPC(err)
	}
	return response.Metadata, nil
}

// BeginExecute starts a transaction and runs an Execute.
func (conn *gRPCQueryClient) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, query string, bindVars map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (result *sqltypes.Result, transactionID int64, alias *topodatapb.TabletAlias, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, nil, tabletconn.ConnClosed
	}

	req := &querypb.BeginExecuteRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		PreQueries:        preQueries,
		Query: &querypb.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
		ReservedId: reservedID,
		Options:    options,
	}
	reply, err := conn.c.BeginExecute(ctx, req)
	if err != nil {
		return nil, 0, nil, tabletconn.ErrorFromGRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.TransactionId, conn.tablet.Alias, tabletconn.ErrorFromVTRPC(reply.Error)
	}
	return sqltypes.Proto3ToResult(reply.Result), reply.TransactionId, conn.tablet.Alias, nil
}

// BeginStreamExecute starts a transaction and runs an Execute.
func (conn *gRPCQueryClient) BeginStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, query string, bindVars map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (transactionID int64, alias *topodatapb.TabletAlias, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, nil, tabletconn.ConnClosed
	}

	stream, err := func() (queryservicepb.Query_BeginStreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &querypb.BeginStreamExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			PreQueries:        preQueries,
			Query: &querypb.BoundQuery{
				Sql:           query,
				BindVariables: bindVars,
			},
			ReservedId: reservedID,
			Options:    options,
		}
		stream, err := conn.c.BeginStreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return 0, nil, err
	}
	var fields []*querypb.Field
	for {
		ser, err := stream.Recv()
		if transactionID == 0 && ser.GetTransactionId() != 0 {
			transactionID = ser.GetTransactionId()
		}
		if alias == nil && ser.GetTabletAlias() != nil {
			alias = ser.GetTabletAlias()
		}

		if err != nil {
			return transactionID, alias, tabletconn.ErrorFromGRPC(err)
		}

		if ser.Error != nil {
			return transactionID, alias, tabletconn.ErrorFromVTRPC(ser.Error)
		}

		// The last stream receive will not have a result, so callback will not be called for it.
		if ser.Result == nil {
			return transactionID, alias, nil
		}

		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == nil || err == io.EOF {
				return transactionID, alias, nil
			}
			return transactionID, alias, err
		}
	}
}

// MessageStream streams messages.
func (conn *gRPCQueryClient) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
			return nil, tabletconn.ErrorFromGRPC(err)
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
			return tabletconn.ErrorFromGRPC(err)
		}
		if fields == nil {
			fields = msr.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, msr.Result)); err != nil {
			if err == nil || err == io.EOF {
				return nil
			}
			return err
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
		return 0, tabletconn.ErrorFromGRPC(err)
	}
	return int64(reply.Result.RowsAffected), nil
}

// StreamHealth starts a streaming RPC for VTTablet health status updates.
func (conn *gRPCQueryClient) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	// Please see comments in StreamExecute to see how this works.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := func() (queryservicepb.Query_StreamHealthClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		stream, err := conn.c.StreamHealth(ctx, &querypb.StreamHealthRequest{})
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		shr, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if err := callback(shr); err != nil {
			if err == nil || err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// VStream starts a VReplication stream.
func (conn *gRPCQueryClient) VStream(ctx context.Context, target *querypb.Target, position string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, send func([]*binlogdatapb.VEvent) error) error {
	stream, err := func() (queryservicepb.Query_VStreamClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &binlogdatapb.VStreamRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Position:          position,
			Filter:            filter,
			TableLastPKs:      tablePKs,
		}
		stream, err := conn.c.VStream(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err := send(r.Events); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// VStreamRows streams rows of a query from the specified starting point.
func (conn *gRPCQueryClient) VStreamRows(ctx context.Context, target *querypb.Target, query string, lastpk *querypb.QueryResult, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	stream, err := func() (queryservicepb.Query_VStreamRowsClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &binlogdatapb.VStreamRowsRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query:             query,
			Lastpk:            lastpk,
		}
		stream, err := conn.c.VStreamRows(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	r := binlogdatapb.VStreamRowsResponseFromVTPool()
	defer r.ReturnToVTPool()
	for {
		err := stream.RecvMsg(r)
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := send(r); err != nil {
			return err
		}
		r.ResetVT()
	}
}

// VStreamResults streams rows of a query from the specified starting point.
func (conn *gRPCQueryClient) VStreamResults(ctx context.Context, target *querypb.Target, query string, send func(*binlogdatapb.VStreamResultsResponse) error) error {
	stream, err := func() (queryservicepb.Query_VStreamResultsClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &binlogdatapb.VStreamResultsRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query:             query,
		}
		stream, err := conn.c.VStreamResults(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return err
	}
	for {
		r, err := stream.Recv()
		if err != nil {
			return tabletconn.ErrorFromGRPC(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := send(r); err != nil {
			return err
		}
	}
}

// HandlePanic is a no-op.
func (conn *gRPCQueryClient) HandlePanic(err *error) {
}

// ReserveBeginExecute implements the queryservice interface
func (conn *gRPCQueryClient) ReserveBeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, int64, *topodatapb.TabletAlias, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, 0, nil, tabletconn.ConnClosed
	}

	req := &querypb.ReserveBeginExecuteRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Options:           options,
		PreQueries:        preQueries,
		PostBeginQueries:  postBeginQueries,
		Query: &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		},
	}
	reply, err := conn.c.ReserveBeginExecute(ctx, req)
	if err != nil {
		return nil, 0, 0, nil, tabletconn.ErrorFromGRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.TransactionId, reply.ReservedId, conn.tablet.Alias, tabletconn.ErrorFromVTRPC(reply.Error)
	}

	return sqltypes.Proto3ToResult(reply.Result), reply.TransactionId, reply.ReservedId, conn.tablet.Alias, nil
}

// ReserveBeginStreamExecute implements the queryservice interface
func (conn *gRPCQueryClient) ReserveBeginStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, postBeginQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (transactionID int64, reservedID int64, alias *topodatapb.TabletAlias, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, 0, nil, tabletconn.ConnClosed
	}

	stream, err := func() (queryservicepb.Query_ReserveBeginStreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &querypb.ReserveBeginStreamExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Options:           options,
			PreQueries:        preQueries,
			PostBeginQueries:  postBeginQueries,
			Query: &querypb.BoundQuery{
				Sql:           sql,
				BindVariables: bindVariables,
			},
		}
		stream, err := conn.c.ReserveBeginStreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return 0, 0, nil, tabletconn.ErrorFromGRPC(err)
	}

	var fields []*querypb.Field
	for {
		ser, err := stream.Recv()
		if transactionID == 0 && ser.GetTransactionId() != 0 {
			transactionID = ser.GetTransactionId()
		}
		if reservedID == 0 && ser.GetReservedId() != 0 {
			reservedID = ser.GetReservedId()
		}
		if alias == nil && ser.GetTabletAlias() != nil {
			alias = ser.GetTabletAlias()
		}

		if err != nil {
			return transactionID, reservedID, alias, tabletconn.ErrorFromGRPC(err)
		}

		if ser.Error != nil {
			return transactionID, reservedID, alias, tabletconn.ErrorFromVTRPC(ser.Error)
		}

		// The last stream receive will not have a result, so callback will not be called for it.
		if ser.Result == nil {
			return transactionID, reservedID, alias, nil
		}

		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == nil || err == io.EOF {
				return transactionID, reservedID, alias, nil
			}
			return transactionID, reservedID, alias, err
		}
	}
}

// ReserveExecute implements the queryservice interface
func (conn *gRPCQueryClient) ReserveExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, *topodatapb.TabletAlias, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, nil, tabletconn.ConnClosed
	}

	req := &querypb.ReserveExecuteRequest{
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Target:            target,
		Query: &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bindVariables,
		},
		TransactionId: transactionID,
		Options:       options,
		PreQueries:    preQueries,
	}
	reply, err := conn.c.ReserveExecute(ctx, req)
	if err != nil {
		return nil, 0, nil, tabletconn.ErrorFromGRPC(err)
	}
	if reply.Error != nil {
		return nil, reply.ReservedId, conn.tablet.Alias, tabletconn.ErrorFromVTRPC(reply.Error)
	}

	return sqltypes.Proto3ToResult(reply.Result), reply.ReservedId, conn.tablet.Alias, nil
}

// ReserveStreamExecute implements the queryservice interface
func (conn *gRPCQueryClient) ReserveStreamExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) (reservedID int64, alias *topodatapb.TabletAlias, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, nil, tabletconn.ConnClosed
	}

	stream, err := func() (queryservicepb.Query_ReserveStreamExecuteClient, error) {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		if conn.cc == nil {
			return nil, tabletconn.ConnClosed
		}

		req := &querypb.ReserveStreamExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Options:           options,
			PreQueries:        preQueries,
			Query: &querypb.BoundQuery{
				Sql:           sql,
				BindVariables: bindVariables,
			},
			TransactionId: transactionID,
		}
		stream, err := conn.c.ReserveStreamExecute(ctx, req)
		if err != nil {
			return nil, tabletconn.ErrorFromGRPC(err)
		}
		return stream, nil
	}()
	if err != nil {
		return 0, nil, tabletconn.ErrorFromGRPC(err)
	}

	var fields []*querypb.Field
	for {
		ser, err := stream.Recv()
		if reservedID == 0 && ser.GetReservedId() != 0 {
			reservedID = ser.GetReservedId()
		}
		if alias == nil && ser.GetTabletAlias() != nil {
			alias = ser.GetTabletAlias()
		}

		if err != nil {
			return reservedID, alias, tabletconn.ErrorFromGRPC(err)
		}

		if ser.Error != nil {
			return reservedID, alias, tabletconn.ErrorFromVTRPC(ser.Error)
		}

		// The last stream receive will not have a result, so callback will not be called for it.
		if ser.Result == nil {
			return reservedID, alias, nil
		}

		if fields == nil {
			fields = ser.Result.Fields
		}
		if err := callback(sqltypes.CustomProto3ToResult(fields, ser.Result)); err != nil {
			if err == nil || err == io.EOF {
				return reservedID, alias, nil
			}
			return reservedID, alias, err
		}
	}
}

func (conn *gRPCQueryClient) Release(ctx context.Context, target *querypb.Target, transactionID, reservedID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.ReleaseRequest{
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Target:            target,
		TransactionId:     transactionID,
		ReservedId:        reservedID,
	}
	_, err := conn.c.Release(ctx, req)
	if err != nil {
		return tabletconn.ErrorFromGRPC(err)
	}
	return nil
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
