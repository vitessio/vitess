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
	cert  = flag.String("tablet_grpc_cert", "", "the cert to use to connect")
	key   = flag.String("tablet_grpc_key", "", "the key to use to connect")
	ca    = flag.String("tablet_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name  = flag.String("tablet_grpc_server_name", "", "the server name to use to validate server certificate")
	combo = flag.Bool("tablet_grpc_combine_begin_execute", false, "combines Begin and Execute / ExecuteBatch calls in one when possible")
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
func (conn *gRPCQueryClient) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
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
	}
	er, err := conn.c.Execute(ctx, req)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	return sqltypes.Proto3ToResult(er.Result), nil
}

// ExecuteBatch sends a batch query to VTTablet.
func (conn *gRPCQueryClient) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
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
func (conn *gRPCQueryClient) StreamExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}) (sqltypes.ResultStream, error) {
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

// BeginExecute starts a transaction and runs an Execute.
func (conn *gRPCQueryClient) BeginExecute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]interface{}) (result *sqltypes.Result, transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, tabletconn.ConnClosed
	}

	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, 0, err
	}

	if *combo {
		// If combo is enabled, we combine both calls
		req := &querypb.BeginExecuteRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Query:             q,
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

	// Begin part.
	breq := &querypb.BeginRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
	}
	br, err := conn.c.Begin(ctx, breq)
	if err != nil {
		return nil, 0, tabletconn.TabletErrorFromGRPC(err)
	}
	transactionID = br.TransactionId

	// Execute part.
	ereq := &querypb.ExecuteRequest{
		Target:            target,
		EffectiveCallerId: breq.EffectiveCallerId,
		ImmediateCallerId: breq.ImmediateCallerId,
		Query:             q,
		TransactionId:     transactionID,
	}
	er, err := conn.c.Execute(ctx, ereq)
	if err != nil {
		return nil, transactionID, tabletconn.TabletErrorFromGRPC(err)
	}
	return sqltypes.Proto3ToResult(er.Result), transactionID, nil

}

// BeginExecuteBatch starts a transaction and runs an ExecuteBatch.
func (conn *gRPCQueryClient) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []querytypes.BoundQuery, asTransaction bool) (results []sqltypes.Result, transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, 0, tabletconn.ConnClosed
	}

	if *combo {
		// If combo is enabled, we combine both calls
		req := &querypb.BeginExecuteBatchRequest{
			Target:            target,
			EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
			ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
			Queries:           make([]*querypb.BoundQuery, len(queries)),
			AsTransaction:     asTransaction,
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

	breq := &querypb.BeginRequest{
		Target:            target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
	}
	br, err := conn.c.Begin(ctx, breq)
	if err != nil {
		return nil, 0, tabletconn.TabletErrorFromGRPC(err)
	}
	transactionID = br.TransactionId

	ereq := &querypb.ExecuteBatchRequest{
		Target:            target,
		EffectiveCallerId: breq.EffectiveCallerId,
		ImmediateCallerId: breq.ImmediateCallerId,
		Queries:           make([]*querypb.BoundQuery, len(queries)),
		AsTransaction:     asTransaction,
		TransactionId:     transactionID,
	}
	for i, q := range queries {
		qq, err := querytypes.BoundQueryToProto3(q.Sql, q.BindVariables)
		if err != nil {
			return nil, transactionID, err
		}
		ereq.Queries[i] = qq
	}
	ebr, err := conn.c.ExecuteBatch(ctx, ereq)
	if err != nil {
		return nil, transactionID, tabletconn.TabletErrorFromGRPC(err)
	}
	return sqltypes.Proto3ToResults(ebr.Results), transactionID, nil
}

// SplitQuery is the stub for TabletServer.SplitQuery RPC
// TODO(erez): Remove this method and rename SplitQueryV2 to SplitQuery once
// the migration to SplitQuery V2 is done.
func (conn *gRPCQueryClient) SplitQuery(ctx context.Context, target *querypb.Target, query querytypes.BoundQuery, splitColumn string, splitCount int64) (queries []querytypes.QuerySplit, err error) {
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
		SplitColumn:         []string{splitColumn},
		SplitCount:          splitCount,
		NumRowsPerQueryPart: 0,
		Algorithm:           querypb.SplitQueryRequest_EQUAL_SPLITS,
		UseSplitQueryV2:     false,
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

// SplitQueryV2 is the stub for TabletServer.SplitQuery RPC
func (conn *gRPCQueryClient) SplitQueryV2(
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
		UseSplitQueryV2:     true,
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
func (conn *gRPCQueryClient) Close() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.cc == nil {
		return
	}

	cc := conn.cc
	conn.cc = nil
	cc.Close()
}

// Tablet returns the rpc end point.
func (conn *gRPCQueryClient) Tablet() *topodatapb.Tablet {
	return conn.tablet
}
