// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctabletconn

import (
	"fmt"
	"io"
	"sync"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/netutil"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbs "github.com/youtube/vitess/go/vt/proto/queryservice"
	pbv "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func init() {
	tabletconn.RegisterDialer("grpc", DialTablet)
}

// gRPCQueryClient implements a gRPC implementation for TabletConn
type gRPCQueryClient struct {
	mu        sync.RWMutex
	endPoint  topo.EndPoint
	cc        *grpc.ClientConn
	c         pbs.QueryClient
	sessionID int64
}

// DialTablet creates and initializes gRPCQueryClient.
func DialTablet(ctx context.Context, endPoint topo.EndPoint, keyspace, shard string, timeout time.Duration) (tabletconn.TabletConn, error) {
	// create the RPC client
	addr := netutil.JoinHostPort(endPoint.Host, endPoint.NamedPortMap["grpc"])
	cc, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}
	c := pbs.NewQueryClient(cc)

	gsir, err := c.GetSessionId(ctx, &pb.GetSessionIdRequest{
		Keyspace: keyspace,
		Shard:    shard,
	})
	if err != nil {
		cc.Close()
		return nil, err
	}
	if gsir.Error != nil {
		cc.Close()
		return nil, tabletErrorFromRPCError(gsir.Error)
	}

	return &gRPCQueryClient{
		endPoint:  endPoint,
		cc:        cc,
		c:         c,
		sessionID: gsir.SessionId,
	}, nil
}

// Execute sends the query to VTTablet.
func (conn *gRPCQueryClient) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &pb.ExecuteRequest{
		Query:         tproto.BoundQueryToProto3(query, bindVars),
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	er, err := conn.c.Execute(ctx, req)
	if err != nil {
		return nil, tabletErrorFromGRPC(err)
	}
	if er.Error != nil {
		return nil, tabletErrorFromRPCError(er.Error)
	}
	return mproto.Proto3ToQueryResult(er.Result), nil
}

// ExecuteBatch sends a batch query to VTTablet.
func (conn *gRPCQueryClient) ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, transactionID int64) (*tproto.QueryResultList, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &pb.ExecuteBatchRequest{
		Queries:       make([]*pb.BoundQuery, len(queries)),
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	for i, q := range queries {
		req.Queries[i] = tproto.BoundQueryToProto3(q.Sql, q.BindVariables)
	}
	ebr, err := conn.c.ExecuteBatch(ctx, req)
	if err != nil {
		return nil, tabletErrorFromGRPC(err)
	}
	if ebr.Error != nil {
		return nil, tabletErrorFromRPCError(ebr.Error)
	}
	return tproto.Proto3ToQueryResultList(ebr.Results), nil
}

// StreamExecute starts a streaming query to VTTablet.
func (conn *gRPCQueryClient) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, nil, tabletconn.ConnClosed
	}

	req := &pb.StreamExecuteRequest{
		Query:     tproto.BoundQueryToProto3(query, bindVars),
		SessionId: conn.sessionID,
	}
	stream, err := conn.c.StreamExecute(ctx, req)
	if err != nil {
		return nil, nil, tabletErrorFromGRPC(err)
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = tabletErrorFromGRPC(err)
				}
				close(sr)
				return
			}
			if ser.Error != nil {
				finalError = tabletErrorFromRPCError(ser.Error)
				close(sr)
				return
			}
			sr <- mproto.Proto3ToQueryResult(ser.Result)
		}
	}()
	return sr, func() error {
		return finalError
	}, nil
}

// StreamExecute2 is the same as StreamExecute for gRPC
func (conn *gRPCQueryClient) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc, error) {
	return conn.StreamExecute(ctx, query, bindVars, transactionID)
}

// Begin starts a transaction.
func (conn *gRPCQueryClient) Begin(ctx context.Context) (transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}

	req := &pb.BeginRequest{
		SessionId: conn.sessionID,
	}
	br, err := conn.c.Begin(ctx, req)
	if err != nil {
		return 0, tabletErrorFromGRPC(err)
	}
	if br.Error != nil {
		return 0, tabletErrorFromRPCError(br.Error)
	}
	return br.TransactionId, nil
}

// Begin2 is the same as Begin for gRPC
func (conn *gRPCQueryClient) Begin2(ctx context.Context) (transactionID int64, err error) {
	return conn.Begin(ctx)
}

// Commit commits the ongoing transaction.
func (conn *gRPCQueryClient) Commit(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &pb.CommitRequest{
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	cr, err := conn.c.Commit(ctx, req)
	if err != nil {
		return tabletErrorFromGRPC(err)
	}
	if cr.Error != nil {
		return tabletErrorFromRPCError(cr.Error)
	}
	return nil
}

// Commit2 is the same as Commit for gRPC
func (conn *gRPCQueryClient) Commit2(ctx context.Context, transactionID int64) error {
	return conn.Commit(ctx, transactionID)
}

// Rollback rolls back the ongoing transaction.
func (conn *gRPCQueryClient) Rollback(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &pb.RollbackRequest{
		TransactionId: transactionID,
		SessionId:     conn.sessionID,
	}
	rr, err := conn.c.Rollback(ctx, req)
	if err != nil {
		return tabletErrorFromGRPC(err)
	}
	if rr.Error != nil {
		return tabletErrorFromRPCError(rr.Error)
	}
	return nil
}

// Rollback2 is the same as Rollback for gRPC
func (conn *gRPCQueryClient) Rollback2(ctx context.Context, transactionID int64) error {
	return conn.Rollback(ctx, transactionID)
}

// SplitQuery is the stub for SqlQuery.SplitQuery RPC
func (conn *gRPCQueryClient) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitCount int) (queries []tproto.QuerySplit, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		err = tabletconn.ConnClosed
		return
	}

	req := &pb.SplitQueryRequest{
		Query:      tproto.BoundQueryToProto3(query.Sql, query.BindVariables),
		SplitCount: int64(splitCount),
		SessionId:  conn.sessionID,
	}
	sqr, err := conn.c.SplitQuery(ctx, req)
	if err != nil {
		return nil, tabletErrorFromGRPC(err)
	}
	if sqr.Error != nil {
		return nil, tabletErrorFromRPCError(sqr.Error)
	}
	return tproto.Proto3ToQuerySplits(sqr.Queries), nil
}

// Close closes underlying bsonrpc.
func (conn *gRPCQueryClient) Close() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.cc == nil {
		return
	}

	conn.sessionID = 0
	cc := conn.cc
	conn.cc = nil
	cc.Close()
}

// EndPoint returns the rpc end point.
func (conn *gRPCQueryClient) EndPoint() topo.EndPoint {
	return conn.endPoint
}

// tabletErrorFromGRPC returns a tabletconn.OperationalError from the
// gRPC error.
func tabletErrorFromGRPC(err error) error {
	return tabletconn.OperationalError(fmt.Sprintf("vttablet: %v", err))
}

// tabletErrorFromRPCError reconstructs a tablet error from the
// RPCError, using the RPCError code.
func tabletErrorFromRPCError(rpcErr *pbv.RPCError) error {
	ve := vterrors.FromVtRPCError(rpcErr)

	// see if the range is in the tablet error range
	if ve.Code >= int64(pbv.ErrorCode_TabletError) && ve.Code <= int64(pbv.ErrorCode_UnknownTabletError) {
		return &tabletconn.ServerError{
			Code: int(ve.Code - int64(pbv.ErrorCode_TabletError)),
			Err:  fmt.Sprintf("vttablet: %v", ve.Error()),
		}
	}

	return tabletconn.OperationalError(fmt.Sprintf("vttablet: %v", ve.Message))
}
