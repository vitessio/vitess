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
	"github.com/youtube/vitess/go/vt/callerid"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pb "github.com/youtube/vitess/go/vt/proto/query"
	pbs "github.com/youtube/vitess/go/vt/proto/queryservice"
	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
)

const protocolName = "grpc"

func init() {
	tabletconn.RegisterDialer(protocolName, DialTablet)
}

// gRPCQueryClient implements a gRPC implementation for TabletConn
type gRPCQueryClient struct {
	// endPoint is set at construction time, and never changed
	endPoint *pbt.EndPoint

	// mu protects the next fields
	mu        sync.RWMutex
	cc        *grpc.ClientConn
	c         pbs.QueryClient
	sessionID int64
	target    *pb.Target
}

// DialTablet creates and initializes gRPCQueryClient.
func DialTablet(ctx context.Context, endPoint *pbt.EndPoint, keyspace, shard string, tabletType pbt.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
	// create the RPC client
	addr := netutil.JoinHostPort(endPoint.Host, endPoint.PortMap["grpc"])
	cc, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}
	c := pbs.NewQueryClient(cc)

	result := &gRPCQueryClient{
		endPoint: endPoint,
		cc:       cc,
		c:        c,
	}
	if tabletType == pbt.TabletType_UNKNOWN {
		// we use session
		gsir, err := c.GetSessionId(ctx, &pb.GetSessionIdRequest{
			Keyspace: keyspace,
			Shard:    shard,
		})
		if err != nil {
			cc.Close()
			return nil, err
		}
		result.sessionID = gsir.SessionId
	} else {
		// we use target
		result.target = &pb.Target{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
		}
	}

	return result, nil
}

// Execute sends the query to VTTablet.
func (conn *gRPCQueryClient) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &pb.ExecuteRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             tproto.BoundQueryToProto3(query, bindVars),
		TransactionId:     transactionID,
		SessionId:         conn.sessionID,
	}
	er, err := conn.c.Execute(ctx, req)
	if err != nil {
		return nil, tabletErrorFromGRPC(err)
	}
	return mproto.Proto3ToQueryResult(er.Result), nil
}

// Execute2 is the same with Execute in gRPC, since Execute is already CallerID enabled
func (conn *gRPCQueryClient) Execute2(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*mproto.QueryResult, error) {
	return conn.Execute(ctx, query, bindVars, transactionID)
}

// ExecuteBatch sends a batch query to VTTablet.
func (conn *gRPCQueryClient) ExecuteBatch(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &pb.ExecuteBatchRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Queries:           make([]*pb.BoundQuery, len(queries)),
		AsTransaction:     asTransaction,
		TransactionId:     transactionID,
		SessionId:         conn.sessionID,
	}
	for i, q := range queries {
		req.Queries[i] = tproto.BoundQueryToProto3(q.Sql, q.BindVariables)
	}
	ebr, err := conn.c.ExecuteBatch(ctx, req)
	if err != nil {
		return nil, tabletErrorFromGRPC(err)
	}
	return tproto.Proto3ToQueryResultList(ebr.Results), nil
}

// ExecuteBatch2 is the same with ExecuteBatch in gRPC, which is already CallerID enabled
func (conn *gRPCQueryClient) ExecuteBatch2(ctx context.Context, queries []tproto.BoundQuery, asTransaction bool, transactionID int64) (*tproto.QueryResultList, error) {
	return conn.ExecuteBatch(ctx, queries, asTransaction, transactionID)
}

// StreamExecute starts a streaming query to VTTablet.
func (conn *gRPCQueryClient) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *mproto.QueryResult, tabletconn.ErrFunc, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, nil, tabletconn.ConnClosed
	}

	req := &pb.StreamExecuteRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             tproto.BoundQueryToProto3(query, bindVars),
		SessionId:         conn.sessionID,
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
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		SessionId:         conn.sessionID,
	}
	br, err := conn.c.Begin(ctx, req)
	if err != nil {
		return 0, tabletErrorFromGRPC(err)
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
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		SessionId:         conn.sessionID,
	}
	_, err := conn.c.Commit(ctx, req)
	if err != nil {
		return tabletErrorFromGRPC(err)
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
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		SessionId:         conn.sessionID,
	}
	_, err := conn.c.Rollback(ctx, req)
	if err != nil {
		return tabletErrorFromGRPC(err)
	}
	return nil
}

// Rollback2 is the same as Rollback for gRPC
func (conn *gRPCQueryClient) Rollback2(ctx context.Context, transactionID int64) error {
	return conn.Rollback(ctx, transactionID)
}

// SplitQuery is the stub for SqlQuery.SplitQuery RPC
func (conn *gRPCQueryClient) SplitQuery(ctx context.Context, query tproto.BoundQuery, splitColumn string, splitCount int) (queries []tproto.QuerySplit, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		err = tabletconn.ConnClosed
		return
	}

	req := &pb.SplitQueryRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             tproto.BoundQueryToProto3(query.Sql, query.BindVariables),
		SplitColumn:       splitColumn,
		SplitCount:        int64(splitCount),
		SessionId:         conn.sessionID,
	}
	sqr, err := conn.c.SplitQuery(ctx, req)
	if err != nil {
		return nil, tabletErrorFromGRPC(err)
	}
	return tproto.Proto3ToQuerySplits(sqr.Queries), nil
}

// StreamHealth is the stub for SqlQuery.StreamHealth RPC
func (conn *gRPCQueryClient) StreamHealth(ctx context.Context) (<-chan *pb.StreamHealthResponse, tabletconn.ErrFunc, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, nil, tabletconn.ConnClosed
	}

	result := make(chan *pb.StreamHealthResponse, 10)
	stream, err := conn.c.StreamHealth(ctx, &pb.StreamHealthRequest{})
	if err != nil {
		return nil, nil, err
	}

	var finalErr error
	go func() {
		for {
			hsr, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalErr = err
				}
				close(result)
				return
			}
			result <- hsr
		}
	}()
	return result, func() error {
		return finalErr
	}, nil
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

// SetTarget can be called to change the target used for subsequent calls.
func (conn *gRPCQueryClient) SetTarget(keyspace, shard string, tabletType pbt.TabletType) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.target == nil {
		return fmt.Errorf("cannot set target on sessionId based conn")
	}
	if tabletType == pbt.TabletType_UNKNOWN {
		return fmt.Errorf("cannot set tablet type to UNKNOWN")
	}
	conn.target = &pb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	return nil
}

// EndPoint returns the rpc end point.
func (conn *gRPCQueryClient) EndPoint() *pbt.EndPoint {
	return conn.endPoint
}

// tabletErrorFromGRPC returns a tabletconn.OperationalError from the
// gRPC error.
func tabletErrorFromGRPC(err error) error {
	return tabletconn.OperationalError(fmt.Sprintf("vttablet: %v", err))
}
