// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpctabletconn

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	queryservicepb "github.com/youtube/vitess/go/vt/proto/queryservice"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const protocolName = "grpc"

func init() {
	tabletconn.RegisterDialer(protocolName, DialTablet)
}

// gRPCQueryClient implements a gRPC implementation for TabletConn
type gRPCQueryClient struct {
	// endPoint is set at construction time, and never changed
	endPoint *topodatapb.EndPoint

	// mu protects the next fields
	mu        sync.RWMutex
	cc        *grpc.ClientConn
	c         queryservicepb.QueryClient
	sessionID int64
	target    *querypb.Target
}

// DialTablet creates and initializes gRPCQueryClient.
func DialTablet(ctx context.Context, endPoint *topodatapb.EndPoint, keyspace, shard string, tabletType topodatapb.TabletType, timeout time.Duration) (tabletconn.TabletConn, error) {
	// create the RPC client
	addr := netutil.JoinHostPort(endPoint.Host, endPoint.PortMap["grpc"])
	cc, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}
	c := queryservicepb.NewQueryClient(cc)

	result := &gRPCQueryClient{
		endPoint: endPoint,
		cc:       cc,
		c:        c,
	}
	if tabletType == topodatapb.TabletType_UNKNOWN {
		// we use session
		gsir, err := c.GetSessionId(ctx, &querypb.GetSessionIdRequest{
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
		result.target = &querypb.Target{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
		}
	}

	return result, nil
}

// Execute sends the query to VTTablet.
func (conn *gRPCQueryClient) Execute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (*sqltypes.Result, error) {
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
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             q,
		TransactionId:     transactionID,
		SessionId:         conn.sessionID,
	}
	er, err := conn.c.Execute(ctx, req)
	if err != nil {
		return nil, tabletconn.TabletErrorFromGRPC(err)
	}
	return sqltypes.Proto3ToResult(er.Result), nil
}

// ExecuteBatch sends a batch query to VTTablet.
func (conn *gRPCQueryClient) ExecuteBatch(ctx context.Context, queries []querytypes.BoundQuery, asTransaction bool, transactionID int64) ([]sqltypes.Result, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, tabletconn.ConnClosed
	}

	req := &querypb.ExecuteBatchRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Queries:           make([]*querypb.BoundQuery, len(queries)),
		AsTransaction:     asTransaction,
		TransactionId:     transactionID,
		SessionId:         conn.sessionID,
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

// StreamExecute starts a streaming query to VTTablet.
func (conn *gRPCQueryClient) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, transactionID int64) (<-chan *sqltypes.Result, tabletconn.ErrFunc, error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return nil, nil, tabletconn.ConnClosed
	}

	q, err := querytypes.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, nil, err
	}
	req := &querypb.StreamExecuteRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             q,
		SessionId:         conn.sessionID,
	}
	stream, err := conn.c.StreamExecute(ctx, req)
	if err != nil {
		return nil, nil, tabletconn.TabletErrorFromGRPC(err)
	}
	sr := make(chan *sqltypes.Result, 10)
	var finalError error
	go func() {
		var fields []*querypb.Field
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = tabletconn.TabletErrorFromGRPC(err)
				}
				close(sr)
				return
			}
			if fields == nil {
				fields = ser.Result.Fields
			}
			sr <- sqltypes.CustomProto3ToResult(fields, ser.Result)
		}
	}()
	return sr, func() error {
		return finalError
	}, nil
}

// Begin starts a transaction.
func (conn *gRPCQueryClient) Begin(ctx context.Context) (transactionID int64, err error) {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return 0, tabletconn.ConnClosed
	}

	req := &querypb.BeginRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		SessionId:         conn.sessionID,
	}
	br, err := conn.c.Begin(ctx, req)
	if err != nil {
		return 0, tabletconn.TabletErrorFromGRPC(err)
	}
	return br.TransactionId, nil
}

// Commit commits the ongoing transaction.
func (conn *gRPCQueryClient) Commit(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.CommitRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		SessionId:         conn.sessionID,
	}
	_, err := conn.c.Commit(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// Rollback rolls back the ongoing transaction.
func (conn *gRPCQueryClient) Rollback(ctx context.Context, transactionID int64) error {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	if conn.cc == nil {
		return tabletconn.ConnClosed
	}

	req := &querypb.RollbackRequest{
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		TransactionId:     transactionID,
		SessionId:         conn.sessionID,
	}
	_, err := conn.c.Rollback(ctx, req)
	if err != nil {
		return tabletconn.TabletErrorFromGRPC(err)
	}
	return nil
}

// SplitQuery is the stub for TabletServer.SplitQuery RPC
func (conn *gRPCQueryClient) SplitQuery(ctx context.Context, query querytypes.BoundQuery, splitColumn string, splitCount int64) (queries []querytypes.QuerySplit, err error) {
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
		Target:            conn.target,
		EffectiveCallerId: callerid.EffectiveCallerIDFromContext(ctx),
		ImmediateCallerId: callerid.ImmediateCallerIDFromContext(ctx),
		Query:             q,
		SplitColumn:       splitColumn,
		SplitCount:        splitCount,
		SessionId:         conn.sessionID,
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
	return conn.c.StreamHealth(ctx, &querypb.StreamHealthRequest{})
}

// Close closes underlying gRPC channel.
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
func (conn *gRPCQueryClient) SetTarget(keyspace, shard string, tabletType topodatapb.TabletType) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.target == nil {
		return fmt.Errorf("cannot set target on sessionId based conn")
	}
	if tabletType == topodatapb.TabletType_UNKNOWN {
		return fmt.Errorf("cannot set tablet type to UNKNOWN")
	}
	conn.target = &querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: tabletType,
	}
	return nil
}

// EndPoint returns the rpc end point.
func (conn *gRPCQueryClient) EndPoint() *topodatapb.EndPoint {
	return conn.endPoint
}
