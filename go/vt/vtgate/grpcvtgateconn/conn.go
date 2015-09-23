// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcvtgateconn provides gRPC connectivity for VTGate.
package grpcvtgateconn

import (
	"io"
	"time"

	"google.golang.org/grpc"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/callerid"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	pb "github.com/youtube/vitess/go/vt/proto/vtgate"
	pbs "github.com/youtube/vitess/go/vt/proto/vtgateservice"
)

func init() {
	vtgateconn.RegisterDialer("grpc", dial)
}

type vtgateConn struct {
	cc *grpc.ClientConn
	c  pbs.VitessClient
}

func dial(ctx context.Context, addr string, timeout time.Duration) (vtgateconn.Impl, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}
	c := pbs.NewVitessClient(cc)
	return &vtgateConn{
		cc: cc,
		c:  c,
	}, nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pbt.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		TabletType:       tabletType,
		NotInTransaction: notInTransaction,
	}
	response, err := conn.c.Execute(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPCError(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pbt.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteShardsRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:         keyspace,
		Shards:           shards,
		TabletType:       tabletType,
		NotInTransaction: notInTransaction,
	}
	response, err := conn.c.ExecuteShards(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPCError(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pbt.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteKeyspaceIdsRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:         keyspace,
		KeyspaceIds:      keyspaceIds,
		TabletType:       tabletType,
		NotInTransaction: notInTransaction,
	}
	response, err := conn.c.ExecuteKeyspaceIds(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPCError(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pbt.KeyRange, bindVars map[string]interface{}, tabletType pbt.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteKeyRangesRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:         keyspace,
		KeyRanges:        keyRanges,
		TabletType:       tabletType,
		NotInTransaction: notInTransaction,
	}
	response, err := conn.c.ExecuteKeyRanges(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPCError(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*pb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType pbt.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteEntityIdsRequest{
		CallerId:          callerid.EffectiveCallerIDFromContext(ctx),
		Session:           s,
		Query:             tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:          keyspace,
		EntityColumnName:  entityColumnName,
		EntityKeyspaceIds: entityKeyspaceIDs,
		TabletType:        tabletType,
		NotInTransaction:  notInTransaction,
	}
	response, err := conn.c.ExecuteEntityIds(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPCError(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pbt.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteBatchShardsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       proto.BoundShardQueriesToProto(queries),
		TabletType:    tabletType,
		AsTransaction: asTransaction,
	}
	response, err := conn.c.ExecuteBatchShards(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPCError(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pbt.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteBatchKeyspaceIdsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       proto.BoundKeyspaceIdQueriesToProto(queries),
		TabletType:    tabletType,
		AsTransaction: asTransaction,
	}
	response, err := conn.c.ExecuteBatchKeyspaceIds(ctx, request)
	if err != nil {
		return nil, session, vterrors.FromGRPCError(err)
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pbt.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		TabletType: tabletType,
	}
	stream, err := conn.c.StreamExecute(ctx, req)
	if err != nil {
		return nil, nil, vterrors.FromGRPCError(err)
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pbt.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	return conn.StreamExecute(ctx, query, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pbt.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteShardsRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:   keyspace,
		Shards:     shards,
		TabletType: tabletType,
	}
	stream, err := conn.c.StreamExecuteShards(ctx, req)
	if err != nil {
		return nil, nil, vterrors.FromGRPCError(err)
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) StreamExecuteShards2(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pbt.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pbt.KeyRange, bindVars map[string]interface{}, tabletType pbt.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteKeyRangesRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:   keyspace,
		KeyRanges:  keyRanges,
		TabletType: tabletType,
	}
	stream, err := conn.c.StreamExecuteKeyRanges(ctx, req)
	if err != nil {
		return nil, nil, vterrors.FromGRPCError(err)
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) StreamExecuteKeyRanges2(ctx context.Context, query string, keyspace string, keyRanges []*pbt.KeyRange, bindVars map[string]interface{}, tabletType pbt.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pbt.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteKeyspaceIdsRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Query:       tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:    keyspace,
		KeyspaceIds: keyspaceIds,
		TabletType:  tabletType,
	}
	stream, err := conn.c.StreamExecuteKeyspaceIds(ctx, req)
	if err != nil {
		return nil, nil, vterrors.FromGRPCError(err)
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) StreamExecuteKeyspaceIds2(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pbt.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType)
}

func (conn *vtgateConn) Begin(ctx context.Context) (interface{}, error) {
	request := &pb.BeginRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
	}
	response, err := conn.c.Begin(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPCError(err)
	}
	return response.Session, nil
}

func (conn *vtgateConn) Commit(ctx context.Context, session interface{}) error {
	request := &pb.CommitRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session.(*pb.Session),
	}
	_, err := conn.c.Commit(ctx, request)
	if err != nil {
		return vterrors.FromGRPCError(err)
	}
	return nil
}

func (conn *vtgateConn) Rollback(ctx context.Context, session interface{}) error {
	request := &pb.RollbackRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session.(*pb.Session),
	}
	_, err := conn.c.Rollback(ctx, request)
	if err != nil {
		return vterrors.FromGRPCError(err)
	}
	return nil
}

func (conn *vtgateConn) Begin2(ctx context.Context) (interface{}, error) {
	return conn.Begin(ctx)
}

func (conn *vtgateConn) Commit2(ctx context.Context, session interface{}) error {
	return conn.Commit(ctx, session)
}

func (conn *vtgateConn) Rollback2(ctx context.Context, session interface{}) error {
	return conn.Rollback(ctx, session)
}

func (conn *vtgateConn) SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]interface{}, splitColumn string, splitCount int) ([]*pb.SplitQueryResponse_Part, error) {
	request := &pb.SplitQueryRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Keyspace:    keyspace,
		Query:       tproto.BoundQueryToProto3(query, bindVars),
		SplitColumn: splitColumn,
		SplitCount:  int64(splitCount),
	}
	response, err := conn.c.SplitQuery(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPCError(err)
	}
	return response.Splits, nil
}

func (conn *vtgateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*pbt.SrvKeyspace, error) {
	request := &pb.GetSrvKeyspaceRequest{
		Keyspace: keyspace,
	}
	response, err := conn.c.GetSrvKeyspace(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPCError(err)
	}
	return response.SrvKeyspace, nil
}

func (conn *vtgateConn) Close() {
	conn.cc.Close()
}
