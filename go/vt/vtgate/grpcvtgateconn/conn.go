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
	"github.com/youtube/vitess/go/vt/key"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

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
	cc, err := grpc.Dial(addr, grpc.WithBlock(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}
	c := pbs.NewVitessClient(cc)
	return &vtgateConn{
		cc: cc,
		c:  c,
	}, nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		TabletType:       topo.TabletTypeToProto(tabletType),
		NotInTransaction: notInTransaction,
	}
	response, err := conn.c.Execute(ctx, request)
	if err != nil {
		return nil, session, err
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
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
		TabletType:       topo.TabletTypeToProto(tabletType),
		NotInTransaction: notInTransaction,
	}
	response, err := conn.c.ExecuteShards(ctx, request)
	if err != nil {
		return nil, session, err
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds []key.KeyspaceId, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteKeyspaceIdsRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:         keyspace,
		KeyspaceIds:      key.KeyspaceIdsToProto(keyspaceIds),
		TabletType:       topo.TabletTypeToProto(tabletType),
		NotInTransaction: notInTransaction,
	}
	response, err := conn.c.ExecuteKeyspaceIds(ctx, request)
	if err != nil {
		return nil, session, err
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []key.KeyRange, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteKeyRangesRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:         keyspace,
		KeyRanges:        key.KeyRangesToProto(keyRanges),
		TabletType:       topo.TabletTypeToProto(tabletType),
		NotInTransaction: notInTransaction,
	}
	response, err := conn.c.ExecuteKeyRanges(ctx, request)
	if err != nil {
		return nil, session, err
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []proto.EntityId, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
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
		EntityKeyspaceIds: proto.EntityIdsToProto(entityKeyspaceIDs),
		TabletType:        topo.TabletTypeToProto(tabletType),
		NotInTransaction:  notInTransaction,
	}
	response, err := conn.c.ExecuteEntityIds(ctx, request)
	if err != nil {
		return nil, session, err
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchShard(ctx context.Context, queries []proto.BoundShardQuery, tabletType topo.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteBatchShardsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       proto.BoundShardQueriesToProto(queries),
		TabletType:    topo.TabletTypeToProto(tabletType),
		AsTransaction: asTransaction,
	}
	response, err := conn.c.ExecuteBatchShards(ctx, request)
	if err != nil {
		return nil, session, err
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType topo.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteBatchKeyspaceIdsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       proto.BoundKeyspaceIdQueriesToProto(queries),
		TabletType:    topo.TabletTypeToProto(tabletType),
		AsTransaction: asTransaction,
	}
	response, err := conn.c.ExecuteBatchKeyspaceIds(ctx, request)
	if err != nil {
		return nil, session, err
	}
	if response.Error != nil {
		return nil, response.Session, vterrors.FromVtRPCError(response.Error)
	}
	return mproto.Proto3ToQueryResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		TabletType: topo.TabletTypeToProto(tabletType),
	}
	stream, err := conn.c.StreamExecute(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = err
				}
				close(sr)
				return
			}
			if ser.Error != nil {
				finalError = vterrors.FromVtRPCError(ser.Error)
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

func (conn *vtgateConn) StreamExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteShardsRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:   keyspace,
		Shards:     shards,
		TabletType: topo.TabletTypeToProto(tabletType),
	}
	stream, err := conn.c.StreamExecuteShards(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = err
				}
				close(sr)
				return
			}
			if ser.Error != nil {
				finalError = vterrors.FromVtRPCError(ser.Error)
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

func (conn *vtgateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []key.KeyRange, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteKeyRangesRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:   keyspace,
		KeyRanges:  key.KeyRangesToProto(keyRanges),
		TabletType: topo.TabletTypeToProto(tabletType),
	}
	stream, err := conn.c.StreamExecuteKeyRanges(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = err
				}
				close(sr)
				return
			}
			if ser.Error != nil {
				finalError = vterrors.FromVtRPCError(ser.Error)
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

func (conn *vtgateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds []key.KeyspaceId, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteKeyspaceIdsRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Query:       tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:    keyspace,
		KeyspaceIds: key.KeyspaceIdsToProto(keyspaceIds),
		TabletType:  topo.TabletTypeToProto(tabletType),
	}
	stream, err := conn.c.StreamExecuteKeyspaceIds(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	sr := make(chan *mproto.QueryResult, 10)
	var finalError error
	go func() {
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = err
				}
				close(sr)
				return
			}
			if ser.Error != nil {
				finalError = vterrors.FromVtRPCError(ser.Error)
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

func (conn *vtgateConn) Begin(ctx context.Context) (interface{}, error) {
	request := &pb.BeginRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
	}
	response, err := conn.c.Begin(ctx, request)
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, vterrors.FromVtRPCError(response.Error)
	}
	return response.Session, nil
}

func (conn *vtgateConn) Commit(ctx context.Context, session interface{}) error {
	request := &pb.CommitRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session.(*pb.Session),
	}
	response, err := conn.c.Commit(ctx, request)
	if err != nil {
		return err
	}
	if response.Error != nil {
		return vterrors.FromVtRPCError(response.Error)
	}
	return nil
}

func (conn *vtgateConn) Rollback(ctx context.Context, session interface{}) error {
	request := &pb.RollbackRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session.(*pb.Session),
	}
	response, err := conn.c.Rollback(ctx, request)
	if err != nil {
		return err
	}
	if response.Error != nil {
		return vterrors.FromVtRPCError(response.Error)
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

func (conn *vtgateConn) SplitQuery(ctx context.Context, keyspace string, query tproto.BoundQuery, splitColumn string, splitCount int) ([]proto.SplitQueryPart, error) {
	request := &pb.SplitQueryRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Keyspace:    keyspace,
		Query:       tproto.BoundQueryToProto3(query.Sql, query.BindVariables),
		SplitColumn: splitColumn,
		SplitCount:  int64(splitCount),
	}
	response, err := conn.c.SplitQuery(ctx, request)
	if err != nil {
		return nil, err
	}
	return proto.ProtoToSplitQueryParts(response), nil
}

func (conn *vtgateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	request := &pb.GetSrvKeyspaceRequest{
		Keyspace: keyspace,
	}
	response, err := conn.c.GetSrvKeyspace(ctx, request)
	if err != nil {
		return nil, err
	}
	return topo.ProtoToSrvKeyspace(response.SrvKeyspace), nil
}

func (conn *vtgateConn) Close() {
	conn.cc.Close()
}
