// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcvtgateconn provides gRPC connectivity for VTGate.
package grpcvtgateconn

import (
	"io"
	"time"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtgateservicepb "github.com/youtube/vitess/go/vt/proto/vtgateservice"
)

func init() {
	vtgateconn.RegisterDialer("grpc", dial)
}

type vtgateConn struct {
	cc *grpc.ClientConn
	c  vtgateservicepb.VitessClient
}

func dial(ctx context.Context, addr string, timeout time.Duration) (vtgateconn.Impl, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}
	c := vtgateservicepb.NewVitessClient(cc)
	return &vtgateConn{
		cc: cc,
		c:  c,
	}, nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            q,
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
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteShardsRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            q,
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
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteKeyspaceIdsRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            q,
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
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteKeyRangesRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Session:          s,
		Query:            q,
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
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, session, err
	}
	request := &vtgatepb.ExecuteEntityIdsRequest{
		CallerId:          callerid.EffectiveCallerIDFromContext(ctx),
		Session:           s,
		Query:             q,
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
	return sqltypes.Proto3ToResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session interface{}) ([]sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	request := &vtgatepb.ExecuteBatchShardsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       queries,
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
	return sqltypes.Proto3ToResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session interface{}) ([]sqltypes.Result, interface{}, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session.(*vtgatepb.Session)
	}
	request := &vtgatepb.ExecuteBatchKeyspaceIdsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Session:       s,
		Queries:       queries,
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
	return sqltypes.Proto3ToResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, nil, err
	}
	req := &vtgatepb.StreamExecuteRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      q,
		TabletType: tabletType,
	}
	stream, err := conn.c.StreamExecute(ctx, req)
	if err != nil {
		return nil, nil, vterrors.FromGRPCError(err)
	}
	sr := make(chan *sqltypes.Result, 10)
	var finalError error
	go func() {
		var fields []*querypb.Field
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	return conn.StreamExecute(ctx, query, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, nil, err
	}
	req := &vtgatepb.StreamExecuteShardsRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      q,
		Keyspace:   keyspace,
		Shards:     shards,
		TabletType: tabletType,
	}
	stream, err := conn.c.StreamExecuteShards(ctx, req)
	if err != nil {
		return nil, nil, vterrors.FromGRPCError(err)
	}
	sr := make(chan *sqltypes.Result, 10)
	var finalError error
	go func() {
		var fields []*querypb.Field
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) StreamExecuteShards2(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, nil, err
	}
	req := &vtgatepb.StreamExecuteKeyRangesRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      q,
		Keyspace:   keyspace,
		KeyRanges:  keyRanges,
		TabletType: tabletType,
	}
	stream, err := conn.c.StreamExecuteKeyRanges(ctx, req)
	if err != nil {
		return nil, nil, vterrors.FromGRPCError(err)
	}
	sr := make(chan *sqltypes.Result, 10)
	var finalError error
	go func() {
		var fields []*querypb.Field
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) StreamExecuteKeyRanges2(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, nil, err
	}
	req := &vtgatepb.StreamExecuteKeyspaceIdsRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Query:       q,
		Keyspace:    keyspace,
		KeyspaceIds: keyspaceIds,
		TabletType:  tabletType,
	}
	stream, err := conn.c.StreamExecuteKeyspaceIds(ctx, req)
	if err != nil {
		return nil, nil, vterrors.FromGRPCError(err)
	}
	sr := make(chan *sqltypes.Result, 10)
	var finalError error
	go func() {
		var fields []*querypb.Field
		for {
			ser, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					finalError = vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) StreamExecuteKeyspaceIds2(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType)
}

func (conn *vtgateConn) Begin(ctx context.Context) (interface{}, error) {
	request := &vtgatepb.BeginRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
	}
	response, err := conn.c.Begin(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPCError(err)
	}
	return response.Session, nil
}

func (conn *vtgateConn) Commit(ctx context.Context, session interface{}) error {
	request := &vtgatepb.CommitRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session.(*vtgatepb.Session),
	}
	_, err := conn.c.Commit(ctx, request)
	return vterrors.FromGRPCError(err)
}

func (conn *vtgateConn) Rollback(ctx context.Context, session interface{}) error {
	request := &vtgatepb.RollbackRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  session.(*vtgatepb.Session),
	}
	_, err := conn.c.Rollback(ctx, request)
	return vterrors.FromGRPCError(err)
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

func (conn *vtgateConn) SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]interface{}, splitColumn string, splitCount int) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	q, err := tproto.BoundQueryToProto3(query, bindVars)
	if err != nil {
		return nil, err
	}

	request := &vtgatepb.SplitQueryRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Keyspace:    keyspace,
		Query:       q,
		SplitColumn: splitColumn,
		SplitCount:  int64(splitCount),
	}
	response, err := conn.c.SplitQuery(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPCError(err)
	}
	return response.Splits, nil
}

func (conn *vtgateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	request := &vtgatepb.GetSrvKeyspaceRequest{
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
