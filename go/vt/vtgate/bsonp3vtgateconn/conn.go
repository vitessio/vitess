// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bsonp3vtgateconn provides go rpc connectivity for VTGate,
// with BSON-encoded proto3 structs.
package bsonp3vtgateconn

import (
	"strings"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/callerid"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

	qpb "github.com/youtube/vitess/go/vt/proto/query"
	topopb "github.com/youtube/vitess/go/vt/proto/topodata"
	pb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func init() {
	vtgateconn.RegisterDialer("bsonp3", dial)
}

type vtgateConn struct {
	rpcConn *rpcplus.Client
}

func dial(ctx context.Context, address string, timeout time.Duration) (vtgateconn.Impl, error) {
	network := "tcp"
	if strings.Contains(address, "/") {
		network = "unix"
	}
	rpcConn, err := bsonrpc.DialHTTP(network, address, timeout)
	if err != nil {
		return nil, err
	}
	return &vtgateConn{rpcConn: rpcConn}, nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topopb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	response := &pb.ExecuteResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.Execute", request, response); err != nil {
		return nil, response.Session, err
	}
	if err := vterrors.FromVtRPCError(response.Error); err != nil {
		return nil, response.Session, err
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topopb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteShardsRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:         keyspace,
		Shards:           shards,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	response := &pb.ExecuteShardsResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.ExecuteShards", request, response); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromVtRPCError(response.Error); err != nil {
		return nil, response.Session, err
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topopb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteKeyspaceIdsRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:         keyspace,
		KeyspaceIds:      keyspaceIds,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	response := &pb.ExecuteKeyspaceIdsResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.ExecuteKeyspaceIds", request, response); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromVtRPCError(response.Error); err != nil {
		return nil, response.Session, err
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topopb.KeyRange, bindVars map[string]interface{}, tabletType topopb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteKeyRangesRequest{
		CallerId:         callerid.EffectiveCallerIDFromContext(ctx),
		Query:            tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:         keyspace,
		KeyRanges:        keyRanges,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	response := &pb.ExecuteKeyRangesResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.ExecuteKeyRanges", request, response); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromVtRPCError(response.Error); err != nil {
		return nil, response.Session, err
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []proto.EntityId, bindVars map[string]interface{}, tabletType topopb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteEntityIdsRequest{
		CallerId:          callerid.EffectiveCallerIDFromContext(ctx),
		Query:             tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:          keyspace,
		EntityColumnName:  entityColumnName,
		EntityKeyspaceIds: proto.EntityIdsToProto(entityKeyspaceIDs),
		TabletType:        tabletType,
		Session:           s,
		NotInTransaction:  notInTransaction,
	}
	response := &pb.ExecuteEntityIdsResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.ExecuteEntityIds", request, response); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromVtRPCError(response.Error); err != nil {
		return nil, response.Session, err
	}
	return mproto.Proto3ToQueryResult(response.Result), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType topopb.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteBatchShardsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Queries:       proto.BoundShardQueriesToProto(queries),
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       s,
	}
	response := &pb.ExecuteBatchShardsResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.ExecuteBatchShards", request, response); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromVtRPCError(response.Error); err != nil {
		return nil, response.Session, err
	}
	return mproto.Proto3ToQueryResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType topopb.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *pb.Session
	if session != nil {
		s = session.(*pb.Session)
	}
	request := &pb.ExecuteBatchKeyspaceIdsRequest{
		CallerId:      callerid.EffectiveCallerIDFromContext(ctx),
		Queries:       proto.BoundKeyspaceIdQueriesToProto(queries),
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       s,
	}
	response := &pb.ExecuteBatchKeyspaceIdsResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.ExecuteBatchKeyspaceIds", request, response); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromVtRPCError(response.Error); err != nil {
		return nil, response.Session, err
	}
	return mproto.Proto3ToQueryResults(response.Results), response.Session, nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topopb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	return conn.StreamExecute2(ctx, query, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topopb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		TabletType: tabletType,
	}
	sr := make(chan *pb.StreamExecuteResponse, 10)
	c := conn.rpcConn.StreamGo("VTGateP3.StreamExecute2", req, sr)
	srr := make(chan streamResult)
	go func() {
		for v := range sr {
			srr <- streamResult{qr: v.Result, err: v.Error}
		}
		close(srr)
	}()
	return sendStreamResults(c, srr)
}

func (conn *vtgateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topopb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteShards2(ctx, query, keyspace, shards, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteShards2(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topopb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteShardsRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:   keyspace,
		Shards:     shards,
		TabletType: tabletType,
	}
	sr := make(chan *pb.StreamExecuteShardsResponse, 10)
	c := conn.rpcConn.StreamGo("VTGateP3.StreamExecuteShards2", req, sr)
	srr := make(chan streamResult)
	go func() {
		for v := range sr {
			srr <- streamResult{qr: v.Result, err: v.Error}
		}
		close(srr)
	}()
	return sendStreamResults(c, srr)
}

func (conn *vtgateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topopb.KeyRange, bindVars map[string]interface{}, tabletType topopb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteKeyRanges2(ctx, query, keyspace, keyRanges, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteKeyRanges2(ctx context.Context, query string, keyspace string, keyRanges []*topopb.KeyRange, bindVars map[string]interface{}, tabletType topopb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteKeyRangesRequest{
		CallerId:   callerid.EffectiveCallerIDFromContext(ctx),
		Query:      tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:   keyspace,
		KeyRanges:  keyRanges,
		TabletType: tabletType,
	}
	sr := make(chan *pb.StreamExecuteKeyRangesResponse, 10)
	c := conn.rpcConn.StreamGo("VTGateP3.StreamExecuteKeyRanges2", req, sr)
	srr := make(chan streamResult)
	go func() {
		for v := range sr {
			srr <- streamResult{qr: v.Result, err: v.Error}
		}
		close(srr)
	}()
	return sendStreamResults(c, srr)
}

func (conn *vtgateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topopb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	return conn.StreamExecuteKeyspaceIds2(ctx, query, keyspace, keyspaceIds, bindVars, tabletType)
}

func (conn *vtgateConn) StreamExecuteKeyspaceIds2(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topopb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &pb.StreamExecuteKeyspaceIdsRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Query:       tproto.BoundQueryToProto3(query, bindVars),
		Keyspace:    keyspace,
		KeyspaceIds: keyspaceIds,
		TabletType:  tabletType,
	}
	sr := make(chan *pb.StreamExecuteKeyspaceIdsResponse, 10)
	c := conn.rpcConn.StreamGo("VTGateP3.StreamExecuteKeyspaceIds2", req, sr)
	srr := make(chan streamResult)
	go func() {
		for v := range sr {
			srr <- streamResult{qr: v.Result, err: v.Error}
		}
		close(srr)
	}()
	return sendStreamResults(c, srr)
}

type streamResult struct {
	qr  *qpb.QueryResult
	err *vtpb.RPCError
}

func sendStreamResults(c *rpcplus.Call, sr chan streamResult) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	srout := make(chan *mproto.QueryResult, 1)
	var err error
	go func() {
		defer close(srout)
		for r := range sr {
			// nil != nil
			vtErr := vterrors.FromVtRPCError(r.err)
			if vtErr != nil {
				err = vtErr
				continue
			}
			// If we get a QueryResult with an RPCError, that was an extra QueryResult sent by
			// the server specifically to indicate an error, and we shouldn't surface it to clients.
			srout <- mproto.Proto3ToQueryResult(r.qr)
		}
	}()
	// errFunc will return either an RPC-layer error or an application error, if one exists.
	// It will only return the most recent application error (i.e, from the QueryResult that
	// most recently contained an error). It will prioritize an RPC-layer error over an apperror,
	// if both exist.
	errFunc := func() error {
		if c.Error != nil {
			return c.Error
		}
		return err
	}
	return srout, errFunc, nil
}

func (conn *vtgateConn) Begin(ctx context.Context) (interface{}, error) {
	return conn.Begin2(ctx)
}

func (conn *vtgateConn) Commit(ctx context.Context, session interface{}) error {
	return conn.Commit2(ctx, session)
}

func (conn *vtgateConn) Rollback(ctx context.Context, session interface{}) error {
	return conn.Rollback2(ctx, session)
}

func (conn *vtgateConn) Begin2(ctx context.Context) (interface{}, error) {
	request := &pb.BeginRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
	}
	response := &pb.BeginResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.Begin2", request, response); err != nil {
		return nil, err
	}
	if err := vterrors.FromVtRPCError(response.Error); err != nil {
		return nil, err
	}
	// Return a non-nil pointer
	session := &pb.Session{}
	if response.Session != nil {
		session = response.Session
	}
	return session, nil
}

func (conn *vtgateConn) Commit2(ctx context.Context, session interface{}) error {
	s := session.(*pb.Session)
	request := &pb.CommitRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  s,
	}
	response := &pb.CommitResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.Commit2", request, response); err != nil {
		return err
	}
	// nil != nil
	if vtErr := vterrors.FromVtRPCError(response.Error); vtErr != nil {
		return vtErr
	}
	return nil
}

func (conn *vtgateConn) Rollback2(ctx context.Context, session interface{}) error {
	s := session.(*pb.Session)
	request := &pb.RollbackRequest{
		CallerId: callerid.EffectiveCallerIDFromContext(ctx),
		Session:  s,
	}
	response := &pb.RollbackResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.Rollback2", request, response); err != nil {
		return err
	}
	if vtErr := vterrors.FromVtRPCError(response.Error); vtErr != nil {
		return vtErr
	}
	return nil
}

func (conn *vtgateConn) SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]interface{}, splitColumn string, splitCount int) ([]proto.SplitQueryPart, error) {
	request := &pb.SplitQueryRequest{
		CallerId:    callerid.EffectiveCallerIDFromContext(ctx),
		Keyspace:    keyspace,
		Query:       tproto.BoundQueryToProto3(query, bindVars),
		SplitColumn: splitColumn,
		SplitCount:  int64(splitCount),
	}
	response := &pb.SplitQueryResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.SplitQuery", request, response); err != nil {
		return nil, err
	}
	return proto.ProtoToSplitQueryParts(response), nil
}

func (conn *vtgateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	request := &pb.GetSrvKeyspaceRequest{
		Keyspace: keyspace,
	}
	response := &pb.GetSrvKeyspaceResponse{}
	if err := conn.rpcConn.Call(ctx, "VTGateP3.GetSrvKeyspace", request, response); err != nil {
		return nil, err
	}
	return topo.ProtoToSrvKeyspace(response.SrvKeyspace), nil
}

func (conn *vtgateConn) Close() {
	conn.rpcConn.Close()
}
