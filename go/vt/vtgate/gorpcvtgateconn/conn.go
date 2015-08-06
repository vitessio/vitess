// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateconn provides go rpc connectivity for VTGate.
package gorpcvtgateconn

import (
	"errors"
	"strings"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/rpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"
)

func init() {
	vtgateconn.RegisterDialer("gorpc", dial)
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

func getEffectiveCallerID(ctx context.Context) *tproto.CallerID {
	if ef := callerid.EffectiveCallerIDFromContext(ctx); ef != nil {
		return &tproto.CallerID{
			Principal:    ef.Principal,
			Component:    ef.Component,
			Subcomponent: ef.Subcomponent,
		}
	}
	return nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.Query{
		CallerID:         getEffectiveCallerID(ctx),
		Sql:              query,
		BindVariables:    bindVars,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.Execute", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, result.Session, err
	}
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.QueryShard{
		CallerID:         getEffectiveCallerID(ctx),
		Sql:              query,
		BindVariables:    bindVars,
		Keyspace:         keyspace,
		Shards:           shards,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteShard", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, result.Session, err
	}
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds []key.KeyspaceId, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.KeyspaceIdQuery{
		CallerID:         getEffectiveCallerID(ctx),
		Sql:              query,
		BindVariables:    bindVars,
		Keyspace:         keyspace,
		KeyspaceIds:      keyspaceIds,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteKeyspaceIds", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, result.Session, err
	}
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []key.KeyRange, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.KeyRangeQuery{
		CallerID:         getEffectiveCallerID(ctx),
		Sql:              query,
		BindVariables:    bindVars,
		Keyspace:         keyspace,
		KeyRanges:        keyRanges,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteKeyRanges", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, result.Session, err
	}
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []proto.EntityId, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.EntityIdsQuery{
		CallerID:          getEffectiveCallerID(ctx),
		Sql:               query,
		BindVariables:     bindVars,
		Keyspace:          keyspace,
		EntityColumnName:  entityColumnName,
		EntityKeyspaceIDs: entityKeyspaceIDs,
		TabletType:        tabletType,
		Session:           s,
		NotInTransaction:  notInTransaction,
	}
	var result proto.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteEntityIds", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, result.Session, err
	}
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteBatchShard(ctx context.Context, queries []proto.BoundShardQuery, tabletType topo.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.BatchQueryShard{
		CallerID:      getEffectiveCallerID(ctx),
		Queries:       queries,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       s,
	}
	var result proto.QueryResultList
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteBatchShard", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, result.Session, err
	}
	return result.List, result.Session, nil
}

func (conn *vtgateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType topo.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.KeyspaceIdBatchQuery{
		CallerID:      getEffectiveCallerID(ctx),
		Queries:       queries,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       s,
	}
	var result proto.QueryResultList
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteBatchKeyspaceIds", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, result.Session, err
	}
	return result.List, result.Session, nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &proto.Query{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *proto.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecute", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &proto.QueryShard{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		Shards:        shards,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *proto.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteShard", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []key.KeyRange, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &proto.KeyRangeQuery{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		KeyRanges:     keyRanges,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *proto.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteKeyRanges", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds []key.KeyspaceId, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	req := &proto.KeyspaceIdQuery{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		KeyspaceIds:   keyspaceIds,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *proto.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteKeyspaceIds", req, sr)
	return sendStreamResults(c, sr)
}

func sendStreamResults(c *rpcplus.Call, sr chan *proto.QueryResult) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	srout := make(chan *mproto.QueryResult, 1)
	go func() {
		defer close(srout)
		for r := range sr {
			srout <- r.Result
		}
	}()
	return srout, func() error { return c.Error }, nil
}

func (conn *vtgateConn) Begin(ctx context.Context) (interface{}, error) {
	session := &proto.Session{}
	if err := conn.rpcConn.Call(ctx, "VTGate.Begin", &rpc.Unused{}, session); err != nil {
		return nil, err
	}
	return session, nil
}

func (conn *vtgateConn) Commit(ctx context.Context, session interface{}) error {
	s := session.(*proto.Session)
	return conn.rpcConn.Call(ctx, "VTGate.Commit", s, &rpc.Unused{})
}

func (conn *vtgateConn) Rollback(ctx context.Context, session interface{}) error {
	s := session.(*proto.Session)
	return conn.rpcConn.Call(ctx, "VTGate.Rollback", s, &rpc.Unused{})
}

func (conn *vtgateConn) Begin2(ctx context.Context) (interface{}, error) {
	request := &proto.BeginRequest{
		CallerID: getEffectiveCallerID(ctx),
	}
	reply := new(proto.BeginResponse)
	if err := conn.rpcConn.Call(ctx, "VTGate.Begin2", request, reply); err != nil {
		return nil, err
	}
	if err := vterrors.FromRPCError(reply.Err); err != nil {
		return nil, err
	}
	// Return a non-nil pointer
	session := &proto.Session{}
	if reply.Session != nil {
		session = reply.Session
	}
	return session, nil
}

func (conn *vtgateConn) Commit2(ctx context.Context, session interface{}) error {
	s := session.(*proto.Session)
	request := &proto.CommitRequest{
		CallerID: getEffectiveCallerID(ctx),
		Session:  s,
	}
	reply := new(proto.CommitResponse)
	if err := conn.rpcConn.Call(ctx, "VTGate.Commit2", request, reply); err != nil {
		return err
	}
	return vterrors.FromRPCError(reply.Err)
}

func (conn *vtgateConn) Rollback2(ctx context.Context, session interface{}) error {
	s := session.(*proto.Session)
	request := &proto.RollbackRequest{
		CallerID: getEffectiveCallerID(ctx),
		Session:  s,
	}
	reply := new(proto.RollbackResponse)
	if err := conn.rpcConn.Call(ctx, "VTGate.Rollback2", request, reply); err != nil {
		return err
	}
	return vterrors.FromRPCError(reply.Err)
}

func (conn *vtgateConn) SplitQuery(ctx context.Context, keyspace string, query tproto.BoundQuery, splitColumn string, splitCount int) ([]proto.SplitQueryPart, error) {
	request := &proto.SplitQueryRequest{
		CallerID:    getEffectiveCallerID(ctx),
		Keyspace:    keyspace,
		Query:       query,
		SplitColumn: splitColumn,
		SplitCount:  splitCount,
	}
	result := &proto.SplitQueryResult{}
	if err := conn.rpcConn.Call(ctx, "VTGate.SplitQuery", request, result); err != nil {
		return nil, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, err
	}
	return result.Splits, nil
}

func (conn *vtgateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topo.SrvKeyspace, error) {
	request := &proto.GetSrvKeyspaceRequest{
		Keyspace: keyspace,
	}
	result := &topo.SrvKeyspace{}
	if err := conn.rpcConn.Call(ctx, "VTGate.GetSrvKeyspace", request, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (conn *vtgateConn) Close() {
	conn.rpcConn.Close()
}
