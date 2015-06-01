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
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/rpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
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
	rpcConn, err := bsonrpc.DialHTTP(network, address, timeout, nil)
	if err != nil {
		return nil, err
	}
	return &vtgateConn{rpcConn: rpcConn}, nil
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.Query{
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
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.QueryShard{
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
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds []key.KeyspaceId, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.KeyspaceIdQuery{
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
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []key.KeyRange, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.KeyRangeQuery{
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
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []proto.EntityId, bindVars map[string]interface{}, tabletType topo.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.EntityIdsQuery{
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
	return result.Result, result.Session, nil
}

func (conn *vtgateConn) ExecuteBatchShard(ctx context.Context, queries []tproto.BoundQuery, keyspace string, shards []string, tabletType topo.TabletType, notInTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.BatchQueryShard{
		Queries:          queries,
		Keyspace:         keyspace,
		Shards:           shards,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result proto.QueryResultList
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteBatchShard", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	return result.List, result.Session, nil
}

func (conn *vtgateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []tproto.BoundQuery, keyspace string, keyspaceIds []key.KeyspaceId, tabletType topo.TabletType, notInTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	request := proto.KeyspaceIdBatchQuery{
		Queries:          queries,
		Keyspace:         keyspace,
		KeyspaceIds:      keyspaceIds,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result proto.QueryResultList
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteBatchKeyspaceIds", request, &result); err != nil {
		return nil, session, err
	}
	if result.Error != "" {
		return nil, result.Session, errors.New(result.Error)
	}
	return result.List, result.Session, nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
	req := &proto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *proto.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecute", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
	req := &proto.QueryShard{
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

func (conn *vtgateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []key.KeyRange, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
	req := &proto.KeyRangeQuery{
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

func (conn *vtgateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds []key.KeyspaceId, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
	req := &proto.KeyspaceIdQuery{
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

func sendStreamResults(c *rpcplus.Call, sr chan *proto.QueryResult) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {
	srout := make(chan *mproto.QueryResult, 1)
	go func() {
		defer close(srout)
		for r := range sr {
			srout <- r.Result
		}
	}()
	return srout, func() error { return c.Error }
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

func (conn *vtgateConn) SplitQuery(ctx context.Context, keyspace string, query tproto.BoundQuery, splitCount int) ([]proto.SplitQueryPart, error) {
	request := &proto.SplitQueryRequest{
		Keyspace:   keyspace,
		Query:      query,
		SplitCount: splitCount,
	}
	result := &proto.SplitQueryResult{}
	if err := conn.rpcConn.Call(ctx, "VTGate.SplitQuery", request, result); err != nil {
		return nil, err
	}
	return result.Splits, nil
}

func (conn *vtgateConn) Close() {
	conn.rpcConn.Close()
}
