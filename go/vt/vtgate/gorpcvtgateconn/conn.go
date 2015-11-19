// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package gorpcvtgateconn provides go rpc connectivity for VTGate.
package gorpcvtgateconn

import (
	"strings"
	"time"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/rpc"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/gorpcvtgatecommon"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
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

func sessionToRPC(session interface{}) *vtgatepb.Session {
	if session == nil {
		return nil
	}
	s := session.(*vtgatepb.Session)
	if s == nil {
		return nil
	}
	if s.ShardSessions == nil {
		return &vtgatepb.Session{
			InTransaction: s.InTransaction,
			ShardSessions: []*vtgatepb.Session_ShardSession{},
		}
	}
	return s
}

func sessionFromRPC(session *vtgatepb.Session) interface{} {
	if session == nil {
		return nil
	}
	if len(session.ShardSessions) == 0 {
		session.ShardSessions = nil
	}
	return session
}

func (conn *vtgateConn) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	s := sessionToRPC(session)
	request := gorpcvtgatecommon.Query{
		CallerID:         getEffectiveCallerID(ctx),
		Sql:              query,
		BindVariables:    bindVars,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result gorpcvtgatecommon.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.Execute", request, &result); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, sessionFromRPC(result.Session), err
	}
	if result.Result != nil {
		result.Result.Repair(result.Result.Fields)
	}
	return result.Result, sessionFromRPC(result.Session), nil
}

func (conn *vtgateConn) ExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	s := sessionToRPC(session)
	request := gorpcvtgatecommon.QueryShard{
		CallerID:         getEffectiveCallerID(ctx),
		Sql:              query,
		BindVariables:    bindVars,
		Keyspace:         keyspace,
		Shards:           shards,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result gorpcvtgatecommon.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteShard", request, &result); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, sessionFromRPC(result.Session), err
	}
	if result.Result != nil {
		result.Result.Repair(result.Result.Fields)
	}
	return result.Result, sessionFromRPC(result.Session), nil
}

func (conn *vtgateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	s := sessionToRPC(session)
	request := gorpcvtgatecommon.KeyspaceIdQuery{
		CallerID:         getEffectiveCallerID(ctx),
		Sql:              query,
		BindVariables:    bindVars,
		Keyspace:         keyspace,
		KeyspaceIds:      keyspaceIds,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result gorpcvtgatecommon.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteKeyspaceIds", request, &result); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, sessionFromRPC(result.Session), err
	}
	if result.Result != nil {
		result.Result.Repair(result.Result.Fields)
	}
	return result.Result, sessionFromRPC(result.Session), nil
}

func (conn *vtgateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	s := sessionToRPC(session)
	request := gorpcvtgatecommon.KeyRangeQuery{
		CallerID:         getEffectiveCallerID(ctx),
		Sql:              query,
		BindVariables:    bindVars,
		Keyspace:         keyspace,
		KeyRanges:        keyRanges,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	var result gorpcvtgatecommon.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteKeyRanges", request, &result); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, sessionFromRPC(result.Session), err
	}
	if result.Result != nil {
		result.Result.Repair(result.Result.Fields)
	}
	return result.Result, sessionFromRPC(result.Session), nil
}

func (conn *vtgateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType topodatapb.TabletType, notInTransaction bool, session interface{}) (*sqltypes.Result, interface{}, error) {
	s := sessionToRPC(session)
	request := gorpcvtgatecommon.EntityIdsQuery{
		CallerID:          getEffectiveCallerID(ctx),
		Sql:               query,
		BindVariables:     bindVars,
		Keyspace:          keyspace,
		EntityColumnName:  entityColumnName,
		EntityKeyspaceIDs: gorpcvtgatecommon.ProtoToEntityIds(entityKeyspaceIDs),
		TabletType:        tabletType,
		Session:           s,
		NotInTransaction:  notInTransaction,
	}
	var result gorpcvtgatecommon.QueryResult
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteEntityIds", request, &result); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, sessionFromRPC(result.Session), err
	}
	if result.Result != nil {
		result.Result.Repair(result.Result.Fields)
	}
	return result.Result, sessionFromRPC(result.Session), nil
}

func (conn *vtgateConn) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session interface{}) ([]sqltypes.Result, interface{}, error) {
	s := sessionToRPC(session)
	qs, err := gorpcvtgatecommon.ProtoToBoundShardQueries(queries)
	if err != nil {
		return nil, session, err
	}
	request := gorpcvtgatecommon.BatchQueryShard{
		CallerID:      getEffectiveCallerID(ctx),
		Queries:       qs,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       s,
	}
	var result gorpcvtgatecommon.QueryResultList
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteBatchShard", request, &result); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, sessionFromRPC(result.Session), err
	}
	for _, r := range result.List {
		r.Repair(r.Fields)
	}
	return result.List, sessionFromRPC(result.Session), nil
}

func (conn *vtgateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session interface{}) ([]sqltypes.Result, interface{}, error) {
	s := sessionToRPC(session)
	qs, err := gorpcvtgatecommon.ProtoToBoundKeyspaceIdQueries(queries)
	if err != nil {
		return nil, session, err
	}
	request := gorpcvtgatecommon.KeyspaceIdBatchQuery{
		CallerID:      getEffectiveCallerID(ctx),
		Queries:       qs,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       s,
	}
	var result gorpcvtgatecommon.QueryResultList
	if err := conn.rpcConn.Call(ctx, "VTGate.ExecuteBatchKeyspaceIds", request, &result); err != nil {
		return nil, session, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, sessionFromRPC(result.Session), err
	}
	for _, r := range result.List {
		r.Repair(r.Fields)
	}
	return result.List, sessionFromRPC(result.Session), nil
}

func (conn *vtgateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	req := &gorpcvtgatecommon.Query{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *gorpcvtgatecommon.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecute", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	req := &gorpcvtgatecommon.Query{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *gorpcvtgatecommon.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecute2", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	req := &gorpcvtgatecommon.QueryShard{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		Shards:        shards,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *gorpcvtgatecommon.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteShard", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteShards2(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	req := &gorpcvtgatecommon.QueryShard{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		Shards:        shards,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *gorpcvtgatecommon.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteShard2", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	req := &gorpcvtgatecommon.KeyRangeQuery{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		KeyRanges:     keyRanges,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *gorpcvtgatecommon.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteKeyRanges", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteKeyRanges2(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	req := &gorpcvtgatecommon.KeyRangeQuery{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		KeyRanges:     keyRanges,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *gorpcvtgatecommon.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteKeyRanges2", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	req := &gorpcvtgatecommon.KeyspaceIdQuery{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		KeyspaceIds:   keyspaceIds,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *gorpcvtgatecommon.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteKeyspaceIds", req, sr)
	return sendStreamResults(c, sr)
}

func (conn *vtgateConn) StreamExecuteKeyspaceIds2(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType topodatapb.TabletType) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	req := &gorpcvtgatecommon.KeyspaceIdQuery{
		CallerID:      getEffectiveCallerID(ctx),
		Sql:           query,
		BindVariables: bindVars,
		Keyspace:      keyspace,
		KeyspaceIds:   keyspaceIds,
		TabletType:    tabletType,
		Session:       nil,
	}
	sr := make(chan *gorpcvtgatecommon.QueryResult, 10)
	c := conn.rpcConn.StreamGo("VTGate.StreamExecuteKeyspaceIds2", req, sr)
	return sendStreamResults(c, sr)
}

func sendStreamResults(c *rpcplus.Call, sr chan *gorpcvtgatecommon.QueryResult) (<-chan *sqltypes.Result, vtgateconn.ErrFunc, error) {
	srout := make(chan *sqltypes.Result, 1)
	var vtErr error
	go func() {
		defer close(srout)
		var fields []*querypb.Field
		for r := range sr {
			vtErr = vterrors.FromRPCError(r.Err)
			// If we get a QueryResult with an RPCError, that was an extra QueryResult sent by
			// the server specifically to indicate an error, and we shouldn't surface it to clients.
			if vtErr == nil {
				if fields == nil {
					// first packet, we remember the fields
					fields = r.Result.Fields
				} else {
					// next packet, fix the result
					r.Result.Repair(fields)
				}
				srout <- r.Result
			}
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
		return vtErr
	}
	return srout, errFunc, nil
}

func (conn *vtgateConn) Begin(ctx context.Context) (interface{}, error) {
	session := &vtgatepb.Session{}
	if err := conn.rpcConn.Call(ctx, "VTGate.Begin", &rpc.Unused{}, session); err != nil {
		return nil, err
	}
	return session, nil
}

func (conn *vtgateConn) Commit(ctx context.Context, session interface{}) error {
	s := sessionToRPC(session)
	return conn.rpcConn.Call(ctx, "VTGate.Commit", s, &rpc.Unused{})
}

func (conn *vtgateConn) Rollback(ctx context.Context, session interface{}) error {
	s := sessionToRPC(session)
	return conn.rpcConn.Call(ctx, "VTGate.Rollback", s, &rpc.Unused{})
}

func (conn *vtgateConn) Begin2(ctx context.Context) (interface{}, error) {
	request := &gorpcvtgatecommon.BeginRequest{
		CallerID: getEffectiveCallerID(ctx),
	}
	reply := new(gorpcvtgatecommon.BeginResponse)
	if err := conn.rpcConn.Call(ctx, "VTGate.Begin2", request, reply); err != nil {
		return nil, err
	}
	if err := vterrors.FromRPCError(reply.Err); err != nil {
		return nil, err
	}
	// Return a non-nil pointer
	session := &vtgatepb.Session{}
	if reply.Session != nil {
		session = reply.Session
	}
	return session, nil
}

func (conn *vtgateConn) Commit2(ctx context.Context, session interface{}) error {
	s := sessionToRPC(session)
	request := &gorpcvtgatecommon.CommitRequest{
		CallerID: getEffectiveCallerID(ctx),
		Session:  s,
	}
	reply := new(gorpcvtgatecommon.CommitResponse)
	if err := conn.rpcConn.Call(ctx, "VTGate.Commit2", request, reply); err != nil {
		return err
	}
	return vterrors.FromRPCError(reply.Err)
}

func (conn *vtgateConn) Rollback2(ctx context.Context, session interface{}) error {
	s := sessionToRPC(session)
	request := &gorpcvtgatecommon.RollbackRequest{
		CallerID: getEffectiveCallerID(ctx),
		Session:  s,
	}
	reply := new(gorpcvtgatecommon.RollbackResponse)
	if err := conn.rpcConn.Call(ctx, "VTGate.Rollback2", request, reply); err != nil {
		return err
	}
	return vterrors.FromRPCError(reply.Err)
}

func (conn *vtgateConn) SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]interface{}, splitColumn string, splitCount int) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	request := &gorpcvtgatecommon.SplitQueryRequest{
		CallerID: getEffectiveCallerID(ctx),
		Keyspace: keyspace,
		Query: tproto.BoundQuery{
			Sql:           query,
			BindVariables: bindVars,
		},
		SplitColumn: splitColumn,
		SplitCount:  splitCount,
	}
	result := &gorpcvtgatecommon.SplitQueryResult{}
	if err := conn.rpcConn.Call(ctx, "VTGate.SplitQuery", request, result); err != nil {
		return nil, err
	}
	if err := vterrors.FromRPCError(result.Err); err != nil {
		return nil, err
	}
	return result.Splits, nil
}

func (conn *vtgateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	request := &gorpcvtgatecommon.GetSrvKeyspaceRequest{
		Keyspace: keyspace,
	}
	result := &topodatapb.SrvKeyspace{}
	if err := conn.rpcConn.Call(ctx, "VTGate.GetSrvKeyspace", request, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (conn *vtgateConn) Close() {
	conn.rpcConn.Close()
}
