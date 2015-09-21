// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakerpcvtgateconn provides a fake implementation of
// vtgateconn.Impl that doesn't do any RPC, but uses a local
// map to return results.
package fakerpcvtgateconn

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// queryExecute contains all the fields we use to test Execute
type queryExecute struct {
	SQL              string
	BindVariables    map[string]interface{}
	TabletType       pb.TabletType
	Session          *proto.Session
	NotInTransaction bool
}

// queryExecuteShards contains all the fields we use to test ExecuteShards
type queryExecuteShards struct {
	SQL              string
	BindVariables    map[string]interface{}
	Keyspace         string
	Shards           []string
	TabletType       pb.TabletType
	Session          *proto.Session
	NotInTransaction bool
}

type queryResponse struct {
	execQuery  *queryExecute
	shardQuery *queryExecuteShards
	reply      *mproto.QueryResult
	err        error
}

// querySplitQuery contains all the fields we use to test SplitQuery
type querySplitQuery struct {
	Keyspace      string
	SQL           string
	BindVariables map[string]interface{}
	SplitColumn   string
	SplitCount    int
}

type splitQueryResponse struct {
	splitQuery *querySplitQuery
	reply      []*pbg.SplitQueryResponse_Part
	err        error
}

// FakeVTGateConn provides a fake implementation of vtgateconn.Impl
type FakeVTGateConn struct {
	execMap       map[string]*queryResponse
	splitQueryMap map[string]*splitQueryResponse
}

// RegisterFakeVTGateConnDialer registers the proper dialer for this fake,
// and returns the underlying instance that will be returned by the dialer,
// and the protocol to use to get this fake.
func RegisterFakeVTGateConnDialer() (*FakeVTGateConn, string) {
	protocol := "fake"
	impl := &FakeVTGateConn{
		execMap:       make(map[string]*queryResponse),
		splitQueryMap: make(map[string]*splitQueryResponse),
	}
	vtgateconn.RegisterDialer(protocol, func(ctx context.Context, address string, timeout time.Duration) (vtgateconn.Impl, error) {
		return impl, nil
	})
	return impl, protocol
}

// AddQuery adds a query and expected result.
func (conn *FakeVTGateConn) AddQuery(
	sql string,
	bindVariables map[string]interface{},
	tabletType pb.TabletType,
	session *proto.Session,
	notInTransaction bool,
	expectedResult *mproto.QueryResult) {
	conn.execMap[sql] = &queryResponse{
		execQuery: &queryExecute{
			SQL:              sql,
			BindVariables:    bindVariables,
			TabletType:       tabletType,
			Session:          session,
			NotInTransaction: notInTransaction,
		},
		reply: expectedResult,
	}
}

// AddShardQuery adds a shard query and expected result.
func (conn *FakeVTGateConn) AddShardQuery(
	sql string,
	bindVariables map[string]interface{},
	keyspace string,
	shards []string,
	tabletType pb.TabletType,
	session *proto.Session,
	notInTransaction bool,
	expectedResult *mproto.QueryResult) {
	conn.execMap[getShardQueryKey(sql, shards)] = &queryResponse{
		shardQuery: &queryExecuteShards{
			SQL:              sql,
			BindVariables:    bindVariables,
			Keyspace:         keyspace,
			Shards:           shards,
			TabletType:       tabletType,
			Session:          session,
			NotInTransaction: notInTransaction,
		},
		reply: expectedResult,
	}
}

// AddSplitQuery adds a split query and expected result.
func (conn *FakeVTGateConn) AddSplitQuery(
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumn string,
	splitCount int,
	expectedResult []*pbg.SplitQueryResponse_Part) {
	reply := make([]*pbg.SplitQueryResponse_Part, splitCount)
	copy(reply, expectedResult)
	key := getSplitQueryKey(keyspace, sql, splitColumn, splitCount)
	conn.splitQueryMap[key] = &splitQueryResponse{
		splitQuery: &querySplitQuery{
			Keyspace:      keyspace,
			SQL:           sql,
			BindVariables: bindVariables,
			SplitColumn:   splitColumn,
			SplitCount:    splitCount,
		},
		reply: expectedResult,
		err:   nil,
	}
}

// Execute please see vtgateconn.Impl.Execute
func (conn *FakeVTGateConn) Execute(ctx context.Context, sql string, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	response, ok := conn.execMap[sql]
	if !ok {
		return nil, nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:              sql,
		BindVariables:    bindVars,
		TabletType:       tabletType,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, response.execQuery) {
		return nil, nil, fmt.Errorf(
			"Execute: %+v, want %+v", query, response.execQuery)
	}
	var reply mproto.QueryResult
	reply = *response.reply
	if s != nil {
		s = newSession(true, "test_keyspace", []string{}, pb.TabletType_MASTER)
	}
	return &reply, s, nil
}

// ExecuteShards please see vtgateconn.Impl.ExecuteShard
func (conn *FakeVTGateConn) ExecuteShards(ctx context.Context, sql string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	var s *proto.Session
	if session != nil {
		s = session.(*proto.Session)
	}
	response, ok := conn.execMap[getShardQueryKey(sql, shards)]
	if !ok {
		return nil, nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteShards{
		SQL:              sql,
		BindVariables:    bindVars,
		TabletType:       tabletType,
		Keyspace:         keyspace,
		Shards:           shards,
		Session:          s,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, response.shardQuery) {
		return nil, nil, fmt.Errorf(
			"ExecuteShards: %+v, want %+v", query, response.shardQuery)
	}
	var reply mproto.QueryResult
	reply = *response.reply
	if s != nil {
		s = newSession(true, keyspace, shards, tabletType)
	}
	return &reply, s, nil
}

// ExecuteKeyspaceIds please see vtgateconn.Impl.ExecuteKeyspaceIds
func (conn *FakeVTGateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	panic("not implemented")
}

// ExecuteKeyRanges please see vtgateconn.Impl.ExecuteKeyRanges
func (conn *FakeVTGateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	panic("not implemented")
}

// ExecuteEntityIds please see vtgateconn.Impl.ExecuteEntityIds
func (conn *FakeVTGateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, bindVars map[string]interface{}, tabletType pb.TabletType, notInTransaction bool, session interface{}) (*mproto.QueryResult, interface{}, error) {
	panic("not implemented")
}

// ExecuteBatchShards please see vtgateconn.Impl.ExecuteBatchShards
func (conn *FakeVTGateConn) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	panic("not implemented")
}

// ExecuteBatchKeyspaceIds please see vtgateconn.Impl.ExecuteBatchKeyspaceIds
func (conn *FakeVTGateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool, session interface{}) ([]mproto.QueryResult, interface{}, error) {
	panic("not implemented")
}

// StreamExecute please see vtgateconn.Impl.StreamExecute
func (conn *FakeVTGateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	response, ok := conn.execMap[query]
	if !ok {
		return nil, nil, fmt.Errorf("no match for: %s", query)
	}
	queryProto := &proto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    topo.ProtoToTabletType(tabletType),
		Session:       nil,
	}
	if !reflect.DeepEqual(queryProto, response.execQuery) {
		return nil, nil, fmt.Errorf("StreamExecute: %+v, want %+v", query, response.execQuery)
	}
	if response.err != nil {
		return nil, nil, response.err
	}
	var resultChan chan *mproto.QueryResult
	defer close(resultChan)
	if response.reply != nil {
		// create a result channel big enough to buffer all of
		// the responses so we don't need to fork a go routine.
		resultChan := make(chan *mproto.QueryResult, len(response.reply.Rows)+1)
		result := &mproto.QueryResult{}
		result.Fields = response.reply.Fields
		resultChan <- result
		for _, row := range response.reply.Rows {
			result := &mproto.QueryResult{}
			result.Rows = [][]sqltypes.Value{row}
			resultChan <- result
		}
	} else {
		resultChan = make(chan *mproto.QueryResult)
	}
	return resultChan, func() error { return nil }, nil
}

// StreamExecute2 please see vtgateconn.Impl.StreamExecute2
func (conn *FakeVTGateConn) StreamExecute2(ctx context.Context, query string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	panic("not implemented")
}

// StreamExecuteShards please see vtgateconn.Impl.StreamExecuteShards
func (conn *FakeVTGateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	panic("not implemented")
}

// StreamExecuteShards2 please see vtgateconn.Impl.StreamExecuteShards2
func (conn *FakeVTGateConn) StreamExecuteShards2(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	panic("not implemented")
}

// StreamExecuteKeyRanges please see vtgateconn.Impl.StreamExecuteKeyRanges
func (conn *FakeVTGateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	panic("not implemented")
}

// StreamExecuteKeyRanges2 please see vtgateconn.Impl.StreamExecuteKeyRanges2
func (conn *FakeVTGateConn) StreamExecuteKeyRanges2(ctx context.Context, query string, keyspace string, keyRanges []*pb.KeyRange, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	panic("not implemented")
}

// StreamExecuteKeyspaceIds please see vtgateconn.Impl.StreamExecuteKeyspaceIds
func (conn *FakeVTGateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	panic("not implemented")
}

// StreamExecuteKeyspaceIds2 please see vtgateconn.Impl.StreamExecuteKeyspaceIds2
func (conn *FakeVTGateConn) StreamExecuteKeyspaceIds2(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]interface{}, tabletType pb.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc, error) {
	panic("not implemented")
}

// Begin please see vtgateconn.Impl.Begin
func (conn *FakeVTGateConn) Begin(ctx context.Context) (interface{}, error) {
	return &proto.Session{
		InTransaction: true,
	}, nil
}

// Commit please see vtgateconn.Impl.Commit
func (conn *FakeVTGateConn) Commit(ctx context.Context, session interface{}) error {
	if session == nil {
		return errors.New("commit: not in transaction")
	}
	return nil
}

// Rollback please see vtgateconn.Impl.Rollback
func (conn *FakeVTGateConn) Rollback(ctx context.Context, session interface{}) error {
	return nil
}

// Begin2 please see vtgateconn.Impl.Begin2
func (conn *FakeVTGateConn) Begin2(ctx context.Context) (interface{}, error) {
	return conn.Begin(ctx)
}

// Commit2 please see vtgateconn.Impl.Commit2
func (conn *FakeVTGateConn) Commit2(ctx context.Context, session interface{}) error {
	return conn.Commit(ctx, session)
}

// Rollback2 please see vtgateconn.Impl.Rollback2
func (conn *FakeVTGateConn) Rollback2(ctx context.Context, session interface{}) error {
	return conn.Rollback(ctx, session)
}

// SplitQuery please see vtgateconn.Impl.SplitQuery
func (conn *FakeVTGateConn) SplitQuery(ctx context.Context, keyspace string, query string, bindVars map[string]interface{}, splitColumn string, splitCount int) ([]*pbg.SplitQueryResponse_Part, error) {
	response, ok := conn.splitQueryMap[getSplitQueryKey(keyspace, query, splitColumn, splitCount)]
	if !ok {
		return nil, fmt.Errorf(
			"no match for keyspace: %s, query: %v, split column: %v, split count: %d",
			keyspace, query, splitColumn, splitCount)
	}
	reply := make([]*pbg.SplitQueryResponse_Part, splitCount, splitCount)
	copy(reply, response.reply)
	return reply, nil
}

// GetSrvKeyspace please see vtgateconn.Impl.SplitQuery
func (conn *FakeVTGateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*pb.SrvKeyspace, error) {
	return nil, fmt.Errorf("NYI")
}

// Close please see vtgateconn.Impl.Close
func (conn *FakeVTGateConn) Close() {
}

func getShardQueryKey(sql string, shards []string) string {
	sort.Strings(shards)
	return fmt.Sprintf("%s-%s", sql, strings.Join(shards, ":"))
}

func getSplitQueryKey(keyspace string, query string, splitColumn string, splitCount int) string {
	return fmt.Sprintf("%s:%v:%v:%d", keyspace, query, splitColumn, splitCount)
}

func newSession(
	inTransaction bool,
	keyspace string,
	shards []string,
	tabletType pb.TabletType) *proto.Session {
	shardSessions := make([]*proto.ShardSession, len(shards))
	for _, shard := range shards {
		shardSessions = append(shardSessions, &proto.ShardSession{
			Keyspace:      keyspace,
			Shard:         shard,
			TabletType:    topo.ProtoToTabletType(tabletType),
			TransactionId: rand.Int63(),
		})
	}
	return &proto.Session{
		InTransaction: inTransaction,
		ShardSessions: shardSessions,
	}
}

// Make sure FakeVTGateConn implements vtgateconn.Impl
var _ (vtgateconn.Impl) = (*FakeVTGateConn)(nil)
