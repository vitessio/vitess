// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"
)

type queryResponse struct {
	execQuery  *proto.Query
	shardQuery *proto.QueryShard
	reply      *mproto.QueryResult
	err        error
}

type splitQueryResponse struct {
	splitQuery *proto.SplitQueryRequest
	reply      []proto.SplitQueryPart
	err        error
}

// FakeVTGateConn provides a fake implementation of vtgateconn.VTGateConn
type FakeVTGateConn struct {
	execMap       map[string]*queryResponse
	splitQueryMap map[string]*splitQueryResponse
}

// NewFakeVTGateConn creates a new FakeVTConn instance
func NewFakeVTGateConn(ctx context.Context, address string, timeout time.Duration) *FakeVTGateConn {
	return &FakeVTGateConn{execMap: make(map[string]*queryResponse)}
}

// AddQuery adds a query and expected result.
func (conn *FakeVTGateConn) AddQuery(request *proto.Query,
	expectedResult *mproto.QueryResult) {
	conn.execMap[request.Sql] = &queryResponse{
		execQuery: request,
		reply:     expectedResult,
	}
}

// AddShardQuery adds a shard query and expected result.
func (conn *FakeVTGateConn) AddShardQuery(
	request *proto.QueryShard, expectedResult *mproto.QueryResult) {
	conn.execMap[getShardQueryKey(request)] = &queryResponse{
		shardQuery: request,
		reply:      expectedResult,
	}
}

// AddSplitQuery adds a split query and expected result.
func (conn *FakeVTGateConn) AddSplitQuery(
	request *proto.SplitQueryRequest, expectedResult []proto.SplitQueryPart) {
	splits := request.SplitCount
	reply := make([]proto.SplitQueryPart, splits, splits)
	copy(reply, expectedResult)
	key := getSplitQueryKey(request.Keyspace, &request.Query, request.SplitCount)
	conn.splitQueryMap[key] = &splitQueryResponse{
		splitQuery: request,
		reply:      expectedResult,
		err:        nil,
	}
}

// Execute please see vtgateconn.VTGateConn.Execute
func (conn *FakeVTGateConn) Execute(
	ctx context.Context,
	query string,
	bindVars map[string]interface{},
	tabletType topo.TabletType) (*mproto.QueryResult, error) {
	return conn.execute(
		ctx,
		&proto.Query{
			Sql:           query,
			BindVariables: bindVars,
			TabletType:    tabletType,
			Session:       nil,
		})
}

func (conn *FakeVTGateConn) execute(ctx context.Context, query *proto.Query) (*mproto.QueryResult, error) {
	response, ok := conn.execMap[query.Sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, response.execQuery) {
		return nil, fmt.Errorf(
			"Execute: %+v, want %+v", query, response.execQuery)
	}
	var reply mproto.QueryResult
	reply = *response.reply
	return &reply, nil
}

// ExecuteShard please see vtgateconn.VTGateConn.ExecuteShard
func (conn *FakeVTGateConn) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	return conn.executeShard(
		ctx,
		&proto.QueryShard{
			Sql:           query,
			BindVariables: bindVars,
			TabletType:    tabletType,
			Keyspace:      keyspace,
			Shards:        shards,
			Session:       nil,
		})
}

func (conn *FakeVTGateConn) executeShard(ctx context.Context, query *proto.QueryShard) (*mproto.QueryResult, error) {
	response, ok := conn.execMap[getShardQueryKey(query)]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", query.Sql)
	}
	if !reflect.DeepEqual(query, response.shardQuery) {
		return nil, fmt.Errorf(
			"Execute: %+v, want %+v", query, response.shardQuery)
	}
	var reply mproto.QueryResult
	reply = *response.reply
	return &reply, nil
}

// StreamExecute please see vtgateconn.VTGateConn.StreamExecute
func (conn *FakeVTGateConn) StreamExecute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (<-chan *mproto.QueryResult, vtgateconn.ErrFunc) {

	resultChan := make(chan *mproto.QueryResult)
	defer close(resultChan)
	response, ok := conn.execMap[query]
	if !ok {
		return resultChan, func() error { return fmt.Errorf("no match for: %s", query) }
	}
	queryProto := &proto.Query{
		Sql:           query,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Session:       nil,
	}
	if !reflect.DeepEqual(queryProto, response.execQuery) {
		err := fmt.Errorf("Execute: %+v, want %+v", query, response.execQuery)
		return resultChan, func() error { return err }
	}
	if response.err != nil {
		return resultChan, func() error { return response.err }
	}
	if response.reply != nil {
		result := &mproto.QueryResult{}
		result.Fields = response.reply.Fields
		resultChan <- result
		for _, row := range response.reply.Rows {
			result := &mproto.QueryResult{}
			result.Rows = [][]sqltypes.Value{row}
			resultChan <- result
		}
	}
	return resultChan, nil
}

// Begin please see vtgateconn.VTGateConn.Begin
func (conn *FakeVTGateConn) Begin(ctx context.Context) (vtgateconn.VTGateTx, error) {
	tx := &fakeVTGateTx{
		conn: conn,
		session: &proto.Session{
			InTransaction: true,
		}}
	return tx, nil
}

// SplitQuery please see vtgateconn.VTGateConn.SplitQuery
func (conn *FakeVTGateConn) SplitQuery(ctx context.Context, keyspace string, query tproto.BoundQuery, splitCount int) ([]proto.SplitQueryPart, error) {
	response, ok := conn.splitQueryMap[getSplitQueryKey(keyspace, &query, splitCount)]
	if !ok {
		return nil, fmt.Errorf(
			"no match for keyspace: %s, query: %v, split count: %d",
			keyspace, query, splitCount)
	}
	reply := make([]proto.SplitQueryPart, splitCount, splitCount)
	copy(reply, response.reply)
	return reply, nil
}

// Close please see vtgateconn.VTGateConn.Close
func (conn *FakeVTGateConn) Close() {
}

type fakeVTGateTx struct {
	conn    *FakeVTGateConn
	session *proto.Session
}

// fakeVtgateTx has to implement vtgateconn.VTGateTx interface
var _ vtgateconn.VTGateTx = (*fakeVTGateTx)(nil)

func (tx *fakeVTGateTx) Execute(ctx context.Context, query string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, errors.New("execute: not in transaction")
	}
	r, err := tx.conn.execute(
		ctx,
		&proto.Query{
			Sql:           query,
			BindVariables: bindVars,
			TabletType:    tabletType,
			Session:       tx.session,
		})
	tx.session = newSession(true, "test_keyspace", []string{}, topo.TYPE_MASTER)
	return r, err
}

func (tx *fakeVTGateTx) ExecuteShard(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]interface{}, tabletType topo.TabletType) (*mproto.QueryResult, error) {
	if tx.session == nil {
		return nil, errors.New("executeShard: not in transaction")
	}
	r, err := tx.conn.executeShard(
		ctx,
		&proto.QueryShard{
			Sql:           query,
			BindVariables: bindVars,
			TabletType:    tabletType,
			Keyspace:      keyspace,
			Shards:        shards,
			Session:       tx.session,
		})
	tx.session = newSession(true, keyspace, shards, tabletType)
	return r, err
}

func (tx *fakeVTGateTx) Commit(ctx context.Context) error {
	if tx.session == nil {
		return errors.New("commit: not in transaction")
	}
	defer func() { tx.session = nil }()
	return nil
}

func (tx *fakeVTGateTx) Rollback(ctx context.Context) error {
	if tx.session == nil {
		return nil
	}
	defer func() { tx.session = nil }()
	return nil
}

func getShardQueryKey(request *proto.QueryShard) string {
	sort.Strings(request.Shards)
	return fmt.Sprintf("%s-%s", request.Sql, strings.Join(request.Shards, ":"))
}

func getSplitQueryKey(keyspace string, query *tproto.BoundQuery, splitCount int) string {
	return fmt.Sprintf("%s:%v:%d", keyspace, query, splitCount)
}

func newSession(
	inTransaction bool,
	keyspace string,
	shards []string,
	tabletType topo.TabletType) *proto.Session {
	shardSessions := make([]*proto.ShardSession, len(shards))
	for _, shard := range shards {
		shardSessions = append(shardSessions, &proto.ShardSession{
			Keyspace:      keyspace,
			Shard:         shard,
			TabletType:    tabletType,
			TransactionId: rand.Int63(),
		})
	}
	return &proto.Session{
		InTransaction: inTransaction,
		ShardSessions: shardSessions,
	}
}

// Make sure FakeVTGateConn implements vtgateconn.VTGateConn
var _ (vtgateconn.VTGateConn) = (*FakeVTGateConn)(nil)

// Make sure fakeVTGateTx implements vtgateconn.VtGateTx
var _ (vtgateconn.VTGateTx) = (*fakeVTGateTx)(nil)
