/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package fakerpcvtgateconn provides a fake implementation of
// vtgateconn.Impl that doesn't do any RPC, but uses a local
// map to return results.
package fakerpcvtgateconn

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// queryExecute contains all the fields we use to test Execute
type queryExecute struct {
	SQL           string
	BindVariables map[string]*querypb.BindVariable
	Session       *vtgatepb.Session
}

// queryExecuteShards contains all the fields we use to test ExecuteShards
type queryExecuteShards struct {
	SQL              string
	BindVariables    map[string]*querypb.BindVariable
	Keyspace         string
	Shards           []string
	TabletType       topodatapb.TabletType
	Session          *vtgatepb.Session
	NotInTransaction bool
}

type queryResponse struct {
	execQuery  *queryExecute
	shardQuery *queryExecuteShards
	reply      *sqltypes.Result
	err        error
}

// querySplitQuery contains all the fields we use to test SplitQuery
type querySplitQuery struct {
	Keyspace            string
	SQL                 string
	BindVariables       map[string]*querypb.BindVariable
	SplitColumns        []string
	SplitCount          int64
	NumRowsPerQueryPart int64
	Algorithm           querypb.SplitQueryRequest_Algorithm
}

type splitQueryResponse struct {
	splitQuery *querySplitQuery
	reply      []*vtgatepb.SplitQueryResponse_Part
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
	bindVariables map[string]*querypb.BindVariable,
	session *vtgatepb.Session,
	expectedResult *sqltypes.Result) {
	conn.execMap[sql] = &queryResponse{
		execQuery: &queryExecute{
			SQL:           sql,
			BindVariables: bindVariables,
			Session:       session,
		},
		reply: expectedResult,
	}
}

// AddShardQuery adds a shard query and expected result.
func (conn *FakeVTGateConn) AddShardQuery(
	sql string,
	bindVariables map[string]*querypb.BindVariable,
	keyspace string,
	shards []string,
	tabletType topodatapb.TabletType,
	session *vtgatepb.Session,
	notInTransaction bool,
	expectedResult *sqltypes.Result) {
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
	bindVariables map[string]*querypb.BindVariable,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
	expectedResult []*vtgatepb.SplitQueryResponse_Part) {

	reply := make([]*vtgatepb.SplitQueryResponse_Part, len(expectedResult))
	copy(reply, expectedResult)
	key := getSplitQueryKey(keyspace, sql, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
	conn.splitQueryMap[key] = &splitQueryResponse{
		splitQuery: &querySplitQuery{
			Keyspace:            keyspace,
			SQL:                 sql,
			BindVariables:       bindVariables,
			SplitColumns:        splitColumns,
			SplitCount:          splitCount,
			NumRowsPerQueryPart: numRowsPerQueryPart,
			Algorithm:           algorithm,
		},
		reply: reply,
		err:   nil,
	}
}

// Execute please see vtgateconn.Impl.Execute
func (conn *FakeVTGateConn) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	response, ok := conn.execMap[sql]
	if !ok {
		return nil, nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVars,
		Session:       session,
	}
	if !reflect.DeepEqual(query, response.execQuery) {
		return nil, nil, fmt.Errorf(
			"Execute: %+v, want %+v", query, response.execQuery)
	}
	var reply sqltypes.Result
	reply = *response.reply
	s := newSession(true, "test_keyspace", []string{}, topodatapb.TabletType_MASTER)
	return s, &reply, nil
}

// ExecuteBatch please see vtgateconn.Impl.ExecuteBatch
func (conn *FakeVTGateConn) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVarsList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	panic("not implemented")
}

// StreamExecute please see vtgateconn.Impl.StreamExecute
func (conn *FakeVTGateConn) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVars map[string]*querypb.BindVariable) (sqltypes.ResultStream, error) {
	response, ok := conn.execMap[sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVars,
		Session:       session,
	}
	if !reflect.DeepEqual(query, response.execQuery) {
		return nil, fmt.Errorf("StreamExecute: %+v, want %+v", sql, response.execQuery)
	}
	if response.err != nil {
		return nil, response.err
	}
	var resultChan chan *sqltypes.Result
	defer close(resultChan)
	if response.reply != nil {
		// create a result channel big enough to buffer all of
		// the responses so we don't need to fork a go routine.
		resultChan = make(chan *sqltypes.Result, len(response.reply.Rows)+1)
		result := &sqltypes.Result{}
		result.Fields = response.reply.Fields
		resultChan <- result
		for _, row := range response.reply.Rows {
			result := &sqltypes.Result{}
			result.Rows = [][]sqltypes.Value{row}
			resultChan <- result
		}
	} else {
		resultChan = make(chan *sqltypes.Result)
	}
	return &streamExecuteAdapter{resultChan}, nil
}

// ExecuteShards please see vtgateconn.Impl.ExecuteShard
func (conn *FakeVTGateConn) ExecuteShards(ctx context.Context, sql string, keyspace string, shards []string, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	var s *vtgatepb.Session
	if session != nil {
		s = session
	}
	response, ok := conn.execMap[getShardQueryKey(sql, shards)]
	if !ok {
		return nil, nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteShards{
		SQL:           sql,
		BindVariables: bindVars,
		TabletType:    tabletType,
		Keyspace:      keyspace,
		Shards:        shards,
		Session:       s,
	}
	if !reflect.DeepEqual(query, response.shardQuery) {
		return nil, nil, fmt.Errorf(
			"ExecuteShards: %+v, want %+v", query, response.shardQuery)
	}
	var reply sqltypes.Result
	reply = *response.reply
	if s != nil {
		s = newSession(true, keyspace, shards, tabletType)
	}
	return s, &reply, nil
}

// ExecuteKeyspaceIds please see vtgateconn.Impl.ExecuteKeyspaceIds
func (conn *FakeVTGateConn) ExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	panic("not implemented")
}

// ExecuteKeyRanges please see vtgateconn.Impl.ExecuteKeyRanges
func (conn *FakeVTGateConn) ExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	panic("not implemented")
}

// ExecuteEntityIds please see vtgateconn.Impl.ExecuteEntityIds
func (conn *FakeVTGateConn) ExecuteEntityIds(ctx context.Context, query string, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	panic("not implemented")
}

// ExecuteBatchShards please see vtgateconn.Impl.ExecuteBatchShards
func (conn *FakeVTGateConn) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.Result, error) {
	panic("not implemented")
}

// ExecuteBatchKeyspaceIds please see vtgateconn.Impl.ExecuteBatchKeyspaceIds
func (conn *FakeVTGateConn) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.Result, error) {
	panic("not implemented")
}

type streamExecuteAdapter struct {
	c chan *sqltypes.Result
}

func (a *streamExecuteAdapter) Recv() (*sqltypes.Result, error) {
	r, ok := <-a.c
	if !ok {
		return nil, io.EOF
	}
	return r, nil
}

// StreamExecuteShards please see vtgateconn.Impl.StreamExecuteShards
func (conn *FakeVTGateConn) StreamExecuteShards(ctx context.Context, query string, keyspace string, shards []string, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	panic("not implemented")
}

// StreamExecuteKeyRanges please see vtgateconn.Impl.StreamExecuteKeyRanges
func (conn *FakeVTGateConn) StreamExecuteKeyRanges(ctx context.Context, query string, keyspace string, keyRanges []*topodatapb.KeyRange, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	panic("not implemented")
}

// StreamExecuteKeyspaceIds please see vtgateconn.Impl.StreamExecuteKeyspaceIds
func (conn *FakeVTGateConn) StreamExecuteKeyspaceIds(ctx context.Context, query string, keyspace string, keyspaceIds [][]byte, bindVars map[string]*querypb.BindVariable, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions) (sqltypes.ResultStream, error) {
	panic("not implemented")
}

// Begin please see vtgateconn.Impl.Begin
func (conn *FakeVTGateConn) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	return &vtgatepb.Session{
		InTransaction: true,
		SingleDb:      singledb,
	}, nil
}

// Commit please see vtgateconn.Impl.Commit
func (conn *FakeVTGateConn) Commit(ctx context.Context, session *vtgatepb.Session, twopc bool) error {
	if session == nil {
		return errors.New("commit: not in transaction")
	}
	return nil
}

// Rollback please see vtgateconn.Impl.Rollback
func (conn *FakeVTGateConn) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	return nil
}

// ResolveTransaction please see vtgateconn.Impl.ResolveTransaction
func (conn *FakeVTGateConn) ResolveTransaction(ctx context.Context, dtid string) error {
	return nil
}

// MessageStream is part of the vtgate service API.
func (conn *FakeVTGateConn) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	panic("not implemented")
}

// MessageAck is part of the vtgate service API.
func (conn *FakeVTGateConn) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	panic("not implemented")
}

// MessageAckKeyspaceIds is part of the vtgate service API.
func (conn *FakeVTGateConn) MessageAckKeyspaceIds(ctx context.Context, keyspace string, name string, idKeyspaceIDs []*vtgatepb.IdKeyspaceId) (int64, error) {
	panic("not implemented")
}

// SplitQuery please see vtgateconn.Impl.SplitQuery
func (conn *FakeVTGateConn) SplitQuery(
	ctx context.Context,
	keyspace string,
	query string,
	bindVars map[string]*querypb.BindVariable,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {

	response, ok := conn.splitQueryMap[getSplitQueryKey(
		keyspace, query, splitColumns, splitCount, numRowsPerQueryPart, algorithm)]
	if !ok {
		return nil, fmt.Errorf(
			"no match for keyspace: %s,"+
				" query: %v,"+
				" splitColumns: %v,"+
				" splitCount: %v"+
				" numRowsPerQueryPart: %v"+
				" algorithm: %v",
			keyspace, query, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
	}
	reply := make([]*vtgatepb.SplitQueryResponse_Part, len(response.reply))
	copy(reply, response.reply)
	return reply, nil
}

// GetSrvKeyspace please see vtgateconn.Impl.GetSrvKeyspace
func (conn *FakeVTGateConn) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return nil, fmt.Errorf("NYI")
}

// UpdateStream please see vtgateconn.Impl.UpdateStream
func (conn *FakeVTGateConn) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken) (vtgateconn.UpdateStreamReader, error) {
	return nil, fmt.Errorf("NYI")
}

// Close please see vtgateconn.Impl.Close
func (conn *FakeVTGateConn) Close() {
}

func getShardQueryKey(sql string, shards []string) string {
	sort.Strings(shards)
	return fmt.Sprintf("%s-%s", sql, strings.Join(shards, ":"))
}

func getSplitQueryKey(
	keyspace string,
	query string,
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) string {
	return fmt.Sprintf(
		"%v:%v:%v:%v:%v:%v",
		keyspace,
		query,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}

func newSession(
	inTransaction bool,
	keyspace string,
	shards []string,
	tabletType topodatapb.TabletType) *vtgatepb.Session {
	shardSessions := make([]*vtgatepb.Session_ShardSession, len(shards))
	for _, shard := range shards {
		shardSessions = append(shardSessions, &vtgatepb.Session_ShardSession{
			Target: &querypb.Target{
				Keyspace:   keyspace,
				Shard:      shard,
				TabletType: tabletType,
			},
			TransactionId: rand.Int63(),
		})
	}
	return &vtgatepb.Session{
		InTransaction: inTransaction,
		ShardSessions: shardSessions,
	}
}

// Make sure FakeVTGateConn implements vtgateconn.Impl
var _ (vtgateconn.Impl) = (*FakeVTGateConn)(nil)
