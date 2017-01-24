// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgateconntest provides the test methods to make sure a
// vtgateconn/vtgateservice pair over RPC works correctly.
package vtgateconntest

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// fakeVTGateService has the server side of this fake
type fakeVTGateService struct {
	t        *testing.T
	panics   bool
	hasError bool
	// If True, calls to Begin will always succeed. This is necessary so that
	// we can test subsequent calls in the transaction (e.g., Commit, Rollback).
	forceBeginSuccess bool
	errorWait         chan struct{}
}

const expectedErrMatch string = "test vtgate error"
const expectedCode vtrpcpb.ErrorCode = vtrpcpb.ErrorCode_BAD_INPUT

var errTestVtGateError = vterrors.FromError(expectedCode, errors.New(expectedErrMatch))

func newContext() context.Context {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, nil)
	return ctx
}

func (f *fakeVTGateService) checkCallerID(ctx context.Context, name string) {
	ef := callerid.EffectiveCallerIDFromContext(ctx)
	if ef == nil {
		f.t.Errorf("no effective caller id for %v", name)
	} else {
		if !reflect.DeepEqual(ef, testCallerID) {
			f.t.Errorf("invalid effective caller id for %v: got %v expected %v", name, ef, testCallerID)
		}
	}
}

// queryExecute contains all the fields we use to test Execute
type queryExecute struct {
	SQL              string
	BindVariables    map[string]interface{}
	Keyspace         string
	TabletType       topodatapb.TabletType
	Session          *vtgatepb.Session
	NotInTransaction bool
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "Execute")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:              sql,
		BindVariables:    bindVariables,
		Keyspace:         keyspace,
		TabletType:       tabletType,
		Session:          session,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.execQuery)
		return nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	return execCase.result, nil
}

// queryExecuteBatch contains all the fields we use to test ExecuteBatch
type queryExecuteBatch struct {
	SQLList           []string
	BindVariablesList []map[string]interface{}
	Keyspace          string
	TabletType        topodatapb.TabletType
	Session           *vtgatepb.Session
	AsTransaction     bool
}

// ExecuteBatch is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatch(ctx context.Context, sqlList []string, bindVariablesList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.QueryResponse, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteBatch")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sqlList[0]]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sqlList)
	}
	query := &queryExecuteBatch{
		SQLList:           sqlList,
		BindVariablesList: bindVariablesList,
		Keyspace:          keyspace,
		TabletType:        tabletType,
		Session:           session,
		AsTransaction:     asTransaction,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.execQuery)
		return nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	return []sqltypes.QueryResponse{{
		QueryResult: execCase.result,
		QueryError:  nil,
	}}, nil
}

// queryExecuteShards contains all the fields we use to test ExecuteShards
type queryExecuteShards struct {
	SQL              string
	BindVariables    map[string]interface{}
	Keyspace         string
	Shards           []string
	TabletType       topodatapb.TabletType
	Session          *vtgatepb.Session
	NotInTransaction bool
}

// ExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteShards")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteShards{
		SQL:              sql,
		BindVariables:    bindVariables,
		TabletType:       tabletType,
		Keyspace:         keyspace,
		Shards:           shards,
		Session:          session,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, execCase.shardQuery) {
		f.t.Errorf("ExecuteShards: %+v, want %+v", query, execCase.shardQuery)
		return nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	return execCase.result, nil
}

// queryExecuteKeyspaceIds contains all the fields we use to test
// ExecuteKeyspaceIds
type queryExecuteKeyspaceIds struct {
	SQL              string
	BindVariables    map[string]interface{}
	Keyspace         string
	KeyspaceIds      [][]byte
	TabletType       topodatapb.TabletType
	Session          *vtgatepb.Session
	NotInTransaction bool
}

// ExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteKeyspaceIds")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteKeyspaceIds{
		SQL:              sql,
		BindVariables:    bindVariables,
		TabletType:       tabletType,
		Keyspace:         keyspace,
		KeyspaceIds:      keyspaceIds,
		Session:          session,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, execCase.keyspaceIDQuery) {
		f.t.Errorf("ExecuteKeyspaceIds: %+v, want %+v", query, execCase.keyspaceIDQuery)
		return nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	return execCase.result, nil
}

// queryExecuteKeyRanges contains all the fields we use to test ExecuteKeyRanges
type queryExecuteKeyRanges struct {
	SQL              string
	BindVariables    map[string]interface{}
	Keyspace         string
	KeyRanges        []*topodatapb.KeyRange
	TabletType       topodatapb.TabletType
	Session          *vtgatepb.Session
	NotInTransaction bool
}

// ExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteKeyRanges")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteKeyRanges{
		SQL:              sql,
		BindVariables:    bindVariables,
		TabletType:       tabletType,
		Keyspace:         keyspace,
		KeyRanges:        keyRanges,
		Session:          session,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, execCase.keyRangeQuery) {
		f.t.Errorf("ExecuteKeyRanges: %+v, want %+v", query, execCase.keyRangeQuery)
		return nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	return execCase.result, nil
}

// queryExecuteEntityIds contains all the fields we use to test ExecuteEntityIds
type queryExecuteEntityIds struct {
	SQL               string
	BindVariables     map[string]interface{}
	Keyspace          string
	EntityColumnName  string
	EntityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId
	TabletType        topodatapb.TabletType
	Session           *vtgatepb.Session
	NotInTransaction  bool
}

// ExecuteEntityIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteEntityIds")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteEntityIds{
		SQL:               sql,
		BindVariables:     bindVariables,
		TabletType:        tabletType,
		Keyspace:          keyspace,
		EntityColumnName:  entityColumnName,
		EntityKeyspaceIDs: entityKeyspaceIDs,
		Session:           session,
		NotInTransaction:  notInTransaction,
	}
	if len(query.EntityKeyspaceIDs) == 1 && len(query.EntityKeyspaceIDs[0].Value) == 0 {
		query.EntityKeyspaceIDs[0].Value = nil
	}
	if !reflect.DeepEqual(query, execCase.entityIdsQuery) {
		f.t.Errorf("ExecuteEntityIds: %+v, want %+v", query, execCase.entityIdsQuery)
		return nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	return execCase.result, nil
}

// queryExecuteBatchShards contains all the fields we use to test
// ExecuteBatchShards
type queryExecuteBatchShards struct {
	Queries       []*vtgatepb.BoundShardQuery
	TabletType    topodatapb.TabletType
	AsTransaction bool
	Session       *vtgatepb.Session
}

// ExecuteBatchShards is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteBatchShards")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[queries[0].Query.Sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", queries[0].Query.Sql)
	}
	query := &queryExecuteBatchShards{
		Queries:       queries,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       session,
	}
	if !reflect.DeepEqual(query, execCase.batchQueryShard) {
		f.t.Errorf("ExecuteBatchShards: %+v, want %+v", query, execCase.batchQueryShard)
		return nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	if execCase.result != nil {
		return []sqltypes.Result{*execCase.result}, nil
	}
	return nil, nil
}

// queryExecuteBatchKeyspaceIds contains all the fields we use to test
// ExecuteBatchKeyspaceIds
type queryExecuteBatchKeyspaceIds struct {
	Queries       []*vtgatepb.BoundKeyspaceIdQuery
	TabletType    topodatapb.TabletType
	AsTransaction bool
	Session       *vtgatepb.Session
}

// ExecuteBatchKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteBatchKeyspaceIds")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[queries[0].Query.Sql]
	if !ok {
		return nil, fmt.Errorf("no match for: %s", queries[0].Query.Sql)
	}
	query := &queryExecuteBatchKeyspaceIds{
		Queries:       queries,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       session,
	}
	if !reflect.DeepEqual(query, execCase.keyspaceIDBatchQuery) {
		f.t.Errorf("ExecuteBatchKeyspaceIds: %+v, want %+v", query, execCase.keyspaceIDBatchQuery)
		return nil, nil
	}
	if execCase.outSession != nil {
		*session = *execCase.outSession
	}
	if execCase.result != nil {
		return []sqltypes.Result{*execCase.result}, nil
	}
	return nil, nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	f.checkCallerID(ctx, "StreamExecute")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVariables,
		Keyspace:      keyspace,
		TabletType:    tabletType,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		f.t.Errorf("StreamExecute: %+v, want %+v", query, execCase.execQuery)
		return nil
	}
	if execCase.result != nil {
		result := &sqltypes.Result{
			Fields: execCase.result.Fields,
		}
		if err := sendReply(result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.result.Rows {
			result := &sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}
			if err := sendReply(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// StreamExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "StreamExecuteShards")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteShards{
		SQL:           sql,
		BindVariables: bindVariables,
		Keyspace:      keyspace,
		Shards:        shards,
		TabletType:    tabletType,
	}
	if !reflect.DeepEqual(query, execCase.shardQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.shardQuery)
		return nil
	}
	if execCase.result != nil {
		result := &sqltypes.Result{
			Fields: execCase.result.Fields,
		}
		if err := sendReply(result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.result.Rows {
			result := &sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}
			if err := sendReply(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// StreamExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "StreamExecuteKeyspaceIds")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteKeyspaceIds{
		SQL:           sql,
		BindVariables: bindVariables,
		Keyspace:      keyspace,
		KeyspaceIds:   keyspaceIds,
		TabletType:    tabletType,
	}
	if !reflect.DeepEqual(query, execCase.keyspaceIDQuery) {
		f.t.Errorf("StreamExecuteKeyspaceIds: %+v, want %+v", query, execCase.keyspaceIDQuery)
		return nil
	}
	if execCase.result != nil {
		result := &sqltypes.Result{
			Fields: execCase.result.Fields,
		}
		if err := sendReply(result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.result.Rows {
			result := &sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}
			if err := sendReply(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// StreamExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, sendReply func(*sqltypes.Result) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "StreamExecuteKeyRanges")
	if !proto.Equal(options, testExecuteOptions) {
		f.t.Errorf("wrong Execute options, got %+v, want %+v", options, testExecuteOptions)
	}
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecuteKeyRanges{
		SQL:           sql,
		BindVariables: bindVariables,
		Keyspace:      keyspace,
		KeyRanges:     keyRanges,
		TabletType:    tabletType,
	}
	if !reflect.DeepEqual(query, execCase.keyRangeQuery) {
		f.t.Errorf("StreamExecuteKeyRanges: %+v, want %+v", query, execCase.keyRangeQuery)
		return nil
	}
	if execCase.result != nil {
		result := &sqltypes.Result{
			Fields: execCase.result.Fields,
		}
		if err := sendReply(result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.result.Rows {
			result := &sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}
			if err := sendReply(result); err != nil {
				return err
			}
		}
	}
	return nil
}

// Begin is part of the VTGateService interface
func (f *fakeVTGateService) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	f.checkCallerID(ctx, "Begin")
	switch {
	case f.forceBeginSuccess:
	case f.hasError:
		return nil, errTestVtGateError
	case f.panics:
		panic(fmt.Errorf("test forced panic"))
	default:
	}
	if singledb {
		// Communicate this as an error.
		return nil, errors.New("single db")
	}
	return session1, nil
}

// Commit is part of the VTGateService interface
func (f *fakeVTGateService) Commit(ctx context.Context, twopc bool, inSession *vtgatepb.Session) error {
	f.checkCallerID(ctx, "Commit")
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	if twopc {
		// Communicate this as an error.
		return errors.New("twopc")
	}
	if !reflect.DeepEqual(inSession, session2) {
		return errors.New("commit: session mismatch")
	}
	return nil
}

// Rollback is part of the VTGateService interface
func (f *fakeVTGateService) Rollback(ctx context.Context, inSession *vtgatepb.Session) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "Rollback")
	if !reflect.DeepEqual(inSession, session2) {
		return errors.New("rollback: session mismatch")
	}
	return nil
}

// ResolveTransaction is part of the VTGateService interface
func (f *fakeVTGateService) ResolveTransaction(ctx context.Context, dtid string) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ResolveTransaction")
	if dtid != dtid2 {
		return errors.New("ResolveTransaction: dtid mismatch")
	}
	return nil
}

func (f *fakeVTGateService) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, sendReply func(*sqltypes.Result) error) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ResolveTransaction")
	if name != messageName {
		return errors.New("MessageStream name mismatch")
	}
	sendReply(messageStreamResult)
	return nil
}

func (f *fakeVTGateService) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	if f.hasError {
		return 0, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ResolveTransaction")
	if !reflect.DeepEqual(ids, messageids) {
		return 0, errors.New("MessageAck ids mismatch")
	}
	return messageAckRowsAffected, nil
}

// querySplitQuery contains all the fields we use to test SplitQuery
type querySplitQuery struct {
	Keyspace            string
	SQL                 string
	BindVariables       map[string]interface{}
	SplitColumns        []string
	SplitCount          int64
	NumRowsPerQueryPart int64
	Algorithm           querypb.SplitQueryRequest_Algorithm
}

// SplitQuery is part of the VTGateService interface
func (f *fakeVTGateService) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "SplitQuery")
	query := &querySplitQuery{
		Keyspace:            keyspace,
		SQL:                 sql,
		BindVariables:       bindVariables,
		SplitColumns:        splitColumns,
		SplitCount:          splitCount,
		NumRowsPerQueryPart: numRowsPerQueryPart,
		Algorithm:           algorithm,
	}
	if !reflect.DeepEqual(query, splitQueryRequest) {
		f.t.Errorf("SplitQuery has wrong input: got %#v wanted %#v", query, splitQueryRequest)
	}
	return splitQueryResult, nil
}

// GetSrvKeyspace is part of the VTGateService interface
func (f *fakeVTGateService) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	if keyspace != getSrvKeyspaceKeyspace {
		f.t.Errorf("GetSrvKeyspace has wrong input: got %v wanted %v", keyspace, getSrvKeyspaceKeyspace)
	}
	return getSrvKeyspaceResult, nil
}

// queryUpdateStream contains all the fields we use to test UpdateStream
type queryUpdateStream struct {
	Keyspace   string
	Shard      string
	KeyRange   *topodatapb.KeyRange
	TabletType topodatapb.TabletType
	Timestamp  int64
	Event      *querypb.EventToken
}

// UpdateStream is part of the VTGateService interface
func (f *fakeVTGateService) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, sendReply func(*querypb.StreamEvent, int64) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[shard]
	if !ok {
		return fmt.Errorf("no match for: %s", shard)
	}
	f.checkCallerID(ctx, "UpdateStream")
	query := &queryUpdateStream{
		Keyspace:   keyspace,
		Shard:      shard,
		KeyRange:   keyRange,
		TabletType: tabletType,
		Timestamp:  timestamp,
		Event:      event,
	}
	if !reflect.DeepEqual(query, execCase.updateStreamQuery) {
		f.t.Errorf("UpdateStream: %+v, want %+v", query, execCase.updateStreamQuery)
		return nil
	}
	if execCase.result != nil {
		// The first result only has statement with fields.
		result := &querypb.StreamEvent{
			Statements: []*querypb.StreamEvent_Statement{
				{
					PrimaryKeyFields: execCase.result.Fields,
				},
			},
		}
		if err := sendReply(result, int64(execCase.result.RowsAffected)); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.result.Rows {

			result := &querypb.StreamEvent{
				Statements: []*querypb.StreamEvent_Statement{
					{
						PrimaryKeyValues: sqltypes.RowsToProto3([][]sqltypes.Value{row}),
					},
				},
			}
			if err := sendReply(result, int64(execCase.result.RowsAffected)); err != nil {
				return err
			}
		}
	}
	return nil
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) vtgateservice.VTGateService {
	return &fakeVTGateService{
		t:         t,
		panics:    false,
		errorWait: make(chan struct{}),
	}
}

// RegisterTestDialProtocol registers a vtgateconn implementation under the "test" protocol
func RegisterTestDialProtocol(impl vtgateconn.Impl) {
	vtgateconn.RegisterDialer("test", func(ctx context.Context, address string, timeout time.Duration) (vtgateconn.Impl, error) {
		return impl, nil
	})
}

// HandlePanic is part of the VTGateService interface
func (f *fakeVTGateService) HandlePanic(err *error) {
	if x := recover(); x != nil {
		// gRPC 0.13 chokes when you return a streaming error that contains newlines.
		*err = fmt.Errorf("uncaught panic: %v, %s", x,
			strings.Replace(string(tb.Stack(4)), "\n", ";", -1))
	}
}

// TestSuite runs all the tests
func TestSuite(t *testing.T, impl vtgateconn.Impl, fakeServer vtgateservice.VTGateService) {
	vtgateconn.RegisterDialer("test", func(ctx context.Context, address string, timeout time.Duration) (vtgateconn.Impl, error) {
		return impl, nil
	})
	conn, err := vtgateconn.DialProtocol(context.Background(), "test", "", 0, "connection_ks")
	if err != nil {
		t.Fatalf("Got err: %v from vtgateconn.DialProtocol", err)
	}

	fs := fakeServer.(*fakeVTGateService)

	testBegin(t, conn)
	testCommit(t, conn)
	testExecute(t, conn)
	testExecuteShards(t, conn)
	testExecuteKeyspaceIds(t, conn)
	testExecuteKeyRanges(t, conn)
	testExecuteEntityIds(t, conn)
	testExecuteBatchShards(t, conn)
	testExecuteBatchKeyspaceIds(t, conn)
	testStreamExecute(t, conn)
	testStreamExecuteShards(t, conn)
	testStreamExecuteKeyRanges(t, conn)
	testStreamExecuteKeyspaceIds(t, conn)
	testTxPass(t, conn)
	testResolveTransaction(t, conn)
	testTxFail(t, conn)
	testMessageStream(t, conn)
	testMessageAck(t, conn)
	testSplitQuery(t, conn)
	testGetSrvKeyspace(t, conn)
	testUpdateStream(t, conn)

	// force a panic at every call, then test that works
	fs.panics = true
	testBeginPanic(t, conn)
	testCommitPanic(t, conn, fs)
	testRollbackPanic(t, conn, fs)
	testResolveTransactionPanic(t, conn, fs)
	testExecutePanic(t, conn)
	testExecuteShardsPanic(t, conn)
	testExecuteKeyspaceIdsPanic(t, conn)
	testExecuteKeyRangesPanic(t, conn)
	testExecuteEntityIdsPanic(t, conn)
	testExecuteBatchShardsPanic(t, conn)
	testExecuteBatchKeyspaceIdsPanic(t, conn)
	testStreamExecutePanic(t, conn)
	testStreamExecuteShardsPanic(t, conn)
	testStreamExecuteKeyRangesPanic(t, conn)
	testStreamExecuteKeyspaceIdsPanic(t, conn)
	testMessageStreamPanic(t, conn)
	testMessageAckPanic(t, conn)
	testSplitQueryPanic(t, conn)
	testGetSrvKeyspacePanic(t, conn)
	testUpdateStreamPanic(t, conn)
	fs.panics = false
}

// TestErrorSuite runs all the tests that expect errors
func TestErrorSuite(t *testing.T, fakeServer vtgateservice.VTGateService) {
	conn, err := vtgateconn.DialProtocol(context.Background(), "test", "", 0, "connection_ks")
	if err != nil {
		t.Fatalf("Got err: %v from vtgateconn.DialProtocol", err)
	}

	fs := fakeServer.(*fakeVTGateService)

	// return an error for every call, make sure they're handled properly
	fs.hasError = true
	testBeginError(t, conn)
	testCommitError(t, conn, fs)
	testRollbackError(t, conn, fs)
	testResolveTransactionError(t, conn, fs)
	testExecuteError(t, conn, fs)
	testExecuteShardsError(t, conn, fs)
	testExecuteKeyspaceIdsError(t, conn, fs)
	testExecuteKeyRangesError(t, conn, fs)
	testExecuteEntityIdsError(t, conn, fs)
	testExecuteBatchShardsError(t, conn, fs)
	testExecuteBatchKeyspaceIdsError(t, conn, fs)
	testStreamExecuteError(t, conn, fs)
	testStreamExecuteShardsError(t, conn, fs)
	testStreamExecuteKeyRangesError(t, conn, fs)
	testStreamExecuteKeyspaceIdsError(t, conn, fs)
	testMessageStreamError(t, conn)
	testMessageAckError(t, conn)
	testSplitQueryError(t, conn)
	testGetSrvKeyspaceError(t, conn)
	testUpdateStreamError(t, conn, fs)
	fs.hasError = false
}

func expectPanic(t *testing.T, err error) {
	expected1 := "test forced panic"
	expected2 := "uncaught panic"
	if err == nil || !strings.Contains(err.Error(), expected1) || !strings.Contains(err.Error(), expected2) {
		t.Fatalf("Expected a panic error with '%v' or '%v' but got: %v", expected1, expected2, err)
	}
}

// Verifies the returned error has the properties that we expect.
func verifyError(t *testing.T, err error, method string) {
	if err == nil {
		t.Errorf("%s was expecting an error, didn't get one", method)
		return
	}
	// verify error code
	code := vterrors.RecoverVtErrorCode(err)
	if code != expectedCode {
		t.Errorf("Unexpected error code from %s: got %v, wanted %v", method, code, expectedCode)
	}
	// verify error type
	if _, ok := err.(*vterrors.VitessError); !ok {
		t.Errorf("Unexpected error type from %s: got %v, wanted *vterrors.VitessError", method, reflect.TypeOf(err))
	}
	verifyErrorString(t, err, method)
}

func verifyErrorString(t *testing.T, err error, method string) {
	if err == nil {
		t.Errorf("%s was expecting an error, didn't get one", method)
		return
	}

	if !strings.Contains(err.Error(), expectedErrMatch) {
		t.Errorf("Unexpected error from %s: got %v, wanted err containing: %v", method, err, errTestVtGateError.Error())
	}
}

func testBegin(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := vtgateconn.WithAtomicity(newContext(), vtgateconn.AtomicitySingle)
	_, err := conn.Begin(ctx)
	want := "single db"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Begin(singldb): %v, want %v", err, want)
	}
}

func testCommit(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := vtgateconn.WithAtomicity(newContext(), vtgateconn.Atomicity2PC)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	err = tx.Commit(ctx)
	want := "twopc"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit(twopc): %v, want %v", err, want)
	}
}

func testExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.result) {
		t.Errorf("Unexpected result from Execute: got\n%#v want\n%#v", qr, execCase.result)
	}

	_, err = conn.Execute(ctx, "none", nil, topodatapb.TabletType_RDONLY, testExecuteOptions)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := conn.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	verifyError(t, err, "Execute")
}

func testExecutePanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	expectPanic(t, err)
}

func testExecuteShards(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.result)
	}

	_, err = conn.ExecuteShards(ctx, "none", "", []string{}, nil, topodatapb.TabletType_RDONLY, testExecuteOptions)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteShardsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := conn.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, testExecuteOptions)
	verifyError(t, err, "ExecuteShards")
}

func testExecuteShardsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteShards(ctx, execCase.execQuery.SQL, "ks", []string{"1", "2"}, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	expectPanic(t, err)
}

func testExecuteKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.result)
	}

	_, err = conn.ExecuteKeyspaceIds(ctx, "none", "", [][]byte{}, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteKeyspaceIdsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, testExecuteOptions)
	verifyError(t, err, "ExecuteKeyspaceIds")
}

func testExecuteKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, testExecuteOptions)
	expectPanic(t, err)
}

func testExecuteKeyRanges(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.result)
	}

	_, err = conn.ExecuteKeyRanges(ctx, "none", "", []*topodatapb.KeyRange{}, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteKeyRangesError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, testExecuteOptions)
	verifyError(t, err, "ExecuteKeyRanges")
}

func testExecuteKeyRangesPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, testExecuteOptions)
	expectPanic(t, err)
}

func testExecuteEntityIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.result)
	}

	_, err = conn.ExecuteEntityIds(ctx, "none", "", "", []*vtgatepb.ExecuteEntityIdsRequest_EntityId{}, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteEntityIdsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, testExecuteOptions)
	verifyError(t, err, "ExecuteEntityIds")
}

func testExecuteEntityIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, testExecuteOptions)
	expectPanic(t, err)
}

func testExecuteBatchShards(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	ql, err := conn.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(&ql[0], execCase.result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", ql, execCase.result)
	}

	_, err = conn.ExecuteBatchShards(ctx, []*vtgatepb.BoundShardQuery{
		{Query: &querypb.BoundQuery{Sql: "none"}}},
		topodatapb.TabletType_REPLICA, true, testExecuteOptions)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteBatchShardsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := conn.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction, testExecuteOptions)
	verifyError(t, err, "ExecuteBatchShards")
}

func testExecuteBatchShardsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction, testExecuteOptions)
	expectPanic(t, err)
}

func testExecuteBatchKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	ql, err := conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.batchQueryShard.AsTransaction, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(&ql[0], execCase.result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", ql, execCase.result)
	}

	_, err = conn.ExecuteBatchKeyspaceIds(ctx, []*vtgatepb.BoundKeyspaceIdQuery{
		{Query: &querypb.BoundQuery{Sql: "none"}, KeyspaceIds: [][]byte{}}},
		topodatapb.TabletType_REPLICA, false, testExecuteOptions)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testExecuteBatchKeyspaceIdsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	_, err := conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction, testExecuteOptions)
	verifyError(t, err, "ExecuteBatchKeyspaceIds")
}

func testExecuteBatchKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction, testExecuteOptions)
	expectPanic(t, err)
}

func testStreamExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	var qr sqltypes.Result
	for {
		packet, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.result
	wantResult.RowsAffected = 0
	wantResult.InsertID = 0
	wantResult.Extras = nil
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from StreamExecute: got %+v want %+v", qr, wantResult)
	}

	stream, err = conn.StreamExecute(ctx, "none", nil, topodatapb.TabletType_RDONLY, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testStreamExecuteError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatalf("StreamExecute failed: %v", err)
	}
	qr, err := stream.Recv()
	if err != nil {
		t.Fatalf("StreamExecute failed: cannot read result1: %v", err)
	}

	if !reflect.DeepEqual(qr, &streamResultFields) {
		t.Errorf("Unexpected result from StreamExecute: got %#v want %#v", qr, &streamResultFields)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, err = stream.Recv()
	if err == nil {
		t.Fatalf("StreamExecute channel wasn't closed")
	}
	verifyError(t, err, "StreamExecute")
}

func testStreamExecutePanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("Received packets instead of panic?")
	}
	expectPanic(t, err)
}

func testStreamExecuteShards(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	var qr sqltypes.Result
	for {
		packet, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.result
	wantResult.RowsAffected = 0
	wantResult.InsertID = 0
	wantResult.Extras = nil
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from StreamExecuteShards: got %+v want %+v", qr, wantResult)
	}

	stream, err = conn.StreamExecuteShards(ctx, "none", "", []string{}, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testStreamExecuteShardsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatalf("StreamExecuteShards failed: %v", err)
	}
	qr, err := stream.Recv()
	if err != nil {
		t.Fatalf("StreamExecuteShards failed: cannot read result1: %v", err)
	}

	if !reflect.DeepEqual(qr, &streamResultFields) {
		t.Errorf("Unexpected result from StreamExecuteShards: got %#v want %#v", qr, &streamResultFields)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, err = stream.Recv()
	if err == nil {
		t.Fatalf("StreamExecuteShards channel wasn't closed")
	}
	verifyError(t, err, "StreamExecuteShards")
}

func testStreamExecuteShardsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("Received packets instead of panic?")
	}
	expectPanic(t, err)
}

func testStreamExecuteKeyRanges(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	var qr sqltypes.Result
	for {
		packet, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.result
	wantResult.RowsAffected = 0
	wantResult.InsertID = 0
	wantResult.Extras = nil
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from StreamExecuteKeyRanges: got %+v want %+v", qr, wantResult)
	}

	stream, err = conn.StreamExecuteKeyRanges(ctx, "none", "", []*topodatapb.KeyRange{}, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testStreamExecuteKeyRangesError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatalf("StreamExecuteKeyRanges failed: %v", err)
	}
	qr, err := stream.Recv()
	if err != nil {
		t.Fatalf("StreamExecuteKeyRanges failed: cannot read result1: %v", err)
	}

	if !reflect.DeepEqual(qr, &streamResultFields) {
		t.Errorf("Unexpected result from StreamExecuteKeyRanges: got %#v want %#v", qr, &streamResultFields)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, err = stream.Recv()
	if err == nil {
		t.Fatalf("StreamExecuteKeyRanges channel wasn't closed")
	}
	verifyError(t, err, "StreamExecuteKeyRanges")
}

func testStreamExecuteKeyRangesPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("Received packets instead of panic?")
	}
	expectPanic(t, err)
}

func testStreamExecuteKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	var qr sqltypes.Result
	for {
		packet, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.result
	wantResult.RowsAffected = 0
	wantResult.InsertID = 0
	wantResult.Extras = nil
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from StreamExecuteKeyspaceIds: got %+v want %+v", qr, wantResult)
	}

	stream, err = conn.StreamExecuteKeyspaceIds(ctx, "none", "", [][]byte{}, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testStreamExecuteKeyspaceIdsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatalf("StreamExecuteKeyspaceIds failed: %v", err)
	}
	qr, err := stream.Recv()
	if err != nil {
		t.Fatalf("StreamExecuteKeyspaceIds failed: cannot read result1: %v", err)
	}

	if !reflect.DeepEqual(qr, &streamResultFields) {
		t.Errorf("Unexpected result from StreamExecuteKeyspaceIds: got %#v want %#v", qr, &streamResultFields)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, err = stream.Recv()
	if err == nil {
		t.Fatalf("StreamExecuteKeyspaceIds channel wasn't closed")
	}
	verifyError(t, err, "StreamExecuteKeyspaceIds")
}

func testStreamExecuteKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatalf("Received packets instead of panic?")
	}
	expectPanic(t, err)
}

func testTxPass(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["txRequest"]

	// Execute
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteShards
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteKeyspaceIds
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteKeyRanges
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteEntityIds
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteBatchShards
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteBatchKeyspaceIds
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, testExecuteOptions)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}
}

func testResolveTransaction(t *testing.T, conn *vtgateconn.VTGateConn) {
	if err := conn.ResolveTransaction(newContext(), dtid2); err != nil {
		t.Error(err)
	}
}

func testBeginError(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.Begin(ctx)
	verifyError(t, err, "Begin")
}

func testCommitError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()

	fake.forceBeginSuccess = true
	tx, err := conn.Begin(ctx)
	fake.forceBeginSuccess = false

	if err != nil {
		t.Error(err)
	}
	err = tx.Commit(ctx)
	verifyError(t, err, "Commit")
}

func testRollbackError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()

	fake.forceBeginSuccess = true
	tx, err := conn.Begin(ctx)
	fake.forceBeginSuccess = false

	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	verifyError(t, err, "Rollback")
}

func testResolveTransactionError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	err := conn.ResolveTransaction(newContext(), "")
	verifyError(t, err, "ResolveTransaction")
}

func testBeginPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.Begin(ctx)
	expectPanic(t, err)
}

func testCommitPanic(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()

	fake.forceBeginSuccess = true
	tx, err := conn.Begin(ctx)
	fake.forceBeginSuccess = false

	if err != nil {
		t.Error(err)
	}
	err = tx.Commit(ctx)
	expectPanic(t, err)
}

func testRollbackPanic(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()

	fake.forceBeginSuccess = true
	tx, err := conn.Begin(ctx)
	fake.forceBeginSuccess = false

	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	expectPanic(t, err)
}

func testResolveTransactionPanic(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	err := conn.ResolveTransaction(newContext(), "")
	expectPanic(t, err)
}

func testTxFail(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	err = tx.Commit(ctx)
	want := "commit: session mismatch"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit: %v, want %v", err, want)
	}

	_, err = tx.Execute(ctx, "", nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want = "execute: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %v", err, want)
	}

	_, err = tx.ExecuteShards(ctx, "", "", nil, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want = "executeShards: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteKeyspaceIds(ctx, "", "", nil, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want = "executeKeyspaceIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteKeyRanges(ctx, "", "", nil, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want = "executeKeyRanges: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteEntityIds(ctx, "", "", "", nil, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want = "executeEntityIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteBatchShards(ctx, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want = "executeBatchShards: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteBatchKeyspaceIds(ctx, nil, topodatapb.TabletType_REPLICA, testExecuteOptions)
	want = "executeBatchKeyspaceIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	err = tx.Commit(ctx)
	want = "commit: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Commit: %v, want %v", err, want)
	}

	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	want = "rollback: session mismatch"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Rollback: %v, want %v", err, want)
	}
}

func testMessageStream(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	err := conn.MessageStream(ctx, "", "", nil, messageName, func(qr *sqltypes.Result) error {
		if !reflect.DeepEqual(qr, messageStreamResult) {
			t.Errorf("reply: %v, want %v", qr, messageStreamResult)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func testMessageStreamError(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	err := conn.MessageStream(ctx, "", "", nil, messageName, func(qr *sqltypes.Result) error {
		return nil
	})
	verifyError(t, err, "MessageStream")
}

func testMessageStreamPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	err := conn.MessageStream(ctx, "", "", nil, messageName, func(qr *sqltypes.Result) error {
		return nil
	})
	expectPanic(t, err)
}

func testMessageAck(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	got, err := conn.MessageAck(ctx, "", messageName, messageids)
	if got != messageAckRowsAffected {
		t.Errorf("MessageAck: %d, want %d", got, messageAckRowsAffected)
	}
	if err != nil {
		t.Error(err)
	}
}

func testMessageAckError(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.MessageAck(ctx, "", messageName, messageids)
	verifyError(t, err, "MessageAck")
}

func testMessageAckPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.MessageAck(ctx, "", messageName, messageids)
	expectPanic(t, err)
}

func testSplitQuery(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	qsl, err := conn.SplitQuery(ctx,
		splitQueryRequest.Keyspace,
		splitQueryRequest.SQL,
		splitQueryRequest.BindVariables,
		splitQueryRequest.SplitColumns,
		splitQueryRequest.SplitCount,
		splitQueryRequest.NumRowsPerQueryPart,
		splitQueryRequest.Algorithm,
	)
	if err != nil {
		t.Fatalf("SplitQuery failed: %v", err)
	}
	if len(qsl) == 1 && len(qsl[0].Query.BindVariables) == 1 {
		bv := qsl[0].Query.BindVariables["bind1"]
		if len(bv.Values) == 0 {
			bv.Values = nil
		}
	}
	if !reflect.DeepEqual(qsl, splitQueryResult) {
		t.Errorf("SplitQuery returned wrong result: got %#v wanted %#v", qsl, splitQueryResult)
	}
}

func testSplitQueryError(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.SplitQuery(ctx,
		splitQueryRequest.Keyspace,
		splitQueryRequest.SQL,
		splitQueryRequest.BindVariables,
		splitQueryRequest.SplitColumns,
		splitQueryRequest.SplitCount,
		splitQueryRequest.NumRowsPerQueryPart,
		splitQueryRequest.Algorithm,
	)
	verifyError(t, err, "SplitQuery")
}

func testSplitQueryPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.SplitQuery(ctx,
		splitQueryRequest.Keyspace,
		splitQueryRequest.SQL,
		splitQueryRequest.BindVariables,
		splitQueryRequest.SplitColumns,
		splitQueryRequest.SplitCount,
		splitQueryRequest.NumRowsPerQueryPart,
		splitQueryRequest.Algorithm,
	)
	expectPanic(t, err)
}

func testGetSrvKeyspace(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	sk, err := conn.GetSrvKeyspace(ctx, getSrvKeyspaceKeyspace)
	if err != nil {
		t.Fatalf("GetSrvKeyspace failed: %v", err)
	}
	if !reflect.DeepEqual(sk, getSrvKeyspaceResult) {
		t.Errorf("GetSrvKeyspace returned wrong result: got %+v wanted %+v", sk, getSrvKeyspaceResult)
	}
}

func testGetSrvKeyspaceError(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.GetSrvKeyspace(ctx, getSrvKeyspaceKeyspace)
	verifyErrorString(t, err, "GetSrvKeyspace")
}

func testGetSrvKeyspacePanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.GetSrvKeyspace(ctx, getSrvKeyspaceKeyspace)
	expectPanic(t, err)
}

func testUpdateStream(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.UpdateStream(ctx, execCase.updateStreamQuery.Shard, execCase.updateStreamQuery.KeyRange, execCase.execQuery.TabletType, execCase.updateStreamQuery.Timestamp, execCase.updateStreamQuery.Event)
	if err != nil {
		t.Fatal(err)
	}
	var qr querypb.QueryResult
	for {
		packet, resumeTimestamp, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				t.Error(err)
			}
			break
		}
		qr.RowsAffected = uint64(resumeTimestamp)
		if len(packet.Statements[0].PrimaryKeyFields) != 0 {
			qr.Fields = packet.Statements[0].PrimaryKeyFields
		}
		if len(packet.Statements[0].PrimaryKeyValues) != 0 {
			qr.Rows = append(qr.Rows, packet.Statements[0].PrimaryKeyValues...)
		}
	}

	sqr := sqltypes.Proto3ToResult(&qr)
	wantResult := *execCase.result
	wantResult.InsertID = 0
	wantResult.Extras = nil
	if !reflect.DeepEqual(sqr, &wantResult) {
		t.Errorf("Unexpected result from UpdateStream: got %+v want %+v", sqr, wantResult)
	}

	stream, err = conn.UpdateStream(ctx, "none", nil, topodatapb.TabletType_RDONLY, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = stream.Recv()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}
}

func testUpdateStreamError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.UpdateStream(ctx, execCase.updateStreamQuery.Shard, execCase.updateStreamQuery.KeyRange, execCase.execQuery.TabletType, execCase.updateStreamQuery.Timestamp, execCase.updateStreamQuery.Event)
	if err != nil {
		t.Fatalf("UpdateStream failed: %v", err)
	}
	qr, _, err := stream.Recv()
	if err != nil {
		t.Fatalf("UpdateStream failed: cannot read result1: %v", err)
	}

	if !reflect.DeepEqual(qr.Statements[0].PrimaryKeyFields, execCase.result.Fields) {
		t.Errorf("Unexpected result from UpdateStream: got %#v want %#v", qr.Statements[0].PrimaryKeyFields, execCase.result.Fields)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, _, err = stream.Recv()
	if err == nil {
		t.Fatalf("UpdateStream channel wasn't closed")
	}
	verifyError(t, err, "UpdateStream")
}

func testUpdateStreamPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, err := conn.UpdateStream(ctx, execCase.updateStreamQuery.Shard, execCase.updateStreamQuery.KeyRange, execCase.execQuery.TabletType, execCase.updateStreamQuery.Timestamp, execCase.updateStreamQuery.Event)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = stream.Recv()
	if err == nil {
		t.Fatalf("Received packets instead of panic?")
	}
	expectPanic(t, err)
}

var testCallerID = &vtrpcpb.CallerID{
	Principal:    "test_principal",
	Component:    "test_component",
	Subcomponent: "test_subcomponent",
}

var testExecuteOptions = &querypb.ExecuteOptions{
	IncludedFields:    querypb.ExecuteOptions_TYPE_ONLY,
	IncludeEventToken: true,
	CompareEventToken: &querypb.EventToken{
		Timestamp: 135,
		Shard:     "shrd",
		Position:  "pstn",
	},
}

var execMap = map[string]struct {
	execQuery            *queryExecute
	shardQuery           *queryExecuteShards
	keyspaceIDQuery      *queryExecuteKeyspaceIds
	keyRangeQuery        *queryExecuteKeyRanges
	entityIdsQuery       *queryExecuteEntityIds
	batchQueryShard      *queryExecuteBatchShards
	keyspaceIDBatchQuery *queryExecuteBatchKeyspaceIds
	updateStreamQuery    *queryUpdateStream
	result               *sqltypes.Result
	outSession           *vtgatepb.Session
	err                  error
}{
	"request1": {
		execQuery: &queryExecute{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace:   "connection_ks",
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		shardQuery: &queryExecuteShards{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace:   "ks",
			Shards:     []string{"1", "2"},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		keyspaceIDQuery: &queryExecuteKeyspaceIds{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace: "ks",
			KeyspaceIds: [][]byte{
				{0x61},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		keyRangeQuery: &queryExecuteKeyRanges{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace: "ks",
			KeyRanges: []*topodatapb.KeyRange{
				{
					Start: []byte{0x72},
					End:   []byte{0x90},
				},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		entityIdsQuery: &queryExecuteEntityIds{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []*vtgatepb.ExecuteEntityIdsRequest_EntityId{
				{
					Type:       sqltypes.VarBinary,
					Value:      []byte{105, 100, 49},
					KeyspaceId: []byte{0x6B},
				},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		batchQueryShard: &queryExecuteBatchShards{
			Queries: []*vtgatepb.BoundShardQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "request1",
						BindVariables: map[string]*querypb.BindVariable{
							"bind1": {
								Type:  sqltypes.Int64,
								Value: []byte("11143"),
							},
						},
					},
					Keyspace: "ks",
					Shards:   []string{"-80", "80-"},
				},
			},
			TabletType:    topodatapb.TabletType_RDONLY,
			AsTransaction: true,
			Session:       nil,
		},
		keyspaceIDBatchQuery: &queryExecuteBatchKeyspaceIds{
			Queries: []*vtgatepb.BoundKeyspaceIdQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "request1",
						BindVariables: map[string]*querypb.BindVariable{
							"bind1": {
								Type:  sqltypes.Int64,
								Value: []byte("11143"),
							},
						},
					},
					Keyspace: "ks",
					KeyspaceIds: [][]byte{
						{'k', 'i', '1'},
					},
				},
			},
			TabletType:    topodatapb.TabletType_RDONLY,
			AsTransaction: true,
			Session:       nil,
		},
		updateStreamQuery: &queryUpdateStream{
			Keyspace: "connection_ks",
			Shard:    "request1",
			KeyRange: &topodatapb.KeyRange{
				Start: []byte{0x72},
				End:   []byte{0x90},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Timestamp:  123789,
			Event: &querypb.EventToken{
				Timestamp: 1234567,
				Shard:     "request1",
				Position:  "streaming_position",
			},
		},
		result:     &result1,
		outSession: nil,
	},
	"errorRequst": {
		execQuery: &queryExecute{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:   "connection_ks",
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		shardQuery: &queryExecuteShards{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Keyspace:   "",
			Shards:     []string{"s1", "s2"},
			Session:    nil,
		},
		keyspaceIDQuery: &queryExecuteKeyspaceIds{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyspaceIds: [][]byte{
				{0x61},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		keyRangeQuery: &queryExecuteKeyRanges{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []*topodatapb.KeyRange{
				{
					Start: []byte{0x73},
					End:   []byte{0x99},
				},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		entityIdsQuery: &queryExecuteEntityIds{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []*vtgatepb.ExecuteEntityIdsRequest_EntityId{
				{
					Type:       sqltypes.VarBinary,
					Value:      []byte{105, 100, 49},
					KeyspaceId: []byte{0x6B},
				},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    nil,
		},
		batchQueryShard: &queryExecuteBatchShards{
			Queries: []*vtgatepb.BoundShardQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "errorRequst",
						BindVariables: map[string]*querypb.BindVariable{
							"bind1": {
								Type:  sqltypes.Int64,
								Value: []byte("11143"),
							},
						},
					},
					Keyspace: "ks",
					Shards:   []string{"-80", "80-"},
				},
			},
			TabletType:    topodatapb.TabletType_RDONLY,
			AsTransaction: false,
			Session:       nil,
		},
		keyspaceIDBatchQuery: &queryExecuteBatchKeyspaceIds{
			Queries: []*vtgatepb.BoundKeyspaceIdQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "errorRequst",
						BindVariables: map[string]*querypb.BindVariable{
							"bind1": {
								Type:  sqltypes.Int64,
								Value: []byte("11143"),
							},
						},
					},
					Keyspace: "ks",
					KeyspaceIds: [][]byte{
						{'k', 'i', '1'},
					},
				},
			},
			TabletType:    topodatapb.TabletType_RDONLY,
			AsTransaction: false,
			Session:       nil,
		},
		updateStreamQuery: &queryUpdateStream{
			Keyspace: "connection_ks",
			Shard:    "errorRequst",
			KeyRange: &topodatapb.KeyRange{
				Start: []byte{0x72},
				End:   []byte{0x90},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Timestamp:  123789,
			Event: &querypb.EventToken{
				Timestamp: 1234567,
				Shard:     "request1",
				Position:  "streaming_position",
			},
		},
		result:     nil,
		outSession: nil,
	},
	"txRequest": {
		execQuery: &queryExecute{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace:   "connection_ks",
			TabletType: topodatapb.TabletType_MASTER,
			Session:    session1,
		},
		shardQuery: &queryExecuteShards{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			TabletType: topodatapb.TabletType_MASTER,
			Keyspace:   "",
			Shards:     []string{"s1", "s2"},
			Session:    session1,
		},
		keyspaceIDQuery: &queryExecuteKeyspaceIds{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace: "ks",
			KeyspaceIds: [][]byte{
				{0x61},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    session1,
		},
		keyRangeQuery: &queryExecuteKeyRanges{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace: "ks",
			KeyRanges: []*topodatapb.KeyRange{
				{
					Start: []byte{0x23},
					End:   []byte{0x66},
				},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    session1,
		},
		entityIdsQuery: &queryExecuteEntityIds{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": &querypb.BindVariable{
					Type:  querypb.Type_INT64,
					Value: []byte("0"),
				},
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []*vtgatepb.ExecuteEntityIdsRequest_EntityId{
				{
					Type:       sqltypes.Int64,
					Value:      []byte("-12345"),
					KeyspaceId: []byte{0x6B},
				},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    session1,
		},
		batchQueryShard: &queryExecuteBatchShards{
			Queries: []*vtgatepb.BoundShardQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "txRequest",
						BindVariables: map[string]*querypb.BindVariable{
							"bind1": {
								Type:  sqltypes.Int64,
								Value: []byte("11143"),
							},
						},
					},
					Keyspace: "ks",
					Shards:   []string{"-80", "80-"},
				},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    session1,
		},
		keyspaceIDBatchQuery: &queryExecuteBatchKeyspaceIds{
			Queries: []*vtgatepb.BoundKeyspaceIdQuery{
				{
					Query: &querypb.BoundQuery{
						Sql: "txRequest",
						BindVariables: map[string]*querypb.BindVariable{
							"bind1": {
								Type:  sqltypes.Int64,
								Value: []byte("11143"),
							},
						},
					},
					Keyspace: "ks",
					KeyspaceIds: [][]byte{
						{'k', 'i', '1'},
					},
				},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Session:    session1,
		},
		updateStreamQuery: &queryUpdateStream{
			Keyspace: "connection_ks",
			Shard:    "txRequest",
			KeyRange: &topodatapb.KeyRange{
				Start: []byte{0x72},
				End:   []byte{0x90},
			},
			TabletType: topodatapb.TabletType_RDONLY,
			Timestamp:  123789,
			Event: &querypb.EventToken{
				Timestamp: 1234567,
				Shard:     "request1",
				Position:  "streaming_position",
			},
		},
		result:     nil,
		outSession: session2,
	},
}

var extras = querypb.ResultExtras{
	EventToken: &querypb.EventToken{
		Timestamp: 123,
		Shard:     "sh",
		Position:  "po",
	},
	Fresher: true,
}

var result1 = sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "field1",
			Type: sqltypes.Int16,
		},
		{
			Name: "field2",
			Type: sqltypes.Int32,
		},
	},
	RowsAffected: 123,
	InsertID:     72,
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(sqltypes.Int16, []byte("1")),
			sqltypes.NULL,
		},
		{
			sqltypes.MakeTrusted(sqltypes.Int16, []byte("2")),
			sqltypes.MakeTrusted(sqltypes.Int32, []byte("3")),
		},
	},
	Extras: &extras,
}

// streamResultFields is only the fields, sent as the first packet
var streamResultFields = sqltypes.Result{
	Fields: result1.Fields,
	Rows:   [][]sqltypes.Value{},
}

var session1 = &vtgatepb.Session{
	InTransaction: true,
}

var session2 = &vtgatepb.Session{
	InTransaction: true,
	ShardSessions: []*vtgatepb.Session_ShardSession{
		{
			Target: &querypb.Target{
				Keyspace:   "ks",
				Shard:      "1",
				TabletType: topodatapb.TabletType_MASTER,
			},
			TransactionId: 1,
		},
	},
}

var dtid2 = "aa"

var splitQueryRequest = &querySplitQuery{
	Keyspace: "ks2",
	SQL:      "in for SplitQuery",
	BindVariables: map[string]interface{}{
		"bind2": &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: []byte("43"),
		},
	},
	SplitColumns:        []string{"split_column1", "split_column2"},
	SplitCount:          145,
	NumRowsPerQueryPart: 4000,
	Algorithm:           querypb.SplitQueryRequest_FULL_SCAN,
}

var splitQueryResult = []*vtgatepb.SplitQueryResponse_Part{
	{
		Query: &querypb.BoundQuery{
			Sql: "out for SplitQuery",
			BindVariables: map[string]*querypb.BindVariable{
				"bind1": {
					Type:  sqltypes.Int64,
					Value: []byte("1114444"),
				},
			},
		},
		KeyRangePart: &vtgatepb.SplitQueryResponse_KeyRangePart{
			Keyspace: "ksout",
			KeyRanges: []*topodatapb.KeyRange{
				{
					Start: []byte{'s'},
					End:   []byte{'e'},
				},
			},
		},
		Size: 12344,
	},
}

var getSrvKeyspaceKeyspace = "test_keyspace"

var getSrvKeyspaceResult = &topodatapb.SrvKeyspace{
	Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
		{
			ServedType: topodatapb.TabletType_REPLICA,
			ShardReferences: []*topodatapb.ShardReference{
				{
					Name: "shard0",
					KeyRange: &topodatapb.KeyRange{
						Start: []byte{'s'},
						End:   []byte{'e'},
					},
				},
			},
		},
	},
	ShardingColumnName: "sharding_column_name",
	ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	ServedFrom: []*topodatapb.SrvKeyspace_ServedFrom{
		{
			TabletType: topodatapb.TabletType_MASTER,
			Keyspace:   "other_keyspace",
		},
	},
}

var messageName = "vitess_message"
var messageStreamResult = &sqltypes.Result{
	Fields: []*querypb.Field{{
		Name: "id",
		Type: sqltypes.VarBinary,
	}, {
		Name: "message",
		Type: sqltypes.VarBinary,
	}},
	Rows: [][]sqltypes.Value{{
		sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("2")),
		sqltypes.MakeTrusted(sqltypes.VarBinary, []byte("3")),
	}},
}
var messageids = []*querypb.Value{
	sqltypes.MakeString([]byte("1")).ToProtoValue(),
	sqltypes.MakeString([]byte("3")).ToProtoValue(),
}
var messageAckRowsAffected = int64(1)
