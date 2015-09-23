// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtgateconntest provides the test methods to make sure a
// vtgateconn/vtgateservice pair over RPC works correctly.
package vtgateconntest

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"golang.org/x/net/context"

	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// fakeVTGateService has the server side of this fake
type fakeVTGateService struct {
	t        *testing.T
	panics   bool
	hasError bool
	// If True, calls to Begin/2 will always succeed. This is necessary so that
	// we can test subsequent calls in the transaction (e.g., Commit, Rollback).
	forceBeginSuccess bool
	hasCallerID       bool
	errorWait         chan struct{}
}

const expectedErrMatch string = "test vtgate error"
const expectedCode vtrpc.ErrorCode = vtrpc.ErrorCode_BAD_INPUT

// the error string that is returned for partial execute errors (i.e., the Execute* rpc call
// succeeds, but returns an error as part of the response).
const executePartialErrString string = "execute partial error"

var errTestVtGateError = vterrors.FromError(expectedCode, errors.New(expectedErrMatch))

func newContext() context.Context {
	ctx := context.Background()
	ctx = callerid.NewContext(ctx, testCallerID, nil)
	return ctx
}

func (f *fakeVTGateService) checkCallerID(ctx context.Context, name string) {
	if !f.hasCallerID {
		return
	}
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
	TabletType       pb.TabletType
	Session          *proto.Session
	NotInTransaction bool
}

// Execute is part of the VTGateService interface
func (f *fakeVTGateService) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "Execute")
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	query := &queryExecute{
		SQL:              sql,
		BindVariables:    bindVariables,
		TabletType:       tabletType,
		Session:          session,
		NotInTransaction: notInTransaction,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		f.t.Errorf("Execute: %+v, want %+v", query, execCase.execQuery)
		return nil
	}
	*reply = *execCase.reply
	return nil
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

// ExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteShards")
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
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
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// queryExecuteKeyspaceIds contains all the fields we use to test
// ExecuteKeyspaceIds
type queryExecuteKeyspaceIds struct {
	SQL              string
	BindVariables    map[string]interface{}
	Keyspace         string
	KeyspaceIds      [][]byte
	TabletType       pb.TabletType
	Session          *proto.Session
	NotInTransaction bool
}

// ExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteKeyspaceIds")
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
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
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// queryExecuteKeyRanges contains all the fields we use to test ExecuteKeyRanges
type queryExecuteKeyRanges struct {
	SQL              string
	BindVariables    map[string]interface{}
	Keyspace         string
	KeyRanges        []*pb.KeyRange
	TabletType       pb.TabletType
	Session          *proto.Session
	NotInTransaction bool
}

// ExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteKeyRanges")
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
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
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// queryExecuteEntityIds contains all the fields we use to test ExecuteEntityIds
type queryExecuteEntityIds struct {
	SQL               string
	BindVariables     map[string]interface{}
	Keyspace          string
	EntityColumnName  string
	EntityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId
	TabletType        pb.TabletType
	Session           *proto.Session
	NotInTransaction  bool
}

// ExecuteEntityIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteEntityIds")
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
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
	if len(query.EntityKeyspaceIDs) == 1 && len(query.EntityKeyspaceIDs[0].XidBytes) == 0 {
		query.EntityKeyspaceIDs[0].XidBytes = nil
	}
	if !reflect.DeepEqual(query, execCase.entityIdsQuery) {
		f.t.Errorf("ExecuteEntityIds: %+v, want %+v", query, execCase.entityIdsQuery)
		return nil
	}
	*reply = *execCase.reply
	return nil
}

// queryExecuteBatchShards contains all the fields we use to test
// ExecuteBatchShards
type queryExecuteBatchShards struct {
	Queries       []proto.BoundShardQuery
	TabletType    pb.TabletType
	AsTransaction bool
	Session       *proto.Session
}

// ExecuteBatchShards is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteBatchShards")
	execCase, ok := execMap[queries[0].Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", queries[0].Sql)
	}
	query := &queryExecuteBatchShards{
		Queries:       queries,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       session,
	}
	if !reflect.DeepEqual(query, execCase.batchQueryShard) {
		f.t.Errorf("ExecuteBatchShards: %+v, want %+v", query, execCase.batchQueryShard)
		return nil
	}
	reply.Error = execCase.reply.Error
	reply.Err = execCase.reply.Err
	if reply.Error == "" && execCase.reply.Result != nil {
		reply.List = []mproto.QueryResult{*execCase.reply.Result}
	}
	reply.Session = execCase.reply.Session
	return nil
}

// queryExecuteBatchKeyspaceIds contains all the fields we use to test
// ExecuteBatchKeyspaceIds
type queryExecuteBatchKeyspaceIds struct {
	Queries       []proto.BoundKeyspaceIdQuery
	TabletType    pb.TabletType
	AsTransaction bool
	Session       *proto.Session
}

// ExecuteBatchKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "ExecuteBatchKeyspaceIds")
	execCase, ok := execMap[queries[0].Sql]
	if !ok {
		return fmt.Errorf("no match for: %s", queries[0].Sql)
	}
	query := &queryExecuteBatchKeyspaceIds{
		Queries:       queries,
		TabletType:    tabletType,
		AsTransaction: asTransaction,
		Session:       session,
	}
	if !reflect.DeepEqual(query, execCase.keyspaceIDBatchQuery) {
		f.t.Errorf("ExecuteBatchKeyspaceIds: %+v, want %+v", query, execCase.keyspaceIDBatchQuery)
		return nil
	}
	reply.Error = execCase.reply.Error
	reply.Err = execCase.reply.Err
	if reply.Error == "" && execCase.reply.Result != nil {
		reply.List = []mproto.QueryResult{*execCase.reply.Result}
	}
	reply.Session = execCase.reply.Session
	return nil
}

// StreamExecute is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	execCase, ok := execMap[sql]
	if !ok {
		return fmt.Errorf("no match for: %s", sql)
	}
	f.checkCallerID(ctx, "StreamExecute")
	query := &queryExecute{
		SQL:           sql,
		BindVariables: bindVariables,
		TabletType:    tabletType,
	}
	if !reflect.DeepEqual(query, execCase.execQuery) {
		f.t.Errorf("StreamExecute: %+v, want %+v", query, execCase.execQuery)
		return nil
	}
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Err != nil {
		return vterrors.FromRPCError(execCase.reply.Err)
	}
	return nil
}

// StreamExecuteShards is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "StreamExecuteShards")
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
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Err != nil {
		return vterrors.FromRPCError(execCase.reply.Err)
	}
	return nil
}

// StreamExecuteKeyspaceIds is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "StreamExecuteKeyspaceIds")
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
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Err != nil {
		return vterrors.FromRPCError(execCase.reply.Err)
	}
	return nil
}

// StreamExecuteKeyRanges is part of the VTGateService interface
func (f *fakeVTGateService) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "StreamExecuteKeyRanges")
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
	if execCase.reply.Result != nil {
		result := proto.QueryResult{Result: &mproto.QueryResult{}}
		result.Result.Fields = execCase.reply.Result.Fields
		if err := sendReply(&result); err != nil {
			return err
		}
		if f.hasError {
			// wait until the client has the response, since all streaming implementation may not
			// send previous messages if an error has been triggered.
			<-f.errorWait
			f.errorWait = make(chan struct{}) // for next test
			return errTestVtGateError
		}
		for _, row := range execCase.reply.Result.Rows {
			result := proto.QueryResult{Result: &mproto.QueryResult{}}
			result.Result.Rows = [][]sqltypes.Value{row}
			if err := sendReply(&result); err != nil {
				return err
			}
		}
	}
	if execCase.reply.Err != nil {
		return vterrors.FromRPCError(execCase.reply.Err)
	}
	return nil
}

// Begin is part of the VTGateService interface
func (f *fakeVTGateService) Begin(ctx context.Context, outSession *proto.Session) error {
	f.checkCallerID(ctx, "Begin")
	switch {
	case f.forceBeginSuccess:
	case f.hasError:
		return errTestVtGateError
	case f.panics:
		panic(fmt.Errorf("test forced panic"))
	default:
	}
	*outSession = *session1
	return nil
}

// Commit is part of the VTGateService interface
func (f *fakeVTGateService) Commit(ctx context.Context, inSession *proto.Session) error {
	f.checkCallerID(ctx, "Commit")
	if f.hasError {
		return errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	if !reflect.DeepEqual(inSession, session2) {
		return errors.New("commit: session mismatch")
	}
	return nil
}

// Rollback is part of the VTGateService interface
func (f *fakeVTGateService) Rollback(ctx context.Context, inSession *proto.Session) error {
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

// querySplitQuery contains all the fields we use to test SplitQuery
type querySplitQuery struct {
	Keyspace      string
	SQL           string
	BindVariables map[string]interface{}
	SplitColumn   string
	SplitCount    int
}

// SplitQuery is part of the VTGateService interface
func (f *fakeVTGateService) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]*pbg.SplitQueryResponse_Part, error) {
	if f.hasError {
		return nil, errTestVtGateError
	}
	if f.panics {
		panic(fmt.Errorf("test forced panic"))
	}
	f.checkCallerID(ctx, "SplitQuery")
	query := &querySplitQuery{
		Keyspace:      keyspace,
		SQL:           sql,
		BindVariables: bindVariables,
		SplitColumn:   splitColumn,
		SplitCount:    splitCount,
	}
	if !reflect.DeepEqual(query, splitQueryRequest) {
		f.t.Errorf("SplitQuery has wrong input: got %#v wanted %#v", query, splitQueryRequest)
	}
	return splitQueryResult, nil
}

// GetSrvKeyspace is part of the VTGateService interface
func (f *fakeVTGateService) GetSrvKeyspace(ctx context.Context, keyspace string) (*pb.SrvKeyspace, error) {
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

// GetSrvShard is part of the VTGateService interface
func (f *fakeVTGateService) GetSrvShard(ctx context.Context, keyspace, shard string) (*pb.SrvShard, error) {
	panic(fmt.Errorf("GetSrvShard is not tested here"))
}

// CreateFakeServer returns the fake server for the tests
func CreateFakeServer(t *testing.T) vtgateservice.VTGateService {
	return &fakeVTGateService{
		t:           t,
		panics:      false,
		hasCallerID: true,
		errorWait:   make(chan struct{}),
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
		*err = fmt.Errorf("uncaught panic: %v\n%s", x, tb.Stack(4))
	}
}

// TestSuite runs all the tests
func TestSuite(t *testing.T, impl vtgateconn.Impl, fakeServer vtgateservice.VTGateService) {
	vtgateconn.RegisterDialer("test", func(ctx context.Context, address string, timeout time.Duration) (vtgateconn.Impl, error) {
		return impl, nil
	})
	conn, err := vtgateconn.DialProtocol(context.Background(), "test", "", 0)
	if err != nil {
		t.Fatalf("Got err: %v from vtgateconn.DialProtocol", err)
	}

	fs := fakeServer.(*fakeVTGateService)

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
	fs.hasCallerID = false
	testTxPass(t, conn)
	testTxPassNotInTransaction(t, conn)
	testTxFail(t, conn)
	fs.hasCallerID = true
	testTx2Pass(t, conn)
	testTx2PassNotInTransaction(t, conn)
	testTx2Fail(t, conn)
	testSplitQuery(t, conn)
	testGetSrvKeyspace(t, conn)

	// force a panic at every call, then test that works
	fs.panics = true

	fs.hasCallerID = false
	testBeginPanic(t, conn)
	testCommitPanic(t, conn, fs)
	testRollbackPanic(t, conn, fs)

	fs.hasCallerID = true
	testBegin2Panic(t, conn)
	testCommit2Panic(t, conn, fs)
	testRollback2Panic(t, conn, fs)

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
	testSplitQueryPanic(t, conn)
	testGetSrvKeyspacePanic(t, conn)
	fs.panics = false
}

// TestErrorSuite runs all the tests that expect errors
func TestErrorSuite(t *testing.T, fakeServer vtgateservice.VTGateService) {
	conn, err := vtgateconn.DialProtocol(context.Background(), "test", "", 0)
	if err != nil {
		t.Fatalf("Got err: %v from vtgateconn.DialProtocol", err)
	}

	fs := fakeServer.(*fakeVTGateService)

	// return an error for every call, make sure they're handled properly
	fs.hasError = true

	fs.hasCallerID = false
	testBeginError(t, conn)
	testCommitError(t, conn, fs)
	testRollbackError(t, conn, fs)

	fs.hasCallerID = true
	testBegin2Error(t, conn)
	testCommit2Error(t, conn, fs)
	testRollback2Error(t, conn, fs)

	testExecuteError(t, conn, fs)
	testExecuteShardsError(t, conn, fs)
	testExecuteKeyspaceIdsError(t, conn, fs)
	testExecuteKeyRangesError(t, conn, fs)
	testExecuteEntityIdsError(t, conn, fs)
	testExecuteBatchShardsError(t, conn, fs)
	testExecuteBatchKeyspaceIdsError(t, conn, fs)
	testStreamExecuteError(t, conn, fs)
	testStreamExecute2Error(t, conn, fs)
	testStreamExecuteShardsError(t, conn, fs)
	testStreamExecuteShards2Error(t, conn, fs)
	testStreamExecuteKeyRangesError(t, conn, fs)
	testStreamExecuteKeyRanges2Error(t, conn, fs)
	testStreamExecuteKeyspaceIdsError(t, conn, fs)
	testStreamExecuteKeyspaceIds2Error(t, conn, fs)
	testSplitQueryError(t, conn)
	testGetSrvKeyspaceError(t, conn)
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

// Verifies the returned partial Execute* error has the properties that we expect.
func verifyExecutePartialError(t *testing.T, err error, method string) {
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
	if err.Error() != executePartialErrString {
		t.Errorf("Unexpected error from %s: got %v, wanted: %v", method, err, executePartialErrString)
	}
}

func testExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.Execute(ctx, "none", nil, pb.TabletType_RDONLY)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	// TODO(aaijazi): get rid of this case from testExecute*, and instead have only testExecute*Error
	execCase = execMap["errorRequst"]
	_, err = conn.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	want = executePartialErrString
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	fake.hasError = false
	_, err := conn.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	verifyExecutePartialError(t, err, "Execute")

	fake.hasError = true
	_, err = conn.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	verifyError(t, err, "Execute")
}

func testExecutePanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteShards(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.ExecuteShards(ctx, "none", "", []string{}, nil, pb.TabletType_RDONLY)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType)
	want = executePartialErrString
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteShardsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	fake.hasError = false
	_, err := conn.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType)
	verifyExecutePartialError(t, err, "ExecuteShards")

	fake.hasError = true
	_, err = conn.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType)
	verifyError(t, err, "ExecuteShards")
}

func testExecuteShardsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteShards(ctx, execCase.execQuery.SQL, "ks", []string{"1", "2"}, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.ExecuteKeyspaceIds(ctx, "none", "", [][]byte{}, nil, pb.TabletType_REPLICA)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	want = executePartialErrString
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteKeyspaceIdsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	fake.hasError = false
	_, err := conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	verifyExecutePartialError(t, err, "ExecuteKeyspaceIds")

	fake.hasError = true
	_, err = conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	verifyError(t, err, "ExecuteKeyspaceIds")
}

func testExecuteKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteKeyRanges(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.ExecuteKeyRanges(ctx, "none", "", []*pb.KeyRange{}, nil, pb.TabletType_REPLICA)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	want = executePartialErrString
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteKeyRangesError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	fake.hasError = false
	_, err := conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	verifyExecutePartialError(t, err, "ExecuteKeyRanges")

	fake.hasError = true
	_, err = conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	verifyError(t, err, "ExecuteKeyRanges")
}

func testExecuteKeyRangesPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteEntityIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	qr, err := conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(qr, execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, execCase.reply.Result)
	}

	_, err = conn.ExecuteEntityIds(ctx, "none", "", "", []*pbg.ExecuteEntityIdsRequest_EntityId{}, nil, pb.TabletType_REPLICA)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType)
	want = executePartialErrString
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteEntityIdsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	fake.hasError = false
	_, err := conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType)
	verifyExecutePartialError(t, err, "ExecuteEntityIds")

	fake.hasError = true
	_, err = conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType)
	verifyError(t, err, "ExecuteEntityIds")
}

func testExecuteEntityIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType)
	expectPanic(t, err)
}

func testExecuteBatchShards(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	ql, err := conn.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(&ql[0], execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", ql, execCase.reply.Result)
	}

	_, err = conn.ExecuteBatchShards(ctx, []proto.BoundShardQuery{proto.BoundShardQuery{Sql: "none"}}, pb.TabletType_REPLICA, true)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
	want = executePartialErrString
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteBatchShardsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	fake.hasError = false
	_, err := conn.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
	verifyExecutePartialError(t, err, "ExecuteBatchShards")

	fake.hasError = true
	_, err = conn.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
	verifyError(t, err, "ExecuteBatchShards")
}

func testExecuteBatchShardsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
	expectPanic(t, err)
}

func testExecuteBatchKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	ql, err := conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.batchQueryShard.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(&ql[0], execCase.reply.Result) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", ql, execCase.reply.Result)
	}

	_, err = conn.ExecuteBatchKeyspaceIds(ctx, []proto.BoundKeyspaceIdQuery{proto.BoundKeyspaceIdQuery{Sql: "none", KeyspaceIds: []key.KeyspaceId{}}}, pb.TabletType_REPLICA, false)
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	_, err = conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction)
	want = executePartialErrString
	if err == nil || err.Error() != want {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testExecuteBatchKeyspaceIdsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["errorRequst"]

	fake.hasError = false
	_, err := conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction)
	verifyExecutePartialError(t, err, "ExecuteBatchKeyspaceIds")

	fake.hasError = true
	_, err = conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction)
	verifyError(t, err, "ExecuteBatchKeyspaceIds")
}

func testExecuteBatchKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	_, err := conn.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction)
	expectPanic(t, err)
}

func testStreamExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	packets, errFunc, err := conn.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	var qr mproto.QueryResult
	for packet := range packets {
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.reply.Result
	wantResult.RowsAffected = 0
	wantResult.InsertId = 0
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, wantResult)
	}
	if err = errFunc(); err != nil {
		t.Error(err)
	}

	packets, errFunc, err = conn.StreamExecute(ctx, "none", nil, pb.TabletType_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	packets, errFunc, err = conn.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want = executePartialErrString
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testStreamExecuteError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, errFunc, err := conn.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatalf("StreamExecute failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecute failed: cannot read result1")
	}

	if !reflect.DeepEqual(qr, &streamResult1) {
		t.Errorf("Unexpected result from StreamExecute: got %#v want %#v", qr, &streamResult1)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecute channel wasn't closed")
	}
	err = errFunc()
	verifyErrorString(t, err, "StreamExecute")
}

func testStreamExecute2Error(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, errFunc, err := conn.StreamExecute2(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatalf("StreamExecute2 failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecute2 failed: cannot read result1")
	}

	if !reflect.DeepEqual(qr, &streamResult1) {
		t.Errorf("Unexpected result from StreamExecute2: got %#v want %#v", qr, &streamResult1)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecute2 channel wasn't closed")
	}
	err = errFunc()
	verifyError(t, err, "StreamExecute2")
}

func testStreamExecutePanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	packets, errFunc, err := conn.StreamExecute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-packets; ok {
		t.Fatalf("Received packets instead of panic?")
	}
	err = errFunc()
	expectPanic(t, err)
}

func testStreamExecuteShards(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	packets, errFunc, err := conn.StreamExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	var qr mproto.QueryResult
	for packet := range packets {
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.reply.Result
	wantResult.RowsAffected = 0
	wantResult.InsertId = 0
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, wantResult)
	}
	if err = errFunc(); err != nil {
		t.Error(err)
	}

	packets, errFunc, err = conn.StreamExecuteShards(ctx, "none", "", []string{}, nil, pb.TabletType_REPLICA)
	if err != nil {
		t.Fatal(err)
	}
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	packets, errFunc, err = conn.StreamExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want = executePartialErrString
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testStreamExecuteShardsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, errFunc, err := conn.StreamExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatalf("StreamExecuteShards failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecuteShards failed: cannot read result1")
	}

	if !reflect.DeepEqual(qr, &streamResult1) {
		t.Errorf("Unexpected result from StreamExecuteShards: got %#v want %#v", qr, &streamResult1)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecuteShards channel wasn't closed")
	}
	err = errFunc()
	verifyErrorString(t, err, "StreamExecuteShards")
}

func testStreamExecuteShards2Error(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, errFunc, err := conn.StreamExecuteShards2(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatalf("StreamExecuteShards2 failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecuteShards2 failed: cannot read result1")
	}

	if !reflect.DeepEqual(qr, &streamResult1) {
		t.Errorf("Unexpected result from StreamExecuteShards2: got %#v want %#v", qr, &streamResult1)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecuteShards2 channel wasn't closed")
	}
	err = errFunc()
	verifyError(t, err, "StreamExecuteShards2")
}

func testStreamExecuteShardsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	packets, errFunc, err := conn.StreamExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.execQuery.BindVariables, execCase.execQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-packets; ok {
		t.Fatalf("Received packets instead of panic?")
	}
	err = errFunc()
	expectPanic(t, err)
}

func testStreamExecuteKeyRanges(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	packets, errFunc, err := conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	var qr mproto.QueryResult
	for packet := range packets {
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.reply.Result
	wantResult.RowsAffected = 0
	wantResult.InsertId = 0
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, wantResult)
	}
	if err = errFunc(); err != nil {
		t.Error(err)
	}

	packets, errFunc, err = conn.StreamExecuteKeyRanges(ctx, "none", "", []*pb.KeyRange{}, nil, pb.TabletType_REPLICA)
	if err != nil {
		t.Fatal(err)
	}
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	packets, errFunc, err = conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want = executePartialErrString
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testStreamExecuteKeyRangesError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, errFunc, err := conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	if err != nil {
		t.Fatalf("StreamExecuteKeyRanges failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecuteKeyRanges failed: cannot read result1")
	}

	if !reflect.DeepEqual(qr, &streamResult1) {
		t.Errorf("Unexpected result from StreamExecuteKeyRanges: got %#v want %#v", qr, &streamResult1)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecuteKeyRanges channel wasn't closed")
	}
	err = errFunc()
	verifyErrorString(t, err, "StreamExecuteKeyRanges")
}

func testStreamExecuteKeyRanges2Error(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, errFunc, err := conn.StreamExecuteKeyRanges2(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	if err != nil {
		t.Fatalf("StreamExecuteKeyRanges2 failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecuteKeyRanges2 failed: cannot read result1")
	}

	if !reflect.DeepEqual(qr, &streamResult1) {
		t.Errorf("Unexpected result from StreamExecuteKeyRanges2: got %#v want %#v", qr, &streamResult1)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecuteKeyRanges2 channel wasn't closed")
	}
	err = errFunc()
	verifyError(t, err, "StreamExecuteKeyRanges2")
}

func testStreamExecuteKeyRangesPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	packets, errFunc, err := conn.StreamExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-packets; ok {
		t.Fatalf("Received packets instead of panic?")
	}
	err = errFunc()
	expectPanic(t, err)
}

func testStreamExecuteKeyspaceIds(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	packets, errFunc, err := conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	var qr mproto.QueryResult
	for packet := range packets {
		if len(packet.Fields) != 0 {
			qr.Fields = packet.Fields
		}
		if len(packet.Rows) != 0 {
			qr.Rows = append(qr.Rows, packet.Rows...)
		}
	}
	wantResult := *execCase.reply.Result
	wantResult.RowsAffected = 0
	wantResult.InsertId = 0
	if !reflect.DeepEqual(qr, wantResult) {
		t.Errorf("Unexpected result from Execute: got %+v want %+v", qr, wantResult)
	}
	if err = errFunc(); err != nil {
		t.Error(err)
	}

	packets, errFunc, err = conn.StreamExecuteKeyspaceIds(ctx, "none", "", [][]byte{}, nil, pb.TabletType_REPLICA)
	if err != nil {
		t.Fatal(err)
	}
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want := "no match for: none"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("none request: %v, want %v", err, want)
	}

	execCase = execMap["errorRequst"]
	packets, errFunc, err = conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	for packet := range packets {
		t.Errorf("packet: %+v, want none", packet)
	}
	err = errFunc()
	want = executePartialErrString
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("errorRequst: %v, want %v", err, want)
	}
}

func testStreamExecuteKeyspaceIdsError(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, errFunc, err := conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	if err != nil {
		t.Fatalf("StreamExecuteKeyspaceIds failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecuteKeyspaceIds failed: cannot read result1")
	}

	if !reflect.DeepEqual(qr, &streamResult1) {
		t.Errorf("Unexpected result from StreamExecuteKeyspaceIds: got %#v want %#v", qr, &streamResult1)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecuteKeyspaceIds channel wasn't closed")
	}
	err = errFunc()
	verifyErrorString(t, err, "StreamExecuteKeyspaceIds")
}

func testStreamExecuteKeyspaceIds2Error(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()
	execCase := execMap["request1"]
	stream, errFunc, err := conn.StreamExecuteKeyspaceIds2(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	if err != nil {
		t.Fatalf("StreamExecuteKeyspaceIds2 failed: %v", err)
	}
	qr, ok := <-stream
	if !ok {
		t.Fatalf("StreamExecuteKeyspaceIds2 failed: cannot read result1")
	}

	if !reflect.DeepEqual(qr, &streamResult1) {
		t.Errorf("Unexpected result from StreamExecuteKeyspaceIds2: got %#v want %#v", qr, &streamResult1)
	}
	// signal to the server that the first result has been received
	close(fake.errorWait)
	// After 1 result, we expect to get an error (no more results).
	qr, ok = <-stream
	if ok {
		t.Fatalf("StreamExecuteKeyspaceIds2 channel wasn't closed")
	}
	err = errFunc()
	verifyError(t, err, "StreamExecuteKeyspaceIds2")
}

func testStreamExecuteKeyspaceIdsPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["request1"]
	packets, errFunc, err := conn.StreamExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := <-packets; ok {
		t.Fatalf("Received packets instead of panic?")
	}
	err = errFunc()
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
	_, err = tx.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, false)
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
	_, err = tx.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, false)
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
	_, err = tx.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, false)
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
	_, err = tx.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, false)
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
	_, err = tx.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, false)
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
	_, err = tx.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
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
	_, err = tx.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		t.Error(err)
	}
}

func testTxPassNotInTransaction(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["txRequestNIT"]

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	// no rollback necessary
}

// Same as testTxPass, but with Begin2/Commit2/Rollback2 instead
func testTx2Pass(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["txRequest"]

	// Execute
	tx, err := conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Commit2(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteShards
	tx, err = conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteKeyspaceIds
	tx, err = conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteKeyRanges
	tx, err = conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteEntityIds
	tx, err = conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, false)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteBatchShards
	tx, err = conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
	if err != nil {
		t.Error(err)
	}

	// ExecuteBatchKeyspaceIds
	tx, err = conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
	if err != nil {
		t.Error(err)
	}
}

// Same as testTxPassNotInTransaction, but with Begin2/Commit2/Rollback2 instead
func testTx2PassNotInTransaction(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	execCase := execMap["txRequestNIT"]

	tx, err := conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.Execute(ctx, execCase.execQuery.SQL, execCase.execQuery.BindVariables, execCase.execQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteShards(ctx, execCase.shardQuery.SQL, execCase.shardQuery.Keyspace, execCase.shardQuery.Shards, execCase.shardQuery.BindVariables, execCase.shardQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyspaceIds(ctx, execCase.keyspaceIDQuery.SQL, execCase.keyspaceIDQuery.Keyspace, execCase.keyspaceIDQuery.KeyspaceIds, execCase.keyspaceIDQuery.BindVariables, execCase.keyspaceIDQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteKeyRanges(ctx, execCase.keyRangeQuery.SQL, execCase.keyRangeQuery.Keyspace, execCase.keyRangeQuery.KeyRanges, execCase.keyRangeQuery.BindVariables, execCase.keyRangeQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteEntityIds(ctx, execCase.entityIdsQuery.SQL, execCase.entityIdsQuery.Keyspace, execCase.entityIdsQuery.EntityColumnName, execCase.entityIdsQuery.EntityKeyspaceIDs, execCase.entityIdsQuery.BindVariables, execCase.entityIdsQuery.TabletType, true)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchShards(ctx, execCase.batchQueryShard.Queries, execCase.batchQueryShard.TabletType, execCase.batchQueryShard.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	_, err = tx.ExecuteBatchKeyspaceIds(ctx, execCase.keyspaceIDBatchQuery.Queries, execCase.keyspaceIDBatchQuery.TabletType, execCase.keyspaceIDBatchQuery.AsTransaction)
	if err != nil {
		t.Error(err)
	}
	// no rollback necessary
}

func testBeginError(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.Begin(ctx)
	verifyErrorString(t, err, "Begin")
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
	verifyErrorString(t, err, "Commit")
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
	verifyErrorString(t, err, "Rollback")
}

func testBegin2Error(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.Begin2(ctx)
	verifyError(t, err, "Begin2")
}

func testCommit2Error(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()

	fake.forceBeginSuccess = true
	tx, err := conn.Begin2(ctx)
	fake.forceBeginSuccess = false

	if err != nil {
		t.Error(err)
	}
	err = tx.Commit2(ctx)
	verifyError(t, err, "Commit2")
}

func testRollback2Error(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()

	fake.forceBeginSuccess = true
	tx, err := conn.Begin2(ctx)
	fake.forceBeginSuccess = false

	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
	verifyError(t, err, "Rollback2")
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

func testBegin2Panic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.Begin2(ctx)
	expectPanic(t, err)
}

func testCommit2Panic(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()

	fake.forceBeginSuccess = true
	tx, err := conn.Begin2(ctx)
	fake.forceBeginSuccess = false

	if err != nil {
		t.Error(err)
	}
	err = tx.Commit2(ctx)
	expectPanic(t, err)
}

func testRollback2Panic(t *testing.T, conn *vtgateconn.VTGateConn, fake *fakeVTGateService) {
	ctx := newContext()

	fake.forceBeginSuccess = true
	tx, err := conn.Begin2(ctx)
	fake.forceBeginSuccess = false

	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
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

	_, err = tx.Execute(ctx, "", nil, pb.TabletType_REPLICA, false)
	want = "execute: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %v", err, want)
	}

	_, err = tx.ExecuteShards(ctx, "", "", nil, nil, pb.TabletType_REPLICA, false)
	want = "executeShards: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteKeyspaceIds(ctx, "", "", nil, nil, pb.TabletType_REPLICA, false)
	want = "executeKeyspaceIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteKeyRanges(ctx, "", "", nil, nil, pb.TabletType_REPLICA, false)
	want = "executeKeyRanges: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteEntityIds(ctx, "", "", "", nil, nil, pb.TabletType_REPLICA, false)
	want = "executeEntityIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteBatchShards(ctx, nil, pb.TabletType_REPLICA, false)
	want = "executeBatchShards: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteBatchKeyspaceIds(ctx, nil, pb.TabletType_REPLICA, false)
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

// Same as testTxFail, but with Begin2/Commit2/Rollback2 instead
func testTx2Fail(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	tx, err := conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	err = tx.Commit2(ctx)
	want := "commit: session mismatch"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Commit2: %v, want %v", err, want)
	}

	_, err = tx.Execute(ctx, "", nil, pb.TabletType_REPLICA, false)
	want = "execute: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Execute: %v, want %v", err, want)
	}

	_, err = tx.ExecuteShards(ctx, "", "", nil, nil, pb.TabletType_REPLICA, false)
	want = "executeShards: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteKeyspaceIds(ctx, "", "", nil, nil, pb.TabletType_REPLICA, false)
	want = "executeKeyspaceIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteKeyRanges(ctx, "", "", nil, nil, pb.TabletType_REPLICA, false)
	want = "executeKeyRanges: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteEntityIds(ctx, "", "", "", nil, nil, pb.TabletType_REPLICA, false)
	want = "executeEntityIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteBatchShards(ctx, nil, pb.TabletType_REPLICA, false)
	want = "executeBatchShards: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	_, err = tx.ExecuteBatchKeyspaceIds(ctx, nil, pb.TabletType_REPLICA, false)
	want = "executeBatchKeyspaceIds: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("ExecuteShards: %v, want %v", err, want)
	}

	err = tx.Commit2(ctx)
	want = "commit: not in transaction"
	if err == nil || err.Error() != want {
		t.Errorf("Commit: %v, want %v", err, want)
	}

	err = tx.Rollback2(ctx)
	if err != nil {
		t.Error(err)
	}

	tx, err = conn.Begin2(ctx)
	if err != nil {
		t.Error(err)
	}
	err = tx.Rollback2(ctx)
	want = "rollback: session mismatch"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Rollback: %v, want %v", err, want)
	}
}

func testSplitQuery(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	qsl, err := conn.SplitQuery(ctx, splitQueryRequest.Keyspace, splitQueryRequest.SQL, splitQueryRequest.BindVariables, splitQueryRequest.SplitColumn, splitQueryRequest.SplitCount)
	if err != nil {
		t.Fatalf("SplitQuery failed: %v", err)
	}
	if len(qsl) == 1 && len(qsl[0].Query.BindVariables) == 1 {
		bv := qsl[0].Query.BindVariables["bind1"]
		if len(bv.ValueBytes) == 0 {
			bv.ValueBytes = nil
		}
		if len(bv.ValueBytesList) == 0 {
			bv.ValueBytesList = nil
		}
		if len(bv.ValueIntList) == 0 {
			bv.ValueIntList = nil
		}
		if len(bv.ValueUintList) == 0 {
			bv.ValueUintList = nil
		}
		if len(bv.ValueFloatList) == 0 {
			bv.ValueFloatList = nil
		}
	}
	if !reflect.DeepEqual(qsl, splitQueryResult) {
		t.Errorf("SplitQuery returned wrong result: got %#v wanted %#v", qsl, splitQueryResult)
	}
}

func testSplitQueryError(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.SplitQuery(ctx, splitQueryRequest.Keyspace, splitQueryRequest.SQL, splitQueryRequest.BindVariables, splitQueryRequest.SplitColumn, splitQueryRequest.SplitCount)
	verifyError(t, err, "SplitQuery")
}

func testSplitQueryPanic(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := newContext()
	_, err := conn.SplitQuery(ctx, splitQueryRequest.Keyspace, splitQueryRequest.SQL, splitQueryRequest.BindVariables, splitQueryRequest.SplitColumn, splitQueryRequest.SplitCount)
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

var testCallerID = &vtrpc.CallerID{
	Principal:    "test_principal",
	Component:    "test_component",
	Subcomponent: "test_subcomponent",
}

var execMap = map[string]struct {
	execQuery            *queryExecute
	shardQuery           *queryExecuteShards
	keyspaceIDQuery      *queryExecuteKeyspaceIds
	keyRangeQuery        *queryExecuteKeyRanges
	entityIdsQuery       *queryExecuteEntityIds
	batchQueryShard      *queryExecuteBatchShards
	keyspaceIDBatchQuery *queryExecuteBatchKeyspaceIds
	reply                *proto.QueryResult
	err                  error
}{
	"request1": {
		execQuery: &queryExecute{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		shardQuery: &queryExecuteShards{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:   "ks",
			Shards:     []string{"1", "2"},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		keyspaceIDQuery: &queryExecuteKeyspaceIds{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyspaceIds: [][]byte{
				[]byte{0x61},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		keyRangeQuery: &queryExecuteKeyRanges{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []*pb.KeyRange{
				&pb.KeyRange{
					Start: []byte{0x72},
					End:   []byte{0x90},
				},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		entityIdsQuery: &queryExecuteEntityIds{
			SQL: "request1",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []*pbg.ExecuteEntityIdsRequest_EntityId{
				&pbg.ExecuteEntityIdsRequest_EntityId{
					XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES,
					XidBytes:   []byte{105, 100, 49},
					KeyspaceId: []byte{0x6B},
				},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		batchQueryShard: &queryExecuteBatchShards{
			Queries: []proto.BoundShardQuery{
				proto.BoundShardQuery{
					Sql: "request1",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
					Keyspace: "ks",
					Shards:   []string{"-80", "80-"},
				},
			},
			TabletType:    pb.TabletType_RDONLY,
			AsTransaction: true,
			Session:       nil,
		},
		keyspaceIDBatchQuery: &queryExecuteBatchKeyspaceIds{
			Queries: []proto.BoundKeyspaceIdQuery{
				proto.BoundKeyspaceIdQuery{
					Sql: "request1",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
					Keyspace: "ks",
					KeyspaceIds: []key.KeyspaceId{
						key.KeyspaceId("ki1"),
					},
				},
			},
			TabletType:    pb.TabletType_RDONLY,
			AsTransaction: true,
			Session:       nil,
		},
		reply: &proto.QueryResult{
			Result:  &result1,
			Session: nil,
			Error:   "",
		},
	},
	"errorRequst": {
		execQuery: &queryExecute{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		shardQuery: &queryExecuteShards{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType: pb.TabletType_RDONLY,
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
				[]byte{0x61},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		keyRangeQuery: &queryExecuteKeyRanges{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []*pb.KeyRange{
				&pb.KeyRange{
					Start: []byte{0x73},
					End:   []byte{0x99},
				},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		entityIdsQuery: &queryExecuteEntityIds{
			SQL: "errorRequst",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []*pbg.ExecuteEntityIdsRequest_EntityId{
				&pbg.ExecuteEntityIdsRequest_EntityId{
					XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES,
					XidBytes:   []byte{105, 100, 49},
					KeyspaceId: []byte{0x6B},
				},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    nil,
		},
		batchQueryShard: &queryExecuteBatchShards{
			Queries: []proto.BoundShardQuery{
				proto.BoundShardQuery{
					Sql: "errorRequst",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
					Keyspace: "ks",
					Shards:   []string{"-80", "80-"},
				},
			},
			TabletType:    pb.TabletType_RDONLY,
			AsTransaction: false,
			Session:       nil,
		},
		keyspaceIDBatchQuery: &queryExecuteBatchKeyspaceIds{
			Queries: []proto.BoundKeyspaceIdQuery{
				proto.BoundKeyspaceIdQuery{
					Sql: "errorRequst",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
					Keyspace: "ks",
					KeyspaceIds: []key.KeyspaceId{
						key.KeyspaceId("ki1"),
					},
				},
			},
			TabletType:    pb.TabletType_RDONLY,
			AsTransaction: false,
			Session:       nil,
		},
		reply: &proto.QueryResult{
			Result:  nil,
			Session: nil,
			Error:   executePartialErrString,
			Err: &mproto.RPCError{
				Code:    int64(expectedCode),
				Message: executePartialErrString,
			},
		},
	},
	"txRequest": {
		execQuery: &queryExecute{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType: pb.TabletType_MASTER,
			Session:    session1,
		},
		shardQuery: &queryExecuteShards{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType: pb.TabletType_MASTER,
			Keyspace:   "",
			Shards:     []string{"s1", "s2"},
			Session:    session1,
		},
		keyspaceIDQuery: &queryExecuteKeyspaceIds{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyspaceIds: [][]byte{
				[]byte{0x61},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    session1,
		},
		keyRangeQuery: &queryExecuteKeyRanges{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []*pb.KeyRange{
				&pb.KeyRange{
					Start: []byte{0x23},
					End:   []byte{0x66},
				},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    session1,
		},
		entityIdsQuery: &queryExecuteEntityIds{
			SQL: "txRequest",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []*pbg.ExecuteEntityIdsRequest_EntityId{
				&pbg.ExecuteEntityIdsRequest_EntityId{
					XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_INT,
					XidInt:     -12345,
					KeyspaceId: []byte{0x6B},
				},
			},
			TabletType: pb.TabletType_RDONLY,
			Session:    session1,
		},
		batchQueryShard: &queryExecuteBatchShards{
			Queries: []proto.BoundShardQuery{
				proto.BoundShardQuery{
					Sql: "txRequest",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
					Keyspace: "ks",
					Shards:   []string{"-80", "80-"},
				},
			},
			TabletType:    pb.TabletType_RDONLY,
			AsTransaction: true,
			Session:       session1,
		},
		keyspaceIDBatchQuery: &queryExecuteBatchKeyspaceIds{
			Queries: []proto.BoundKeyspaceIdQuery{
				proto.BoundKeyspaceIdQuery{
					Sql: "txRequest",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
					Keyspace: "ks",
					KeyspaceIds: []key.KeyspaceId{
						key.KeyspaceId("ki1"),
					},
				},
			},
			TabletType:    pb.TabletType_RDONLY,
			AsTransaction: true,
			Session:       session1,
		},
		reply: &proto.QueryResult{
			Result:  nil,
			Session: session2,
			Error:   "",
		},
	},
	"txRequestNIT": {
		execQuery: &queryExecute{
			SQL: "txRequestNIT",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType:       pb.TabletType_MASTER,
			Session:          session1,
			NotInTransaction: true,
		},
		shardQuery: &queryExecuteShards{
			SQL: "txRequestNIT",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			TabletType:       pb.TabletType_MASTER,
			Keyspace:         "",
			Shards:           []string{"s1", "s2"},
			Session:          session1,
			NotInTransaction: true,
		},
		keyspaceIDQuery: &queryExecuteKeyspaceIds{
			SQL: "txRequestNIT",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyspaceIds: [][]byte{
				[]byte{0x61},
			},
			TabletType:       pb.TabletType_RDONLY,
			Session:          session1,
			NotInTransaction: true,
		},
		keyRangeQuery: &queryExecuteKeyRanges{
			SQL: "txRequestNIT",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace: "ks",
			KeyRanges: []*pb.KeyRange{
				&pb.KeyRange{
					Start: []byte{0x34},
					End:   []byte{0x77},
				},
			},
			TabletType:       pb.TabletType_RDONLY,
			Session:          session1,
			NotInTransaction: true,
		},
		entityIdsQuery: &queryExecuteEntityIds{
			SQL: "txRequestNIT",
			BindVariables: map[string]interface{}{
				"bind1": int64(0),
			},
			Keyspace:         "ks",
			EntityColumnName: "column",
			EntityKeyspaceIDs: []*pbg.ExecuteEntityIdsRequest_EntityId{
				&pbg.ExecuteEntityIdsRequest_EntityId{
					XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_INT,
					XidInt:     123456,
					KeyspaceId: []byte{0x6B},
				},
			},
			TabletType:       pb.TabletType_RDONLY,
			Session:          session1,
			NotInTransaction: true,
		},
		batchQueryShard: &queryExecuteBatchShards{
			Queries: []proto.BoundShardQuery{
				proto.BoundShardQuery{
					Sql: "txRequestNIT",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
					Keyspace: "ks",
					Shards:   []string{"-80", "80-"},
				},
			},
			TabletType:    pb.TabletType_RDONLY,
			AsTransaction: true,
			Session:       session1,
		},
		keyspaceIDBatchQuery: &queryExecuteBatchKeyspaceIds{
			Queries: []proto.BoundKeyspaceIdQuery{
				proto.BoundKeyspaceIdQuery{
					Sql: "txRequestNIT",
					BindVariables: map[string]interface{}{
						"bind1": int64(0),
					},
					Keyspace: "ks",
					KeyspaceIds: []key.KeyspaceId{
						key.KeyspaceId("ki1"),
					},
				},
			},
			TabletType:    pb.TabletType_RDONLY,
			AsTransaction: true,
			Session:       session1,
		},
		reply: &proto.QueryResult{
			Result:  nil,
			Session: session1,
			Error:   "",
		},
	},
}

var result1 = mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{
			Name: "field1",
			Type: 42,
		},
		mproto.Field{
			Name: "field2",
			Type: 73,
		},
	},
	RowsAffected: 123,
	InsertId:     72,
	Rows: [][]sqltypes.Value{
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("row1 value1")),
			sqltypes.MakeString([]byte("row1 value2")),
		},
		[]sqltypes.Value{
			sqltypes.MakeString([]byte("row2 value1")),
			sqltypes.MakeString([]byte("row2 value2")),
		},
	},
}

var streamResult1 = mproto.QueryResult{
	Fields: []mproto.Field{
		mproto.Field{
			Name: "field1",
			Type: 42,
		},
		mproto.Field{
			Name: "field2",
			Type: 73,
		},
	},
	RowsAffected: 0,
	InsertId:     0,
	Rows:         [][]sqltypes.Value{},
}

var session1 = &proto.Session{
	InTransaction: true,
	ShardSessions: []*proto.ShardSession{},
}

var session2 = &proto.Session{
	InTransaction: true,
	ShardSessions: []*proto.ShardSession{
		&proto.ShardSession{
			Keyspace:      "ks",
			Shard:         "1",
			TabletType:    topo.TYPE_MASTER,
			TransactionId: 1,
		},
	},
}

var splitQueryRequest = &querySplitQuery{
	Keyspace: "ks",
	SQL:      "in for SplitQuery",
	BindVariables: map[string]interface{}{
		"bind1": int64(43),
	},
	SplitColumn: "split_column",
	SplitCount:  13,
}

var splitQueryResult = []*pbg.SplitQueryResponse_Part{
	&pbg.SplitQueryResponse_Part{
		Query: &pbq.BoundQuery{
			Sql: "out for SplitQuery",
			BindVariables: map[string]*pbq.BindVariable{
				"bind1": &pbq.BindVariable{
					Type:     pbq.BindVariable_TYPE_INT,
					ValueInt: 1114444,
				},
			},
		},
		KeyRangePart: &pbg.SplitQueryResponse_KeyRangePart{
			Keyspace: "ksout",
			KeyRanges: []*pb.KeyRange{
				&pb.KeyRange{
					Start: []byte{'s'},
					End:   []byte{'e'},
				},
			},
		},
		Size: 12344,
	},
}

var getSrvKeyspaceKeyspace = "test_keyspace"

var getSrvKeyspaceResult = &pb.SrvKeyspace{
	Partitions: []*pb.SrvKeyspace_KeyspacePartition{
		&pb.SrvKeyspace_KeyspacePartition{
			ServedType: pb.TabletType_REPLICA,
			ShardReferences: []*pb.ShardReference{
				&pb.ShardReference{
					Name: "shard0",
					KeyRange: &pb.KeyRange{
						Start: []byte{'s'},
						End:   []byte{'e'},
					},
				},
			},
		},
	},
	ShardingColumnName: "sharding_column_name",
	ShardingColumnType: pb.KeyspaceIdType_UINT64,
	ServedFrom: []*pb.SrvKeyspace_ServedFrom{
		&pb.SrvKeyspace_ServedFrom{
			TabletType: pb.TabletType_MASTER,
			Keyspace:   "other_keyspace",
		},
	},
	SplitShardCount: 128,
}
