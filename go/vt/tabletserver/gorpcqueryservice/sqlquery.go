// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcqueryservice

import (
	"sync"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/query"
)

// SqlQuery is the server object for gorpc SqlQuery
type SqlQuery struct {
	server queryservice.QueryService
}

// GetSessionId is exposing tabletserver.SqlQuery.GetSessionId
func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.GetSessionId(sessionParams, sessionInfo)
	tabletserver.AddTabletErrorToSessionInfo(tErr, sessionInfo)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// GetSessionId2 should not be used by anything other than tests.
// It will eventually replace GetSessionId, but it breaks compatibility with older clients.
// Once all clients are upgraded, it can be replaced.
func (sq *SqlQuery) GetSessionId2(sessionIdReq *proto.GetSessionIdRequest, sessionInfo *proto.SessionInfo) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.GetSessionId(&sessionIdReq.Params, sessionInfo)
	tabletserver.AddTabletErrorToSessionInfo(tErr, sessionInfo)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// Begin is exposing tabletserver.SqlQuery.Begin
func (sq *SqlQuery) Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.Begin(callinfo.RPCWrapCallInfo(ctx), nil, session, txInfo)
	tabletserver.AddTabletErrorToTransactionInfo(tErr, txInfo)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// Begin2 should not be used by anything other than tests.
// It will eventually replace Begin, but it breaks compatibility with older clients.
// Once all clients are upgraded, it can be replaced.
func (sq *SqlQuery) Begin2(ctx context.Context, beginRequest *proto.BeginRequest, beginResponse *proto.BeginResponse) (err error) {
	defer sq.server.HandlePanic(&err)
	session := &proto.Session{
		SessionId: beginRequest.SessionId,
	}
	txInfo := new(proto.TransactionInfo)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(beginRequest.EffectiveCallerID),
		callerid.GoRPCImmediateCallerID(beginRequest.ImmediateCallerID),
	)
	tErr := sq.server.Begin(callinfo.RPCWrapCallInfo(ctx), proto.TargetToProto3(beginRequest.Target), session, txInfo)
	// Convert from TxInfo => beginResponse for the output
	beginResponse.TransactionId = txInfo.TransactionId
	tabletserver.AddTabletErrorToBeginResponse(tErr, beginResponse)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// Commit is exposing tabletserver.SqlQuery.Commit
func (sq *SqlQuery) Commit(ctx context.Context, session *proto.Session, noOutput *rpc.Unused) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.Commit(callinfo.RPCWrapCallInfo(ctx), nil, session)
}

// Commit2 should not be used by anything other than tests.
// It will eventually replace Commit, but it breaks compatibility with older clients.
// Once all clients are upgraded, it can be replaced.
func (sq *SqlQuery) Commit2(ctx context.Context, commitRequest *proto.CommitRequest, commitResponse *proto.CommitResponse) (err error) {
	defer sq.server.HandlePanic(&err)
	session := &proto.Session{
		SessionId:     commitRequest.SessionId,
		TransactionId: commitRequest.TransactionId,
	}
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(commitRequest.EffectiveCallerID),
		callerid.GoRPCImmediateCallerID(commitRequest.ImmediateCallerID),
	)
	tErr := sq.server.Commit(callinfo.RPCWrapCallInfo(ctx), proto.TargetToProto3(commitRequest.Target), session)
	tabletserver.AddTabletErrorToCommitResponse(tErr, commitResponse)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// Rollback is exposing tabletserver.SqlQuery.Rollback
func (sq *SqlQuery) Rollback(ctx context.Context, session *proto.Session, noOutput *rpc.Unused) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.Rollback(callinfo.RPCWrapCallInfo(ctx), nil, session)
}

// Rollback2 should not be used by anything other than tests.
// It will eventually replace Rollback, but it breaks compatibility with older clients.
// Once all clients are upgraded, it can be replaced.
func (sq *SqlQuery) Rollback2(ctx context.Context, rollbackRequest *proto.RollbackRequest, rollbackResponse *proto.RollbackResponse) (err error) {
	defer sq.server.HandlePanic(&err)
	session := &proto.Session{
		SessionId:     rollbackRequest.SessionId,
		TransactionId: rollbackRequest.TransactionId,
	}
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(rollbackRequest.EffectiveCallerID),
		callerid.GoRPCImmediateCallerID(rollbackRequest.ImmediateCallerID),
	)
	tErr := sq.server.Rollback(callinfo.RPCWrapCallInfo(ctx), proto.TargetToProto3(rollbackRequest.Target), session)
	tabletserver.AddTabletErrorToRollbackResponse(tErr, rollbackResponse)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// Execute is exposing tabletserver.SqlQuery.Execute
func (sq *SqlQuery) Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.Execute(callinfo.RPCWrapCallInfo(ctx), nil, query, reply)
	tabletserver.AddTabletErrorToQueryResult(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// Execute2 should not be used by anything other than tests
// It will eventually replace Execute, but it breaks compatibility with older clients
// Once all clients are upgraded, it can be replaced
func (sq *SqlQuery) Execute2(ctx context.Context, executeRequest *proto.ExecuteRequest, reply *mproto.QueryResult) (err error) {
	defer sq.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(executeRequest.EffectiveCallerID),
		callerid.GoRPCImmediateCallerID(executeRequest.ImmediateCallerID),
	)
	tErr := sq.server.Execute(callinfo.RPCWrapCallInfo(ctx), proto.TargetToProto3(executeRequest.Target), &executeRequest.QueryRequest, reply)
	tabletserver.AddTabletErrorToQueryResult(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// StreamExecute is exposing tabletserver.SqlQuery.StreamExecute
func (sq *SqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.StreamExecute(callinfo.RPCWrapCallInfo(ctx), nil, query, func(reply *mproto.QueryResult) error {
		return sendReply(reply)
	})
}

// StreamExecute2 should not be used by anything other than tests.
// It will eventually replace Rollback, but it breaks compatibility with older clients.
// Once all clients are upgraded, it can be replaced.
func (sq *SqlQuery) StreamExecute2(ctx context.Context, req *proto.StreamExecuteRequest, sendReply func(reply interface{}) error) (err error) {
	defer sq.server.HandlePanic(&err)
	if req == nil || req.Query == nil {
		return nil
	}
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(req.EffectiveCallerID),
		callerid.GoRPCImmediateCallerID(req.ImmediateCallerID),
	)
	tErr := sq.server.StreamExecute(callinfo.RPCWrapCallInfo(ctx), proto.TargetToProto3(req.Target), req.Query, func(reply *mproto.QueryResult) error {
		return sendReply(reply)
	})
	if tErr == nil {
		return nil
	}
	if *tabletserver.RPCErrorOnlyInReply {
		// If there was an app error, send a QueryResult back with it.
		qr := new(mproto.QueryResult)
		tabletserver.AddTabletErrorToQueryResult(tErr, qr)
		// Sending back errors this way is not backwards compatible. If a (new) server sends an additional
		// QueryResult with an error, and the (old) client doesn't know how to read it, it will cause
		// problems where the client will get out of sync with the number of QueryResults sent.
		// That's why this the error is only sent this way when the --rpc_errors_only_in_reply flag is set
		// (signalling that all clients are able to handle new-style errors).
		return sendReply(qr)
	}
	return tErr
}

// ExecuteBatch is exposing tabletserver.SqlQuery.ExecuteBatch
func (sq *SqlQuery) ExecuteBatch(ctx context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.ExecuteBatch(callinfo.RPCWrapCallInfo(ctx), nil, queryList, reply)
	tabletserver.AddTabletErrorToQueryResultList(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// ExecuteBatch2 should not be used by anything other than tests
// It will eventually replace ExecuteBatch, but it breaks compatibility with older clients.
// Once all clients are upgraded, it can be replaced.
func (sq *SqlQuery) ExecuteBatch2(ctx context.Context, req *proto.ExecuteBatchRequest, reply *proto.QueryResultList) (err error) {
	defer sq.server.HandlePanic(&err)
	if req == nil {
		return nil
	}
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(req.EffectiveCallerID),
		callerid.GoRPCImmediateCallerID(req.ImmediateCallerID),
	)
	tErr := sq.server.ExecuteBatch(callinfo.RPCWrapCallInfo(ctx), proto.TargetToProto3(req.Target), &req.QueryBatch, reply)
	tabletserver.AddTabletErrorToQueryResultList(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// SplitQuery is exposing tabletserver.SqlQuery.SplitQuery
func (sq *SqlQuery) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	defer sq.server.HandlePanic(&err)
	ctx = callerid.NewContext(ctx,
		callerid.GoRPCEffectiveCallerID(req.EffectiveCallerID),
		callerid.GoRPCImmediateCallerID(req.ImmediateCallerID),
	)
	tErr := sq.server.SplitQuery(callinfo.RPCWrapCallInfo(ctx), proto.TargetToProto3(req.Target), req, reply)
	tabletserver.AddTabletErrorToSplitQueryResult(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// StreamHealth is exposing tabletserver.SqlQuery.StreamHealthRegister and
// tabletserver.SqlQuery.StreamHealthUnregister
func (sq *SqlQuery) StreamHealth(ctx context.Context, query *rpc.Unused, sendReply func(reply interface{}) error) (err error) {
	defer sq.server.HandlePanic(&err)

	c := make(chan *pb.StreamHealthResponse, 10)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for shr := range c {
			// we send until the client disconnects
			if err := sendReply(shr); err != nil {
				return
			}
		}
	}()

	id, err := sq.server.StreamHealthRegister(c)
	if err != nil {
		close(c)
		wg.Wait()
		return err
	}
	wg.Wait()
	return sq.server.StreamHealthUnregister(id)
}

// New returns a new SqlQuery based on the QueryService implementation
func New(server queryservice.QueryService) *SqlQuery {
	return &SqlQuery{server}
}

func init() {
	tabletserver.QueryServiceControlRegisterFunctions = append(tabletserver.QueryServiceControlRegisterFunctions, func(qsc tabletserver.QueryServiceControl) {
		servenv.Register("queryservice", New(qsc.QueryService()))
	})
}
