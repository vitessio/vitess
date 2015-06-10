// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcqueryservice

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/rpc"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
	"golang.org/x/net/context"
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

// Begin is exposing tabletserver.SqlQuery.Begin
func (sq *SqlQuery) Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.Begin(callinfo.RPCWrapCallInfo(ctx), session, txInfo)
	tabletserver.AddTabletErrorToTransactionInfo(tErr, txInfo)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// UnsupportedNewCommit should not be used by anything other than tests.
// It will eventually replace Commit, but it breaks compatibility with older clients.
// Once all clients are upgraded, it can be replaced.
func (sq *SqlQuery) UnsupportedNewCommit(ctx context.Context, session *proto.Session, reply *proto.ErrorOnly) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.Commit(callinfo.RPCWrapCallInfo(ctx), session)
	tabletserver.AddTabletErrorToErrorOnly(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// Commit is exposing tabletserver.SqlQuery.Commit
func (sq *SqlQuery) Commit(ctx context.Context, session *proto.Session, noOutput *rpc.Unused) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.Commit(callinfo.RPCWrapCallInfo(ctx), session)
}

// UnsupportedNewRollback should not be used by anything other than tests.
// It will eventually replace Rollback, but it breaks compatibility with older clients.
// Once all clients are upgraded, it can be replaced.
func (sq *SqlQuery) UnsupportedNewRollback(ctx context.Context, session *proto.Session, reply *proto.ErrorOnly) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.Rollback(callinfo.RPCWrapCallInfo(ctx), session)
	tabletserver.AddTabletErrorToErrorOnly(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// Rollback is exposing tabletserver.SqlQuery.Rollback
func (sq *SqlQuery) Rollback(ctx context.Context, session *proto.Session, noOutput *rpc.Unused) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.Rollback(callinfo.RPCWrapCallInfo(ctx), session)
}

// Execute is exposing tabletserver.SqlQuery.Execute
func (sq *SqlQuery) Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.Execute(callinfo.RPCWrapCallInfo(ctx), query, reply)
	tabletserver.AddTabletErrorToQueryResult(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// StreamExecute is exposing tabletserver.SqlQuery.StreamExecute
func (sq *SqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.StreamExecute(callinfo.RPCWrapCallInfo(ctx), query, func(reply *mproto.QueryResult) error {
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
	tErr := sq.server.ExecuteBatch(callinfo.RPCWrapCallInfo(ctx), queryList, reply)
	tabletserver.AddTabletErrorToQueryResultList(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
}

// SplitQuery is exposing tabletserver.SqlQuery.SplitQuery
func (sq *SqlQuery) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	defer sq.server.HandlePanic(&err)
	tErr := sq.server.SplitQuery(callinfo.RPCWrapCallInfo(ctx), req, reply)
	tabletserver.AddTabletErrorToSplitQueryResult(tErr, reply)
	if *tabletserver.RPCErrorOnlyInReply {
		return nil
	}
	return tErr
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
