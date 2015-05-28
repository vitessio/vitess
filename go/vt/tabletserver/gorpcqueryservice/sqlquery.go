// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcqueryservice

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/callinfo"
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
	return sq.server.GetSessionId(sessionParams, sessionInfo)
}

// Begin is exposing tabletserver.SqlQuery.Begin
func (sq *SqlQuery) Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.Begin(callinfo.RPCWrapCallInfo(ctx), session, txInfo)
}

// Commit is exposing tabletserver.SqlQuery.Commit
func (sq *SqlQuery) Commit(ctx context.Context, session *proto.Session, noOutput *string) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.Commit(callinfo.RPCWrapCallInfo(ctx), session)
}

// Rollback is exposing tabletserver.SqlQuery.Rollback
func (sq *SqlQuery) Rollback(ctx context.Context, session *proto.Session, noOutput *string) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.Rollback(callinfo.RPCWrapCallInfo(ctx), session)
}

// Execute is exposing tabletserver.SqlQuery.Execute
func (sq *SqlQuery) Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) (err error) {
	defer sq.server.HandlePanic(&err)
	execErr := sq.server.Execute(callinfo.RPCWrapCallInfo(ctx), query, reply)
	tabletserver.AddTabletErrorToQueryResult(execErr, reply)
	return nil
}

// StreamExecute is exposing tabletserver.SqlQuery.StreamExecute
func (sq *SqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.StreamExecute(callinfo.RPCWrapCallInfo(ctx), query, func(reply *mproto.QueryResult) error {
		return sendReply(reply)
	})
}

// ExecuteBatch is exposing tabletserver.SqlQuery.ExecuteBatch
func (sq *SqlQuery) ExecuteBatch(ctx context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.ExecuteBatch(callinfo.RPCWrapCallInfo(ctx), queryList, reply)
}

// SplitQuery is exposing tabletserver.SqlQuery.SplitQuery
func (sq *SqlQuery) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) (err error) {
	defer sq.server.HandlePanic(&err)
	return sq.server.SplitQuery(callinfo.RPCWrapCallInfo(ctx), req, reply)
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
