// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcqueryservice

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
	"golang.org/x/net/context"
)

// SqlQuery is the server object for gorpc SqlQuery
type SqlQuery struct {
	server *tabletserver.SqlQuery
}

// GetSessionId is exposing tabletserver.SqlQuery.GetSessionId
func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return sq.server.GetSessionId(sessionParams, sessionInfo)
}

// Begin is exposing tabletserver.SqlQuery.Begin
func (sq *SqlQuery) Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) error {
	return sq.server.Begin(ctx, session, txInfo)
}

// Commit is exposing tabletserver.SqlQuery.Commit
func (sq *SqlQuery) Commit(ctx context.Context, session *proto.Session, noOutput *string) error {
	return sq.server.Commit(ctx, session)
}

// Rollback is exposing tabletserver.SqlQuery.Rollback
func (sq *SqlQuery) Rollback(ctx context.Context, session *proto.Session, noOutput *string) error {
	return sq.server.Rollback(ctx, session)
}

// Execute is exposing tabletserver.SqlQuery.Execute
func (sq *SqlQuery) Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) error {
	return sq.server.Execute(ctx, query, reply)
}

// StreamExecute is exposing tabletserver.SqlQuery.StreamExecute
func (sq *SqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) error {
	return sq.server.StreamExecute(ctx, query, func(reply *mproto.QueryResult) error {
		return sendReply(reply)
	})
}

// ExecuteBatch is exposing tabletserver.SqlQuery.ExecuteBatch
func (sq *SqlQuery) ExecuteBatch(ctx context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) error {
	return sq.server.ExecuteBatch(ctx, queryList, reply)
}

// SplitQuery is exposing tabletserver.SqlQuery.SplitQuery
func (sq *SqlQuery) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	return sq.server.SplitQuery(ctx, req, reply)
}

func init() {
	tabletserver.QueryServiceControlRegisterFunctions = append(tabletserver.QueryServiceControlRegisterFunctions, func(qsc tabletserver.QueryServiceControl) {
		servenv.Register("queryservice", &SqlQuery{qsc.SqlQuery()})
	})
}
