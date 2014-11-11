// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcqueryservice

import (
	"code.google.com/p/go.net/context"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

type SqlQuery struct {
	server *tabletserver.SqlQuery
}

func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return sq.server.GetSessionId(sessionParams, sessionInfo)
}

func (sq *SqlQuery) Begin(ctx context.Context, session *proto.Session, txInfo *proto.TransactionInfo) error {
	return sq.server.Begin(ctx, session, txInfo)
}

func (sq *SqlQuery) Commit(ctx context.Context, session *proto.Session, noOutput *string) error {
	return sq.server.Commit(ctx, session)
}

func (sq *SqlQuery) Rollback(ctx context.Context, session *proto.Session, noOutput *string) error {
	return sq.server.Rollback(ctx, session)
}

func (sq *SqlQuery) Execute(ctx context.Context, query *proto.Query, reply *mproto.QueryResult) error {
	return sq.server.Execute(ctx, query, reply)
}

func (sq *SqlQuery) StreamExecute(ctx context.Context, query *proto.Query, sendReply func(reply interface{}) error) error {
	return sq.server.StreamExecute(ctx, query, func(reply *mproto.QueryResult) error {
		return sendReply(reply)
	})
}

func (sq *SqlQuery) ExecuteBatch(ctx context.Context, queryList *proto.QueryList, reply *proto.QueryResultList) error {
	return sq.server.ExecuteBatch(ctx, queryList, reply)
}

func (sq *SqlQuery) SplitQuery(ctx context.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	return sq.server.SplitQuery(ctx, req, reply)
}

func init() {
	tabletserver.SqlQueryRegisterFunctions = append(tabletserver.SqlQueryRegisterFunctions, func(sq *tabletserver.SqlQuery) {
		servenv.Register("queryservice", &SqlQuery{sq})
	})
}
