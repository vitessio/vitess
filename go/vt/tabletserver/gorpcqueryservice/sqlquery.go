// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcqueryservice

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/rpcwrap"
	rpcproto "github.com/youtube/vitess/go/rpcwrap/proto"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

type SqlQuery struct {
	server *tabletserver.SqlQuery
}

func (sq *SqlQuery) GetSessionId(sessionParams *proto.SessionParams, sessionInfo *proto.SessionInfo) error {
	return sq.server.GetSessionId(sessionParams, sessionInfo)
}

func (sq *SqlQuery) Begin(ctx *rpcproto.Context, session *proto.Session, txInfo *proto.TransactionInfo) error {
	return sq.server.Begin(ctx, session, txInfo)
}

func (sq *SqlQuery) Commit(ctx *rpcproto.Context, session *proto.Session, noOutput *string) error {
	return sq.server.Commit(ctx, session)
}

func (sq *SqlQuery) Rollback(ctx *rpcproto.Context, session *proto.Session, noOutput *string) error {
	return sq.server.Rollback(ctx, session)
}

func (sq *SqlQuery) Execute(ctx *rpcproto.Context, query *proto.Query, reply *mproto.QueryResult) error {
	return sq.server.Execute(ctx, query, reply)
}

func (sq *SqlQuery) StreamExecute(ctx *rpcproto.Context, query *proto.Query, sendReply func(reply interface{}) error) error {
	return sq.server.StreamExecute(ctx, query, func(reply *mproto.QueryResult) error {
		return sendReply(reply)
	})
}

func (sq *SqlQuery) ExecuteBatch(ctx *rpcproto.Context, queryList *proto.QueryList, reply *proto.QueryResultList) error {
	return sq.server.ExecuteBatch(ctx, queryList, reply)
}

func (sq *SqlQuery) SplitQuery(ctx *rpcproto.Context, req *proto.SplitQueryRequest, reply *proto.SplitQueryResult) error {
	return sq.server.SplitQuery(ctx, req, reply)
}

func init() {
	tabletserver.SqlQueryRegisterFunctions = append(tabletserver.SqlQueryRegisterFunctions, func(sq *tabletserver.SqlQuery) {
		rpcwrap.RegisterAuthenticated(&SqlQuery{sq})
	})
}
