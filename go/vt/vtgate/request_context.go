// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"golang.org/x/net/context"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

type requestContext struct {
	ctx              context.Context
	sql              string
	bindVariables    map[string]interface{}
	tabletType       pb.TabletType
	session          *proto.Session
	notInTransaction bool
	router           *Router
}

func newRequestContext(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, router *Router) *requestContext {
	return &requestContext{
		ctx:              ctx,
		sql:              sql,
		bindVariables:    bindVariables,
		tabletType:       tabletType,
		session:          session,
		notInTransaction: notInTransaction,
		router:           router,
	}
}

func (vc *requestContext) Execute(boundQuery *tproto.BoundQuery) (*mproto.QueryResult, error) {
	return vc.router.Execute(vc.ctx, boundQuery.Sql, boundQuery.BindVariables, vc.tabletType, vc.session, false)
}
