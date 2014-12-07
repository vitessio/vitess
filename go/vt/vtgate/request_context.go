// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"code.google.com/p/go.net/context"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

type requestContext struct {
	ctx    context.Context
	query  *proto.Query
	router *Router
}

func newRequestContext(ctx context.Context, query *proto.Query, router *Router) *requestContext {
	return &requestContext{
		ctx:    ctx,
		query:  query,
		router: router,
	}
}

func (vc *requestContext) Execute(boundQuery *tproto.BoundQuery) (*mproto.QueryResult, error) {
	q := &proto.Query{
		Sql:           boundQuery.Sql,
		BindVariables: boundQuery.BindVariables,
		TabletType:    vc.query.TabletType,
		Session:       vc.query.Session,
	}
	return vc.router.Execute(vc.ctx, q)
}
