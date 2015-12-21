// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"

	"github.com/youtube/vitess/go/vt/proto/topodatapb"
	"github.com/youtube/vitess/go/vt/proto/vtgatepb"
)

type requestContext struct {
	ctx              context.Context
	sql              string
	bindVariables    map[string]interface{}
	tabletType       topodatapb.TabletType
	session          *vtgatepb.Session
	notInTransaction bool
	router           *Router
}

func newRequestContext(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, router *Router) *requestContext {
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

func (vc *requestContext) Execute(boundQuery *querytypes.BoundQuery) (*sqltypes.Result, error) {
	return vc.router.Execute(vc.ctx, boundQuery.Sql, boundQuery.BindVariables, vc.tabletType, vc.session, false)
}
