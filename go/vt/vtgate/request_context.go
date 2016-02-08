// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"github.com/youtube/vitess/go/sqltypes"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
)

type requestContext struct {
	ctx              context.Context
	sql              string
	bindVars         map[string]interface{}
	tabletType       topodatapb.TabletType
	session          *vtgatepb.Session
	notInTransaction bool
	router           *Router
	// JoinVars is set by Join and used by Route.
	JoinVars map[string]interface{}
}

func newRequestContext(ctx context.Context, sql string, bindVars map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, router *Router) *requestContext {
	return &requestContext{
		ctx:              ctx,
		sql:              sql,
		bindVars:         bindVars,
		tabletType:       tabletType,
		session:          session,
		notInTransaction: notInTransaction,
		router:           router,
		JoinVars:         make(map[string]interface{}),
	}
}

func (vc *requestContext) Execute(boundQuery *querytypes.BoundQuery) (*sqltypes.Result, error) {
	return vc.router.Execute(vc.ctx, boundQuery.Sql, boundQuery.BindVariables, vc.tabletType, vc.session, false)
}
