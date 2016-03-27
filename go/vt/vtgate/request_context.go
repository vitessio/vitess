// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"github.com/youtube/vitess/go/sqltypes"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

type requestContext struct {
	ctx              context.Context
	sql              string
	bindVars         map[string]interface{}
	tabletType       topodatapb.TabletType
	session          *vtgatepb.Session
	notInTransaction bool
	router           *Router
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
	}
}

func (vc *requestContext) Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	return vc.router.Execute(vc.ctx, query, bindvars, vc.tabletType, vc.session, false)
}

func (vc *requestContext) ExecRoute(route *engine.Route, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	return vc.router.ExecRoute(vc, route, joinvars)
}

func (vc *requestContext) StreamExecRoute(route *engine.Route, joinvars map[string]interface{}, sendReply func(*sqltypes.Result) error) error {
	return vc.router.StreamExecRoute(vc, route, joinvars, sendReply)
}

func (vc *requestContext) GetRouteFields(route *engine.Route, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	return vc.router.GetRouteFields(vc, route, joinvars)
}
