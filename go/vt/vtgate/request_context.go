// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

type requestContext struct {
	ctx              context.Context
	sql, comments    string
	bindVars         map[string]interface{}
	keyspace         string
	tabletType       topodatapb.TabletType
	session          *vtgatepb.Session
	notInTransaction bool
	options          *querypb.ExecuteOptions
	router           *Router
}

func newRequestContext(ctx context.Context, sql string, bindVars map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions, router *Router) *requestContext {
	query, comments := sqlparser.SplitTrailingComments(sql)
	return &requestContext{
		ctx:              ctx,
		sql:              query,
		comments:         comments,
		bindVars:         bindVars,
		keyspace:         keyspace,
		tabletType:       tabletType,
		session:          session,
		notInTransaction: notInTransaction,
		options:          options,
		router:           router,
	}
}

func (vc *requestContext) Execute(query string, bindvars map[string]interface{}) (*sqltypes.Result, error) {
	// We have to use an empty keyspace here, becasue vindexes that call back can reference
	// any table.
	return vc.router.Execute(vc.ctx, query, bindvars, "", vc.tabletType, vc.session, false, vc.options)
}

func (vc *requestContext) ExecuteRoute(route *engine.Route, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	return vc.router.ExecuteRoute(vc, route, joinvars)
}

func (vc *requestContext) StreamExecuteRoute(route *engine.Route, joinvars map[string]interface{}, sendReply func(*sqltypes.Result) error) error {
	return vc.router.StreamExecuteRoute(vc, route, joinvars, sendReply)
}

func (vc *requestContext) GetRouteFields(route *engine.Route, joinvars map[string]interface{}) (*sqltypes.Result, error) {
	return vc.router.GetRouteFields(vc, route, joinvars)
}
