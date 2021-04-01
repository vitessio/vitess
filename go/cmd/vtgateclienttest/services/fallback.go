/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package services

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// fallbackClient implements vtgateservice.VTGateService, and always passes
// through to its fallback service. This is useful to embed into other clients
// so the fallback behavior doesn't have to be explicitly implemented in each
// one.
type fallbackClient struct {
	fallback vtgateservice.VTGateService
}

func newFallbackClient(fallback vtgateservice.VTGateService) fallbackClient {
	return fallbackClient{fallback: fallback}
}

func (c fallbackClient) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	return c.fallback.Execute(ctx, session, sql, bindVariables)
}

func (c fallbackClient) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	return c.fallback.ExecuteBatch(ctx, session, sqlList, bindVariablesList)
}

func (c fallbackClient) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecute(ctx, session, sql, bindVariables, callback)
}

func (c fallbackClient) ResolveTransaction(ctx context.Context, dtid string) error {
	return c.fallback.ResolveTransaction(ctx, dtid)
}

func (c fallbackClient) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, send func([]*binlogdatapb.VEvent) error) error {
	return c.fallback.VStream(ctx, tabletType, vgtid, filter, flags, send)
}

func (c fallbackClient) HandlePanic(err *error) {
	c.fallback.HandlePanic(err)
}
