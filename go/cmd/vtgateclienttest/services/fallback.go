// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
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

func (c fallbackClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	return c.fallback.Execute(ctx, sql, bindVariables, keyspace, tabletType, session, notInTransaction, options)
}

func (c fallbackClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return c.fallback.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction, options)
}

func (c fallbackClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return c.fallback.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, options)
}

func (c fallbackClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return c.fallback.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction, options)
}

func (c fallbackClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	return c.fallback.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, options)
}

func (c fallbackClient) ExecuteBatch(ctx context.Context, sqlList []string, bindVariablesList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	return c.fallback.ExecuteBatch(ctx, sqlList, bindVariablesList, keyspace, tabletType, session, options)
}

func (c fallbackClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return c.fallback.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session, options)
}

func (c fallbackClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	return c.fallback.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session, options)
}

func (c fallbackClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecute(ctx, sql, bindVariables, keyspace, tabletType, options, callback)
}

func (c fallbackClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, options, callback)
}

func (c fallbackClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, options, callback)
}

func (c fallbackClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, options, callback)
}

func (c fallbackClient) Begin(ctx context.Context, singledb bool) (*vtgatepb.Session, error) {
	return c.fallback.Begin(ctx, singledb)
}

func (c fallbackClient) Commit(ctx context.Context, twopc bool, session *vtgatepb.Session) error {
	return c.fallback.Commit(ctx, twopc, session)
}

func (c fallbackClient) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	return c.fallback.Rollback(ctx, session)
}

func (c fallbackClient) ResolveTransaction(ctx context.Context, dtid string) error {
	return c.fallback.ResolveTransaction(ctx, dtid)
}

func (c fallbackClient) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	return c.fallback.MessageStream(ctx, keyspace, shard, keyRange, name, callback)
}

func (c fallbackClient) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	return c.fallback.MessageAck(ctx, keyspace, name, ids)
}

func (c fallbackClient) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	return c.fallback.SplitQuery(
		ctx, sql, keyspace, bindVariables, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
}

func (c fallbackClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return c.fallback.GetSrvKeyspace(ctx, keyspace)
}

func (c fallbackClient) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	return c.fallback.UpdateStream(ctx, keyspace, shard, keyRange, tabletType, timestamp, event, callback)
}

func (c fallbackClient) HandlePanic(err *error) {
	c.fallback.HandlePanic(err)
}
