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

func (c fallbackClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return c.fallback.Execute(ctx, sql, bindVariables, tabletType, session, notInTransaction)
}

func (c fallbackClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return c.fallback.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction)
}

func (c fallbackClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return c.fallback.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction)
}

func (c fallbackClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return c.fallback.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction)
}

func (c fallbackClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	return c.fallback.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction)
}

func (c fallbackClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
	return c.fallback.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session)
}

func (c fallbackClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
	return c.fallback.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session)
}

func (c fallbackClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecute(ctx, sql, bindVariables, tabletType, sendReply)
}

func (c fallbackClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, sendReply)
}

func (c fallbackClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, sendReply)
}

func (c fallbackClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	return c.fallback.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, sendReply)
}

func (c fallbackClient) Begin(ctx context.Context) (*vtgatepb.Session, error) {
	return c.fallback.Begin(ctx)
}

func (c fallbackClient) Commit(ctx context.Context, session *vtgatepb.Session) error {
	return c.fallback.Commit(ctx, session)
}

func (c fallbackClient) Rollback(ctx context.Context, session *vtgatepb.Session) error {
	return c.fallback.Rollback(ctx, session)
}

func (c fallbackClient) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	return c.fallback.SplitQuery(ctx, sql, keyspace, bindVariables, splitColumn, splitCount)
}

// TODO(erez): Rename after migration to SplitQuery V2 is done.
func (c fallbackClient) SplitQueryV2(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm,
) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	return c.fallback.SplitQueryV2(
		ctx, sql, keyspace, bindVariables, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
}

func (c fallbackClient) GetSrvKeyspace(ctx context.Context, keyspace string) (*topodatapb.SrvKeyspace, error) {
	return c.fallback.GetSrvKeyspace(ctx, keyspace)
}

func (c fallbackClient) GetSrvShard(ctx context.Context, keyspace, shard string) (*topodatapb.SrvShard, error) {
	return c.fallback.GetSrvShard(ctx, keyspace, shard)
}

func (c fallbackClient) HandlePanic(err *error) {
	c.fallback.HandlePanic(err)
}
