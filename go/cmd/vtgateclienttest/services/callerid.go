// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// CallerIDPrefix is the prefix to send with queries so they go
// through this test suite.
const CallerIDPrefix = "callerid://"

// callerIDClient implements vtgateservice.VTGateService, and checks
// the received callerid matches the one passed out of band by the client.
type callerIDClient struct {
	fallbackClient
}

func newCallerIDClient(fallback vtgateservice.VTGateService) *callerIDClient {
	return &callerIDClient{
		fallbackClient: newFallbackClient(fallback),
	}
}

// checkCallerID will see if this module is handling the request, and
// if it is, check the callerID from the context.  Returns false if
// the query is not for this module.  Returns true and the error to
// return with the call if this module is handling the request.
func (c *callerIDClient) checkCallerID(ctx context.Context, received string) (bool, error) {
	if !strings.HasPrefix(received, CallerIDPrefix) {
		return false, nil
	}

	jsonCallerID := []byte(received[len(CallerIDPrefix):])
	expectedCallerID := &vtrpcpb.CallerID{}
	if err := json.Unmarshal(jsonCallerID, expectedCallerID); err != nil {
		return true, fmt.Errorf("cannot unmarshal provided callerid: %v", err)
	}

	receivedCallerID := callerid.EffectiveCallerIDFromContext(ctx)
	if receivedCallerID == nil {
		return true, fmt.Errorf("no callerid received in the query")
	}

	if !reflect.DeepEqual(receivedCallerID, expectedCallerID) {
		return true, fmt.Errorf("callerid mismatch, got %v expected %v", receivedCallerID, expectedCallerID)
	}

	return true, fmt.Errorf("SUCCESS: callerid matches")
}

func (c *callerIDClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return session, nil, err
	}
	return c.fallbackClient.Execute(ctx, sql, bindVariables, keyspace, tabletType, session, notInTransaction, options)
}

func (c *callerIDClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return nil, err
	}
	return c.fallbackClient.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction, options)
}

func (c *callerIDClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return nil, err
	}
	return c.fallbackClient.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, options)
}

func (c *callerIDClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return nil, err
	}
	return c.fallbackClient.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction, options)
}

func (c *callerIDClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return nil, err
	}
	return c.fallbackClient.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, options)
}

func (c *callerIDClient) ExecuteBatch(ctx context.Context, sqlList []string, bindVariablesList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sqlList) == 1 {
		if ok, err := c.checkCallerID(ctx, sqlList[0]); ok {
			return session, nil, err
		}
	}
	return c.fallbackClient.ExecuteBatch(ctx, sqlList, bindVariablesList, keyspace, tabletType, session, options)
}

func (c *callerIDClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if len(queries) == 1 {
		if ok, err := c.checkCallerID(ctx, queries[0].Query.Sql); ok {
			return nil, err
		}
	}
	return c.fallbackClient.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session, options)
}

func (c *callerIDClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if len(queries) == 1 {
		if ok, err := c.checkCallerID(ctx, queries[0].Query.Sql); ok {
			return nil, err
		}
	}
	return c.fallbackClient.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session, options)
}

func (c *callerIDClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return err
	}
	return c.fallbackClient.StreamExecute(ctx, sql, bindVariables, keyspace, tabletType, options, callback)
}

func (c *callerIDClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return err
	}
	return c.fallbackClient.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, options, callback)
}

func (c *callerIDClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return err
	}
	return c.fallbackClient.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, options, callback)
}

func (c *callerIDClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return err
	}
	return c.fallbackClient.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, options, callback)
}

func (c *callerIDClient) MessageStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, name string, callback func(*sqltypes.Result) error) error {
	if ok, err := c.checkCallerID(ctx, name); ok {
		return err
	}
	return c.fallback.MessageStream(ctx, keyspace, shard, keyRange, name, callback)
}

func (c *callerIDClient) MessageAck(ctx context.Context, keyspace string, name string, ids []*querypb.Value) (int64, error) {
	if ok, err := c.checkCallerID(ctx, name); ok {
		return 0, err
	}
	return c.fallback.MessageAck(ctx, keyspace, name, ids)
}

func (c *callerIDClient) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	if ok, err := c.checkCallerID(ctx, sql); ok {
		return nil, err
	}
	return c.fallbackClient.SplitQuery(
		ctx,
		sql,
		keyspace,
		bindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}

func (c *callerIDClient) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	if ok, err := c.checkCallerID(ctx, shard); ok {
		return err
	}
	return c.fallbackClient.UpdateStream(ctx, keyspace, shard, keyRange, tabletType, timestamp, event, callback)
}
