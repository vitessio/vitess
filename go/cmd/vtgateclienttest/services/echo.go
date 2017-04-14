// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package services

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/querytypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// EchoPrefix is the prefix to send with queries so they go
// through this test suite.
const EchoPrefix = "echo://"

// echoClient implements vtgateservice.VTGateService, and prints the method
// params into a QueryResult as fields and a single row. This allows checking
// of request/result encoding/decoding.
type echoClient struct {
	fallbackClient
}

func newEchoClient(fallback vtgateservice.VTGateService) *echoClient {
	return &echoClient{
		fallbackClient: newFallbackClient(fallback),
	}
}

func printSortedMap(val reflect.Value) []byte {
	var keys []string
	for _, key := range val.MapKeys() {
		keys = append(keys, key.String())
	}
	sort.Strings(keys)
	buf := &bytes.Buffer{}
	buf.WriteString("map[")
	for i, key := range keys {
		if i > 0 {
			buf.WriteRune(' ')
		}
		fmt.Fprintf(buf, "%s:%v", key, val.MapIndex(reflect.ValueOf(key)).Interface())
	}
	buf.WriteRune(']')
	return buf.Bytes()
}

func echoQueryResult(vals map[string]interface{}) *sqltypes.Result {
	qr := &sqltypes.Result{}

	var row []sqltypes.Value

	// The first two returned fields are always a field with a MySQL NULL value,
	// and another field with a zero-length string.
	// Client tests can use this to check that they correctly distinguish the two.
	qr.Fields = append(qr.Fields, &querypb.Field{Name: "null", Type: sqltypes.VarBinary})
	row = append(row, sqltypes.NULL)
	qr.Fields = append(qr.Fields, &querypb.Field{Name: "emptyString", Type: sqltypes.VarBinary})
	row = append(row, sqltypes.MakeString([]byte("")))

	for k, v := range vals {
		qr.Fields = append(qr.Fields, &querypb.Field{Name: k, Type: sqltypes.VarBinary})

		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Map {
			row = append(row, sqltypes.MakeString(printSortedMap(val)))
			continue
		}
		row = append(row, sqltypes.MakeString([]byte(fmt.Sprintf("%v", v))))
	}
	qr.Rows = [][]sqltypes.Value{row}

	if options, ok := vals["options"]; ok {
		o := options.(*querypb.ExecuteOptions)
		if o != nil && o.CompareEventToken != nil {
			qr.Extras = &querypb.ResultExtras{
				Fresher:    true,
				EventToken: o.CompareEventToken,
			}
		}
	}

	return qr
}

func (c *echoClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*vtgatepb.Session, *sqltypes.Result, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		return session, echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
			"options":          options,
		}), nil
	}
	return c.fallbackClient.Execute(ctx, sql, bindVariables, keyspace, tabletType, session, notInTransaction, options)
}

func (c *echoClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		return echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"shards":           shards,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
			"options":          options,
		}), nil
	}
	return c.fallbackClient.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction, options)
}

func (c *echoClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		return echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"keyspaceIds":      keyspaceIds,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
			"options":          options,
		}), nil
	}
	return c.fallbackClient.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, options)
}

func (c *echoClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		return echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"keyRanges":        keyRanges,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
			"options":          options,
		}), nil
	}
	return c.fallbackClient.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction, options)
}

func (c *echoClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		return echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"entityColumnName": entityColumnName,
			"entityIds":        entityKeyspaceIDs,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
			"options":          options,
		}), nil
	}
	return c.fallbackClient.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, options)
}

func (c *echoClient) ExecuteBatch(ctx context.Context, sqlList []string, bindVariablesList []map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, session *vtgatepb.Session, options *querypb.ExecuteOptions) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sqlList) > 0 && strings.HasPrefix(sqlList[0], EchoPrefix) {
		var queryResponse []sqltypes.QueryResponse
		if bindVariablesList == nil {
			bindVariablesList = make([]map[string]interface{}, len(sqlList))
		}
		for queryNum, query := range sqlList {
			result := echoQueryResult(map[string]interface{}{
				"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
				"query":      query,
				"bindVars":   bindVariablesList[queryNum],
				"keyspace":   keyspace,
				"tabletType": tabletType,
				"session":    session,
				"options":    options,
			})
			queryResponse = append(queryResponse, sqltypes.QueryResponse{QueryResult: result, QueryError: nil})
		}
		return session, queryResponse, nil
	}
	return c.fallbackClient.ExecuteBatch(ctx, sqlList, bindVariablesList, keyspace, tabletType, session, options)
}

func (c *echoClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if len(queries) > 0 && strings.HasPrefix(queries[0].Query.Sql, EchoPrefix) {
		var result []sqltypes.Result
		for _, query := range queries {
			result = append(result, *echoQueryResult(map[string]interface{}{
				"callerId":      callerid.EffectiveCallerIDFromContext(ctx),
				"query":         query.Query.Sql,
				"bindVars":      query.Query.BindVariables,
				"keyspace":      query.Keyspace,
				"shards":        query.Shards,
				"tabletType":    tabletType,
				"session":       session,
				"asTransaction": asTransaction,
				"options":       options,
			}))
		}
		return result, nil
	}
	return c.fallbackClient.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session, options)
}

func (c *echoClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	if len(queries) > 0 && strings.HasPrefix(queries[0].Query.Sql, EchoPrefix) {
		var result []sqltypes.Result
		for _, query := range queries {
			result = append(result, *echoQueryResult(map[string]interface{}{
				"callerId":      callerid.EffectiveCallerIDFromContext(ctx),
				"query":         query.Query.Sql,
				"bindVars":      query.Query.BindVariables,
				"keyspace":      query.Keyspace,
				"keyspaceIds":   query.KeyspaceIds,
				"tabletType":    tabletType,
				"session":       session,
				"asTransaction": asTransaction,
				"options":       options,
			}))
		}
		return result, nil
	}
	return c.fallbackClient.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session, options)
}

func (c *echoClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		callback(echoQueryResult(map[string]interface{}{
			"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
			"query":      sql,
			"bindVars":   bindVariables,
			"keyspace":   keyspace,
			"tabletType": tabletType,
			"options":    options,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecute(ctx, sql, bindVariables, keyspace, tabletType, options, callback)
}

func (c *echoClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		callback(echoQueryResult(map[string]interface{}{
			"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
			"query":      sql,
			"bindVars":   bindVariables,
			"keyspace":   keyspace,
			"shards":     shards,
			"tabletType": tabletType,
			"options":    options,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, options, callback)
}

func (c *echoClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		callback(echoQueryResult(map[string]interface{}{
			"callerId":    callerid.EffectiveCallerIDFromContext(ctx),
			"query":       sql,
			"bindVars":    bindVariables,
			"keyspace":    keyspace,
			"keyspaceIds": keyspaceIds,
			"tabletType":  tabletType,
			"options":     options,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, options, callback)
}

func (c *echoClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		callback(echoQueryResult(map[string]interface{}{
			"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
			"query":      sql,
			"bindVars":   bindVariables,
			"keyspace":   keyspace,
			"keyRanges":  keyRanges,
			"tabletType": tabletType,
			"options":    options,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, options, callback)
}

func (c *echoClient) SplitQuery(
	ctx context.Context,
	keyspace string,
	sql string,
	bindVariables map[string]interface{},
	splitColumns []string,
	splitCount int64,
	numRowsPerQueryPart int64,
	algorithm querypb.SplitQueryRequest_Algorithm) ([]*vtgatepb.SplitQueryResponse_Part, error) {

	if strings.HasPrefix(sql, EchoPrefix) {
		bv, err := querytypes.BindVariablesToProto3(bindVariables)
		if err != nil {
			return nil, err
		}
		return []*vtgatepb.SplitQueryResponse_Part{
			{
				Query: &querypb.BoundQuery{
					Sql: fmt.Sprintf("%v:%v:%v:%v:%v",
						sql, splitColumns, splitCount, numRowsPerQueryPart, algorithm),
					BindVariables: bv,
				},
				KeyRangePart: &vtgatepb.SplitQueryResponse_KeyRangePart{
					Keyspace: keyspace,
				},
			},
		}, nil
	}
	return c.fallback.SplitQuery(
		ctx,
		sql,
		keyspace,
		bindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}

func (c *echoClient) UpdateStream(ctx context.Context, keyspace string, shard string, keyRange *topodatapb.KeyRange, tabletType topodatapb.TabletType, timestamp int64, event *querypb.EventToken, callback func(*querypb.StreamEvent, int64) error) error {
	if strings.HasPrefix(shard, EchoPrefix) {
		m := map[string]interface{}{
			"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
			"keyspace":   keyspace,
			"shard":      shard,
			"keyRange":   keyRange,
			"timestamp":  timestamp,
			"tabletType": tabletType,
			"event":      event,
		}
		bytes := printSortedMap(reflect.ValueOf(m))
		callback(&querypb.StreamEvent{
			EventToken: &querypb.EventToken{
				Position: string(bytes),
			},
		}, 0)
		return nil
	}
	return c.fallbackClient.UpdateStream(ctx, keyspace, shard, keyRange, tabletType, timestamp, event, callback)
}
