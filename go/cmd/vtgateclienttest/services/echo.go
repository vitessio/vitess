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
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

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

	return qr
}

func (c *echoClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		return echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
		}), nil
	}
	return c.fallbackClient.Execute(ctx, sql, bindVariables, tabletType, session, notInTransaction)
}

func (c *echoClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
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
		}), nil
	}
	return c.fallbackClient.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction)
}

func (c *echoClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
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
		}), nil
	}
	return c.fallbackClient.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction)
}

func (c *echoClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
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
		}), nil
	}
	return c.fallbackClient.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction)
}

func (c *echoClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*vtgatepb.ExecuteEntityIdsRequest_EntityId, tabletType topodatapb.TabletType, session *vtgatepb.Session, notInTransaction bool) (*sqltypes.Result, error) {
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
		}), nil
	}
	return c.fallbackClient.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction)
}

func (c *echoClient) ExecuteBatchShards(ctx context.Context, queries []*vtgatepb.BoundShardQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
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
			}))
		}
		return result, nil
	}
	return c.fallbackClient.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session)
}

func (c *echoClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []*vtgatepb.BoundKeyspaceIdQuery, tabletType topodatapb.TabletType, asTransaction bool, session *vtgatepb.Session) ([]sqltypes.Result, error) {
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
			}))
		}
		return result, nil
	}
	return c.fallbackClient.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session)
}

func (c *echoClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		sendReply(echoQueryResult(map[string]interface{}{
			"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
			"query":      sql,
			"bindVars":   bindVariables,
			"tabletType": tabletType,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecute(ctx, sql, bindVariables, tabletType, sendReply)
}

func (c *echoClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		sendReply(echoQueryResult(map[string]interface{}{
			"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
			"query":      sql,
			"bindVars":   bindVariables,
			"keyspace":   keyspace,
			"shards":     shards,
			"tabletType": tabletType,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, sendReply)
}

func (c *echoClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		sendReply(echoQueryResult(map[string]interface{}{
			"callerId":    callerid.EffectiveCallerIDFromContext(ctx),
			"query":       sql,
			"bindVars":    bindVariables,
			"keyspace":    keyspace,
			"keyspaceIds": keyspaceIds,
			"tabletType":  tabletType,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, sendReply)
}

func (c *echoClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*topodatapb.KeyRange, tabletType topodatapb.TabletType, sendReply func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		sendReply(echoQueryResult(map[string]interface{}{
			"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
			"query":      sql,
			"bindVars":   bindVariables,
			"keyspace":   keyspace,
			"keyRanges":  keyRanges,
			"tabletType": tabletType,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, sendReply)
}

func (c *echoClient) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int64) ([]*vtgatepb.SplitQueryResponse_Part, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		bv, err := querytypes.BindVariablesToProto3(bindVariables)
		if err != nil {
			return nil, err
		}
		return []*vtgatepb.SplitQueryResponse_Part{
			{
				Query: &querypb.BoundQuery{
					Sql:           fmt.Sprintf("%v:%v:%v", sql, splitColumn, splitCount),
					BindVariables: bv,
				},
				KeyRangePart: &vtgatepb.SplitQueryResponse_KeyRangePart{
					Keyspace: keyspace,
				},
			},
		}, nil
	}
	return c.fallback.SplitQuery(ctx, sql, keyspace, bindVariables, splitColumn, splitCount)
}

// TODO(erez): Rename after migration to SplitQuery V2 is done.
func (c *echoClient) SplitQueryV2(
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
	return c.fallback.SplitQueryV2(
		ctx,
		sql,
		keyspace,
		bindVariables,
		splitColumns,
		splitCount,
		numRowsPerQueryPart,
		algorithm)
}
