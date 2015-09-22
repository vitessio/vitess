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
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateservice"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	pbq "github.com/youtube/vitess/go/vt/proto/query"
	pb "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
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

func echoQueryResult(vals map[string]interface{}) *mproto.QueryResult {
	qr := &mproto.QueryResult{}

	var row []sqltypes.Value
	for k, v := range vals {
		qr.Fields = append(qr.Fields, mproto.Field{Name: k})

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

func (c *echoClient) Execute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		reply.Result = echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
		})
		reply.Session = session
		return nil
	}
	return c.fallbackClient.Execute(ctx, sql, bindVariables, tabletType, session, notInTransaction, reply)
}

func (c *echoClient) ExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		reply.Result = echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"shards":           shards,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
		})
		reply.Session = session
		return nil
	}
	return c.fallbackClient.ExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, session, notInTransaction, reply)
}

func (c *echoClient) ExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		reply.Result = echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"keyspaceIds":      keyspaceIds,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
		})
		reply.Session = session
		return nil
	}
	return c.fallbackClient.ExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, session, notInTransaction, reply)
}

func (c *echoClient) ExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		reply.Result = echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"keyRanges":        keyRanges,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
		})
		reply.Session = session
		return nil
	}
	return c.fallbackClient.ExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, session, notInTransaction, reply)
}

func (c *echoClient) ExecuteEntityIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, entityColumnName string, entityKeyspaceIDs []*pbg.ExecuteEntityIdsRequest_EntityId, tabletType pb.TabletType, session *proto.Session, notInTransaction bool, reply *proto.QueryResult) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		reply.Result = echoQueryResult(map[string]interface{}{
			"callerId":         callerid.EffectiveCallerIDFromContext(ctx),
			"query":            sql,
			"bindVars":         bindVariables,
			"keyspace":         keyspace,
			"entityColumnName": entityColumnName,
			"entityIds":        entityKeyspaceIDs,
			"tabletType":       tabletType,
			"session":          session,
			"notInTransaction": notInTransaction,
		})
		reply.Session = session
		return nil
	}
	return c.fallbackClient.ExecuteEntityIds(ctx, sql, bindVariables, keyspace, entityColumnName, entityKeyspaceIDs, tabletType, session, notInTransaction, reply)
}

func (c *echoClient) ExecuteBatchShards(ctx context.Context, queries []proto.BoundShardQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	if len(queries) > 0 && strings.HasPrefix(queries[0].Sql, EchoPrefix) {
		for _, query := range queries {
			reply.List = append(reply.List, *echoQueryResult(map[string]interface{}{
				"callerId":      callerid.EffectiveCallerIDFromContext(ctx),
				"query":         query.Sql,
				"bindVars":      query.BindVariables,
				"keyspace":      query.Keyspace,
				"shards":        query.Shards,
				"tabletType":    tabletType,
				"session":       session,
				"asTransaction": asTransaction,
			}))
		}
		reply.Session = session
		return nil
	}
	return c.fallbackClient.ExecuteBatchShards(ctx, queries, tabletType, asTransaction, session, reply)
}

func (c *echoClient) ExecuteBatchKeyspaceIds(ctx context.Context, queries []proto.BoundKeyspaceIdQuery, tabletType pb.TabletType, asTransaction bool, session *proto.Session, reply *proto.QueryResultList) error {
	if len(queries) > 0 && strings.HasPrefix(queries[0].Sql, EchoPrefix) {
		for _, query := range queries {
			reply.List = append(reply.List, *echoQueryResult(map[string]interface{}{
				"callerId":      callerid.EffectiveCallerIDFromContext(ctx),
				"query":         query.Sql,
				"bindVars":      query.BindVariables,
				"keyspace":      query.Keyspace,
				"keyspaceIds":   query.KeyspaceIds,
				"tabletType":    tabletType,
				"session":       session,
				"asTransaction": asTransaction,
			}))
		}
		reply.Session = session
		return nil
	}
	return c.fallbackClient.ExecuteBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, session, reply)
}

func (c *echoClient) StreamExecute(ctx context.Context, sql string, bindVariables map[string]interface{}, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		sendReply(&proto.QueryResult{
			Result: echoQueryResult(map[string]interface{}{
				"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
				"query":      sql,
				"bindVars":   bindVariables,
				"tabletType": tabletType,
			})})
		return nil
	}
	return c.fallbackClient.StreamExecute(ctx, sql, bindVariables, tabletType, sendReply)
}

func (c *echoClient) StreamExecuteShards(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, shards []string, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		sendReply(&proto.QueryResult{
			Result: echoQueryResult(map[string]interface{}{
				"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
				"query":      sql,
				"bindVars":   bindVariables,
				"keyspace":   keyspace,
				"shards":     shards,
				"tabletType": tabletType,
			})})
		return nil
	}
	return c.fallbackClient.StreamExecuteShards(ctx, sql, bindVariables, keyspace, shards, tabletType, sendReply)
}

func (c *echoClient) StreamExecuteKeyspaceIds(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyspaceIds [][]byte, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		sendReply(&proto.QueryResult{
			Result: echoQueryResult(map[string]interface{}{
				"callerId":    callerid.EffectiveCallerIDFromContext(ctx),
				"query":       sql,
				"bindVars":    bindVariables,
				"keyspace":    keyspace,
				"keyspaceIds": keyspaceIds,
				"tabletType":  tabletType,
			})})
		return nil
	}
	return c.fallbackClient.StreamExecuteKeyspaceIds(ctx, sql, bindVariables, keyspace, keyspaceIds, tabletType, sendReply)
}

func (c *echoClient) StreamExecuteKeyRanges(ctx context.Context, sql string, bindVariables map[string]interface{}, keyspace string, keyRanges []*pb.KeyRange, tabletType pb.TabletType, sendReply func(*proto.QueryResult) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		sendReply(&proto.QueryResult{
			Result: echoQueryResult(map[string]interface{}{
				"callerId":   callerid.EffectiveCallerIDFromContext(ctx),
				"query":      sql,
				"bindVars":   bindVariables,
				"keyspace":   keyspace,
				"keyRanges":  keyRanges,
				"tabletType": tabletType,
			})})
		return nil
	}
	return c.fallbackClient.StreamExecuteKeyRanges(ctx, sql, bindVariables, keyspace, keyRanges, tabletType, sendReply)
}

func (c *echoClient) SplitQuery(ctx context.Context, keyspace string, sql string, bindVariables map[string]interface{}, splitColumn string, splitCount int) ([]*pbg.SplitQueryResponse_Part, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		return []*pbg.SplitQueryResponse_Part{
			&pbg.SplitQueryResponse_Part{
				Query: &pbq.BoundQuery{
					Sql:           fmt.Sprintf("%v:%v:%v", sql, splitColumn, splitCount),
					BindVariables: tproto.BindVariablesToProto3(bindVariables),
				},
				KeyRangePart: &pbg.SplitQueryResponse_KeyRangePart{
					Keyspace: keyspace,
				},
			},
		}, nil
	}
	return c.fallback.SplitQuery(ctx, sql, keyspace, bindVariables, splitColumn, splitCount)
}
