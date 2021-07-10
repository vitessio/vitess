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
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
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
	row = append(row, sqltypes.NewVarBinary(""))

	for k, v := range vals {
		qr.Fields = append(qr.Fields, &querypb.Field{Name: k, Type: sqltypes.VarBinary})

		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Map {
			row = append(row, sqltypes.MakeTrusted(sqltypes.VarBinary, printSortedMap(val)))
			continue
		}
		row = append(row, sqltypes.NewVarBinary(fmt.Sprintf("%v", v)))
	}
	qr.Rows = [][]sqltypes.Value{row}

	return qr
}

func (c *echoClient) Execute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable) (*vtgatepb.Session, *sqltypes.Result, error) {
	if strings.HasPrefix(sql, EchoPrefix) {
		return session, echoQueryResult(map[string]interface{}{
			"callerId": callerid.EffectiveCallerIDFromContext(ctx),
			"query":    sql,
			"bindVars": bindVariables,
			"session":  session,
		}), nil
	}
	return c.fallbackClient.Execute(ctx, session, sql, bindVariables)
}

func (c *echoClient) StreamExecute(ctx context.Context, session *vtgatepb.Session, sql string, bindVariables map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	if strings.HasPrefix(sql, EchoPrefix) {
		callback(echoQueryResult(map[string]interface{}{
			"callerId": callerid.EffectiveCallerIDFromContext(ctx),
			"query":    sql,
			"bindVars": bindVariables,
			"session":  session,
		}))
		return nil
	}
	return c.fallbackClient.StreamExecute(ctx, session, sql, bindVariables, callback)
}

func (c *echoClient) ExecuteBatch(ctx context.Context, session *vtgatepb.Session, sqlList []string, bindVariablesList []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	if len(sqlList) > 0 && strings.HasPrefix(sqlList[0], EchoPrefix) {
		var queryResponse []sqltypes.QueryResponse
		if bindVariablesList == nil {
			bindVariablesList = make([]map[string]*querypb.BindVariable, len(sqlList))
		}
		for queryNum, query := range sqlList {
			result := echoQueryResult(map[string]interface{}{
				"callerId": callerid.EffectiveCallerIDFromContext(ctx),
				"query":    query,
				"bindVars": bindVariablesList[queryNum],
				"session":  session,
			})
			queryResponse = append(queryResponse, sqltypes.QueryResponse{QueryResult: result, QueryError: nil})
		}
		return session, queryResponse, nil
	}
	return c.fallbackClient.ExecuteBatch(ctx, session, sqlList, bindVariablesList)
}

func (c *echoClient) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, callback func([]*binlogdatapb.VEvent) error) error {
	if strings.HasPrefix(vgtid.ShardGtids[0].Shard, EchoPrefix) {
		_ = callback([]*binlogdatapb.VEvent{
			{
				Type:      1,
				Timestamp: 1234,
				Gtid:      "echo-gtid-1",
				Statement: "echo-ddl-1",
				Vgtid:     vgtid,
				RowEvent: &binlogdatapb.RowEvent{
					TableName: "echo-table-1",
				},
			},
			{
				Type:      2,
				Timestamp: 4321,
				Gtid:      "echo-gtid-2",
				Statement: "echo-ddl-2",
				Vgtid:     vgtid,
				FieldEvent: &binlogdatapb.FieldEvent{
					TableName: "echo-table-2",
				},
			},
		})
		return nil
	}

	return c.fallbackClient.VStream(ctx, tabletType, vgtid, filter, flags, callback)
}
