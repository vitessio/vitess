/*
Copyright 2017 Google Inc.

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

package goclienttest

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var (
	echoPrefix = "echo://"

	query    = "test query with unicode: \u6211\u80fd\u541e\u4e0b\u73bb\u7483\u800c\u4e0d\u50b7\u8eab\u9ad4"
	keyspace = "test_keyspace"

	shards     = []string{"-80", "80-"}
	shardsEcho = "[-80 80-]"

	keyspaceIDs = [][]byte{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
	}
	keyspaceIDsEcho = "[[1 2 3 4] [5 6 7 8]]"

	keyRanges = []*topodatapb.KeyRange{
		{Start: []byte{1, 2, 3, 4}, End: []byte{5, 6, 7, 8}},
	}
	keyRangeZeroEcho = "start:\"\\001\\002\\003\\004\" end:\"\\005\\006\\007\\010\" "
	keyRangesEcho    = "[" + keyRangeZeroEcho + "]"

	entityKeyspaceIDs = []*vtgatepb.ExecuteEntityIdsRequest_EntityId{
		{
			KeyspaceId: []byte{1, 2, 3},
			Type:       sqltypes.Int64,
			Value:      []byte("123"),
		},
		{
			KeyspaceId: []byte{4, 5, 6},
			Type:       sqltypes.Float64,
			Value:      []byte("2"),
		},
		{
			KeyspaceId: []byte{7, 8, 9},
			Type:       sqltypes.VarBinary,
			Value:      []byte{1, 2, 3},
		},
	}
	entityKeyspaceIDsEcho = "[type:INT64 value:\"123\" keyspace_id:\"\\001\\002\\003\"  type:FLOAT64 value:\"2\" keyspace_id:\"\\004\\005\\006\"  type:VARBINARY value:\"\\001\\002\\003\" keyspace_id:\"\\007\\010\\t\" ]"

	tabletType     = topodatapb.TabletType_REPLICA
	tabletTypeEcho = topodatapb.TabletType_name[int32(tabletType)]

	bindVars = map[string]*querypb.BindVariable{
		"int":   sqltypes.Int64BindVariable(123),
		"float": sqltypes.Float64BindVariable(2.1),
		"bytes": sqltypes.BytesBindVariable([]byte{1, 2, 3}),
	}
	bindVarsEcho = "map[bytes:type:VARBINARY value:\"\\001\\002\\003\"  float:type:FLOAT64 value:\"2.1\"  int:type:INT64 value:\"123\" ]"

	sessionEcho = "in_transaction:true "

	callerID     = callerid.NewEffectiveCallerID("test_principal", "test_component", "test_subcomponent")
	callerIDEcho = "principal:\"test_principal\" component:\"test_component\" subcomponent:\"test_subcomponent\" "

	eventToken = &querypb.EventToken{
		Timestamp: 876543,
		Shard:     shards[0],
		Position:  "test_position",
	}
	eventTokenEcho = "timestamp:876543 shard:\"-80\" position:\"test_position\" "

	options = &querypb.ExecuteOptions{
		IncludedFields:    querypb.ExecuteOptions_TYPE_ONLY,
		IncludeEventToken: true,
		CompareEventToken: eventToken,
	}
	optionsEcho = "include_event_token:true compare_event_token:<" + eventTokenEcho + "> included_fields:TYPE_ONLY "
	extrasEcho  = "event_token:<" + eventTokenEcho + "> fresher:true "

	updateStreamEcho = "map[callerId:" + callerIDEcho + " event:" + eventTokenEcho + " keyRange:" + keyRangeZeroEcho + " keyspace:conn_ks shard:echo://" + query + " tabletType:REPLICA timestamp:0]"
)

// testEcho exercises the test cases provided by the "echo" service.
func testEcho(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	testEchoExecute(t, conn, session)
	testEchoStreamExecute(t, conn, session)
	testEchoTransactionExecute(t, conn)
	testEchoSplitQuery(t, conn)
	testEchoUpdateStream(t, conn)
}

func testEchoExecute(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	var qr *sqltypes.Result
	var err error

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	qr, err = session.Execute(ctx, echoPrefix+query, bindVars)
	checkEcho(t, "Execute", qr, err, map[string]string{
		"callerId": callerIDEcho,
		"query":    echoPrefix + query,
		"bindVars": bindVarsEcho,
	})

	qr, err = conn.ExecuteShards(ctx, echoPrefix+query, keyspace, shards, bindVars, tabletType, options)
	checkEcho(t, "ExecuteShards", qr, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"keyspace":   keyspace,
		"shards":     shardsEcho,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
		"options":    optionsEcho,
		"extras":     extrasEcho,
	})

	qr, err = conn.ExecuteKeyspaceIds(ctx, echoPrefix+query, keyspace, keyspaceIDs, bindVars, tabletType, options)
	checkEcho(t, "ExecuteKeyspaceIds", qr, err, map[string]string{
		"callerId":    callerIDEcho,
		"query":       echoPrefix + query,
		"keyspace":    keyspace,
		"keyspaceIds": keyspaceIDsEcho,
		"bindVars":    bindVarsEcho,
		"tabletType":  tabletTypeEcho,
		"options":     optionsEcho,
		"extras":      extrasEcho,
	})

	qr, err = conn.ExecuteKeyRanges(ctx, echoPrefix+query, keyspace, keyRanges, bindVars, tabletType, options)
	checkEcho(t, "ExecuteKeyRanges", qr, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"keyspace":   keyspace,
		"keyRanges":  keyRangesEcho,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
		"options":    optionsEcho,
		"extras":     extrasEcho,
	})

	qr, err = conn.ExecuteEntityIds(ctx, echoPrefix+query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType, options)
	checkEcho(t, "ExecuteEntityIds", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"entityColumnName": "column1",
		"entityIds":        entityKeyspaceIDsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"options":          optionsEcho,
		"extras":           extrasEcho,
	})

	var qrs []sqltypes.Result

	qrs, err = conn.ExecuteBatchShards(ctx, []*vtgatepb.BoundShardQuery{
		{
			Query: &querypb.BoundQuery{
				Sql:           echoPrefix + query,
				BindVariables: bindVars,
			},
			Keyspace: keyspace,
			Shards:   shards,
		},
	}, tabletType, true, options)
	checkEcho(t, "ExecuteBatchShards", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"shards":        shardsEcho,
		"bindVars":      bindVarsEcho,
		"tabletType":    tabletTypeEcho,
		"asTransaction": "true",
		"options":       optionsEcho,
	})

	qrs, err = conn.ExecuteBatchKeyspaceIds(ctx, []*vtgatepb.BoundKeyspaceIdQuery{
		{
			Query: &querypb.BoundQuery{
				Sql:           echoPrefix + query,
				BindVariables: bindVars,
			},
			Keyspace:    keyspace,
			KeyspaceIds: keyspaceIDs,
		},
	}, tabletType, true, options)
	checkEcho(t, "ExecuteBatchKeyspaceIds", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"keyspaceIds":   keyspaceIDsEcho,
		"bindVars":      bindVarsEcho,
		"tabletType":    tabletTypeEcho,
		"asTransaction": "true",
		"options":       optionsEcho,
	})
}

func testEchoStreamExecute(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	var stream sqltypes.ResultStream
	var err error
	var qr *sqltypes.Result

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	stream, err = session.StreamExecute(ctx, echoPrefix+query, bindVars)
	if err != nil {
		t.Fatal(err)
	}
	qr, err = stream.Recv()
	checkEcho(t, "StreamExecute", qr, err, map[string]string{
		"callerId": callerIDEcho,
		"query":    echoPrefix + query,
		"bindVars": bindVarsEcho,
	})

	stream, err = conn.StreamExecuteShards(ctx, echoPrefix+query, keyspace, shards, bindVars, tabletType, options)
	if err != nil {
		t.Fatal(err)
	}
	qr, err = stream.Recv()
	checkEcho(t, "StreamExecuteShards", qr, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"keyspace":   keyspace,
		"shards":     shardsEcho,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
		"options":    optionsEcho,
	})

	stream, err = conn.StreamExecuteKeyspaceIds(ctx, echoPrefix+query, keyspace, keyspaceIDs, bindVars, tabletType, options)
	if err != nil {
		t.Fatal(err)
	}
	qr, err = stream.Recv()
	checkEcho(t, "StreamExecuteKeyspaceIds", qr, err, map[string]string{
		"callerId":    callerIDEcho,
		"query":       echoPrefix + query,
		"keyspace":    keyspace,
		"keyspaceIds": keyspaceIDsEcho,
		"bindVars":    bindVarsEcho,
		"tabletType":  tabletTypeEcho,
		"options":     optionsEcho,
	})

	stream, err = conn.StreamExecuteKeyRanges(ctx, echoPrefix+query, keyspace, keyRanges, bindVars, tabletType, options)
	if err != nil {
		t.Fatal(err)
	}
	qr, err = stream.Recv()
	checkEcho(t, "StreamExecuteKeyRanges", qr, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"keyspace":   keyspace,
		"keyRanges":  keyRangesEcho,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
		"options":    optionsEcho,
	})
}

func testEchoTransactionExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	var qr *sqltypes.Result
	var err error

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin error: %v", err)
	}

	qr, err = tx.ExecuteShards(ctx, echoPrefix+query, keyspace, shards, bindVars, tabletType, options)
	checkEcho(t, "ExecuteShards", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"shards":           shardsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "false",
		"options":          optionsEcho,
	})

	qr, err = tx.ExecuteKeyspaceIds(ctx, echoPrefix+query, keyspace, keyspaceIDs, bindVars, tabletType, options)
	checkEcho(t, "ExecuteKeyspaceIds", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"keyspaceIds":      keyspaceIDsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "false",
		"options":          optionsEcho,
	})

	qr, err = tx.ExecuteKeyRanges(ctx, echoPrefix+query, keyspace, keyRanges, bindVars, tabletType, options)
	checkEcho(t, "ExecuteKeyRanges", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"keyRanges":        keyRangesEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "false",
		"options":          optionsEcho,
	})

	qr, err = tx.ExecuteEntityIds(ctx, echoPrefix+query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType, options)
	checkEcho(t, "ExecuteEntityIds", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"entityColumnName": "column1",
		"entityIds":        entityKeyspaceIDsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "false",
		"options":          optionsEcho,
	})

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback error: %v", err)
	}
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin (again) error: %v", err)
	}

	var qrs []sqltypes.Result

	qrs, err = tx.ExecuteBatchShards(ctx, []*vtgatepb.BoundShardQuery{
		{
			Query: &querypb.BoundQuery{
				Sql:           echoPrefix + query,
				BindVariables: bindVars,
			},
			Keyspace: keyspace,
			Shards:   shards,
		},
	}, tabletType, options)
	checkEcho(t, "ExecuteBatchShards", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"shards":        shardsEcho,
		"bindVars":      bindVarsEcho,
		"tabletType":    tabletTypeEcho,
		"session":       sessionEcho,
		"asTransaction": "false",
		"options":       optionsEcho,
	})

	qrs, err = tx.ExecuteBatchKeyspaceIds(ctx, []*vtgatepb.BoundKeyspaceIdQuery{
		{
			Query: &querypb.BoundQuery{
				Sql:           echoPrefix + query,
				BindVariables: bindVars,
			},
			Keyspace:    keyspace,
			KeyspaceIds: keyspaceIDs,
		},
	}, tabletType, options)
	checkEcho(t, "ExecuteBatchKeyspaceIds", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"keyspaceIds":   keyspaceIDsEcho,
		"bindVars":      bindVarsEcho,
		"tabletType":    tabletTypeEcho,
		"session":       sessionEcho,
		"asTransaction": "false",
		"options":       optionsEcho,
	})
}

func testEchoSplitQuery(t *testing.T, conn *vtgateconn.VTGateConn) {
	want := &vtgatepb.SplitQueryResponse_Part{
		Query: &querypb.BoundQuery{
			Sql:           echoPrefix + query + ":[split_column1,split_column2]:123:1000:FULL_SCAN",
			BindVariables: bindVars,
		},
		KeyRangePart: &vtgatepb.SplitQueryResponse_KeyRangePart{Keyspace: keyspace},
	}
	got, err := conn.SplitQuery(context.Background(), keyspace, echoPrefix+query, bindVars, []string{"split_column1,split_column2"}, 123, 1000, querypb.SplitQueryRequest_FULL_SCAN)
	if err != nil {
		t.Fatalf("SplitQuery error: %v", err)
	}
	// For some reason, proto.Equal() is calling them unequal even though no diffs
	// are found.
	gotstr, wantstr := got[0].String(), want.String()
	if gotstr != wantstr {
		t.Errorf("SplitQuery() = %v, want %v", gotstr, wantstr)
	}
}

func testEchoUpdateStream(t *testing.T, conn *vtgateconn.VTGateConn) {
	var stream vtgateconn.UpdateStreamReader
	var err error

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	stream, err = conn.UpdateStream(ctx, "conn_ks", echoPrefix+query, keyRanges[0], tabletType, 0, eventToken)
	if err != nil {
		t.Fatal(err)
	}
	se, _, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if se.EventToken.Position != updateStreamEcho {
		t.Errorf("UpdateStream(0) =\n%v, want\n%v", se.EventToken.Position, updateStreamEcho)
	}
}

// getEcho extracts the echoed field values from a query result.
func getEcho(qr *sqltypes.Result) map[string]sqltypes.Value {
	values := map[string]sqltypes.Value{}
	if qr == nil {
		return values
	}
	for i, field := range qr.Fields {
		values[field.Name] = qr.Rows[0][i]
	}
	return values
}

// checkEcho verifies that the values present in 'want' are equal to those in
// 'got'. Note that extra values in 'got' are fine.
func checkEcho(t *testing.T, name string, qr *sqltypes.Result, err error, want map[string]string) {
	if err != nil {
		t.Fatalf("%v error: %v", name, err)
	}
	got := getEcho(qr)
	for k, v := range want {
		if k == "extras" {
			gotExtras := qr.Extras.String()
			if gotExtras != v {
				t.Errorf("%v: extras = \n%q, want \n%q", name, gotExtras, v)
			}
			continue
		}
		if got[k].ToString() != v {
			t.Errorf("%v: %v = \n%q, want \n%q", name, k, got[k], v)
		}
	}

	// Check NULL and empty string.
	if !got["null"].IsNull() {
		t.Errorf("MySQL NULL value wasn't preserved")
	}
	if !got["emptyString"].IsQuoted() || got["emptyString"].ToString() != "" {
		t.Errorf("Empty string value wasn't preserved: %#v", got)
	}
}
