// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goclienttest

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/tabletserver/querytypes"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var (
	echoPrefix = "echo://"

	query    = "test query"
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
	keyRangesEcho = "[start:\"\\001\\002\\003\\004\" end:\"\\005\\006\\007\\010\" ]"

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

	bindVars = map[string]interface{}{
		"int":   123,
		"float": 2.0,
		"bytes": []byte{1, 2, 3},
	}
	bindVarsEcho = "map[bytes:[1 2 3] float:2 int:123]"
	bindVarsP3   = map[string]*querypb.BindVariable{
		"int": {
			Type:  querypb.Type_INT64,
			Value: []byte{'1', '2', '3'},
		},
		"float": {
			Type:  querypb.Type_FLOAT64,
			Value: []byte{'2', '.', '1'},
		},
		"bytes": {
			Type:  querypb.Type_VARBINARY,
			Value: []byte{1, 2, 3},
		},
	}
	bindVarsP3Echo = "map[bytes:type:VARBINARY value:\"\\001\\002\\003\"  float:type:FLOAT64 value:\"2.1\"  int:type:INT64 value:\"123\" ]"

	sessionEcho = "in_transaction:true "

	callerID     = callerid.NewEffectiveCallerID("test_principal", "test_component", "test_subcomponent")
	callerIDEcho = "principal:\"test_principal\" component:\"test_component\" subcomponent:\"test_subcomponent\" "
)

// testEcho exercises the test cases provided by the "echo" service.
func testEcho(t *testing.T, conn *vtgateconn.VTGateConn) {
	testEchoExecute(t, conn)
	testEchoStreamExecute(t, conn)
	testEchoTransactionExecute(t, conn)
	testEchoSplitQuery(t, conn)
}

func testEchoExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	var qr *sqltypes.Result
	var err error

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	qr, err = conn.Execute(ctx, echoPrefix+query, bindVars, tabletType)
	checkEcho(t, "Execute", qr, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
	})

	qr, err = conn.ExecuteShards(ctx, echoPrefix+query, keyspace, shards, bindVars, tabletType)
	checkEcho(t, "ExecuteShards", qr, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"keyspace":   keyspace,
		"shards":     shardsEcho,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
	})

	qr, err = conn.ExecuteKeyspaceIds(ctx, echoPrefix+query, keyspace, keyspaceIDs, bindVars, tabletType)
	checkEcho(t, "ExecuteKeyspaceIds", qr, err, map[string]string{
		"callerId":    callerIDEcho,
		"query":       echoPrefix + query,
		"keyspace":    keyspace,
		"keyspaceIds": keyspaceIDsEcho,
		"bindVars":    bindVarsEcho,
		"tabletType":  tabletTypeEcho,
	})

	qr, err = conn.ExecuteKeyRanges(ctx, echoPrefix+query, keyspace, keyRanges, bindVars, tabletType)
	checkEcho(t, "ExecuteKeyRanges", qr, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"keyspace":   keyspace,
		"keyRanges":  keyRangesEcho,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
	})

	qr, err = conn.ExecuteEntityIds(ctx, echoPrefix+query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType)
	checkEcho(t, "ExecuteEntityIds", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"entityColumnName": "column1",
		"entityIds":        entityKeyspaceIDsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
	})

	var qrs []sqltypes.Result

	qrs, err = conn.ExecuteBatchShards(ctx, []*vtgatepb.BoundShardQuery{
		{
			Query: &querypb.BoundQuery{
				Sql:           echoPrefix + query,
				BindVariables: bindVarsP3,
			},
			Keyspace: keyspace,
			Shards:   shards,
		},
	}, tabletType, true)
	checkEcho(t, "ExecuteBatchShards", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"shards":        shardsEcho,
		"bindVars":      bindVarsP3Echo,
		"tabletType":    tabletTypeEcho,
		"asTransaction": "true",
	})

	qrs, err = conn.ExecuteBatchKeyspaceIds(ctx, []*vtgatepb.BoundKeyspaceIdQuery{
		{
			Query: &querypb.BoundQuery{
				Sql:           echoPrefix + query,
				BindVariables: bindVarsP3,
			},
			Keyspace:    keyspace,
			KeyspaceIds: keyspaceIDs,
		},
	}, tabletType, true)
	checkEcho(t, "ExecuteBatchKeyspaceIds", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"keyspaceIds":   keyspaceIDsEcho,
		"bindVars":      bindVarsP3Echo,
		"tabletType":    tabletTypeEcho,
		"asTransaction": "true",
	})
}

func testEchoStreamExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	var qrc <-chan *sqltypes.Result
	var err error

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	qrc, _, err = conn.StreamExecute(ctx, echoPrefix+query, bindVars, tabletType)
	checkEcho(t, "StreamExecute", <-qrc, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
	})

	qrc, _, err = conn.StreamExecuteShards(ctx, echoPrefix+query, keyspace, shards, bindVars, tabletType)
	checkEcho(t, "StreamExecuteShards", <-qrc, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"keyspace":   keyspace,
		"shards":     shardsEcho,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
	})

	qrc, _, err = conn.StreamExecuteKeyspaceIds(ctx, echoPrefix+query, keyspace, keyspaceIDs, bindVars, tabletType)
	checkEcho(t, "StreamExecuteKeyspaceIds", <-qrc, err, map[string]string{
		"callerId":    callerIDEcho,
		"query":       echoPrefix + query,
		"keyspace":    keyspace,
		"keyspaceIds": keyspaceIDsEcho,
		"bindVars":    bindVarsEcho,
		"tabletType":  tabletTypeEcho,
	})

	qrc, _, err = conn.StreamExecuteKeyRanges(ctx, echoPrefix+query, keyspace, keyRanges, bindVars, tabletType)
	checkEcho(t, "StreamExecuteKeyRanges", <-qrc, err, map[string]string{
		"callerId":   callerIDEcho,
		"query":      echoPrefix + query,
		"keyspace":   keyspace,
		"keyRanges":  keyRangesEcho,
		"bindVars":   bindVarsEcho,
		"tabletType": tabletTypeEcho,
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

	qr, err = tx.Execute(ctx, echoPrefix+query, bindVars, tabletType)
	checkEcho(t, "Execute", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "false",
	})

	qr, err = tx.ExecuteShards(ctx, echoPrefix+query, keyspace, shards, bindVars, tabletType)
	checkEcho(t, "ExecuteShards", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"shards":           shardsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "false",
	})

	qr, err = tx.ExecuteKeyspaceIds(ctx, echoPrefix+query, keyspace, keyspaceIDs, bindVars, tabletType)
	checkEcho(t, "ExecuteKeyspaceIds", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"keyspaceIds":      keyspaceIDsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "false",
	})

	qr, err = tx.ExecuteKeyRanges(ctx, echoPrefix+query, keyspace, keyRanges, bindVars, tabletType)
	checkEcho(t, "ExecuteKeyRanges", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"keyRanges":        keyRangesEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "false",
	})

	qr, err = tx.ExecuteEntityIds(ctx, echoPrefix+query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType)
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
				BindVariables: bindVarsP3,
			},
			Keyspace: keyspace,
			Shards:   shards,
		},
	}, tabletType)
	checkEcho(t, "ExecuteBatchShards", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"shards":        shardsEcho,
		"bindVars":      bindVarsP3Echo,
		"tabletType":    tabletTypeEcho,
		"session":       sessionEcho,
		"asTransaction": "false",
	})

	qrs, err = tx.ExecuteBatchKeyspaceIds(ctx, []*vtgatepb.BoundKeyspaceIdQuery{
		{
			Query: &querypb.BoundQuery{
				Sql:           echoPrefix + query,
				BindVariables: bindVarsP3,
			},
			Keyspace:    keyspace,
			KeyspaceIds: keyspaceIDs,
		},
	}, tabletType)
	checkEcho(t, "ExecuteBatchKeyspaceIds", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"keyspaceIds":   keyspaceIDsEcho,
		"bindVars":      bindVarsP3Echo,
		"tabletType":    tabletTypeEcho,
		"session":       sessionEcho,
		"asTransaction": "false",
	})
}

func testEchoSplitQuery(t *testing.T, conn *vtgateconn.VTGateConn) {
	q, err := querytypes.BoundQueryToProto3(echoPrefix+query+":split_column:123", bindVars)
	if err != nil {
		t.Fatalf("BoundQueryToProto3 error: %v", err)
	}
	want := &vtgatepb.SplitQueryResponse_Part{
		Query:        q,
		KeyRangePart: &vtgatepb.SplitQueryResponse_KeyRangePart{Keyspace: keyspace},
	}
	got, err := conn.SplitQuery(context.Background(), keyspace, echoPrefix+query, bindVars, "split_column", 123)
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

// getEcho extracts the echoed field values from a query result.
func getEcho(qr *sqltypes.Result) map[string]sqltypes.Value {
	values := map[string]sqltypes.Value{}
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
		if got[k].String() != v {
			t.Errorf("%v: %v = \n%q, want \n%q", name, k, got[k], v)
		}
	}

	// Check NULL and empty string.
	if !got["null"].IsNull() {
		t.Errorf("MySQL NULL value wasn't preserved")
	}
	if !got["emptyString"].IsQuoted() || got["emptyString"].String() != "" {
		t.Errorf("Empty string value wasn't preserved: %#v", got)
	}
}
