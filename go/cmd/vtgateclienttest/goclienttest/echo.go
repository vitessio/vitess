// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goclienttest

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	gproto "github.com/youtube/vitess/go/vt/vtgate/proto"

	pbt "github.com/youtube/vitess/go/vt/proto/topodata"
	pbg "github.com/youtube/vitess/go/vt/proto/vtgate"
)

var (
	echoPrefix = "echo://"

	query    = "test query"
	keyspace = "test_keyspace"

	shards     = []string{"-80", "80-"}
	shardsEcho = "[-80 80-]"

	keyspaceIDs = [][]byte{
		[]byte{1, 2, 3, 4},
		[]byte{5, 6, 7, 8},
	}
	keyspaceIDsEcho    = "[[1 2 3 4] [5 6 7 8]]"
	keyspaceIDsEchoOld = "[01020304 05060708]"

	keyRanges = []*pbt.KeyRange{
		&pbt.KeyRange{Start: []byte{1, 2, 3, 4}, End: []byte{5, 6, 7, 8}},
	}
	keyRangesEcho = "[start:\"\\001\\002\\003\\004\" end:\"\\005\\006\\007\\010\" ]"

	entityKeyspaceIDs = []*pbg.ExecuteEntityIdsRequest_EntityId{
		&pbg.ExecuteEntityIdsRequest_EntityId{
			KeyspaceId: []byte{1, 2, 3},
			XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_INT,
			XidInt:     123,
		},
		&pbg.ExecuteEntityIdsRequest_EntityId{
			KeyspaceId: []byte{4, 5, 6},
			XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_FLOAT,
			XidFloat:   2.0,
		},
		&pbg.ExecuteEntityIdsRequest_EntityId{
			KeyspaceId: []byte{7, 8, 9},
			XidType:    pbg.ExecuteEntityIdsRequest_EntityId_TYPE_BYTES,
			XidBytes:   []byte{1, 2, 3},
		},
	}
	entityKeyspaceIDsEcho = "[xid_type:TYPE_INT xid_int:123 keyspace_id:\"\\001\\002\\003\"  xid_type:TYPE_FLOAT xid_float:2 keyspace_id:\"\\004\\005\\006\"  xid_type:TYPE_BYTES xid_bytes:\"\\001\\002\\003\" keyspace_id:\"\\007\\010\\t\" ]"

	tabletType     = pbt.TabletType_REPLICA
	tabletTypeEcho = pbt.TabletType_name[int32(tabletType)]

	bindVars = map[string]interface{}{
		"int":   123,
		"float": 2.0,
		"bytes": []byte{1, 2, 3},
	}
	bindVarsEcho = "map[bytes:[1 2 3] float:2 int:123]"

	sessionEcho = "InTransaction: true, ShardSession: []"

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
	var qr *mproto.QueryResult
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

	var qrs []mproto.QueryResult

	qrs, err = conn.ExecuteBatchShards(ctx, []gproto.BoundShardQuery{
		gproto.BoundShardQuery{
			Sql:           echoPrefix + query,
			Keyspace:      keyspace,
			Shards:        shards,
			BindVariables: bindVars,
		},
	}, tabletType, true)
	checkEcho(t, "ExecuteBatchShards", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"shards":        shardsEcho,
		"bindVars":      bindVarsEcho,
		"tabletType":    tabletTypeEcho,
		"asTransaction": "true",
	})

	qrs, err = conn.ExecuteBatchKeyspaceIds(ctx, []gproto.BoundKeyspaceIdQuery{
		gproto.BoundKeyspaceIdQuery{
			Sql:           echoPrefix + query,
			Keyspace:      keyspace,
			KeyspaceIds:   key.ProtoToKeyspaceIds(keyspaceIDs),
			BindVariables: bindVars,
		},
	}, tabletType, true)
	checkEcho(t, "ExecuteBatchKeyspaceIds", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"keyspaceIds":   keyspaceIDsEchoOld,
		"bindVars":      bindVarsEcho,
		"tabletType":    tabletTypeEcho,
		"asTransaction": "true",
	})
}

func testEchoStreamExecute(t *testing.T, conn *vtgateconn.VTGateConn) {
	var qrc <-chan *mproto.QueryResult
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
	var qr *mproto.QueryResult
	var err error

	ctx := callerid.NewContext(context.Background(), callerID, nil)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin error: %v", err)
	}

	qr, err = tx.Execute(ctx, echoPrefix+query, bindVars, tabletType, true)
	checkEcho(t, "Execute", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "true",
	})

	qr, err = tx.ExecuteShards(ctx, echoPrefix+query, keyspace, shards, bindVars, tabletType, true)
	checkEcho(t, "ExecuteShards", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"shards":           shardsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "true",
	})

	qr, err = tx.ExecuteKeyspaceIds(ctx, echoPrefix+query, keyspace, keyspaceIDs, bindVars, tabletType, true)
	checkEcho(t, "ExecuteKeyspaceIds", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"keyspaceIds":      keyspaceIDsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "true",
	})

	qr, err = tx.ExecuteKeyRanges(ctx, echoPrefix+query, keyspace, keyRanges, bindVars, tabletType, true)
	checkEcho(t, "ExecuteKeyRanges", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"keyRanges":        keyRangesEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "true",
	})

	qr, err = tx.ExecuteEntityIds(ctx, echoPrefix+query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType, true)
	checkEcho(t, "ExecuteEntityIds", qr, err, map[string]string{
		"callerId":         callerIDEcho,
		"query":            echoPrefix + query,
		"keyspace":         keyspace,
		"entityColumnName": "column1",
		"entityIds":        entityKeyspaceIDsEcho,
		"bindVars":         bindVarsEcho,
		"tabletType":       tabletTypeEcho,
		"session":          sessionEcho,
		"notInTransaction": "true",
	})

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback error: %v", err)
	}
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin (again) error: %v", err)
	}

	var qrs []mproto.QueryResult

	qrs, err = tx.ExecuteBatchShards(ctx, []gproto.BoundShardQuery{
		gproto.BoundShardQuery{
			Sql:           echoPrefix + query,
			Keyspace:      keyspace,
			Shards:        shards,
			BindVariables: bindVars,
		},
	}, tabletType, true)
	checkEcho(t, "ExecuteBatchShards", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"shards":        shardsEcho,
		"bindVars":      bindVarsEcho,
		"tabletType":    tabletTypeEcho,
		"session":       sessionEcho,
		"asTransaction": "true",
	})

	qrs, err = tx.ExecuteBatchKeyspaceIds(ctx, []gproto.BoundKeyspaceIdQuery{
		gproto.BoundKeyspaceIdQuery{
			Sql:           echoPrefix + query,
			Keyspace:      keyspace,
			KeyspaceIds:   key.ProtoToKeyspaceIds(keyspaceIDs),
			BindVariables: bindVars,
		},
	}, tabletType, true)
	checkEcho(t, "ExecuteBatchKeyspaceIds", &qrs[0], err, map[string]string{
		"callerId":      callerIDEcho,
		"query":         echoPrefix + query,
		"keyspace":      keyspace,
		"keyspaceIds":   keyspaceIDsEchoOld,
		"bindVars":      bindVarsEcho,
		"tabletType":    tabletTypeEcho,
		"session":       sessionEcho,
		"asTransaction": "true",
	})
}

func testEchoSplitQuery(t *testing.T, conn *vtgateconn.VTGateConn) {
	q, err := tproto.BoundQueryToProto3(echoPrefix+query+":split_column:123", bindVars)
	if err != nil {
		t.Fatalf("BoundQueryToProto3 error: %v", err)
	}
	want := &pbg.SplitQueryResponse_Part{
		Query:        q,
		KeyRangePart: &pbg.SplitQueryResponse_KeyRangePart{Keyspace: keyspace},
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
func getEcho(qr *mproto.QueryResult) map[string]string {
	values := map[string]string{}
	for i, field := range qr.Fields {
		values[field.Name] = qr.Rows[0][i].String()
	}
	return values
}

// checkEcho verifies that the values present in 'want' are equal to those in
// 'got'. Note that extra values in 'got' are fine.
func checkEcho(t *testing.T, name string, qr *mproto.QueryResult, err error, want map[string]string) {
	if err != nil {
		t.Fatalf("%v error: %v", name, err)
	}
	got := getEcho(qr)
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%v: %v = %q, want %q", name, k, got[k], v)
		}
	}
}
