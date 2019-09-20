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

package goclienttest

import (
	"io"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	errorPrefix        = "error://"
	partialErrorPrefix = "partialerror://"

	executeErrors = map[string]vtrpcpb.Code{
		"bad input":         vtrpcpb.Code_INVALID_ARGUMENT,
		"deadline exceeded": vtrpcpb.Code_DEADLINE_EXCEEDED,
		"integrity error":   vtrpcpb.Code_ALREADY_EXISTS,
		"transient error":   vtrpcpb.Code_UNAVAILABLE,
		"unauthenticated":   vtrpcpb.Code_UNAUTHENTICATED,
		"aborted":           vtrpcpb.Code_ABORTED,
		"unknown error":     vtrpcpb.Code_UNKNOWN,
	}
)

// testErrors exercises the test cases provided by the "errors" service.
func testErrors(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	testExecuteErrors(t, conn, session)
	testStreamExecuteErrors(t, conn, session)
	testTransactionExecuteErrors(t, conn)
	testUpdateStreamErrors(t, conn)
}

func testExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	ctx := context.Background()

	checkExecuteErrors(t, func(query string) error {
		_, err := session.Execute(ctx, query, bindVars)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, nil)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIDs, bindVars, tabletType, nil)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, nil)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteEntityIds(ctx, query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType, nil)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteBatchShards(ctx, []*vtgatepb.BoundShardQuery{
			{
				Query: &querypb.BoundQuery{
					Sql:           query,
					BindVariables: bindVars,
				},
				Keyspace: keyspace,
				Shards:   shards,
			},
		}, tabletType, true, nil)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteBatchKeyspaceIds(ctx, []*vtgatepb.BoundKeyspaceIdQuery{
			{
				Query: &querypb.BoundQuery{
					Sql:           query,
					BindVariables: bindVars,
				},
				Keyspace:    keyspace,
				KeyspaceIds: keyspaceIDs,
			},
		}, tabletType, true, nil)
		return err
	})
}

func testStreamExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	ctx := context.Background()

	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(session.StreamExecute(ctx, query, bindVars))
	})
	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(conn.StreamExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, nil))
	})
	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(conn.StreamExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIDs, bindVars, tabletType, nil))
	})
	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(conn.StreamExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, nil))
	})
}

func testUpdateStreamErrors(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()

	checkStreamExecuteErrors(t, func(query string) error {
		return getUpdateStreamError(conn.UpdateStream(ctx, "", query, nil, tabletType, 0, nil))
	})
}

func testTransactionExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()

	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, nil)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIDs, bindVars, tabletType, nil)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, nil)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteEntityIds(ctx, query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType, nil)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteBatchShards(ctx, []*vtgatepb.BoundShardQuery{
			{
				Query: &querypb.BoundQuery{
					Sql:           query,
					BindVariables: bindVars,
				},
				Keyspace: keyspace,
				Shards:   shards,
			},
		}, tabletType, nil)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteBatchKeyspaceIds(ctx, []*vtgatepb.BoundKeyspaceIdQuery{
			{
				Query: &querypb.BoundQuery{
					Sql:           query,
					BindVariables: bindVars,
				},
				Keyspace:    keyspace,
				KeyspaceIds: keyspaceIDs,
			},
		}, tabletType, nil)
		return err
	})
}

func getStreamError(stream sqltypes.ResultStream, err error) error {
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv()
		switch err {
		case nil:
			// keep going
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

func getUpdateStreamError(stream vtgateconn.UpdateStreamReader, err error) error {
	if err != nil {
		return err
	}
	for {
		_, _, err := stream.Recv()
		switch err {
		case nil:
			// keep going
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

func checkExecuteErrors(t *testing.T, execute func(string) error) {
	for errStr, errCode := range executeErrors {
		query := errorPrefix + errStr
		checkError(t, execute(query), query, errStr, errCode)

		query = partialErrorPrefix + errStr
		checkError(t, execute(query), query, errStr, errCode)
	}
}

func checkStreamExecuteErrors(t *testing.T, execute func(string) error) {
	for errStr, errCode := range executeErrors {
		query := errorPrefix + errStr
		checkError(t, execute(query), query, errStr, errCode)
	}
}

func checkTransactionExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn, execute func(tx *vtgateconn.VTGateTx, query string) error) {
	ctx := context.Background()

	for errStr, errCode := range executeErrors {
		query := errorPrefix + errStr
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Errorf("[%v] Begin error: %v", query, err)
		}
		checkError(t, execute(tx, query), query, errStr, errCode)

		// Partial error where server doesn't close the session.
		query = partialErrorPrefix + errStr
		tx, err = conn.Begin(ctx)
		if err != nil {
			t.Errorf("[%v] Begin error: %v", query, err)
		}
		checkError(t, execute(tx, query), query, errStr, errCode)
		// The transaction should still be usable now.
		if err := tx.Rollback(ctx); err != nil {
			t.Errorf("[%v] Rollback error: %v", query, err)
		}

		// Partial error where server closes the session.
		tx, err = conn.Begin(ctx)
		if err != nil {
			t.Errorf("[%v] Begin error: %v", query, err)
		}
		query = partialErrorPrefix + errStr + "/close transaction"
		checkError(t, execute(tx, query), query, errStr, errCode)
		// The transaction should be unusable now.
		if tx.Rollback(ctx) == nil {
			t.Errorf("[%v] expected Rollback error, got nil", query)
		}
	}
}

func checkError(t *testing.T, err error, query, errStr string, errCode vtrpcpb.Code) {
	if err == nil {
		t.Errorf("[%v] expected error, got nil", query)
		return
	}
	if got, want := vterrors.Code(err), errCode; got != want {
		t.Errorf("[%v] error code = %v, want %v", query, got, want)
	}
}
