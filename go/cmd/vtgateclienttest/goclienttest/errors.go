// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goclienttest

import (
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/sqltypes"
	gproto "github.com/youtube/vitess/go/vt/vtgate/proto"

	"github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
)

var (
	errorPrefix        = "error://"
	partialErrorPrefix = "partialerror://"

	executeErrors = map[string]vtrpc.ErrorCode{
		"bad input":         vtrpc.ErrorCode_BAD_INPUT,
		"deadline exceeded": vtrpc.ErrorCode_DEADLINE_EXCEEDED,
		"integrity error":   vtrpc.ErrorCode_INTEGRITY_ERROR,
		"transient error":   vtrpc.ErrorCode_TRANSIENT_ERROR,
		"unauthenticated":   vtrpc.ErrorCode_UNAUTHENTICATED,
		"unknown error":     vtrpc.ErrorCode_UNKNOWN_ERROR,
	}
)

// testErrors exercises the test cases provided by the "errors" service.
func testErrors(t *testing.T, conn *vtgateconn.VTGateConn) {
	testExecuteErrors(t, conn)
	testStreamExecuteErrors(t, conn)
	testTransactionExecuteErrors(t, conn)
}

func testExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()

	checkExecuteErrors(t, func(query string) error {
		_, err := conn.Execute(ctx, query, bindVars, tabletType)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIDs, bindVars, tabletType)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteEntityIds(ctx, query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteBatchShards(ctx, []gproto.BoundShardQuery{
			gproto.BoundShardQuery{
				Sql:           query,
				Keyspace:      keyspace,
				Shards:        shards,
				BindVariables: bindVars,
			},
		}, tabletType, true)
		return err
	})
	checkExecuteErrors(t, func(query string) error {
		_, err := conn.ExecuteBatchKeyspaceIds(ctx, []gproto.BoundKeyspaceIdQuery{
			gproto.BoundKeyspaceIdQuery{
				Sql:           query,
				Keyspace:      keyspace,
				KeyspaceIds:   keyspaceIDs,
				BindVariables: bindVars,
			},
		}, tabletType, true)
		return err
	})
}

func testStreamExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()

	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(conn.StreamExecute(ctx, query, bindVars, tabletType))
	})
	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(conn.StreamExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType))
	})
	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(conn.StreamExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIDs, bindVars, tabletType))
	})
	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(conn.StreamExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType))
	})
}

func testTransactionExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn) {
	ctx := context.Background()

	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.Execute(ctx, query, bindVars, tabletType, true)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, true)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIDs, bindVars, tabletType, true)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, true)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteEntityIds(ctx, query, keyspace, "column1", entityKeyspaceIDs, bindVars, tabletType, true)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteBatchShards(ctx, []gproto.BoundShardQuery{
			gproto.BoundShardQuery{
				Sql:           query,
				Keyspace:      keyspace,
				Shards:        shards,
				BindVariables: bindVars,
			},
		}, tabletType, true)
		return err
	})
	checkTransactionExecuteErrors(t, conn, func(tx *vtgateconn.VTGateTx, query string) error {
		_, err := tx.ExecuteBatchKeyspaceIds(ctx, []gproto.BoundKeyspaceIdQuery{
			gproto.BoundKeyspaceIdQuery{
				Sql:           query,
				Keyspace:      keyspace,
				KeyspaceIds:   keyspaceIDs,
				BindVariables: bindVars,
			},
		}, tabletType, true)
		return err
	})
}

func getStreamError(qrChan <-chan *sqltypes.Result, errFunc vtgateconn.ErrFunc, err error) error {
	if err != nil {
		return err
	}
	for range qrChan {
	}
	return errFunc()
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

func checkError(t *testing.T, err error, query, errStr string, errCode vtrpc.ErrorCode) {
	if err == nil {
		t.Errorf("[%v] expected error, got nil", query)
		return
	}
	switch vtErr := err.(type) {
	case *vterrors.VitessError:
		if got, want := vtErr.VtErrorCode(), errCode; got != want {
			t.Errorf("[%v] error code = %v, want %v", query, got, want)
		}
	case rpcplus.ServerError:
		if !strings.Contains(string(vtErr), errStr) {
			t.Errorf("[%v] error = %q, want contains %q", query, vtErr, errStr)
		}
	default:
		t.Errorf("[%v] unrecognized error type: %T, error: %#v", query, err, err)
		return
	}

}
