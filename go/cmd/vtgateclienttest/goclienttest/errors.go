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
}

func testExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	ctx := context.Background()

	checkExecuteErrors(t, func(query string) error {
		_, err := session.Execute(ctx, query, bindVars)
		return err
	})
}

func testStreamExecuteErrors(t *testing.T, conn *vtgateconn.VTGateConn, session *vtgateconn.VTGateSession) {
	ctx := context.Background()

	checkStreamExecuteErrors(t, func(query string) error {
		return getStreamError(session.StreamExecute(ctx, query, bindVars))
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

func checkError(t *testing.T, err error, query, errStr string, errCode vtrpcpb.Code) {
	if err == nil {
		t.Errorf("[%v] expected error, got nil", query)
		return
	}
	if got, want := vterrors.Code(err), errCode; got != want {
		t.Errorf("[%v] error code = %v, want %v", query, got, want)
	}
}
