/*
Copyright 2023 The Vitess Authors.

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

package grpc_api

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// TestTransactionsWithGRPCAPI test the transaction queries through vtgate grpc apis.
// It is done through both streaming api and non-streaming api.
func TestTransactionsWithGRPCAPI(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "user_with_access", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	vtSession := vtgateConn.Session(keyspaceName, nil)
	workload := []string{"OLTP", "OLAP"}
	for i := 0; i < 4; i++ { // running all switch combinations.
		index := i % len(workload)
		_, session, err := exec(ctx, vtSession, fmt.Sprintf("set workload = %s", workload[index]), nil)
		require.NoError(t, err)

		require.Equal(t, workload[index], session.Options.Workload.String())
		execTest(ctx, t, workload[index], vtSession)
	}

}

func execTest(ctx context.Context, t *testing.T, workload string, vtSession *vtgateconn.VTGateSession) {
	tcases := []struct {
		query string

		expRowCount      int
		expRowAffected   int
		expInTransaction bool
	}{{
		query: "select id, val from test_table",
	}, {
		query:            "begin",
		expInTransaction: true,
	}, {
		query:            "insert into test_table(id, val) values (1, 'A')",
		expRowAffected:   1,
		expInTransaction: true,
	}, {
		query:            "select id, val from test_table",
		expRowCount:      1,
		expInTransaction: true,
	}, {
		query: "commit",
	}, {
		query:       "select id, val from test_table",
		expRowCount: 1,
	}, {
		query:          "delete from test_table",
		expRowAffected: 1,
	}}

	for _, tc := range tcases {
		t.Run(workload+":"+tc.query, func(t *testing.T) {
			qr, session, err := exec(ctx, vtSession, tc.query, nil)
			require.NoError(t, err)

			assert.Len(t, qr.Rows, tc.expRowCount)
			assert.EqualValues(t, tc.expRowAffected, qr.RowsAffected)
			assert.EqualValues(t, tc.expInTransaction, session.InTransaction)
		})
	}
}

func exec(ctx context.Context, conn *vtgateconn.VTGateSession, sql string, bv map[string]*querypb.BindVariable) (*sqltypes.Result, *vtgatepb.Session, error) {
	options := conn.SessionPb().GetOptions()
	if options != nil && options.Workload == querypb.ExecuteOptions_OLAP {
		return streamExec(ctx, conn, sql, bv)
	}
	res, err := conn.Execute(ctx, sql, bv)
	return res, conn.SessionPb(), err
}

func streamExec(ctx context.Context, conn *vtgateconn.VTGateSession, sql string, bv map[string]*querypb.BindVariable) (*sqltypes.Result, *vtgatepb.Session, error) {
	stream, err := conn.StreamExecute(ctx, sql, bv)
	if err != nil {
		return nil, conn.SessionPb(), err
	}
	result := &sqltypes.Result{}
	for {
		res, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return result, conn.SessionPb(), nil
			}
			return nil, conn.SessionPb(), err
		}
		result.Rows = append(result.Rows, res.Rows...)
		result.RowsAffected += res.RowsAffected
		if res.InsertID != 0 {
			result.InsertID = res.InsertID
		}
		if res.Fields != nil {
			result.Fields = res.Fields
		}
	}
}
