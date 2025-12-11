/*
Copyright 2025 The Vitess Authors.

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

package multi_query

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestMultiQuery tests the new way of handling queries in vtgate
// that runs multiple queries together.
func TestMultiQuery(t *testing.T) {
	testcases := []struct {
		name        string
		sql         string
		errExpected bool
	}{
		{
			name:        "single route query",
			sql:         "select * from t1",
			errExpected: false,
		},
		{
			name:        "join query",
			sql:         "select * from t1 join t2",
			errExpected: false,
		},
		{
			name:        "join query that can be pushed down",
			sql:         "select * from t1 join t2 on t1.id1 = t2.id5 where t1.id1 = 4",
			errExpected: false,
		},
		{
			name:        "multiple select queries",
			sql:         "select * from t1; select * from t2; select * from t1 join t2;",
			errExpected: false,
		},
		{
			name:        "multiple queries with dml in between",
			sql:         "select * from t1; insert into t2(id5, id6, id7) values (40, 43, 46); select * from t2; delete from t2; select * from t1 join t2;",
			errExpected: false,
		},
		{
			name:        "parsing error in single query",
			sql:         "unexpected query;",
			errExpected: true,
		},
		{
			name:        "parsing error in multiple queries",
			sql:         "select * from t1; select * from t2; unexpected query; select * from t1 join t2;",
			errExpected: true,
		},
	}

	ctx := context.Background()
	vtgateGrpcAddress := fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateGrpcPort)
	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "test_user", "")
	require.NoError(t, err)
	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			for _, tt := range testcases {
				t.Run(tt.name, func(t *testing.T) {
					t.Run("MySQL Protocol", func(t *testing.T) {
						mcmp, closer := start(t)
						defer closer()
						utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
						defer utils.Exec(t, mcmp.VtConn, `set workload = oltp`)

						if !tt.errExpected {
							mcmp.ExecMulti(tt.sql)
							return
						}
						mcmp.ExecMultiAllowError(tt.sql)
					})
					t.Run("GRPC Protocol", func(t *testing.T) {
						mcmp, closer := start(t)
						defer closer()

						// We get the results from MySQL to compare with the results we get from Vitess.
						mysqlRes, mysqlErr := getMySqlResults(mcmp.MySQLConn, tt.sql)

						// Create the session
						session := vtgateConn.Session("", &querypb.ExecuteOptions{})
						var results []*sqltypes.Result
						var vtErr error

						if workload == "olap" {
							// Run the query using the gRPC connection in a streaming mode
							// and aggregate the results.
							stream, streamErr := session.StreamExecuteMulti(ctx, tt.sql)
							require.NoError(t, streamErr)
							var curRes *sqltypes.Result
							for {
								res, newRes, rcvErr := stream.Recv()
								if rcvErr == io.EOF {
									break
								}
								if newRes && curRes != nil {
									results = append(results, curRes)
									curRes = nil
								}
								if rcvErr != nil {
									vtErr = rcvErr
									break
								}
								if curRes == nil {
									curRes = &sqltypes.Result{}
								}
								if res.Fields != nil {
									curRes.Fields = res.Fields
								}
								if res.RowsAffected != 0 {
									curRes.RowsAffected = res.RowsAffected
								}
								curRes.Rows = append(curRes.Rows, res.Rows...)
							}
							if curRes != nil {
								results = append(results, curRes)
							}
						} else {
							// Run the query using the gRPC connection in a non-streaming mode
							results, vtErr = session.ExecuteMulti(ctx, tt.sql)
						}

						// Verify the expectations.
						if !tt.errExpected {
							require.NoError(t, mysqlErr)
							require.NoError(t, vtErr)
						} else {
							require.Error(t, mysqlErr)
							require.Error(t, vtErr)
						}
						require.EqualValues(t, len(results), len(mysqlRes))
						for idx, result := range results {
							err = utils.CompareVitessAndMySQLResults(t, "select 1", mcmp.VtConn, result, mysqlRes[idx], utils.CompareOptions{})
							require.NoError(t, err)
						}
					})
				})
			}
		})
	}
}

// getMySqlResults executes the given SQL query using the MySQL connection
// and returns the results. It is used to compare the results with the
// results obtained from the gRPC connection.
func getMySqlResults(conn *mysql.Conn, sql string) ([]*sqltypes.Result, error) {
	var results []*sqltypes.Result
	mysqlQr, mysqlMore, mysqlErr := conn.ExecuteFetchMulti(sql, 1000, true)
	if mysqlQr != nil {
		results = append(results, mysqlQr)
	}
	for mysqlMore {
		mysqlQr, mysqlMore, _, mysqlErr = conn.ReadQueryResult(1000, true)
		if mysqlQr != nil {
			results = append(results, mysqlQr)
		}
	}
	return results, mysqlErr
}
