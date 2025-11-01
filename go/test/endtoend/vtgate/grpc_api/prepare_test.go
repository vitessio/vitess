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

package grpc_api

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestTransactionsWithGRPCAPI test the transaction queries through vtgate grpc apis.
// It is done through both streaming api and non-streaming api.
func TestPrepareWithGRPCAPI(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtgateConn, err := cluster.DialVTGate(ctx, t.Name(), vtgateGrpcAddress, "user_with_access", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	query := `SELECT DISTINCT
                BINARY table_info.table_name AS table_name,
                table_info.create_options AS create_options,
                table_info.table_comment AS table_comment
              FROM information_schema.tables AS table_info
              JOIN information_schema.columns AS column_info
                  ON BINARY column_info.table_name = BINARY table_info.table_name
              WHERE
                  table_info.table_schema = ?
                  AND column_info.table_schema = ?
                  -- Exclude views.
                  AND table_info.table_type = 'BASE TABLE'
              ORDER BY BINARY table_info.table_name`

	vtSession := vtgateConn.Session(keyspaceName, nil)
	fields, paramsCount, err := vtSession.Prepare(t.Context(), query)
	require.NoError(t, err)
	assert.Equal(t, `[name:"table_name" type:VARBINARY name:"create_options" type:VARCHAR name:"table_comment" type:VARCHAR]`, fmt.Sprintf("%v", fields))
	assert.EqualValues(t, 2, paramsCount)
}
