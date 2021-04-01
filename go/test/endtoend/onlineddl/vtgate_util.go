/*
Copyright 2021 The Vitess Authors.

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

package onlineddl

import (
	"context"
	"fmt"
	"os"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// VtgateExecQuery runs a query on VTGate using given query params
func VtgateExecQuery(t *testing.T, vtParams *mysql.ConnParams, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(query, 1000, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}

// VtgateExecDDL executes a DDL query with given strategy
func VtgateExecDDL(t *testing.T, vtParams *mysql.ConnParams, ddlStrategy string, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	setSession := fmt.Sprintf("set @@ddl_strategy='%s'", ddlStrategy)
	_, err = conn.ExecuteFetch(setSession, 1000, true)
	assert.NoError(t, err)

	qr, err := conn.ExecuteFetch(query, 1000, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}

// CheckRetryMigration attempts to retry a migration, and expects success/failure by counting affected rows
func CheckRetryMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectRetryPossible bool) {
	retryQuery := fmt.Sprintf("alter vitess_migration '%s' retry", uuid)
	r := VtgateExecQuery(t, vtParams, retryQuery, "")

	if expectRetryPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckCancelMigration attempts to cancel a migration, and expects success/failure by counting affected rows
func CheckCancelMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectCancelPossible bool) {
	cancelQuery := fmt.Sprintf("alter vitess_migration '%s' cancel", uuid)
	r := VtgateExecQuery(t, vtParams, cancelQuery, "")

	if expectCancelPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckCancelAllMigrations cancels all pending migrations and expect number of affected rows
func CheckCancelAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	cancelQuery := "alter vitess_migration cancel all"
	r := VtgateExecQuery(t, vtParams, cancelQuery, "")

	assert.Equal(t, expectCount, int(r.RowsAffected))
}

// CheckMigrationStatus verifies that the migration indicated by given UUID has the given expected status
func CheckMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectStatus schema.OnlineDDLStatus) {
	showQuery := fmt.Sprintf("show vitess_migrations like '%s'", uuid)
	r := VtgateExecQuery(t, vtParams, showQuery, "")
	fmt.Printf("# output for `%s`:\n", showQuery)
	PrintQueryResult(os.Stdout, r)

	count := 0
	for _, row := range r.Named().Rows {
		if row["migration_uuid"].ToString() == uuid && row["migration_status"].ToString() == string(expectStatus) {
			count++
		}
	}
	assert.Equal(t, len(shards), count)
}
