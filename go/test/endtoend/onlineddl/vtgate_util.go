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
	"math"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

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

	qr, err := conn.ExecuteFetch(query, math.MaxInt64, true)
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
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a retry",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectRetryPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckCancelMigration attempts to cancel a migration, and expects success/failure by counting affected rows
func CheckCancelMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectCancelPossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a cancel",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

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
func CheckMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectStatuses ...schema.OnlineDDLStatus) {
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := VtgateExecQuery(t, vtParams, query, "")
	fmt.Printf("# output for `%s`:\n", query)
	PrintQueryResult(os.Stdout, r)

	count := 0
	for _, row := range r.Named().Rows {
		if row["migration_uuid"].ToString() != uuid {
			continue
		}
		for _, expectStatus := range expectStatuses {
			if row["migration_status"].ToString() == string(expectStatus) {
				count++
				break
			}
		}
	}
	assert.Equal(t, len(shards), count)
}

// WaitForMigrationStatus waits for a migration to reach either provided statuses (returns immediately), or eventually time out
func WaitForMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[string(status)] = true
	}
	startTime := time.Now()
	lastKnownStatus := ""
	for time.Since(startTime) < timeout {
		countMatchedShards := 0
		r := VtgateExecQuery(t, vtParams, query, "")
		for _, row := range r.Named().Rows {
			lastKnownStatus = row["migration_status"].ToString()
			if row["migration_uuid"].ToString() == uuid && statusesMap[lastKnownStatus] {
				countMatchedShards++
			}
		}
		if countMatchedShards == len(shards) {
			return schema.OnlineDDLStatus(lastKnownStatus)
		}
		time.Sleep(1 * time.Second)
	}
	return schema.OnlineDDLStatus(lastKnownStatus)
}

// CheckMigrationArtifacts verifies given migration exists, and checks if it has artifacts
func CheckMigrationArtifacts(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectArtifacts bool) {
	r := ReadMigrations(t, vtParams, uuid)

	assert.Equal(t, len(shards), len(r.Named().Rows))
	for _, row := range r.Named().Rows {
		hasArtifacts := (row["artifacts"].ToString() != "")
		assert.Equal(t, expectArtifacts, hasArtifacts)
	}
}

// ReadMigrations reads migration entries
func ReadMigrations(t *testing.T, vtParams *mysql.ConnParams, like string) *sqltypes.Result {
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(like),
	)
	require.NoError(t, err)

	return VtgateExecQuery(t, vtParams, query, "")
}

// ReadMigrationLogs reads migration logs for a given migration, on all shards
func ReadMigrationLogs(t *testing.T, vtParams *mysql.ConnParams, uuid string) (logs []string) {
	query, err := sqlparser.ParseAndBind("show vitess_migration %a logs",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := VtgateExecQuery(t, vtParams, query, "")
	for _, row := range r.Named().Rows {
		migrationLog := row["migration_log"].ToString()
		logs = append(logs, migrationLog)
	}
	return logs
}
