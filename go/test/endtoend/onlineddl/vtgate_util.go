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
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ThrottledAppsTimeout = 60 * time.Second
)

var testsStartupTime time.Time

func init() {
	testsStartupTime = time.Now()
}

// VtgateExecQuery runs a query on VTGate using given query params
func VtgateExecQuery(t *testing.T, vtParams *mysql.ConnParams, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := t.Context()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(query, -1, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}

// VtgateExecQueryInTransaction runs a query on VTGate using given query params, inside a transaction
func VtgateExecQueryInTransaction(t *testing.T, vtParams *mysql.ConnParams, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := t.Context()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("begin", -1, true)
	require.NoError(t, err)
	qr, err := conn.ExecuteFetch(query, -1, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	_, err = conn.ExecuteFetch("commit", -1, true)
	require.NoError(t, err)
	return qr
}

// VtgateExecDDL executes a DDL query with given strategy
func VtgateExecDDL(t *testing.T, vtParams *mysql.ConnParams, ddlStrategy string, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := t.Context()
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

// CheckRetryPartialMigration attempts to retry a migration where a subset of shards failed
func CheckRetryPartialMigration(t *testing.T, vtParams *mysql.ConnParams, uuid string, expectAtLeastRowsAffected uint64) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a retry",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	assert.GreaterOrEqual(t, expectAtLeastRowsAffected, r.RowsAffected)
}

// CheckCompleteContextMigrations completes all pending migrations with a given context and expects number of affected rows.
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCompleteContextMigrations(t *testing.T, vtParams *mysql.ConnParams, migrationContext string, expectCount int) {
	query := fmt.Sprintf("alter vitess_migration complete context '%s'", migrationContext)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckCompleteAllMigrations completes all pending migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCompleteAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	completeQuery := "alter vitess_migration complete all"
	r := VtgateExecQuery(t, vtParams, completeQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckPostponeCompleteContextMigrations postpones completion of all pending migrations with a given context and expects number of affected rows.
// A negative value for expectCount indicates "don't care, no need to check"
func CheckPostponeCompleteContextMigrations(t *testing.T, vtParams *mysql.ConnParams, migrationContext string, expectCount int) {
	query := fmt.Sprintf("alter vitess_migration postpone complete context '%s'", migrationContext)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckPostponeCompleteAllMigrations postpones all pending migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckPostponeCompleteAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	completeQuery := "alter vitess_migration postpone complete all"
	r := VtgateExecQuery(t, vtParams, completeQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckCancelAllMigrations cancels all pending migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCancelAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	cancelQuery := "alter vitess_migration cancel all"
	r := VtgateExecQuery(t, vtParams, cancelQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckCancelContextMigrations cancels all pending migrations with a given context and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCancelContextMigrations(t *testing.T, vtParams *mysql.ConnParams, migrationContext string, expectCount int) {
	cancelQuery := fmt.Sprintf("alter vitess_migration cancel context '%s'", migrationContext)
	r := VtgateExecQuery(t, vtParams, cancelQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckCleanupContextMigrations cleans up terminal migrations with a given context and expects number of affected rows.
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCleanupContextMigrations(t *testing.T, vtParams *mysql.ConnParams, migrationContext string, expectCount int) uint64 {
	query := fmt.Sprintf("alter vitess_migration cleanup context '%s'", migrationContext)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
	return r.RowsAffected
}

// CheckCleanupAllMigrations cleans up all applicable migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCleanupAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) uint64 {
	cleanupQuery := "alter vitess_migration cleanup all"
	r := VtgateExecQuery(t, vtParams, cleanupQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
	return r.RowsAffected
}

// CheckLaunchContextMigrations launches all queued postponed migrations with a given context and expects number of affected rows.
// A negative value for expectCount indicates "don't care, no need to check"
func CheckLaunchContextMigrations(t *testing.T, vtParams *mysql.ConnParams, migrationContext string, expectCount int) {
	query := fmt.Sprintf("alter vitess_migration launch context '%s'", migrationContext)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckLaunchAllMigrations launches all queued posponed migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckLaunchAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	completeQuery := "alter vitess_migration launch all"
	r := VtgateExecQuery(t, vtParams, completeQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckForceCutOverContextMigrations marks all pending migrations with a given context for forced cut-over and expects number of affected rows.
// A negative value for expectCount indicates "don't care, no need to check"
func CheckForceCutOverContextMigrations(t *testing.T, vtParams *mysql.ConnParams, migrationContext string, expectCount int) {
	query := fmt.Sprintf("alter vitess_migration force_cutover context '%s'", migrationContext)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
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

// ThrottleAllMigrations fully throttles online-ddl apps
func ThrottleAllMigrations(t *testing.T, vtParams *mysql.ConnParams) {
	query := "alter vitess_migration throttle all expire '24h' ratio 1"
	_ = VtgateExecQuery(t, vtParams, query, "")
}

// ThrottleContextMigrations throttles all pending migrations with a given context.
func ThrottleContextMigrations(t *testing.T, vtParams *mysql.ConnParams, migrationContext string) {
	query := fmt.Sprintf("alter vitess_migration throttle context '%s' expire '24h' ratio 1", migrationContext)
	_ = VtgateExecQuery(t, vtParams, query, "")
}

// UnthrottleAllMigrations cancels migration throttling
func UnthrottleAllMigrations(t *testing.T, vtParams *mysql.ConnParams) {
	query := "alter vitess_migration unthrottle all"
	_ = VtgateExecQuery(t, vtParams, query, "")
}

// UnthrottleContextMigrations unthrottles all pending migrations with a given context.
func UnthrottleContextMigrations(t *testing.T, vtParams *mysql.ConnParams, migrationContext string) {
	query := fmt.Sprintf("alter vitess_migration unthrottle context '%s'", migrationContext)
	_ = VtgateExecQuery(t, vtParams, query, "")
}

// CheckThrottledApps checks for existence or non-existence of an app in the throttled apps list
func CheckThrottledApps(t *testing.T, vtParams *mysql.ConnParams, throttlerApp throttlerapp.Name, expectFind bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), ThrottledAppsTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		query := "show vitess_throttled_apps"
		r := VtgateExecQuery(t, vtParams, query, "")

		appFound := false
		for _, row := range r.Named().Rows {
			if throttlerApp.Equals(row.AsString("app", "")) {
				appFound = true
			}
		}
		if appFound == expectFind {
			// we're all good
			return true
		}

		select {
		case <-ctx.Done():
			assert.Fail(t, "CheckThrottledApps timed out", "waiting for '%v' to be in throttled status '%v'", throttlerApp.String(), expectFind)
			return false
		case <-ticker.C:
		}
	}
}

// WaitForThrottledTimestamp waits for a migration to have a non-empty last_throttled_timestamp
func WaitForThrottledTimestamp(t *testing.T, vtParams *mysql.ConnParams, uuid string, timeout time.Duration) (
	row sqltypes.RowNamedValues,
	startedTimestamp string,
	lastThrottledTimestamp string,
) {
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		rs := ReadMigrations(t, vtParams, uuid)
		require.NotNil(t, rs)
		for _, row = range rs.Named().Rows {
			startedTimestamp = row.AsString("started_timestamp", "")
			require.NotEmpty(t, startedTimestamp)
			lastThrottledTimestamp = row.AsString("last_throttled_timestamp", "")
			if lastThrottledTimestamp != "" {
				// good. This is what we've been waiting for.
				return row, startedTimestamp, lastThrottledTimestamp
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Error("timeout waiting for last_throttled_timestamp to have nonempty value")
	return
}

// ValidateCompletedTimestamp ensures that any migration in `cancelled`, `completed`, `failed` statuses
// has a non-nil and valid `completed_timestamp` value.
func ValidateCompletedTimestamp(t *testing.T, vtParams *mysql.ConnParams) {
	require.False(t, testsStartupTime.IsZero())
	r := VtgateExecQuery(t, vtParams, "show vitess_migrations", "")

	completedTimestampNumValidations := 0
	for _, row := range r.Named().Rows {
		migrationStatus := row.AsString("migration_status", "")
		require.NotEmpty(t, migrationStatus)
		switch migrationStatus {
		case string(schema.OnlineDDLStatusComplete),
			string(schema.OnlineDDLStatusFailed),
			string(schema.OnlineDDLStatusCancelled):
			{
				assert.False(t, row["completed_timestamp"].IsNull())
				// Also make sure the timestamp is "real", and that it is recent.
				timestamp := row.AsString("completed_timestamp", "")
				completedTime, err := time.Parse(sqltypes.TimestampFormat, timestamp)
				assert.NoError(t, err)
				assert.Greater(t, completedTime.Unix(), testsStartupTime.Unix())
				completedTimestampNumValidations++
			}
		}
	}
	assert.NotZero(t, completedTimestampNumValidations)
}
