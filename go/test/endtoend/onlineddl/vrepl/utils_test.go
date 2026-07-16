/*
Copyright 2026 The Vitess Authors.

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

package vrepl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type (
	// applySchemaParams are the optional arguments of an ApplySchema command.
	applySchemaParams struct {
		DDLStrategy      string
		MigrationContext string
		UUIDs            string
	}

	// throttlerConfig is the expected throttler configuration of a tablet.
	throttlerConfig struct {
		Query     string
		Threshold float64
	}
)

const (
	throttledAppsTimeout = 60 * time.Second

	// defaultThrottlerQuery is the lag query the throttler uses by default.
	defaultThrottlerQuery = "select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from _vt.heartbeat"

	// throttlerConfigTimeout is how long we wait for a throttler config command to take effect.
	throttlerConfigTimeout = 60 * time.Second
)

var testsStartupTime time.Time

func init() {
	testsStartupTime = time.Now()
}

// applySchema applies SQL schema to the keyspace
func applySchema(ctx context.Context, keyspace string, sql string) error {
	_, err := applySchemaWithOutput(ctx, keyspace, sql, applySchemaParams{DDLStrategy: "direct -allow-zero-in-date"})
	return err
}

// applySchemaWithOutput applies SQL schema to the keyspace and returns the command output
func applySchemaWithOutput(ctx context.Context, keyspace string, sql string, params applySchemaParams) (string, error) {
	args := []string{
		"ApplySchema",
		"--sql", sql,
	}
	if params.MigrationContext != "" {
		args = append(args, "--migration-context", params.MigrationContext)
	}
	if params.DDLStrategy != "" {
		args = append(args, "--ddl-strategy", params.DDLStrategy)
	}
	if params.UUIDs != "" {
		args = append(args, "--uuid", params.UUIDs)
	}
	args = append(args, keyspace)
	return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
}

// paddedAlias returns the tablet alias in the zero-padded form stored in topology.
func paddedAlias(tablet *vitesst.Tablet) string {
	return topoproto.TabletAliasString(&topodatapb.TabletAlias{
		Cell: tablet.Cell,
		Uid:  uint32(tablet.UID),
	})
}

// printQueryResult will pretty-print a QueryResult to the logger.
func printQueryResult(writer io.Writer, qr *sqltypes.Result) {
	if qr == nil {
		return
	}
	if len(qr.Rows) == 0 {
		return
	}

	table := tablewriter.NewTable(
		writer,
		tablewriter.WithSymbols(tw.NewSymbols(tw.StyleASCII)),
		tablewriter.WithHeaderAutoFormat(tw.State(-1)),
		tablewriter.WithRowMaxWidth(30),
	)

	// Make header.
	header := make([]any, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		header = append(header, field.Name)
	}
	table.Header(header...)

	// Add rows.
	for _, row := range qr.Rows {
		vals := make([]any, 0, len(row))
		for _, val := range row {
			v := val.ToString()
			v = strings.ReplaceAll(v, "\r", " ")
			v = strings.ReplaceAll(v, "\n", " ")
			vals = append(vals, v)
		}
		if err := table.Append(vals...); err != nil {
			// If append fails, continue with remaining rows
			continue
		}
	}

	// Print table.
	_ = table.Render() // Ignore render error as this is output formatting
}

// vtgateExecQuery runs a query on VTGate using given query params
func vtgateExecQuery(t *testing.T, vtParams *mysql.ConnParams, query string, expectError string) *sqltypes.Result {
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

// vtgateExecDDL executes a DDL query with given strategy
func vtgateExecDDL(t *testing.T, vtParams *mysql.ConnParams, ddlStrategy string, query string, expectError string) *sqltypes.Result {
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

// checkRetryMigration attempts to retry a migration, and expects success/failure by counting affected rows
func checkRetryMigration(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectRetryPossible bool) {
	query, err := sqlparser.ParseAndBind(
		"alter vitess_migration %a retry",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := vtgateExecQuery(t, vtParams, query, "")

	if expectRetryPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// checkRetryPartialMigration attempts to retry a migration where a subset of shards failed
func checkRetryPartialMigration(t *testing.T, vtParams *mysql.ConnParams, uuid string, expectAtLeastRowsAffected uint64) {
	query, err := sqlparser.ParseAndBind(
		"alter vitess_migration %a retry",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := vtgateExecQuery(t, vtParams, query, "")

	assert.GreaterOrEqual(t, expectAtLeastRowsAffected, r.RowsAffected)
}

// checkCancelMigration attempts to cancel a migration, and expects success/failure by counting affected rows
func checkCancelMigration(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectCancelPossible bool) {
	query, err := sqlparser.ParseAndBind(
		"alter vitess_migration %a cancel",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := vtgateExecQuery(t, vtParams, query, "")

	if expectCancelPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// checkCleanupMigration attempts to cleanup a migration, and expects success by counting affected rows
func checkCleanupMigration(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string) {
	query, err := sqlparser.ParseAndBind(
		"alter vitess_migration %a cleanup",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := vtgateExecQuery(t, vtParams, query, "")

	assert.Equal(t, len(shards), int(r.RowsAffected))
}

// checkCompleteMigration attempts to complete a migration, and expects success by counting affected rows
func checkCompleteMigration(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectCompletePossible bool) {
	query, err := sqlparser.ParseAndBind(
		"alter vitess_migration %a complete",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := vtgateExecQuery(t, vtParams, query, "")

	if expectCompletePossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// checkLaunchMigration attempts to launch a migration, and expects success by counting affected rows
func checkLaunchMigration(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, launchShards string, expectLaunchPossible bool) {
	query, err := sqlparser.ParseAndBind(
		"alter vitess_migration %a launch vitess_shards %a",
		sqltypes.StringBindVariable(uuid),
		sqltypes.StringBindVariable(launchShards),
	)
	require.NoError(t, err)
	r := vtgateExecQuery(t, vtParams, query, "")

	if expectLaunchPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// checkCompleteAllMigrations completes all pending migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func checkCompleteAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	completeQuery := "alter vitess_migration complete all"
	r := vtgateExecQuery(t, vtParams, completeQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// checkCancelAllMigrations cancels all pending migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func checkCancelAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	cancelQuery := "alter vitess_migration cancel all"
	r := vtgateExecQuery(t, vtParams, cancelQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// checkCancelAllMigrationsViaVtctld cancels all pending migrations. There is no validation for affected migrations.
func checkCancelAllMigrationsViaVtctld(t *testing.T, keyspace string) {
	cancelQuery := "alter vitess_migration cancel all"

	_, err := applySchemaWithOutput(t.Context(), keyspace, cancelQuery, applySchemaParams{})
	assert.NoError(t, err)
}

// checkLaunchAllMigrations launches all queued posponed migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func checkLaunchAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	completeQuery := "alter vitess_migration launch all"
	r := vtgateExecQuery(t, vtParams, completeQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// checkMigrationStatus verifies that the migration indicated by given UUID has the given expected status
func checkMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectStatuses ...schema.OnlineDDLStatus) bool {
	ksName := shards[0].Keyspace.Name
	query, err := sqlparser.ParseAndBind(
		fmt.Sprintf("show vitess_migrations from %s like %%a", ksName),
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := vtgateExecQuery(t, vtParams, query, "")
	fmt.Printf("# output for `%s`:\n", query)
	printQueryResult(os.Stdout, r)

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
	return assert.Equal(t, len(shards), count)
}

// waitForMigrationStatus waits for a migration to reach either provided statuses (returns immediately), or eventually time out
func waitForMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	shardNames := map[string]bool{}
	for _, shard := range shards {
		shardNames[shard.Name] = true
	}
	query, err := sqlparser.ParseAndBind(
		"show vitess_migrations like %a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[string(status)] = true
	}
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	lastKnownStatus := ""
	for {
		countMatchedShards := 0
		r := vtgateExecQuery(t, vtParams, query, "")
		for _, row := range r.Named().Rows {
			shardName := row["shard"].ToString()
			if !shardNames[shardName] {
				// irrelevant shard
				continue
			}
			lastKnownStatus = row["migration_status"].ToString()
			if row["migration_uuid"].ToString() == uuid && statusesMap[lastKnownStatus] {
				countMatchedShards++
			}
		}
		if countMatchedShards == len(shards) {
			return schema.OnlineDDLStatus(lastKnownStatus)
		}
		select {
		case <-ctx.Done():
			return schema.OnlineDDLStatus(lastKnownStatus)
		case <-ticker.C:
		}
	}
}

// checkMigrationArtifacts verifies given migration exists, and checks if it has artifacts
func checkMigrationArtifacts(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectArtifacts bool) {
	r := readMigrations(t, vtParams, uuid)

	assert.Equal(t, len(shards), len(r.Named().Rows))
	for _, row := range r.Named().Rows {
		hasArtifacts := (row["artifacts"].ToString() != "")
		assert.Equal(t, expectArtifacts, hasArtifacts)
	}
}

// readMigrations reads migration entries
func readMigrations(t *testing.T, vtParams *mysql.ConnParams, like string) *sqltypes.Result {
	query, err := sqlparser.ParseAndBind(
		"show vitess_migrations like %a",
		sqltypes.StringBindVariable(like),
	)
	require.NoError(t, err)

	return vtgateExecQuery(t, vtParams, query, "")
}

// throttleAllMigrations fully throttles online-ddl apps
func throttleAllMigrations(t *testing.T, vtParams *mysql.ConnParams) {
	query := "alter vitess_migration throttle all expire '24h' ratio 1"
	_ = vtgateExecQuery(t, vtParams, query, "")
}

// unthrottleAllMigrations cancels migration throttling
func unthrottleAllMigrations(t *testing.T, vtParams *mysql.ConnParams) {
	query := "alter vitess_migration unthrottle all"
	_ = vtgateExecQuery(t, vtParams, query, "")
}

// checkThrottledApps checks for existence or non-existence of an app in the throttled apps list
func checkThrottledApps(t *testing.T, vtParams *mysql.ConnParams, throttlerApp throttlerapp.Name, expectFind bool) bool {
	ctx, cancel := context.WithTimeout(t.Context(), throttledAppsTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		query := "show vitess_throttled_apps"
		r := vtgateExecQuery(t, vtParams, query, "")

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
			assert.Fail(t, "checkThrottledApps timed out", "waiting for '%v' to be in throttled status '%v'", throttlerApp.String(), expectFind)
			return false
		case <-ticker.C:
		}
	}
}

// waitForThrottledTimestamp waits for a migration to have a non-empty last_throttled_timestamp
func waitForThrottledTimestamp(t *testing.T, vtParams *mysql.ConnParams, uuid string, timeout time.Duration) (
	row sqltypes.RowNamedValues,
	startedTimestamp string,
	lastThrottledTimestamp string,
) {
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		rs := readMigrations(t, vtParams, uuid)
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
	assert.Fail(t, "timeout waiting for last_throttled_timestamp to have nonempty value")
	return row, startedTimestamp, lastThrottledTimestamp
}

// validateSequentialMigrationIDs validates that schem_migrations.id column, which is an AUTO_INCREMENT, does
// not have gaps
func validateSequentialMigrationIDs(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard) {
	r := vtgateExecQuery(t, vtParams, "show vitess_migrations", "")
	shardMin := map[string]uint64{}
	shardMax := map[string]uint64{}
	shardCount := map[string]uint64{}

	for _, row := range r.Named().Rows {
		id := row.AsUint64("id", 0)
		require.NotZero(t, id)

		shard := row.AsString("shard", "")
		require.NotEmpty(t, shard)

		if _, ok := shardMin[shard]; !ok {
			shardMin[shard] = id
			shardMax[shard] = id
		}
		if id < shardMin[shard] {
			shardMin[shard] = id
		}
		if id > shardMax[shard] {
			shardMax[shard] = id
		}
		shardCount[shard]++
	}
	require.NotEmpty(t, shards)
	assert.Equal(t, len(shards), len(shardMin))
	assert.Equal(t, len(shards), len(shardMax))
	assert.Equal(t, len(shards), len(shardCount))
	for shard, count := range shardCount {
		assert.NotZero(t, count)
		assert.Equalf(t, count, shardMax[shard]-shardMin[shard]+1, "mismatch: shared=%v, count=%v, min=%v, max=%v", shard, count, shardMin[shard], shardMax[shard])
	}
}

// validateCompletedTimestamp ensures that any migration in `cancelled`, `completed`, `failed` statuses
// has a non-nil and valid `completed_timestamp` value.
func validateCompletedTimestamp(t *testing.T, vtParams *mysql.ConnParams) {
	require.False(t, testsStartupTime.IsZero())
	r := vtgateExecQuery(t, vtParams, "show vitess_migrations", "")

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

// waitForVReplicationStatus waits for a vreplication stream to be in one of given states, or timeout
func waitForVReplicationStatus(t *testing.T, tablet *vitesst.Tablet, uuid string, timeout time.Duration, expectStatuses ...string) (status string) {
	query, err := sqlparser.ParseAndBind(
		"select state from _vt.vreplication where workflow=%a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[status] = true
	}
	startTime := time.Now()
	lastKnownStatus := ""
	for time.Since(startTime) < timeout {
		r, err := tablet.QueryTabletWithDB(t.Context(), query, "")
		require.NoError(t, err)

		if row := r.Named().Row(); row != nil {
			lastKnownStatus, err = row.ToString("state")
			assert.NoError(t, err)
			if statusesMap[lastKnownStatus] {
				return lastKnownStatus
			}
		}
		time.Sleep(1 * time.Second)
	}
	return lastKnownStatus
}

// waitForSrvKeyspace waits until the given srvkeyspace entry is found in the given cell
func waitForSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	args := []string{"GetSrvKeyspaceNames", cell}

	ctx, cancel := context.WithTimeout(ctx, throttlerConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err != nil {
			return err
		}
		if strings.Contains(result, `"`+keyspace+`"`) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for GetSrvKeyspaceNames to contain '%v'", keyspace)
		case <-ticker.C:
		}
	}
}

// updateThrottlerTopoConfigRaw runs vtctldclient UpdateThrottlerConfig for a single keyspace.
// This retries the command until it succeeds or times out as the SrvKeyspace record may not yet
// exist for a newly created keyspace that is still initializing before it becomes serving.
func updateThrottlerTopoConfigRaw(
	ctx context.Context,
	keyspaceName string,
	opts *vtctldatapb.UpdateThrottlerConfigRequest,
	appRule *topodatapb.ThrottledAppRule,
	appCheckedMetrics map[string]*topodatapb.ThrottlerConfig_MetricNames,
) (result string, err error) {
	args := []string{}
	args = append(args, "UpdateThrottlerConfig")
	if opts.Enable {
		args = append(args, "--enable")
	}
	if opts.Disable {
		args = append(args, "--disable")
	}
	if opts.MetricName != "" {
		args = append(args, "--metric-name", opts.MetricName)
	}
	if opts.Threshold > 0 || opts.MetricName != "" {
		args = append(args, "--threshold", fmt.Sprintf("%f", opts.Threshold))
	}
	args = append(args, "--custom-query", opts.CustomQuery)
	if appRule != nil {
		args = append(args, "--throttle-app", appRule.Name)
		args = append(args, "--throttle-app-duration", time.Until(protoutil.TimeFromProto(appRule.ExpiresAt).UTC()).String())
		args = append(args, "--throttle-app-ratio", fmt.Sprintf("%f", appRule.Ratio))
		if appRule.Exempt {
			args = append(args, "--throttle-app-exempt")
		}
	}
	if appCheckedMetrics != nil {
		if len(appCheckedMetrics) != 1 {
			return "", errors.New("appCheckedMetrics must either be nil or have exactly one entry")
		}
		for app, metrics := range appCheckedMetrics {
			args = append(args, "--app-name", app)
			args = append(args, "--app-metrics", strings.Join(metrics.Names, ","))
		}
	}
	args = append(args, keyspaceName)

	ctx, cancel := context.WithTimeout(ctx, throttlerConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", throttlerConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}

// updateThrottlerTopoConfig runs vtctldclient UpdateThrottlerConfig on all keyspaces.
func updateThrottlerTopoConfig(
	ctx context.Context,
	opts *vtctldatapb.UpdateThrottlerConfigRequest,
	appRule *topodatapb.ThrottledAppRule,
	appCheckedMetrics map[string]*topodatapb.ThrottlerConfig_MetricNames,
) (string, error) {
	var (
		errs []error
		res  strings.Builder
	)
	for _, ks := range clusterInstance.Keyspaces() {
		ires, err := updateThrottlerTopoConfigRaw(ctx, ks.Name, opts, appRule, appCheckedMetrics)
		if err != nil {
			errs = append(errs, err)
		}
		res.WriteString(ires)
	}
	return res.String(), errors.Join(errs...)
}

// getThrottlerStatus runs vtctldclient GetThrottlerStatus
func getThrottlerStatus(ctx context.Context, tablet *vitesst.Tablet) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	output, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetThrottlerStatus", tablet.Alias())
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return resp.Status, nil
}

// throttleAppRaw runs vtctldclient UpdateThrottlerConfig with --throttle-app flags on a single keyspace.
// This retries the command until it succeeds or times out as the SrvKeyspace record may not yet exist
// for a newly created keyspace that is still initializing before it becomes serving.
func throttleAppRaw(ctx context.Context, keyspaceName string, throttlerApp throttlerapp.Name, throttle bool) (result string, err error) {
	args := []string{}
	args = append(args, "UpdateThrottlerConfig")
	if throttle {
		args = append(args, "--throttle-app", throttlerApp.String())
		args = append(args, "--throttle-app-duration", "1h")
	} else {
		args = append(args, "--unthrottle-app", throttlerApp.String())
	}
	args = append(args, keyspaceName)

	ctx, cancel := context.WithTimeout(ctx, throttlerConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", throttlerConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}

// throttleApp throttles or unthrottles an app on all keyspaces
func throttleApp(ctx context.Context, throttlerApp throttlerapp.Name, throttle bool) (string, error) {
	var (
		errs []error
		res  strings.Builder
	)
	for _, ks := range clusterInstance.Keyspaces() {
		ires, err := throttleAppRaw(ctx, ks.Name, throttlerApp, throttle)
		if err != nil {
			errs = append(errs, err)
		}
		res.WriteString(ires)
	}
	return res.String(), errors.Join(errs...)
}

// waitUntilTabletsConfirmThrottledApp waits until all tablets report the given app's throttled state
func waitUntilTabletsConfirmThrottledApp(t *testing.T, throttlerApp throttlerapp.Name, expectThrottled bool) {
	for _, tablet := range clusterInstance.Tablets() {
		waitForThrottledApp(t, tablet, throttlerApp, expectThrottled, throttlerConfigTimeout)
	}
}

// throttleAppAndWaitUntilTabletsConfirm throttles an app and waits for all tablets to confirm
func throttleAppAndWaitUntilTabletsConfirm(t *testing.T, throttlerApp throttlerapp.Name) (string, error) {
	res, err := throttleApp(t.Context(), throttlerApp, true)
	if err != nil {
		return res, err
	}
	waitUntilTabletsConfirmThrottledApp(t, throttlerApp, true)
	return res, nil
}

// unthrottleAppAndWaitUntilTabletsConfirm unthrottles an app and waits for all tablets to confirm
func unthrottleAppAndWaitUntilTabletsConfirm(t *testing.T, throttlerApp throttlerapp.Name) (string, error) {
	res, err := throttleApp(t.Context(), throttlerApp, false)
	if err != nil {
		return res, err
	}
	waitUntilTabletsConfirmThrottledApp(t, throttlerApp, false)
	return res, nil
}

// waitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled and have the provided config (if any) until the specified timeout.
func waitForThrottlerStatusEnabled(t *testing.T, tablet *vitesst.Tablet, enabled bool, config *throttlerConfig, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		if tabletNotServing(ctx, tablet) {
			t.Logf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias())
			return
		}

		status, err := getThrottlerStatus(ctx, tablet)
		good := func() bool {
			if err != nil {
				t.Logf("getThrottlerStatus failed: %v", err)
				return false
			}
			if status.IsEnabled != enabled {
				return false
			}
			if status.IsEnabled && len(status.MetricsHealth) == 0 {
				// throttler is enabled, but no metrics collected yet. Wait for something to be collected.
				return false
			}
			if config == nil {
				return true
			}
			if status.LagMetricQuery == config.Query && status.DefaultThreshold == config.Threshold {
				return true
			}
			return false
		}
		if good() {
			return
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, "timeout", "waiting for the %s tablet's throttler status enabled to be %t with the correct config after %v; last seen status: %+v",
				tablet.Alias(), enabled, timeout, status)
			return
		case <-ticker.C:
		}
	}
}

// waitForThrottledApp waits for a tablet to report the given app as throttled or not throttled,
// until the specified timeout.
func waitForThrottledApp(t *testing.T, tablet *vitesst.Tablet, throttlerApp throttlerapp.Name, expectThrottled bool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		status, err := getThrottlerStatus(ctx, tablet)
		require.NoError(t, err)
		throttledApps := status.ThrottledApps
		require.NotEmpty(t, throttledApps) // "always-throttled-app" is always there.
		appFoundThrottled := false
		for _, throttledApp := range throttledApps {
			expiresAt := protoutil.TimeFromProto(throttledApp.ExpiresAt)
			if throttledApp.Name == throttlerApp.String() && expiresAt.After(time.Now()) {
				appFoundThrottled = true
				break
			}
		}
		if appFoundThrottled == expectThrottled {
			return
		}
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		if tabletNotServing(ctx, tablet) {
			t.Logf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias())
			return
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, "timeout", "waiting for the %s tablet's throttled apps with the correct config (expecting %s to be %v) after %v; last seen throttled apps: %+v",
				tablet.Alias(), throttlerApp.String(), expectThrottled, timeout, throttledApps)
			return
		case <-ticker.C:
		}
	}
}

// tabletNotServing reports whether the tablet's status page says it is not serving
func tabletNotServing(ctx context.Context, tablet *vitesst.Tablet) bool {
	status, body, err := tablet.MakeAPICall(ctx, "/debug/status_details")
	if err != nil || status != 200 {
		return false
	}
	class := strings.ToLower(gjson.Get(body, "0.Class").String())
	value := strings.ToLower(gjson.Get(body, "0.Value").String())
	return class == "unhappy" && strings.Contains(value, "not serving")
}
