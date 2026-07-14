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

package vreplstress

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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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
	// throttlerConfigTimeout is how long we wait for a throttler config command to take effect.
	throttlerConfigTimeout = 60 * time.Second

	// throttlerStatusTimeout is how long we wait for a tablet to report the expected throttler status.
	throttlerStatusTimeout = time.Minute
)

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

// printQueryResult will pretty-print a QueryResult to the logger.
func printQueryResult(writer io.Writer, qr *sqltypes.Result) {
	if qr == nil {
		return
	}
	if len(qr.Rows) == 0 {
		return
	}

	table := tablewriter.NewTable(writer,
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
			continue
		}
	}

	_ = table.Render()
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

// checkMigrationStatus verifies that the migration indicated by given UUID has the given expected status
func checkMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectStatuses ...schema.OnlineDDLStatus) bool {
	ksName := shards[0].Keyspace.Name
	query, err := sqlparser.ParseAndBind(fmt.Sprintf("show vitess_migrations from %s like %%a", ksName),
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
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
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

// readMigrations reads migration entries
func readMigrations(t *testing.T, vtParams *mysql.ConnParams, like string) *sqltypes.Result {
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(like),
	)
	require.NoError(t, err)

	return vtgateExecQuery(t, vtParams, query, "")
}

// updateThrottlerTopoConfigRaw runs vtctldclient UpdateThrottlerConfig for a single keyspace.
// This retries the command until it succeeds or times out as the SrvKeyspace record may not yet
// exist for a newly created keyspace that is still initializing before it becomes serving.
func updateThrottlerTopoConfigRaw(
	ctx context.Context,
	keyspaceName string,
	opts *vtctldatapb.UpdateThrottlerConfigRequest,
) (result string, err error) {
	args := []string{"UpdateThrottlerConfig"}
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
func updateThrottlerTopoConfig(ctx context.Context, opts *vtctldatapb.UpdateThrottlerConfigRequest) (string, error) {
	var (
		errs []error
		res  strings.Builder
	)
	for _, ks := range clusterInstance.Keyspaces() {
		ires, err := updateThrottlerTopoConfigRaw(ctx, ks.Name, opts)
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

// enableLagThrottlerAndWaitForStatus enables the throttler on all keyspaces and waits for
// all tablets to report the throttler as enabled.
func enableLagThrottlerAndWaitForStatus(t *testing.T) {
	req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true}
	_, err := updateThrottlerTopoConfig(t.Context(), req)
	require.NoError(t, err)

	for _, tablet := range clusterInstance.Tablets() {
		waitForThrottlerStatusEnabled(t, tablet, true, nil, throttlerStatusTimeout)
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
