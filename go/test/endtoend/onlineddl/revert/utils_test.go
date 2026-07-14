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

package revert

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
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
	"vitess.io/vitess/go/vt/log"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

const throttlerConfigTimeout = 60 * time.Second

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

// checkCompleteMigration attempts to complete a migration, and expects success by counting affected rows
func checkCompleteMigration(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectCompletePossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a complete",
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

// readMigrations reads migration entries
func readMigrations(t *testing.T, vtParams *mysql.ConnParams, like string) *sqltypes.Result {
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(like),
	)
	require.NoError(t, err)

	return vtgateExecQuery(t, vtParams, query, "")
}

// getMySQLVersion returns the version of the MySQL server backing the given tablet
func getMySQLVersion(t *testing.T, tablet *vitesst.Tablet) string {
	query := `select @@version as version`
	rs, err := tablet.QueryTabletWithDB(t.Context(), query, "")
	assert.NoError(t, err)
	row := rs.Named().Row()
	assert.NotNil(t, row)
	version := row["version"].ToString()
	assert.NotEmpty(t, row)
	return version
}

// printQueryResult will pretty-print a QueryResult to the given writer.
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

	header := make([]any, 0, len(qr.Fields))
	for _, field := range qr.Fields {
		header = append(header, field.Name)
	}
	table.Header(header...)

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

// checkThrottlerRaw runs vtctldclient CheckThrottler
func checkThrottlerRaw(ctx context.Context, tablet *vitesst.Tablet, appName throttlerapp.Name, flags *throttle.CheckFlags) (result string, err error) {
	args := []string{}
	args = append(args, "CheckThrottler")
	if flags == nil {
		flags = &throttle.CheckFlags{
			Scope: base.SelfScope,
		}
	}
	if appName != "" {
		args = append(args, "--app-name", appName.String())
	}
	if flags.Scope != base.UndefinedScope {
		args = append(args, "--scope", flags.Scope.String())
	}
	if flags.OKIfNotExists {
		args = append(args, "--ok-if-not-exists")
	}
	if !flags.SkipRequestHeartbeats {
		args = append(args, "--request-heartbeats")
	}
	args = append(args, tablet.Alias())

	return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
}

// getThrottlerStatusRaw runs vtctldclient GetThrottlerStatus
func getThrottlerStatusRaw(ctx context.Context, tablet *vitesst.Tablet) (result string, err error) {
	return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetThrottlerStatus", tablet.Alias())
}

// checkThrottler runs vtctldclient CheckThrottler.
func checkThrottler(ctx context.Context, tablet *vitesst.Tablet, appName throttlerapp.Name, flags *throttle.CheckFlags) (*vtctldatapb.CheckThrottlerResponse, error) {
	output, err := checkThrottlerRaw(ctx, tablet, appName, flags)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.CheckThrottlerResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// getThrottlerStatus runs vtctldclient GetThrottlerStatus and parses its response.
func getThrottlerStatus(ctx context.Context, tablet *vitesst.Tablet) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	output, err := getThrottlerStatusRaw(ctx, tablet)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return resp.Status, nil
}

// updateThrottlerTopoConfig runs vtctldclient UpdateThrottlerConfig on every keyspace.
// This retries the command until it succeeds or times out as the SrvKeyspace record may
// not yet exist for a newly created keyspace that is still initializing before it becomes serving.
func updateThrottlerTopoConfig(ctx context.Context, cluster *vitesst.Cluster, opts *vtctldatapb.UpdateThrottlerConfigRequest) error {
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

	for _, ks := range cluster.Keyspaces() {
		keyspaceArgs := append(slices.Clone(args), ks.Name)

		if err := func() error {
			ctx, cancel := context.WithTimeout(ctx, throttlerConfigTimeout)
			defer cancel()

			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			var (
				result string
				err    error
			)
			for {
				result, err = cluster.Vtctld().ExecuteCommandWithOutput(ctx, keyspaceArgs...)
				if err == nil {
					return nil
				}
				select {
				case <-ctx.Done():
					return fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", throttlerConfigTimeout, result, err)
				case <-ticker.C:
				}
			}
		}(); err != nil {
			return err
		}
	}
	return nil
}

// waitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled until the specified timeout.
func waitForThrottlerStatusEnabled(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, enabled bool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		_, tabletBody, err := tablet.MakeAPICall(ctx, "/debug/status_details")
		if err == nil {
			class := strings.ToLower(gjson.Get(tabletBody, "0.Class").String())
			value := strings.ToLower(gjson.Get(tabletBody, "0.Value").String())
			if class == "unhappy" && strings.Contains(value, "not serving") {
				log.Info(fmt.Sprintf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias()))
				return
			}
		}

		status, err := getThrottlerStatus(ctx, tablet)
		good := func() bool {
			if err != nil {
				log.Error(fmt.Sprintf("GetThrottlerStatus failed: %v", err))
				return false
			}
			if status.IsEnabled != enabled {
				return false
			}
			if status.IsEnabled && len(status.MetricsHealth) == 0 {
				// throttler is enabled, but no metrics collected yet. Wait for something to be collected.
				return false
			}
			return true
		}
		if good() {
			return
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, "timeout", "waiting for the %s tablet's throttler status enabled to be %t after %v; last seen status: %+v",
				tablet.Alias(), enabled, timeout, status)
			return
		case <-ticker.C:
		}
	}
}

// enableLagThrottlerAndWaitForStatus enables the throttler with the standard replication lag
// metric, and waits until the throttler is confirmed to be running on all tablets.
func enableLagThrottlerAndWaitForStatus(ctx context.Context, t *testing.T, cluster *vitesst.Cluster) {
	req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true}
	err := updateThrottlerTopoConfig(ctx, cluster, req)
	require.NoError(t, err)

	for _, tablet := range cluster.Tablets() {
		waitForThrottlerStatusEnabled(ctx, t, tablet, true, time.Minute)
	}
}

// waitForCheckThrottlerResult waits until a throttler check on the given tablet returns the wanted response code
func waitForCheckThrottlerResult(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, appName throttlerapp.Name, flags *throttle.CheckFlags, wantCode tabletmanagerdatapb.CheckThrottlerResponseCode, timeout time.Duration) (*vtctldatapb.CheckThrottlerResponse, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		resp, err := checkThrottler(ctx, tablet, appName, flags)
		require.NoError(t, err)
		if resp.Check.ResponseCode == wantCode {
			return resp, true
		}
		select {
		case <-ctx.Done():
			assert.Failf(t, "timeout", "waiting for %s tablet's throttler to return a %v check result after %v; last seen value: %+v", tablet.Alias(), wantCode, timeout, resp.Check.ResponseCode)
			return resp, false
		case <-ticker.C:
		}
	}
}
