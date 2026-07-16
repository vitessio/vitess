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
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// applySchemaParams are the optional arguments of an ApplySchema command.
type applySchemaParams struct {
	DDLStrategy      string
	MigrationContext string
	UUIDs            string
}

const throttlerConfigTimeout = 60 * time.Second

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

// checkMigrationStatus verifies that the migration indicated by given UUID has the given expected status
func checkMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard, uuid string, expectStatuses ...schema.OnlineDDLStatus) bool {
	ksName := shards[0].Keyspace.Name
	query, err := sqlparser.ParseAndBind(
		fmt.Sprintf("show vitess_migrations from %s like %%a", ksName),
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := onlineddl.VtgateExecQuery(t, vtParams, query, "")
	fmt.Printf("# output for `%s`:\n", query)
	onlineddl.PrintQueryResult(os.Stdout, r)

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
		r := onlineddl.VtgateExecQuery(t, vtParams, query, "")
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

// validateSequentialMigrationIDs validates that schem_migrations.id column, which is an AUTO_INCREMENT, does
// not have gaps
func validateSequentialMigrationIDs(t *testing.T, vtParams *mysql.ConnParams, shards []*vitesst.Shard) {
	r := onlineddl.VtgateExecQuery(t, vtParams, "show vitess_migrations", "")
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

// getThrottlerStatusRaw runs vtctldclient GetThrottlerStatus
func getThrottlerStatusRaw(ctx context.Context, tablet *vitesst.Tablet) (result string, err error) {
	return clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetThrottlerStatus", tablet.Alias())
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
