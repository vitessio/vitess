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

package primaryfailure

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vitesst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	cell1        = "zone1"
	cell2        = "zone2"

	// promotionRuleTabletUID is the tablet id the "test" durability policy
	// hardwires to a Prefer promotion rule (alias zone2-0000000200), so the
	// promotion-rule tests provision their cross-cell replica with this id.
	promotionRuleTabletUID = 200
)

// lastUsedValue is the last id inserted by checkReplication and
// verifyWritesSucceed. setupVttablets resets it for each fresh cluster.
var lastUsedValue int

// setupVttablets builds and starts a cluster with numReplicas replica tablets
// and numRdonly rdonly tablets in cell1, plus one replica for each cell2 uid,
// under the given durability policy. It starts no vtgate, no vtorc, and elects
// no primary, so the tablets come up as read-only replicas the test drives.
func setupVttablets(t *testing.T, numReplicas, numRdonly int, durability string, cell2UIDs ...int) *vitesst.Cluster {
	t.Helper()
	ctx := t.Context()

	lastUsedValue = 100
	if durability == "" {
		durability = "none"
	}

	total := numReplicas + numRdonly + len(cell2UIDs)
	idx := 0
	keyspace := vitesst.WithKeyspace(keyspaceName).
		WithShardNames(shardName).
		WithReplicas(total - 1).
		WithDurabilityPolicy(durability).
		WithoutPrimaryElection().
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			switch {
			case idx < numReplicas:
				spec.Cell = cell1
				spec.Type = "replica"
				spec.UID = 100 + idx
			case idx < numReplicas+numRdonly:
				spec.Cell = cell1
				spec.Type = "rdonly"
				spec.UID = 100 + idx
			default:
				spec.Cell = cell2
				spec.Type = "replica"
				spec.UID = cell2UIDs[idx-numReplicas-numRdonly]
			}
			idx++
		})

	clusterInstance, err := vitesst.NewCluster(t,
		vitesst.WithCells(cell1, cell2),
		vitesst.WithoutVTGate(),
		vitesst.WithVTTabletArgs("--lock-tables-timeout", "5s"),
		keyspace,
	)
	require.NoError(t, err)

	cleanup, err := clusterInstance.Start(t, ctx)
	t.Cleanup(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		if cleanupErr := cleanup(cleanupCtx); cleanupErr != nil {
			t.Logf("cluster teardown: %v", cleanupErr)
		}
	})
	require.NoError(t, err)

	// Clear super_read_only on every tablet so that writes issued directly
	// against a replica's mysqld succeed before the shard has a primary.
	for _, tablet := range clusterInstance.Tablets() {
		_, err := tablet.QueryTabletWithDB(ctx, "SET GLOBAL super_read_only = OFF", "")
		require.NoError(t, err)
	}
	return clusterInstance
}

// startVTOrc starts a VTOrc instance in cell1 with the given extra arguments.
func startVTOrc(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, extraArgs ...string) *vitesst.VTOrc {
	t.Helper()
	vtorc, err := clusterInstance.AddVTOrc(t, ctx, cell1, extraArgs...)
	require.NoError(t, err)
	return vtorc
}

// setupVttabletsAndVTOrcs builds a cluster with the requested cell1 tablets and
// then starts a VTOrc with the given extra arguments. The tablets come up
// without a primary, so VTOrc elects one.
func setupVttabletsAndVTOrcs(t *testing.T, numReplicas, numRdonly int, durability string, orcExtraArgs ...string) (*vitesst.Cluster, *vitesst.VTOrc) {
	t.Helper()
	clusterInstance := setupVttablets(t, numReplicas, numRdonly, durability)
	vtorc := startVTOrc(t.Context(), t, clusterInstance, orcExtraArgs...)
	return clusterInstance, vtorc
}

// startVttablet adds one more tablet of the given kind serving the shard, in
// the given cell, and returns it.
func startVttablet(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, cell string, isRdonly bool) *vitesst.Tablet {
	t.Helper()
	tabletType := "replica"
	if isRdonly {
		tabletType = "rdonly"
	}
	tablet, err := clusterInstance.AddTablet(t, ctx, cell, keyspaceName, shardName, tabletType)
	require.NoError(t, err)
	return tablet
}

// cell2Tablet returns the single cell2 tablet of the shard.
func cell2Tablet(t *testing.T, shard *vitesst.Shard) *vitesst.Tablet {
	t.Helper()
	for _, tablet := range shard.Tablets() {
		if tablet.Cell == cell2 {
			return tablet
		}
	}
	require.FailNow(t, "could not find cell2 tablet")
	return nil
}

// initializeShard elects the given tablet as the shard's primary.
func initializeShard(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, primary *vitesst.Tablet) {
	t.Helper()
	err := clusterInstance.Vtctld().ExecuteCommand(ctx,
		"PlannedReparentShard",
		keyspaceName+"/"+shardName,
		"--wait-replicas-timeout", "31s",
		"--new-primary", primary.Alias())
	require.NoError(t, err)
}

// runSQL runs a SQL statement directly on the given tablet's mysqld.
func runSQL(ctx context.Context, sql string, tablet *vitesst.Tablet, db string) (*sqltypes.Result, error) {
	return tablet.QueryTabletWithDB(ctx, sql, db)
}

// shardPrimaryTablet waits until a primary tablet has been elected for the
// given shard and returns it.
func shardPrimaryTablet(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, shard *vitesst.Shard) *vitesst.Tablet {
	t.Helper()
	start := time.Now()
	for {
		require.False(t, time.Since(start) > time.Second*60, "failed to elect primary before timeout")

		primaryAlias, err := shardPrimaryAlias(ctx, clusterInstance, shard)
		require.NoError(t, err)
		if primaryAlias != "" {
			for _, tablet := range shard.Tablets() {
				if tablet.Alias() == primaryAlias {
					return tablet
				}
			}
		}
		time.Sleep(time.Second)
	}
}

// shardPrimaryAlias returns the alias of the shard's primary as recorded in the
// topology server, or "" when the shard has no primary yet.
func shardPrimaryAlias(ctx context.Context, clusterInstance *vitesst.Cluster, shard *vitesst.Shard) (string, error) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetShard", shard.Ref())
	if err != nil {
		return "", err
	}
	var record struct {
		Shard struct {
			PrimaryAlias *struct {
				Cell string `json:"cell"`
				UID  uint32 `json:"uid"`
			} `json:"primary_alias"`
		} `json:"shard"`
	}
	if err := json.Unmarshal([]byte(out), &record); err != nil {
		return "", err
	}
	if record.Shard.PrimaryAlias != nil {
		return fmt.Sprintf("%s-%d", record.Shard.PrimaryAlias.Cell, record.Shard.PrimaryAlias.UID), nil
	}
	return "", nil
}

// checkPrimaryTablet waits until the given tablet becomes the primary tablet.
// It makes sure the tablet type is primary and, when checkServing is set, that
// its health check agrees it is serving.
func checkPrimaryTablet(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet, checkServing bool) {
	t.Helper()
	start := time.Now()
	for {
		require.False(t, time.Since(start) > time.Second*60, "failed to elect primary before timeout")

		typ, err := tabletTopoType(ctx, clusterInstance, tablet)
		require.NoError(t, err)
		if typ != topodatapb.TabletType_PRIMARY {
			time.Sleep(time.Second)
			continue
		}

		vars, err := tablet.GetVars(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if checkServing && vars["TabletStateName"] != "SERVING" {
			time.Sleep(time.Second)
			continue
		}
		if vars["TabletType"] != "primary" {
			time.Sleep(time.Second)
			continue
		}
		return
	}
}

// tabletTopoType returns the tablet type recorded in the topology server.
func tabletTopoType(ctx context.Context, clusterInstance *vitesst.Cluster, tablet *vitesst.Tablet) (topodatapb.TabletType, error) {
	out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetTablet", tablet.Alias())
	if err != nil {
		return topodatapb.TabletType_UNKNOWN, err
	}
	record := &topodatapb.Tablet{}
	if err := protojson.Unmarshal([]byte(out), record); err != nil {
		return topodatapb.TabletType_UNKNOWN, err
	}
	return record.GetType(), nil
}

// checkReplication checks that replication is setup correctly and writes
// succeed and are replicated on all the replicas.
func checkReplication(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, primary *vitesst.Tablet, replicas []*vitesst.Tablet, timeToWait time.Duration) {
	t.Helper()
	endTime := time.Now().Add(timeToWait)
	sqlSchema := `
		create table if not exists vt_ks.vt_insert_test (
		id bigint,
		msg varchar(64),
		primary key (id)
		) Engine=InnoDB
		`
	for {
		require.False(t, time.Now().After(endTime), "timed out waiting for keyspace vt_ks to be created by schema engine")
		if _, err := runSQL(ctx, sqlSchema, primary, ""); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	lastUsedValue++
	confirmReplication(ctx, t, primary, replicas, time.Until(endTime), lastUsedValue)
	validateTopology(ctx, t, clusterInstance, true, time.Until(endTime))
}

// verifyWritesSucceed inserts more data into vt_insert_test and checks that it
// is replicated too. Call it only after checkReplication has run once, since
// that function creates the table this one uses.
func verifyWritesSucceed(ctx context.Context, t *testing.T, primary *vitesst.Tablet, replicas []*vitesst.Tablet, timeToWait time.Duration) {
	t.Helper()
	lastUsedValue++
	confirmReplication(ctx, t, primary, replicas, timeToWait, lastUsedValue)
}

func confirmReplication(ctx context.Context, t *testing.T, primary *vitesst.Tablet, replicas []*vitesst.Tablet, timeToWait time.Duration, valueToInsert int) {
	t.Helper()
	insertSQL := fmt.Sprintf("insert into vt_insert_test(id, msg) values (%d, 'test %d')", valueToInsert, valueToInsert)
	_, err := runSQL(ctx, insertSQL, primary, "vt_ks")
	require.NoError(t, err)
	endTime := time.Now().Add(timeToWait)
	for {
		var lastErr error
		for _, tab := range replicas {
			if errInReplication := checkInsertedValue(ctx, tab, valueToInsert); errInReplication != nil {
				lastErr = errInReplication
			}
		}
		if lastErr == nil {
			return
		}
		require.False(t, time.Now().After(endTime), "timed out waiting for replication, data not yet replicated: %v", lastErr)
		time.Sleep(300 * time.Millisecond)
	}
}

// checkTabletUptoDate verifies that the tablet has all the writes so far.
func checkTabletUptoDate(ctx context.Context, t *testing.T, tablet *vitesst.Tablet) {
	t.Helper()
	require.NoError(t, checkInsertedValue(ctx, tablet, lastUsedValue))
}

func checkInsertedValue(ctx context.Context, tablet *vitesst.Tablet, index int) error {
	selectSQL := fmt.Sprintf("select msg from vt_ks.vt_insert_test where id=%d", index)
	qr, err := runSQL(ctx, selectSQL, tablet, "")
	// The error may be not nil, if the replication has not caught upto the point where the table exists.
	// We can safely skip this error and retry reading after wait
	if err == nil && len(qr.Rows) == 1 {
		return nil
	}
	return errors.New("data is not yet replicated")
}

// validateTopology validates the topology.
func validateTopology(ctx context.Context, t *testing.T, clusterInstance *vitesst.Cluster, pingTablets bool, timeToWait time.Duration) {
	t.Helper()
	args := []string{"Validate"}
	if pingTablets {
		args = append(args, "--ping-tablets")
	}
	endTime := time.Now().Add(timeToWait)
	for {
		out, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err == nil {
			return
		}
		require.False(t, time.Now().After(endTime), "time out waiting for validation to pass, output - %s", out)
		time.Sleep(100 * time.Millisecond)
	}
}

// resetPrimaryLogs resets the binary logs on the given tablet.
func resetPrimaryLogs(ctx context.Context, t *testing.T, curPrimary *vitesst.Tablet) {
	t.Helper()
	_, err := runSQL(ctx, "FLUSH BINARY LOGS", curPrimary, "")
	require.NoError(t, err)

	binLogsOutput, err := runSQL(ctx, "SHOW BINARY LOGS", curPrimary, "")
	require.NoError(t, err)
	require.True(t, len(binLogsOutput.Rows) >= 2, "there should be atlease 2 binlog files")

	lastLogFile := binLogsOutput.Rows[len(binLogsOutput.Rows)-1][0].ToString()

	_, err = runSQL(ctx, "PURGE BINARY LOGS TO '"+lastLogFile+"'", curPrimary, "")
	require.NoError(t, err)
}

// enableGlobalRecoveries enables global recoveries for the given VTOrc.
func enableGlobalRecoveries(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc) {
	t.Helper()
	status, resp := makeAPICallRetry(ctx, t, vtorc, "/api/enable-global-recoveries", func(code int, _ string) bool {
		return code != 200
	})
	assert.Equal(t, 200, status)
	assert.Equal(t, "Global recoveries enabled\n", resp)
}

// disableGlobalRecoveries disables global recoveries for the given VTOrc.
func disableGlobalRecoveries(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc) {
	t.Helper()
	status, resp := makeAPICallRetry(ctx, t, vtorc, "/api/disable-global-recoveries", func(code int, _ string) bool {
		return code != 200
	})
	assert.Equal(t, 200, status)
	assert.Equal(t, "Global recoveries disabled\n", resp)
}

// makeAPICallRetry calls a VTOrc API endpoint, retrying while the given
// predicate returns true, and returns the status and body of the first
// response the predicate accepts.
func makeAPICallRetry(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, path string, retry func(int, string) bool) (int, string) {
	t.Helper()
	status, resp, err := vtorc.MakeAPICallRetry(ctx, path, 30*time.Second, func(code int, response string) bool {
		return !retry(code, response)
	})
	require.NoError(t, err)
	return status, resp
}

// waitForSuccessfulRecoveryCount waits until the given recovery name's count of
// successful runs matches the count expected.
func waitForSuccessfulRecoveryCount(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, recoveryName, keyspace, shard string, countExpected int) {
	t.Helper()
	mapKey := fmt.Sprintf("%s.%s.%s", recoveryName, keyspace, shard)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(ctx)
		require.NoError(c, err)
		successfulRecoveriesMap, ok := vars["SuccessfulRecoveries"].(map[string]any)
		require.True(c, ok, "SuccessfulRecoveries metric not yet available")
		assert.EqualValues(c, countExpected, getIntFromValue(successfulRecoveriesMap[mapKey]))
	}, 15*time.Second, time.Second, "timed out waiting for successful recovery count")
}

// waitForSkippedRecoveryCount waits until the given recovery name's count of
// skipped runs matches the count expected or greater.
func waitForSkippedRecoveryCount(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, recoveryName, keyspace, shard string, recoverySkipCode logic.RecoverySkipCode, countExpected int) {
	t.Helper()
	mapKey := fmt.Sprintf("%s.%s.%s.%s", recoveryName, keyspace, shard, recoverySkipCode)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(ctx)
		require.NoError(c, err)
		skippedRecoveriesMap, ok := vars["SkippedRecoveries"].(map[string]any)
		require.True(c, ok, "SkippedRecoveries metric not yet available")
		assert.GreaterOrEqual(c, getIntFromValue(skippedRecoveriesMap[mapKey]), countExpected)
	}, 15*time.Second, time.Second, "timeout waiting for skipped recoveries")
}

// waitForSuccessfulPRSCount waits until the given keyspace-shard's count of
// successful prs runs matches the count expected.
func waitForSuccessfulPRSCount(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, keyspace, shard string, countExpected int) {
	t.Helper()
	mapKey := fmt.Sprintf("%v.%v.success", keyspace, shard)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(ctx)
		require.NoError(c, err)
		prsCountsMap, ok := vars["PlannedReparentCounts"].(map[string]any)
		require.True(c, ok, "PlannedReparentCounts metric not yet available")
		assert.EqualValues(c, countExpected, getIntFromValue(prsCountsMap[mapKey]))
	}, 15*time.Second, time.Second, "timed out waiting for successful PRS count")
}

// waitForSuccessfulERSCount waits until the given keyspace-shard's count of
// successful ers runs matches the count expected.
func waitForSuccessfulERSCount(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, keyspace, shard string, countExpected int) {
	t.Helper()
	mapKey := fmt.Sprintf("%v.%v.success", keyspace, shard)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(ctx)
		require.NoError(c, err)
		ersCountsMap, ok := vars["EmergencyReparentCounts"].(map[string]any)
		require.True(c, ok, "EmergencyReparentCounts metric not yet available")
		assert.EqualValues(c, countExpected, getIntFromValue(ersCountsMap[mapKey]))
	}, 15*time.Second, time.Second, "timed out waiting for successful ERS count")
}

// waitForShardERSDisabledState waits until the keyspace/shard has the given
// ERS-disabled state exposed by VTOrc.
func waitForShardERSDisabledState(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, keyspace, shard string, stateExpected bool) {
	t.Helper()
	var expectedValue int
	if stateExpected {
		expectedValue = 1
	}
	mapKey := fmt.Sprintf("%v.%v", keyspace, shard)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(ctx)
		require.NoError(c, err)
		ersDisabledMap, ok := vars["EmergencyReparentShardDisabled"].(map[string]any)
		require.True(c, ok, "EmergencyReparentShardDisabled metric not yet available")
		assert.EqualValues(c, expectedValue, getIntFromValue(ersDisabledMap[mapKey]))
	}, 15*time.Second, time.Second, "timed out waiting for shard ERS-disabled state")
}

// waitForInstancePollSecondsExceededCount waits to find the minimum occurrence
// or exact count for the DiscoveriesInstancePollSecondsExceeded metric.
func waitForInstancePollSecondsExceededCount(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, minCountExpected int, enforceEquality bool) {
	t.Helper()
	assert.EventuallyWithTf(t, func(c *assert.CollectT) {
		vars, err := vtorc.GetVars(ctx)
		require.NoError(c, err)
		exceeded := getIntFromValue(vars["DiscoveriesInstancePollSecondsExceeded"])
		if enforceEquality {
			ok := assert.EqualValues(
				c, minCountExpected, exceeded,
				"The metric DiscoveriesInstancePollSecondsExceeded should be %v but is %v",
				minCountExpected, exceeded,
			)
			// fail early if we wanted 0 and don't find it
			if minCountExpected == 0 && !ok {
				c.FailNow()
			}
		} else {
			assert.GreaterOrEqual(c, exceeded, minCountExpected)
		}
	}, 30*time.Second, time.Second, "timed out waiting for DiscoveriesInstancePollSecondsExceeded value >= %+v", minCountExpected)
}

// checkVarExists checks whether the given variable exists in /debug/vars.
func checkVarExists(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, name string) {
	t.Helper()
	vars, err := vtorc.GetVars(ctx)
	require.NoError(t, err)
	_, exists := vars[name]
	assert.True(t, exists, vars)
}

// checkMetricExists checks whether the given metric exists in /metrics.
func checkMetricExists(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, name string) {
	t.Helper()
	metrics, err := vtorc.GetMetrics(ctx)
	require.NoError(t, err)
	assert.Contains(t, metrics, name)
}

// getIntFromValue gets an integer from the given value. If it is convertible to
// a float, then we round the number to the nearest integer. If the value is not
// numeric at all, we return 0.
func getIntFromValue(val any) int {
	value := reflect.ValueOf(val)
	if value.CanFloat() {
		return int(math.Round(value.Float()))
	}
	if value.CanInt() {
		return int(value.Int())
	}
	return 0
}

// recoveryDuration returns the duration of the actual recovery execution by
// parsing VTOrc's log for the given analysis code and shard.
func recoveryDuration(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, analysisCode string, keyspace string, shard string) time.Duration {
	t.Helper()

	logs, err := vtorc.Logs(ctx)
	require.NoError(t, err, "failed to read VTOrc logs")

	recoveryPrefix := fmt.Sprintf("Recovery for %s on %s/%s", analysisCode, keyspace, shard)
	startMarker := "proceeding with recovery on"
	endMarker := "Recovery succeeded"

	var startTime, endTime time.Time
	scanner := bufio.NewScanner(strings.NewReader(logs))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, recoveryPrefix) {
			continue
		}

		ts, ok := parseLogTimestamp(line)
		if !ok {
			continue
		}

		if strings.Contains(line, startMarker) {
			startTime = ts
		} else if strings.Contains(line, endMarker) {
			endTime = ts
		}
	}

	require.NoError(t, scanner.Err())
	require.NotZero(t, startTime, "could not find recovery start log line for %s", analysisCode)
	require.NotZero(t, endTime, "could not find recovery succeeded log line for %s", analysisCode)
	require.False(t, endTime.Before(startTime), "recovery end time %v is before start time %v", endTime, startTime)

	return endTime.Sub(startTime)
}

// parseLogTimestamp parses the timestamp from the first three fields of a VTOrc
// text log line. This assumes that the `--log-format` is `text`, which is the
// default for tests.
func parseLogTimestamp(line string) (time.Time, bool) {
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return time.Time{}, false
	}

	t, err := time.Parse("2006-01-02 15:04:05.000 MST", fields[0]+" "+fields[1]+" "+fields[2])
	if err != nil {
		return time.Time{}, false
	}

	return t, true
}
