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
package throttler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type throttlerConfig struct {
	Query     string
	Threshold float64
}

const (
	defaultQuery     = "select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from _vt.heartbeat"
	defaultThreshold = 5 * time.Second
	configTimeout    = 60 * time.Second
)

var defaultConfig = &throttlerConfig{
	Query:     defaultQuery,
	Threshold: defaultThreshold.Seconds(),
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

	result, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
	return result, err
}

// getThrottlerStatusRaw runs vtctldclient GetThrottlerStatus
func getThrottlerStatusRaw(ctx context.Context, tablet *vitesst.Tablet) (result string, err error) {
	args := []string{}
	args = append(args, "GetThrottlerStatus")
	args = append(args, tablet.Alias())

	result, err = clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, args...)
	return result, err
}

// updateThrottlerTopoConfigRaw runs vtctldclient UpdateThrottlerConfig.
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
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

	ctx, cancel := context.WithTimeout(ctx, configTimeout)
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
			return "", fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", configTimeout, result, err)
		case <-ticker.C:
		}
	}
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
	return &resp, err
}

// getThrottlerStatus runs vtctldclient GetThrottlerStatus.
func getThrottlerStatus(ctx context.Context, tablet *vitesst.Tablet) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	output, err := getThrottlerStatusRaw(ctx, tablet)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return resp.Status, err
}

// updateThrottlerTopoConfig runs vtctldclient UpdateThrottlerConfig on every keyspace.
func updateThrottlerTopoConfig(
	ctx context.Context,
	opts *vtctldatapb.UpdateThrottlerConfigRequest,
	appRule *topodatapb.ThrottledAppRule,
	appCheckedMetrics map[string]*topodatapb.ThrottlerConfig_MetricNames,
) (string, error) {
	rec := concurrency.AllErrorRecorder{}
	var (
		err error
		res strings.Builder
	)
	for _, ks := range clusterInstance.Keyspaces() {
		ires, err := updateThrottlerTopoConfigRaw(ctx, ks.Name, opts, appRule, appCheckedMetrics)
		if err != nil {
			rec.RecordError(err)
		}
		res.WriteString(ires)
	}
	if rec.HasErrors() {
		err = rec.Error()
	}
	return res.String(), err
}

// waitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled and have the provided config (if any) until the specified timeout.
func waitForThrottlerStatusEnabled(ctx context.Context, t *testing.T, tablet *vitesst.Tablet, enabled bool, config *throttlerConfig, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		_, tabletBody, _ := tablet.MakeAPICall(ctx, "/debug/status_details")
		class := strings.ToLower(gjson.Get(tabletBody, "0.Class").String())
		value := strings.ToLower(gjson.Get(tabletBody, "0.Value").String())
		if class == "unhappy" && strings.Contains(value, "not serving") {
			log.Info(fmt.Sprintf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias()))
			return
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

// waitForCheckThrottlerResult waits until the tablet's throttler returns the wanted check result.
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

// getKeyspace executes the vtctldclient command to get a keyspace, and parses the response.
func getKeyspace(ctx context.Context, keyspace string) (*vtctldatapb.Keyspace, error) {
	data, err := clusterInstance.Vtctld().ExecuteCommandWithOutput(ctx, "GetKeyspace", keyspace)
	if err != nil {
		return nil, err
	}

	var ks vtctldatapb.Keyspace
	if err := json2.UnmarshalPB([]byte(data), &ks); err != nil {
		return nil, err
	}
	return &ks, nil
}

// validateReplicationIsHealthy checks that both replication threads are running on the tablet.
func validateReplicationIsHealthy(ctx context.Context, t *testing.T, tablet *vitesst.Tablet) bool {
	query := "show replica status"
	rs, err := tablet.QueryTabletWithDB(ctx, query, "")
	assert.NoError(t, err)
	row := rs.Named().Row()
	require.NotNil(t, row)

	ioRunning := row.AsString("Replica_IO_Running", "")
	require.NotEmpty(t, ioRunning)
	ioHealthy := assert.Equalf(t, "Yes", ioRunning, "Replication is broken. Replication status: %v", row)
	sqlRunning := row.AsString("Replica_SQL_Running", "")
	require.NotEmpty(t, sqlRunning)
	sqlHealthy := assert.Equalf(t, "Yes", sqlRunning, "Replication is broken. Replication status: %v", row)

	return ioHealthy && sqlHealthy
}

// disableVTOrcRecoveries stops all VTOrcs from running any recoveries
func disableVTOrcRecoveries(ctx context.Context, t *testing.T) {
	for _, vtorc := range clusterInstance.VTOrcs() {
		_, _, err := vtorc.MakeAPICallRetry(ctx, "/api/disable-global-recoveries", 10*time.Second, func(status int, body string) bool {
			return status == 200
		})
		require.NoError(t, err)
	}
}

// enableVTOrcRecoveries allows all VTOrcs to run any recoveries
func enableVTOrcRecoveries(ctx context.Context, t *testing.T) {
	for _, vtorc := range clusterInstance.VTOrcs() {
		_, _, err := vtorc.MakeAPICallRetry(ctx, "/api/enable-global-recoveries", 10*time.Second, func(status int, body string) bool {
			return status == 200
		})
		require.NoError(t, err)
	}
}
