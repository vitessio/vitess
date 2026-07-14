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
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/test/vitesst"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type (
	Config struct {
		Query     string
		Threshold float64
	}
)

const (
	DefaultQuery     = "select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from _vt.heartbeat"
	DefaultThreshold = 5 * time.Second
	ConfigTimeout    = 60 * time.Second
)

var DefaultConfig = &Config{
	Query:     DefaultQuery,
	Threshold: DefaultThreshold.Seconds(),
}

// CheckThrottlerRaw runs vtctldclient CheckThrottler
func CheckThrottlerRaw(ctx context.Context, c *vitesst.Cluster, tablet *vitesst.Tablet, appName throttlerapp.Name, flags *throttle.CheckFlags) (result string, err error) {
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

	result, err = c.Vtctld().ExecuteCommandWithOutput(ctx, args...)
	return result, err
}

// GetThrottlerStatusRaw runs vtctldclient GetThrottlerStatus
func GetThrottlerStatusRaw(ctx context.Context, c *vitesst.Cluster, tablet *vitesst.Tablet) (result string, err error) {
	args := []string{}
	args = append(args, "GetThrottlerStatus")
	args = append(args, tablet.Alias())

	result, err = c.Vtctld().ExecuteCommandWithOutput(ctx, args...)
	return result, err
}

// UpdateThrottlerTopoConfigRaw runs vtctldclient UpdateThrottlerConfig.
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func UpdateThrottlerTopoConfigRaw(
	ctx context.Context,
	c *vitesst.Cluster,
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
			return "", vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "appCheckedMetrics must either be nil or have exactly one entry")
		}
		for app, metrics := range appCheckedMetrics {
			args = append(args, "--app-name", app)
			args = append(args, "--app-metrics", strings.Join(metrics.Names, ","))
		}
	}
	args = append(args, keyspaceName)

	ctx, cancel := context.WithTimeout(ctx, ConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = c.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED,
				"timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", ConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}

// CheckThrottler runs vtctldclient CheckThrottler.
func CheckThrottler(ctx context.Context, c *vitesst.Cluster, tablet *vitesst.Tablet, appName throttlerapp.Name, flags *throttle.CheckFlags) (*vtctldatapb.CheckThrottlerResponse, error) {
	output, err := CheckThrottlerRaw(ctx, c, tablet, appName, flags)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.CheckThrottlerResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, vterrors.Wrapf(err, "parsing CheckThrottler output %q", output)
	}
	return &resp, nil
}

// GetThrottlerStatus runs vtctldclient GetThrottlerStatus.
func GetThrottlerStatus(ctx context.Context, c *vitesst.Cluster, tablet *vitesst.Tablet) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	output, err := GetThrottlerStatusRaw(ctx, c, tablet)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, vterrors.Wrapf(err, "parsing GetThrottlerStatus output %q", output)
	}
	return resp.Status, nil
}

// UpdateThrottlerTopoConfig runs vtctldclient UpdateThrottlerConfig on every
// keyspace in the cluster.
func UpdateThrottlerTopoConfig(
	ctx context.Context,
	c *vitesst.Cluster,
	opts *vtctldatapb.UpdateThrottlerConfigRequest,
	appRule *topodatapb.ThrottledAppRule,
	appCheckedMetrics map[string]*topodatapb.ThrottlerConfig_MetricNames,
) (string, error) {
	rec := concurrency.AllErrorRecorder{}
	var (
		err error
		res strings.Builder
	)
	for _, ks := range c.Keyspaces() {
		ires, err := UpdateThrottlerTopoConfigRaw(ctx, c, ks.Name, opts, appRule, appCheckedMetrics)
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

// WaitForSrvKeyspace waits until the given srvkeyspace entry is found in the given cell
func WaitForSrvKeyspace(ctx context.Context, c *vitesst.Cluster, cell, keyspace string) error {
	args := []string{"GetSrvKeyspaceNames", cell}

	ctx, cancel := context.WithTimeout(ctx, ConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err := c.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err != nil {
			return err
		}
		if strings.Contains(result, `"`+keyspace+`"`) {
			return nil
		}
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out waiting for GetSrvKeyspaceNames to contain '%v'", keyspace)
		case <-ticker.C:
		}
	}
}

// throttleAppRaw runs vtctldclient UpdateThrottlerConfig with --throttle-app flags
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func throttleAppRaw(ctx context.Context, c *vitesst.Cluster, keyspaceName string, throttlerApp throttlerapp.Name, throttle bool) (result string, err error) {
	args := []string{}
	args = append(args, "UpdateThrottlerConfig")
	if throttle {
		args = append(args, "--throttle-app", throttlerApp.String())
		args = append(args, "--throttle-app-duration", "1h")
	} else {
		args = append(args, "--unthrottle-app", throttlerApp.String())
	}
	args = append(args, keyspaceName)

	ctx, cancel := context.WithTimeout(ctx, ConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = c.Vtctld().ExecuteCommandWithOutput(ctx, args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED,
				"timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", ConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}

// throttleApp throttles or unthrottles an app
func throttleApp(ctx context.Context, c *vitesst.Cluster, throttlerApp throttlerapp.Name, throttle bool) (string, error) {
	rec := concurrency.AllErrorRecorder{}
	var (
		err error
		res strings.Builder
	)
	for _, ks := range c.Keyspaces() {
		ires, err := throttleAppRaw(ctx, c, ks.Name, throttlerApp, throttle)
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

// ThrottleKeyspaceApp throttles given app name for the next hour
func ThrottleKeyspaceApp(ctx context.Context, c *vitesst.Cluster, keyspaceName string, throttlerApp throttlerapp.Name) error {
	_, err := throttleAppRaw(ctx, c, keyspaceName, throttlerApp, true)
	return err
}

// UnthrottleKeyspaceApp unthrottles given app name
func UnthrottleKeyspaceApp(ctx context.Context, c *vitesst.Cluster, keyspaceName string, throttlerApp throttlerapp.Name) error {
	_, err := throttleAppRaw(ctx, c, keyspaceName, throttlerApp, false)
	return err
}

func WaitUntilTabletsConfirmThrottledApp(ctx context.Context, t *testing.T, c *vitesst.Cluster, throttlerApp throttlerapp.Name, expectThrottled bool) {
	for _, tablet := range c.Tablets() {
		WaitForThrottledApp(ctx, t, c, tablet, throttlerApp, expectThrottled, ConfigTimeout)
	}
}

// ThrottleAppAndWaitUntilTabletsConfirm throttles an app and waits until every
// tablet reports it as throttled.
func ThrottleAppAndWaitUntilTabletsConfirm(ctx context.Context, t *testing.T, c *vitesst.Cluster, throttlerApp throttlerapp.Name) (string, error) {
	res, err := throttleApp(ctx, c, throttlerApp, true)
	if err != nil {
		return res, err
	}
	WaitUntilTabletsConfirmThrottledApp(ctx, t, c, throttlerApp, true)
	return res, nil
}

// UnthrottleAppAndWaitUntilTabletsConfirm unthrottles an app and waits until
// every tablet reports it as no longer throttled.
func UnthrottleAppAndWaitUntilTabletsConfirm(ctx context.Context, t *testing.T, c *vitesst.Cluster, throttlerApp throttlerapp.Name) (string, error) {
	res, err := throttleApp(ctx, c, throttlerApp, false)
	if err != nil {
		return res, err
	}
	WaitUntilTabletsConfirmThrottledApp(ctx, t, c, throttlerApp, false)
	return res, nil
}

// WaitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled and have the provided config (if any) until the specified timeout.
func WaitForThrottlerStatusEnabled(ctx context.Context, t *testing.T, c *vitesst.Cluster, tablet *vitesst.Tablet, enabled bool, config *Config, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		if notServing(ctx, tablet) {
			log.Info(fmt.Sprintf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias()))
			return
		}

		status, err := GetThrottlerStatus(ctx, c, tablet)
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

// WaitForThrottledApp waits for a tablet to report the given app as
// throttled/unthrottled until the specified timeout.
func WaitForThrottledApp(ctx context.Context, t *testing.T, c *vitesst.Cluster, tablet *vitesst.Tablet, throttlerApp throttlerapp.Name, expectThrottled bool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		status, err := GetThrottlerStatus(ctx, c, tablet)
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
		if notServing(ctx, tablet) {
			log.Info(fmt.Sprintf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias()))
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

// EnableLagThrottlerAndWaitForStatus is a utility function to enable the throttler at the beginning of an endtoend test.
// The throttler is configued to use the standard replication lag metric. The function waits until the throttler is confirmed
// to be running on all tablets.
func EnableLagThrottlerAndWaitForStatus(ctx context.Context, t *testing.T, c *vitesst.Cluster) {
	req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true}
	_, err := UpdateThrottlerTopoConfig(ctx, c, req, nil, nil)
	require.NoError(t, err)

	for _, tablet := range c.Tablets() {
		WaitForThrottlerStatusEnabled(ctx, t, c, tablet, true, nil, time.Minute)
	}
}

func WaitForCheckThrottlerResult(ctx context.Context, t *testing.T, c *vitesst.Cluster, tablet *vitesst.Tablet, appName throttlerapp.Name, flags *throttle.CheckFlags, wantCode tabletmanagerdatapb.CheckThrottlerResponseCode, timeout time.Duration) (*vtctldatapb.CheckThrottlerResponse, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		resp, err := CheckThrottler(ctx, c, tablet, appName, flags)
		require.NoError(t, err)
		if resp.Check.ResponseCode == wantCode {
			return resp, true
		}
		select {
		case <-ctx.Done():
			assert.Failf(t, "timeout", "waiting for %s tablet's throttler to return a %v check result after %v; last seen value: %+v",
				tablet.Alias(), wantCode, timeout, resp.Check.ResponseCode)
			return resp, false
		case <-ticker.C:
		}
	}
}

// notServing reports whether the tablet's status page shows it as not serving.
func notServing(ctx context.Context, tablet *vitesst.Tablet) bool {
	status, body, err := tablet.MakeAPICall(ctx, "/debug/status_details")
	if err != nil {
		log.Info(fmt.Sprintf("status_details returns %+v", err))
		return false
	}
	if status != http.StatusOK {
		log.Info(fmt.Sprintf("status_details returns status %d", status))
		return false
	}

	class := strings.ToLower(gjson.Get(body, "0.Class").String())
	value := strings.ToLower(gjson.Get(body, "0.Value").String())
	return class == "unhappy" && strings.Contains(value, "not serving")
}
