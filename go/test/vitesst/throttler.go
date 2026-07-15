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

package vitesst

import (
	"context"
	"errors"
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
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type (
	// ThrottlerConfig is the lag-metric configuration a test expects the
	// tablet throttler to be running with.
	ThrottlerConfig struct {
		Query     string
		Threshold float64
	}

	// TabletThrottler drives and observes the throttler on one tablet. Get one
	// from Tablet.Throttler.
	TabletThrottler struct {
		tablet *Tablet
	}

	// KeyspaceThrottler configures the throttler for one keyspace. Get one from
	// Keyspace.Throttler.
	KeyspaceThrottler struct {
		keyspace *Keyspace
	}

	// ClusterThrottler configures the throttler across every keyspace and waits
	// for every tablet to converge. Get one from Cluster.Throttler.
	ClusterThrottler struct {
		cluster *Cluster
	}
)

const (
	// DefaultThrottlerQuery is the standard replication-lag metric query.
	DefaultThrottlerQuery = "select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from _vt.heartbeat"

	// DefaultThrottlerThreshold is the standard replication-lag threshold.
	DefaultThrottlerThreshold = 5 * time.Second

	// throttlerConfigTimeout bounds the retry loops that wait for a newly
	// created keyspace's SrvKeyspace record to exist before a config applies.
	throttlerConfigTimeout = 60 * time.Second
)

// DefaultThrottlerConfig is the standard replication-lag configuration.
var DefaultThrottlerConfig = &ThrottlerConfig{
	Query:     DefaultThrottlerQuery,
	Threshold: DefaultThrottlerThreshold.Seconds(),
}

// Throttler returns a handle for driving and observing this tablet's throttler.
func (t *Tablet) Throttler() *TabletThrottler {
	return &TabletThrottler{tablet: t}
}

// Throttler returns a handle for configuring this keyspace's throttler.
func (k *Keyspace) Throttler() *KeyspaceThrottler {
	return &KeyspaceThrottler{keyspace: k}
}

// Throttler returns a handle for configuring the throttler across the cluster.
func (c *Cluster) Throttler() *ClusterThrottler {
	return &ClusterThrottler{cluster: c}
}

// CheckRaw runs vtctldclient CheckThrottler and returns its raw output.
func (tt *TabletThrottler) CheckRaw(ctx context.Context, appName throttlerapp.Name, flags *throttle.CheckFlags) (string, error) {
	args := []string{"CheckThrottler"}
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
	args = append(args, tt.tablet.Alias())

	return tt.tablet.cluster.Vtctld().ExecuteCommandWithOutput(ctx, args...)
}

// Check runs vtctldclient CheckThrottler and parses the response.
func (tt *TabletThrottler) Check(ctx context.Context, appName throttlerapp.Name, flags *throttle.CheckFlags) (*vtctldatapb.CheckThrottlerResponse, error) {
	output, err := tt.CheckRaw(ctx, appName, flags)
	if err != nil {
		return nil, err
	}

	var resp vtctldatapb.CheckThrottlerResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, fmt.Errorf("parsing CheckThrottler output %q: %w", output, err)
	}
	return &resp, nil
}

// StatusRaw runs vtctldclient GetThrottlerStatus and returns its raw output.
func (tt *TabletThrottler) StatusRaw(ctx context.Context) (string, error) {
	return tt.tablet.cluster.Vtctld().ExecuteCommandWithOutput(ctx, "GetThrottlerStatus", tt.tablet.Alias())
}

// Status runs vtctldclient GetThrottlerStatus and parses the response.
func (tt *TabletThrottler) Status(ctx context.Context) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	output, err := tt.StatusRaw(ctx)
	if err != nil {
		return nil, err
	}

	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, fmt.Errorf("parsing GetThrottlerStatus output %q: %w", output, err)
	}
	return resp.Status, nil
}

// WaitForStatusEnabled waits for the tablet to report its throttler status as
// enabled/disabled and, when config is given, running with that config.
func (tt *TabletThrottler) WaitForStatusEnabled(ctx context.Context, t *testing.T, enabled bool, config *ThrottlerConfig, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		if tt.notServing(ctx) {
			log.Info(fmt.Sprintf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tt.tablet.Alias()))
			return
		}

		status, err := tt.Status(ctx)
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
				tt.tablet.Alias(), enabled, timeout, status)
			return
		case <-ticker.C:
		}
	}
}

// WaitForApp waits for the tablet to report the given app as
// throttled/unthrottled.
func (tt *TabletThrottler) WaitForApp(ctx context.Context, t *testing.T, app throttlerapp.Name, expectThrottled bool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		status, err := tt.Status(ctx)
		require.NoError(t, err)
		throttledApps := status.ThrottledApps
		require.NotEmpty(t, throttledApps) // "always-throttled-app" is always there.
		appFoundThrottled := false
		for _, throttledApp := range throttledApps {
			expiresAt := protoutil.TimeFromProto(throttledApp.ExpiresAt)
			if throttledApp.Name == app.String() && expiresAt.After(time.Now()) {
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
		if tt.notServing(ctx) {
			log.Info(fmt.Sprintf("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tt.tablet.Alias()))
			return
		}

		select {
		case <-ctx.Done():
			assert.Fail(t, "timeout", "waiting for the %s tablet's throttled apps with the correct config (expecting %s to be %v) after %v; last seen throttled apps: %+v",
				tt.tablet.Alias(), app.String(), expectThrottled, timeout, throttledApps)
			return
		case <-ticker.C:
		}
	}
}

// WaitForCheckResult waits for the tablet's throttler check to return the
// wanted response code.
func (tt *TabletThrottler) WaitForCheckResult(ctx context.Context, t *testing.T, appName throttlerapp.Name, flags *throttle.CheckFlags, wantCode tabletmanagerdatapb.CheckThrottlerResponseCode, timeout time.Duration) (*vtctldatapb.CheckThrottlerResponse, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		resp, err := tt.Check(ctx, appName, flags)
		require.NoError(t, err)
		if resp.Check.ResponseCode == wantCode {
			return resp, true
		}

		select {
		case <-ctx.Done():
			assert.Failf(t, "timeout", "waiting for %s tablet's throttler to return a %v check result after %v; last seen value: %+v",
				tt.tablet.Alias(), wantCode, timeout, resp.Check.ResponseCode)
			return resp, false
		case <-ticker.C:
		}
	}
}

// notServing reports whether the tablet's status page shows it as not serving.
func (tt *TabletThrottler) notServing(ctx context.Context) bool {
	status, body, err := tt.tablet.MakeAPICall(ctx, "/debug/status_details")
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

// UpdateConfig runs vtctldclient UpdateThrottlerConfig for this keyspace.
// It retries until the command succeeds or times out, as the SrvKeyspace record
// may not yet exist for a newly created keyspace that is still initializing
// before it becomes serving.
func (kt *KeyspaceThrottler) UpdateConfig(
	ctx context.Context,
	opts *vtctldatapb.UpdateThrottlerConfigRequest,
	appRule *topodatapb.ThrottledAppRule,
	appCheckedMetrics map[string]*topodatapb.ThrottlerConfig_MetricNames,
) (string, error) {
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
	args = append(args, kt.keyspace.Name)

	return kt.retryVtctldConfig(ctx, args)
}

// ThrottleApp throttles the given app on this keyspace for the next hour.
func (kt *KeyspaceThrottler) ThrottleApp(ctx context.Context, app throttlerapp.Name) error {
	_, err := kt.throttleAppRaw(ctx, app, true)
	return err
}

// UnthrottleApp unthrottles the given app on this keyspace.
func (kt *KeyspaceThrottler) UnthrottleApp(ctx context.Context, app throttlerapp.Name) error {
	_, err := kt.throttleAppRaw(ctx, app, false)
	return err
}

// throttleAppRaw runs vtctldclient UpdateThrottlerConfig with the throttle-app
// flags. It retries like UpdateConfig for the same SrvKeyspace reason.
func (kt *KeyspaceThrottler) throttleAppRaw(ctx context.Context, app throttlerapp.Name, throttle bool) (string, error) {
	args := []string{"UpdateThrottlerConfig"}
	if throttle {
		args = append(args, "--throttle-app", app.String())
		args = append(args, "--throttle-app-duration", "1h")
	} else {
		args = append(args, "--unthrottle-app", app.String())
	}
	args = append(args, kt.keyspace.Name)

	return kt.retryVtctldConfig(ctx, args)
}

// retryVtctldConfig runs a vtctldclient command until it succeeds or times out.
func (kt *KeyspaceThrottler) retryVtctldConfig(ctx context.Context, args []string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, throttlerConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var (
		result string
		err    error
	)
	for {
		result, err = kt.keyspace.cluster.Vtctld().ExecuteCommandWithOutput(ctx, args...)
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

// UpdateConfig runs vtctldclient UpdateThrottlerConfig on every keyspace in the
// cluster, returning the concatenated output.
func (ct *ClusterThrottler) UpdateConfig(
	ctx context.Context,
	opts *vtctldatapb.UpdateThrottlerConfigRequest,
	appRule *topodatapb.ThrottledAppRule,
	appCheckedMetrics map[string]*topodatapb.ThrottlerConfig_MetricNames,
) (string, error) {
	rec := concurrency.AllErrorRecorder{}
	var res strings.Builder
	for _, ks := range ct.cluster.Keyspaces() {
		ires, err := ks.Throttler().UpdateConfig(ctx, opts, appRule, appCheckedMetrics)
		if err != nil {
			rec.RecordError(err)
		}
		res.WriteString(ires)
	}
	if rec.HasErrors() {
		return res.String(), rec.Error()
	}
	return res.String(), nil
}

// ThrottleAppAndWaitUntilTabletsConfirm throttles an app on every keyspace and
// waits until every tablet reports it as throttled.
func (ct *ClusterThrottler) ThrottleAppAndWaitUntilTabletsConfirm(ctx context.Context, t *testing.T, app throttlerapp.Name) (string, error) {
	res, err := ct.throttleApp(ctx, app, true)
	if err != nil {
		return res, err
	}

	ct.WaitUntilTabletsConfirmThrottledApp(ctx, t, app, true)
	return res, nil
}

// UnthrottleAppAndWaitUntilTabletsConfirm unthrottles an app on every keyspace
// and waits until every tablet reports it as no longer throttled.
func (ct *ClusterThrottler) UnthrottleAppAndWaitUntilTabletsConfirm(ctx context.Context, t *testing.T, app throttlerapp.Name) (string, error) {
	res, err := ct.throttleApp(ctx, app, false)
	if err != nil {
		return res, err
	}

	ct.WaitUntilTabletsConfirmThrottledApp(ctx, t, app, false)
	return res, nil
}

// WaitUntilTabletsConfirmThrottledApp waits for every tablet to report the given
// app as throttled/unthrottled.
func (ct *ClusterThrottler) WaitUntilTabletsConfirmThrottledApp(ctx context.Context, t *testing.T, app throttlerapp.Name, expectThrottled bool) {
	for _, tablet := range ct.cluster.Tablets() {
		tablet.Throttler().WaitForApp(ctx, t, app, expectThrottled, throttlerConfigTimeout)
	}
}

// EnableLagAndWaitForStatus enables the replication-lag throttler on every
// keyspace and waits until every tablet confirms it is running.
func (ct *ClusterThrottler) EnableLagAndWaitForStatus(ctx context.Context, t *testing.T) {
	req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true}
	_, err := ct.UpdateConfig(ctx, req, nil, nil)
	require.NoError(t, err)

	for _, tablet := range ct.cluster.Tablets() {
		tablet.Throttler().WaitForStatusEnabled(ctx, t, true, nil, time.Minute)
	}
}

// WaitForSrvKeyspace waits until the given SrvKeyspace entry appears in the cell.
func (ct *ClusterThrottler) WaitForSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	args := []string{"GetSrvKeyspaceNames", cell}

	ctx, cancel := context.WithTimeout(ctx, throttlerConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err := ct.cluster.Vtctld().ExecuteCommandWithOutput(ctx, args...)
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

// throttleApp throttles or unthrottles an app on every keyspace in the cluster.
func (ct *ClusterThrottler) throttleApp(ctx context.Context, app throttlerapp.Name, throttle bool) (string, error) {
	rec := concurrency.AllErrorRecorder{}
	var res strings.Builder
	for _, ks := range ct.cluster.Keyspaces() {
		ires, err := ks.Throttler().throttleAppRaw(ctx, app, throttle)
		if err != nil {
			rec.RecordError(err)
		}
		res.WriteString(ires)
	}
	if rec.HasErrors() {
		return res.String(), rec.Error()
	}
	return res.String(), nil
}
