/*
Copyright 2023 The Vitess Authors.

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
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type Config struct {
	Query     string
	Threshold float64
}

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
func CheckThrottlerRaw(vtctldProcess *cluster.VtctldClientProcess, tablet *cluster.Vttablet, appName throttlerapp.Name, flags *throttle.CheckFlags) (result string, err error) {
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
	args = append(args, tablet.Alias)

	result, err = vtctldProcess.ExecuteCommandWithOutput(args...)
	return result, err
}

// GetThrottlerStatusRaw runs vtctldclient GetThrottlerStatus
func GetThrottlerStatusRaw(vtctldProcess *cluster.VtctldClientProcess, tablet *cluster.Vttablet) (result string, err error) {
	args := []string{}
	args = append(args, "GetThrottlerStatus")
	args = append(args, tablet.Alias)

	result, err = vtctldProcess.ExecuteCommandWithOutput(args...)
	return result, err
}

// UpdateThrottlerTopoConfig runs vtctldclient UpdateThrottlerConfig.
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func UpdateThrottlerTopoConfigRaw(
	vtctldProcess *cluster.VtctldClientProcess,
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

	ctx, cancel := context.WithTimeout(context.Background(), ConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = vtctldProcess.ExecuteCommandWithOutput(args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", ConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}

// CheckThrottler runs vtctldclient CheckThrottler.
func CheckThrottler(vtctldProcess *cluster.VtctldClientProcess, tablet *cluster.Vttablet, appName throttlerapp.Name, flags *throttle.CheckFlags) (*vtctldatapb.CheckThrottlerResponse, error) {
	output, err := CheckThrottlerRaw(vtctldProcess, tablet, appName, flags)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.CheckThrottlerResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return &resp, err
}

// GetThrottlerStatus runs vtctldclient CheckThrottler.
func GetThrottlerStatus(vtctldProcess *cluster.VtctldClientProcess, tablet *cluster.Vttablet) (*tabletmanagerdatapb.GetThrottlerStatusResponse, error) {
	output, err := GetThrottlerStatusRaw(vtctldProcess, tablet)
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return resp.Status, err
}

// UpdateThrottlerTopoConfig runs vtctldclient UpdateThrottlerConfig.
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func UpdateThrottlerTopoConfig(
	clusterInstance *cluster.LocalProcessCluster,
	opts *vtctldatapb.UpdateThrottlerConfigRequest,
	appRule *topodatapb.ThrottledAppRule,
	appCheckedMetrics map[string]*topodatapb.ThrottlerConfig_MetricNames,
) (string, error) {
	rec := concurrency.AllErrorRecorder{}
	var (
		err error
		res strings.Builder
	)
	for _, ks := range clusterInstance.Keyspaces {
		ires, err := UpdateThrottlerTopoConfigRaw(&clusterInstance.VtctldClientProcess, ks.Name, opts, appRule, appCheckedMetrics)
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
func WaitForSrvKeyspace(clusterInstance *cluster.LocalProcessCluster, cell, keyspace string) error {
	args := []string{"GetSrvKeyspaceNames", cell}

	ctx, cancel := context.WithTimeout(context.Background(), ConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(args...)
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

// throttleAppRaw runs vtctldclient UpdateThrottlerConfig with --throttle-app flags
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func throttleAppRaw(vtctldProcess *cluster.VtctldClientProcess, keyspaceName string, throttlerApp throttlerapp.Name, throttle bool) (result string, err error) {
	args := []string{}
	args = append(args, "UpdateThrottlerConfig")
	if throttle {
		args = append(args, "--throttle-app", throttlerApp.String())
		args = append(args, "--throttle-app-duration", "1h")
	} else {
		args = append(args, "--unthrottle-app", throttlerApp.String())
	}
	args = append(args, keyspaceName)

	ctx, cancel := context.WithTimeout(context.Background(), ConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = vtctldProcess.ExecuteCommandWithOutput(args...)
		if err == nil {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for UpdateThrottlerConfig to succeed after %v; last seen value: %+v, error: %v", ConfigTimeout, result, err)
		case <-ticker.C:
		}
	}
}

// throttleApp throttles or unthrottles an app
func throttleApp(clusterInstance *cluster.LocalProcessCluster, throttlerApp throttlerapp.Name, throttle bool) (string, error) {
	rec := concurrency.AllErrorRecorder{}
	var (
		err error
		res strings.Builder
	)
	for _, ks := range clusterInstance.Keyspaces {
		ires, err := throttleAppRaw(&clusterInstance.VtctldClientProcess, ks.Name, throttlerApp, throttle)
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
func ThrottleKeyspaceApp(vtctldProcess *cluster.VtctldClientProcess, keyspaceName string, throttlerApp throttlerapp.Name) error {
	_, err := throttleAppRaw(vtctldProcess, keyspaceName, throttlerApp, true)
	return err
}

// ThrottleApp unthrottles given app name
func UnthrottleKeyspaceApp(vtctldProcess *cluster.VtctldClientProcess, keyspaceName string, throttlerApp throttlerapp.Name) error {
	_, err := throttleAppRaw(vtctldProcess, keyspaceName, throttlerApp, false)
	return err
}

func WaitUntilTabletsConfirmThrottledApp(t *testing.T, clusterInstance *cluster.LocalProcessCluster, throttlerApp throttlerapp.Name, expectThrottled bool) {
	for _, ks := range clusterInstance.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Vttablets {
				WaitForThrottledApp(t, &clusterInstance.VtctldClientProcess, tablet, throttlerApp, expectThrottled, ConfigTimeout)
			}
		}
	}
}

// ThrottleAppAndWaitUntilTabletsConfirm
func ThrottleAppAndWaitUntilTabletsConfirm(t *testing.T, clusterInstance *cluster.LocalProcessCluster, throttlerApp throttlerapp.Name) (string, error) {
	res, err := throttleApp(clusterInstance, throttlerApp, true)
	if err != nil {
		return res, err
	}
	WaitUntilTabletsConfirmThrottledApp(t, clusterInstance, throttlerApp, true)
	return res, nil
}

// UnthrottleAppAndWaitUntilTabletsConfirm
func UnthrottleAppAndWaitUntilTabletsConfirm(t *testing.T, clusterInstance *cluster.LocalProcessCluster, throttlerApp throttlerapp.Name) (string, error) {
	res, err := throttleApp(clusterInstance, throttlerApp, false)
	if err != nil {
		return res, err
	}
	WaitUntilTabletsConfirmThrottledApp(t, clusterInstance, throttlerApp, false)
	return res, nil
}

// WaitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled and have the provided config (if any) until the specified timeout.
func WaitForThrottlerStatusEnabled(t *testing.T, vtctldProcess *cluster.VtctldClientProcess, tablet *cluster.Vttablet, enabled bool, config *Config, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	tabletURL := fmt.Sprintf("http://localhost:%d/debug/status_details", tablet.HTTPPort)

	for {
		// If the tablet is Not Serving due to e.g. being involved in a
		// Reshard where its QueryService is explicitly disabled, then
		// we should not fail the test as the throttler will not be Open.
		tabletBody := getHTTPBody(tabletURL)
		class := strings.ToLower(gjson.Get(tabletBody, "0.Class").String())
		value := strings.ToLower(gjson.Get(tabletBody, "0.Value").String())
		if class == "unhappy" && strings.Contains(value, "not serving") {
			log.Infof("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias)
			return
		}

		status, err := GetThrottlerStatus(vtctldProcess, tablet)
		good := func() bool {
			if err != nil {
				log.Errorf("GetThrottlerStatus failed: %v", err)
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
				tablet.Alias, enabled, timeout, status)
			return
		case <-ticker.C:
		}
	}
}

// WaitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled and have the provided config (if any) until the specified timeout.
func WaitForThrottledApp(t *testing.T, vtctldProcess *cluster.VtctldClientProcess, tablet *cluster.Vttablet, throttlerApp throttlerapp.Name, expectThrottled bool, timeout time.Duration) {
	tabletURL := fmt.Sprintf("http://localhost:%d/debug/status_details", tablet.HTTPPort)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		status, err := GetThrottlerStatus(vtctldProcess, tablet)
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
		tabletBody := getHTTPBody(tabletURL)
		class := strings.ToLower(gjson.Get(tabletBody, "0.Class").String())
		value := strings.ToLower(gjson.Get(tabletBody, "0.Value").String())
		if class == "unhappy" && strings.Contains(value, "not serving") {
			log.Infof("tablet %s is Not Serving, so ignoring throttler status as the throttler will not be Opened", tablet.Alias)
			return
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, "timeout", "waiting for the %s tablet's throttled apps with the correct config (expecting %s to be %v) after %v; last seen throttled apps: %+v",
				tablet.Alias, throttlerApp.String(), expectThrottled, timeout, throttledApps)
			return
		case <-ticker.C:
		}
	}
}

// EnableLagThrottlerAndWaitForStatus is a utility function to enable the throttler at the beginning of an endtoend test.
// The throttler is configued to use the standard replication lag metric. The function waits until the throttler is confirmed
// to be running on all tablets.
func EnableLagThrottlerAndWaitForStatus(t *testing.T, clusterInstance *cluster.LocalProcessCluster) {
	req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true}
	_, err := UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
	require.NoError(t, err)

	for _, ks := range clusterInstance.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Vttablets {
				WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, tablet, true, nil, time.Minute)
			}
		}
	}
}

func WaitForCheckThrottlerResult(t *testing.T, vtctldProcess *cluster.VtctldClientProcess, tablet *cluster.Vttablet, appName throttlerapp.Name, flags *throttle.CheckFlags, wantCode tabletmanagerdatapb.CheckThrottlerResponseCode, timeout time.Duration) (*vtctldatapb.CheckThrottlerResponse, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		resp, err := CheckThrottler(vtctldProcess, tablet, appName, flags)
		require.NoError(t, err)
		if resp.Check.ResponseCode == wantCode {
			return resp, true
		}
		select {
		case <-ctx.Done():
			assert.Failf(t, "timeout", "waiting for %s tablet's throttler to return a %v check result after %v; last seen value: %+v", tablet.Alias, wantCode, timeout, resp.Check.ResponseCode)
			return resp, false
		case <-ticker.C:
		}
	}
}

func getHTTPBody(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		log.Infof("http Get returns %+v", err)
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Infof("http Get returns status %d", resp.StatusCode)
		return ""
	}
	respByte, _ := io.ReadAll(resp.Body)
	body := string(respByte)
	return body
}
