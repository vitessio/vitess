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
	"encoding/json"
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
			Scope:               base.SelfScope,
			MultiMetricsEnabled: true,
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

// UpdateThrottlerTopoConfig runs vtctlclient UpdateThrottlerConfig.
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
	if opts.CustomQuery != "" {
		args = append(args, "--check-as-check-self")
	} else {
		args = append(args, "--check-as-check-shard")
	}
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
			return "", fmt.Errorf("appCheckedMetrics must either be nil or have exactly one entry")
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
func CheckThrottler(clusterInstance *cluster.LocalProcessCluster, tablet *cluster.Vttablet, appName throttlerapp.Name, flags *throttle.CheckFlags) (*vtctldatapb.CheckThrottlerResponse, error) {
	output, err := CheckThrottlerRaw(&clusterInstance.VtctldClientProcess, tablet, appName, flags)
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
	if err != nil && strings.HasSuffix(tablet.VttabletProcess.Binary, "-last") {
		// TODO(shlomi): Remove in v22!
		// GetThrottlerStatus gRPC was added in v21. Upgrade-downgrade tests which run a
		// v20 tablet for cross-version compatibility check will fail this command because the
		// tablet server will not serve this gRPC call.
		// We therefore resort to checking the /throttler/status endpoint
		throttlerURL := fmt.Sprintf("http://localhost:%d/throttler/status", tablet.HTTPPort)
		throttlerBody := getHTTPBody(throttlerURL)
		if throttlerBody == "" {
			return nil, fmt.Errorf("failed to get throttler status from %s. Empty result via /status endpoint, and GetThrottlerStatus error: %v", tablet.Alias, err)
		}
		resp := vtctldatapb.GetThrottlerStatusResponse{
			Status: &tabletmanagerdatapb.GetThrottlerStatusResponse{},
		}
		resp.Status.IsEnabled = gjson.Get(throttlerBody, "IsEnabled").Bool()
		resp.Status.LagMetricQuery = gjson.Get(throttlerBody, "Query").String()
		resp.Status.DefaultThreshold = gjson.Get(throttlerBody, "Threshold").Float()
		resp.Status.MetricsHealth = make(map[string]*tabletmanagerdatapb.GetThrottlerStatusResponse_MetricHealth)
		gjson.Get(throttlerBody, "MetricsHealth").ForEach(func(key, value gjson.Result) bool {
			// We just need to know that metrics health is non-empty. We don't need to parse the actual values.
			resp.Status.MetricsHealth[key.String()] = &tabletmanagerdatapb.GetThrottlerStatusResponse_MetricHealth{}
			return true
		})
		return resp.Status, nil
	}
	if err != nil {
		return nil, err
	}
	var resp vtctldatapb.GetThrottlerStatusResponse
	if err := protojson.Unmarshal([]byte(output), &resp); err != nil {
		return nil, err
	}
	return resp.Status, err
}

// UpdateThrottlerTopoConfig runs vtctlclient UpdateThrottlerConfig.
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

// throttleAppRaw runs vtctlclient UpdateThrottlerConfig with --throttle-app flags
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

// ThrottleApp throttles given app name for the next hour
func ThrottleApp(clusterInstance *cluster.LocalProcessCluster, throttlerApp throttlerapp.Name) (string, error) {
	return throttleApp(clusterInstance, throttlerApp, true)
}

// ThrottleApp unthrottles given app name
func UnthrottleApp(clusterInstance *cluster.LocalProcessCluster, throttlerApp throttlerapp.Name) (string, error) {
	return throttleApp(clusterInstance, throttlerApp, false)
}

func WaitUntilTabletsConfirmThrottledApp(t *testing.T, clusterInstance *cluster.LocalProcessCluster, throttlerApp throttlerapp.Name, expectThrottled bool) {
	for _, ks := range clusterInstance.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Vttablets {
				WaitForThrottledApp(t, tablet, throttlerApp, expectThrottled, ConfigTimeout)
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
func WaitForThrottledApp(t *testing.T, tablet *cluster.Vttablet, throttlerApp throttlerapp.Name, expectThrottled bool, timeout time.Duration) {
	throttledAppsURL := fmt.Sprintf("http://localhost:%d/throttler/throttled-apps", tablet.HTTPPort)
	tabletURL := fmt.Sprintf("http://localhost:%d/debug/status_details", tablet.HTTPPort)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		throttledAppsBody := getHTTPBody(throttledAppsURL)
		var throttledApps []base.AppThrottle
		err := json.Unmarshal([]byte(throttledAppsBody), &throttledApps)
		assert.NoError(t, err)
		require.NotEmpty(t, throttledApps) // "always-throttled-app" is always there.
		appFoundThrottled := false
		for _, throttledApp := range throttledApps {
			if throttledApp.AppName == throttlerApp.String() && throttledApp.ExpireAt.After(time.Now()) {
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
			assert.Fail(t, "timeout", "waiting for the %s tablet's throttled apps with the correct config (expecting %s to be %v) after %v; last seen value: %s",
				tablet.Alias, throttlerApp.String(), expectThrottled, timeout, throttledAppsBody)
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

func WaitForCheckThrottlerResult(t *testing.T, clusterInstance *cluster.LocalProcessCluster, tablet *cluster.Vttablet, appName throttlerapp.Name, flags *throttle.CheckFlags, expect int32, timeout time.Duration) (*vtctldatapb.CheckThrottlerResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		resp, err := CheckThrottler(clusterInstance, tablet, appName, flags)
		require.NoError(t, err)
		if resp.Check.StatusCode == expect {
			return resp, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for %s tablet's throttler to return a valid result after %v", tablet.Alias, timeout)
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

// WaitForValidData waits for a tablet's checks to return a non 500 http response
// which indicates that it's not able to provide valid results. This is most
// commonly caused by the throttler still gathering the initial results for
// the given configuration.
func WaitForValidData(t *testing.T, tablet *cluster.Vttablet, timeout time.Duration) {
	checkURL := fmt.Sprintf("http://localhost:%d/throttler/check", tablet.HTTPPort)
	selfCheckURL := fmt.Sprintf("http://localhost:%d/throttler/check-self", tablet.HTTPPort)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		checkResp, checkErr := http.Get(checkURL)
		if checkErr != nil {
			defer checkResp.Body.Close()
		}
		selfCheckResp, selfCheckErr := http.Get(selfCheckURL)
		if selfCheckErr != nil {
			defer selfCheckResp.Body.Close()
		}
		if checkErr == nil && selfCheckErr == nil &&
			checkResp.StatusCode != http.StatusInternalServerError &&
			selfCheckResp.StatusCode != http.StatusInternalServerError {
			return
		}
		select {
		case <-ctx.Done():
			t.Errorf("timed out waiting for %s tablet's throttler to return a valid result after %v; last seen value: %+v",
				tablet.Alias, timeout, checkResp)
			return
		case <-ticker.C:
		}
	}
}
