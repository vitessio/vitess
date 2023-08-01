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

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
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

// UpdateThrottlerTopoConfig runs vtctlclient UpdateThrottlerConfig.
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func UpdateThrottlerTopoConfigRaw(vtctldProcess *cluster.VtctldClientProcess, keyspaceName string, enable bool, disable bool, threshold float64, metricsQuery string) (result string, err error) {
	args := []string{}
	args = append(args, "UpdateThrottlerConfig")
	if enable {
		args = append(args, "--enable")
	}
	if disable {
		args = append(args, "--disable")
	}
	if threshold > 0 {
		args = append(args, "--threshold", fmt.Sprintf("%f", threshold))
	}
	args = append(args, "--custom-query", metricsQuery)
	if metricsQuery != "" {
		args = append(args, "--check-as-check-self")
	} else {
		args = append(args, "--check-as-check-shard")
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

// UpdateThrottlerTopoConfig runs vtctlclient UpdateThrottlerConfig.
// This retries the command until it succeeds or times out as the
// SrvKeyspace record may not yet exist for a newly created
// Keyspace that is still initializing before it becomes serving.
func UpdateThrottlerTopoConfig(clusterInstance *cluster.LocalProcessCluster, enable bool, disable bool, threshold float64, metricsQuery string) (string, error) {
	rec := concurrency.AllErrorRecorder{}
	var (
		err error
		res strings.Builder
	)
	for _, ks := range clusterInstance.Keyspaces {
		ires, err := UpdateThrottlerTopoConfigRaw(&clusterInstance.VtctldClientProcess, ks.Name, enable, disable, threshold, metricsQuery)
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
func WaitForThrottlerStatusEnabled(t *testing.T, tablet *cluster.Vttablet, enabled bool, config *Config, timeout time.Duration) {
	enabledJSONPath := "IsEnabled"
	queryJSONPath := "Query"
	thresholdJSONPath := "Threshold"
	throttlerURL := fmt.Sprintf("http://localhost:%d/throttler/status", tablet.HTTPPort)
	tabletURL := fmt.Sprintf("http://localhost:%d/debug/status_details", tablet.HTTPPort)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		throttlerBody := getHTTPBody(throttlerURL)
		isEnabled := gjson.Get(throttlerBody, enabledJSONPath).Bool()
		if isEnabled == enabled {
			if config == nil {
				return
			}
			query := gjson.Get(throttlerBody, queryJSONPath).String()
			threshold := gjson.Get(throttlerBody, thresholdJSONPath).Float()
			if query == config.Query && threshold == config.Threshold {
				return
			}
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
			t.Errorf("timed out waiting for the %s tablet's throttler status enabled to be %t with the correct config after %v; last seen value: %s",
				tablet.Alias, enabled, timeout, throttlerBody)
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
			t.Errorf("timed out waiting for the %s tablet's throttled apps with the correct config (expecting %s to be %v) after %v; last seen value: %s",
				tablet.Alias, throttlerApp.String(), expectThrottled, timeout, throttledAppsBody)
			return
		case <-ticker.C:
		}
	}
}

// EnableLagThrottlerAndWaitForStatus is a utility function to enable the throttler at the beginning of an endtoend test.
// The throttler is configued to use the standard replication lag metric. The function waits until the throttler is confirmed
// to be running on all tablets.
func EnableLagThrottlerAndWaitForStatus(t *testing.T, clusterInstance *cluster.LocalProcessCluster, lag time.Duration) {
	_, err := UpdateThrottlerTopoConfig(clusterInstance, true, false, lag.Seconds(), "")
	require.NoError(t, err)

	for _, ks := range clusterInstance.Keyspaces {
		for _, shard := range ks.Shards {
			for _, tablet := range shard.Vttablets {
				WaitForThrottlerStatusEnabled(t, tablet, true, nil, time.Minute)
			}
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
