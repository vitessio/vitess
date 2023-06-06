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
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
)

type Config struct {
	Query     string
	Threshold float64
}

const (
	DefaultQuery     = "select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from _vt.heartbeat"
	DefaultThreshold = 1 * time.Second
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
