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
	"testing"
	"time"

	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
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
func UpdateThrottlerTopoConfig(clusterInstance *cluster.LocalProcessCluster, enable bool, disable bool, threshold float64, metricsQuery string, viaVtctldClient bool) (result string, err error) {
	args := []string{}
	clientfunc := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput
	if !viaVtctldClient {
		args = append(args, "--")
		clientfunc = clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput
	}
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
	args = append(args, clusterInstance.Keyspaces[0].Name)

	ctx, cancel := context.WithTimeout(context.Background(), ConfigTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		result, err = clientfunc(args...)
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

// WaitForThrottlerStatusEnabled waits for a tablet to report its throttler status as
// enabled/disabled and have the provided config (if any) until the specified timeout.
func WaitForThrottlerStatusEnabled(t *testing.T, tablet *cluster.Vttablet, enabled bool, config *Config, timeout time.Duration) {
	enabledJSONPath := "IsEnabled"
	queryJSONPath := "Query"
	thresholdJSONPath := "Threshold"
	url := fmt.Sprintf("http://localhost:%d/throttler/status", tablet.HTTPPort)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		body := getHTTPBody(url)
		isEnabled, err := jsonparser.GetBoolean([]byte(body), enabledJSONPath)
		require.NoError(t, err)
		if isEnabled == enabled {
			if config == nil {
				return
			}
			query, err := jsonparser.GetString([]byte(body), queryJSONPath)
			require.NoError(t, err)
			threshold, err := jsonparser.GetFloat([]byte(body), thresholdJSONPath)
			require.NoError(t, err)
			if query == config.Query && threshold == config.Threshold {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Errorf("timed out waiting for the %s tablet's throttler status enabled to be %t with the correct config after %v; last seen value: %s",
				tablet.Alias, enabled, timeout, body)
			return
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

// WaitForQueryResult waits for a tablet to return the given result for the given
// query until the specified timeout.
// This is for simple queries that return 1 column in 1 row. It compares the result
// for that column as a string with the provided result.
func WaitForQueryResult(t *testing.T, tablet *cluster.Vttablet, query, result string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		res, err := tablet.VttabletProcess.QueryTablet(query, "", false)
		require.NoError(t, err)
		if res != nil && len(res.Rows) == 1 && res.Rows[0][0].ToString() == result {
			return
		}
		select {
		case <-ctx.Done():
			t.Errorf("timed out waiting for the %q query to produce a result of %q on tablet %s after %v; last seen value: %s",
				query, result, tablet.Alias, timeout, res.Rows[0][0].ToString())
			return
		case <-ticker.C:
		}
	}
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
