/*
Copyright 2020 The Vitess Authors.

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
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	primaryTablet   *cluster.Vttablet
	replicaTablet   *cluster.Vttablet
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	sqlSchema       = `
	create table t1(
		id bigint,
		value varchar(16),
		primary key(id)
	) Engine=InnoDB;
`

	vSchema = `
	{
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "t1": {
        "column_vindexes": [
          {
            "column": "id",
            "name": "hash"
          }
        ]
      }
    }
	}`

	httpClient           = base.SetupHTTPClient(time.Second)
	throttledAppsAPIPath = "throttler/throttled-apps"
	checkAPIPath         = "throttler/check"
	checkSelfAPIPath     = "throttler/check-self"
)

const (
	throttlerInitWait            = 30 * time.Second
	accumulateLagWait            = 2 * time.Second
	throttlerRefreshIntervalWait = 20 * time.Second
	replicationCatchUpWait       = 10 * time.Second
	onDemandHeartbeatDuration    = 5 * time.Second
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		err := clusterInstance.StartTopo()
		if err != nil {
			return 1
		}

		// Set extra tablet args for lock timeout
		clusterInstance.VtTabletExtraArgs = []string{
			"--lock_tables_timeout", "5s",
			"--watch_replication_stream",
			"--enable_replication_reporter",
			"--enable-lag-throttler",
			"--throttle_threshold", "1s",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", onDemandHeartbeatDuration.String(),
		}
		// We do not need semiSync for this test case.
		clusterInstance.EnableSemiSync = false

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1
		}

		// Collect table paths and ports
		tablets := clusterInstance.Keyspaces[0].Shards[0].Vttablets
		for _, tablet := range tablets {
			if tablet.Type == "primary" {
				primaryTablet = tablet
			} else if tablet.Type != "rdonly" {
				replicaTablet = tablet
			}
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func throttledApps(tablet *cluster.Vttablet) (resp *http.Response, respBody string, err error) {
	resp, err = httpClient.Get(fmt.Sprintf("http://localhost:%d/%s", tablet.HTTPPort, throttledAppsAPIPath))
	if err != nil {
		return resp, respBody, err
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, respBody, err
	}
	respBody = string(b)
	return resp, respBody, err
}

func throttleCheck(tablet *cluster.Vttablet) (*http.Response, error) {
	return httpClient.Head(fmt.Sprintf("http://localhost:%d/%s", tablet.HTTPPort, checkAPIPath))
}

func throttleCheckSelf(tablet *cluster.Vttablet) (*http.Response, error) {
	return httpClient.Head(fmt.Sprintf("http://localhost:%d/%s", tablet.HTTPPort, checkSelfAPIPath))
}

func TestThrottlerBeforeMetricsCollected(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Immediately after startup, we expect this response:
	// {"StatusCode":404,"Value":0,"Threshold":0,"Message":"No such metric"}
	{
		resp, err := throttleCheck(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	}
}

func warmUpHeartbeat(t *testing.T) (respStatus int) {
	//  because we run with -heartbeat_on_demand_duration=5s, the heartbeat is "cold" right now.
	// Let's warm it up.
	resp, err := throttleCheck(primaryTablet)
	require.NoError(t, err)
	defer resp.Body.Close()
	time.Sleep(time.Second)
	return resp.StatusCode
}

func TestThrottlerAfterMetricsCollected(t *testing.T) {
	defer cluster.PanicHandler(t)

	time.Sleep(throttlerInitWait)
	// By this time metrics will have been collected. We expect no lag, and something like:
	// {"StatusCode":200,"Value":0.282278,"Threshold":1,"Message":""}
	//
	respStatus := warmUpHeartbeat(t)
	assert.NotEqual(t, http.StatusOK, respStatus)
	time.Sleep(time.Second)
	{
		resp, err := throttleCheck(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}
	{
		resp, body, err := throttledApps(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, body, "always-throttled-app")
	}
	{
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}
	{
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}
}

func TestLag(t *testing.T) {
	defer cluster.PanicHandler(t)

	{
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", replicaTablet.Alias)
		assert.NoError(t, err)

		time.Sleep(accumulateLagWait)
		// Lag will have accumulated
		// {"StatusCode":429,"Value":4.864921,"Threshold":1,"Message":"Threshold exceeded"}
		{
			resp, err := throttleCheck(primaryTablet)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		}
		{
			resp, err := throttleCheckSelf(primaryTablet)
			require.NoError(t, err)
			defer resp.Body.Close()
			// self (on primary) is unaffected by replication lag
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		}
		{
			resp, err := throttleCheckSelf(replicaTablet)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		}
	}
	{
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", replicaTablet.Alias)
		assert.NoError(t, err)

		time.Sleep(replicationCatchUpWait)
		// Restore
		// by now heartbeat lease has expired. Let's warm it up
		respStatus := warmUpHeartbeat(t)
		assert.NotEqual(t, http.StatusOK, respStatus)
		time.Sleep(time.Second)
		{
			resp, err := throttleCheck(primaryTablet)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		}
		{
			resp, err := throttleCheckSelf(primaryTablet)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		}
		{
			resp, err := throttleCheckSelf(replicaTablet)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		}
	}
}

func TestNoReplicas(t *testing.T) {
	defer cluster.PanicHandler(t)
	{
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replicaTablet.Alias, "RDONLY")
		assert.NoError(t, err)

		time.Sleep(throttlerRefreshIntervalWait)
		// This makes no REPLICA servers available. We expect something like:
		// {"StatusCode":200,"Value":0,"Threshold":1,"Message":""}
		respStatus := warmUpHeartbeat(t)
		assert.Equal(t, http.StatusOK, respStatus)
		resp, err := throttleCheck(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}
	{
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replicaTablet.Alias, "REPLICA")
		assert.NoError(t, err)

		time.Sleep(throttlerRefreshIntervalWait)
		// Restore valid replica
		respStatus := warmUpHeartbeat(t)
		assert.NotEqual(t, http.StatusOK, respStatus)
		resp, err := throttleCheck(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}
}
