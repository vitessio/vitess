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
	"context"
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
	throttlerThreshold        = 1 * time.Second // standard, tight threshold
	onDemandHeartbeatDuration = 5 * time.Second
	applyConfigWait           = 15 * time.Second // time after which we're sure the throttler has refreshed config and tablets
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
			"--throttle_threshold", throttlerThreshold.String(),
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", onDemandHeartbeatDuration.String(),
			"--disable_active_reparents",
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

func throttleCheck(tablet *cluster.Vttablet, skipRequestHeartbeats bool) (*http.Response, error) {
	return httpClient.Get(fmt.Sprintf("http://localhost:%d/%s?s=%t", tablet.HTTPPort, checkAPIPath, skipRequestHeartbeats))
}

func throttleCheckSelf(tablet *cluster.Vttablet) (*http.Response, error) {
	return httpClient.Head(fmt.Sprintf("http://localhost:%d/%s", tablet.HTTPPort, checkSelfAPIPath))
}

func warmUpHeartbeat(t *testing.T) (respStatus int) {
	//  because we run with -heartbeat_on_demand_duration=5s, the heartbeat is "cold" right now.
	// Let's warm it up.
	resp, err := throttleCheck(primaryTablet, false)
	require.NoError(t, err)
	defer resp.Body.Close()
	time.Sleep(time.Second)
	return resp.StatusCode
}

// waitForThrottleCheckStatus waits for the tablet to return the provided HTTP code in a throttle check
func waitForThrottleCheckStatus(t *testing.T, tablet *cluster.Vttablet, wantCode int) {
	_ = warmUpHeartbeat(t)
	ctx, cancel := context.WithTimeout(context.Background(), onDemandHeartbeatDuration+applyConfigWait)
	defer cancel()

	for {
		resp, err := throttleCheck(tablet, true)
		require.NoError(t, err)

		if wantCode == resp.StatusCode {
			// Wait for any cached check values to be cleared and the new
			// status value to be in effect everywhere before returning.
			resp.Body.Close()
			return
		}
		select {
		case <-ctx.Done():
			b, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			resp.Body.Close()

			assert.Equal(t, wantCode, resp.StatusCode, "body: %v", string(b))
			return
		default:
			resp.Body.Close()
			time.Sleep(time.Second)
		}
	}
}

func TestThrottlerAfterMetricsCollected(t *testing.T) {
	defer cluster.PanicHandler(t)

	// We run with on-demand heartbeats. Immediately as the tablet manager opens, it sends a one-time
	// request for heartbeats, which means the throttler is able to collect initial "good" data.
	// After a few seconds, the heartbeat lease terminates. We wait for that.
	// {"StatusCode":429,"Value":4.864921,"Threshold":1,"Message":"Threshold exceeded"}
	t.Run("expect push back once initial heartbeat lease terminates", func(t *testing.T) {
		time.Sleep(onDemandHeartbeatDuration)
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
	t.Run("requesting heartbeats", func(t *testing.T) {
		respStatus := warmUpHeartbeat(t)
		assert.NotEqual(t, http.StatusOK, respStatus)
	})
	t.Run("expect OK once heartbeats lease renewed", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("expect OK once heartbeats lease renewed, still", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("validate throttled-apps", func(t *testing.T) {
		resp, body, err := throttledApps(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, body, "always-throttled-app")
	})
	t.Run("validate check-self", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("validate check-self, again", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestLag(t *testing.T) {
	defer cluster.PanicHandler(t)

	t.Run("stopping replication", func(t *testing.T) {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", replicaTablet.Alias)
		assert.NoError(t, err)
	})
	t.Run("accumulating lag, expecting throttler push back", func(t *testing.T) {
		time.Sleep(2 * throttlerThreshold)

		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	})
	t.Run("primary self-check should still be fine", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		// self (on primary) is unaffected by replication lag
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("replica self-check should show error", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	})
	t.Run("starting replication", func(t *testing.T) {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("StartReplication", replicaTablet.Alias)
		assert.NoError(t, err)
	})
	t.Run("expecting replication to catch up and throttler check to return OK", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
	t.Run("primary self-check should be fine", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		// self (on primary) is unaffected by replication lag
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("replica self-check should be fine", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestNoReplicas(t *testing.T) {
	defer cluster.PanicHandler(t)
	t.Run("changing replica to RDONLY", func(t *testing.T) {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replicaTablet.Alias, "RDONLY")
		assert.NoError(t, err)

		// This makes no REPLICA servers available. We expect something like:
		// {"StatusCode":200,"Value":0,"Threshold":1,"Message":""}
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
	t.Run("restoring to REPLICA", func(t *testing.T) {

		err := clusterInstance.VtctlclientProcess.ExecuteCommand("ChangeTabletType", replicaTablet.Alias, "REPLICA")
		assert.NoError(t, err)

		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
}
