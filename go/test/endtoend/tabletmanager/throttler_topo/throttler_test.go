/*
Copyright 2022 The Vitess Authors.

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
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/throttler"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	customQuery               = "show global status like 'threads_running'"
	customThreshold           = 5 * time.Second
	unreasonablyLowThreshold  = 1 * time.Millisecond
	extremelyHighThreshold    = 1 * time.Hour
	onDemandHeartbeatDuration = 5 * time.Second
	throttlerEnabledTimeout   = 60 * time.Second
	useDefaultQuery           = ""
)

var (
	clusterInstance *cluster.LocalProcessCluster
	primaryTablet   *cluster.Vttablet
	replicaTablet   *cluster.Vttablet
	vtParams        mysql.ConnParams
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
	getResponseBody      = func(resp *http.Response) string {
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}
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
			"--throttler-config-via-topo",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
			"--heartbeat_on_demand_duration", onDemandHeartbeatDuration.String(),
			"--disable_active_reparents",
		}

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

		vtgateInstance := clusterInstance.NewVtgateInstance()
		// Start vtgate
		if err := vtgateInstance.Setup(); err != nil {
			return 1
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		clusterInstance.VtctldClientProcess = *cluster.VtctldClientProcessInstance("localhost", clusterInstance.VtctldProcess.GrpcPort, clusterInstance.TmpDirectory)

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
	resp, err := httpClient.Get(fmt.Sprintf("http://localhost:%d/%s?app=test&s=%t", tablet.HTTPPort, checkAPIPath, skipRequestHeartbeats))
	return resp, err
}

func throttleCheckSelf(tablet *cluster.Vttablet) (*http.Response, error) {
	return httpClient.Get(fmt.Sprintf("http://localhost:%d/%s?app=test", tablet.HTTPPort, checkSelfAPIPath))
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
	ctx, cancel := context.WithTimeout(context.Background(), onDemandHeartbeatDuration*4)
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

			assert.Equalf(t, wantCode, resp.StatusCode, "body: %s", string(b))
			return
		default:
			resp.Body.Close()
			time.Sleep(time.Second)
		}
	}
}

func vtgateExec(t *testing.T, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(query, 1000, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}

func TestInitialThrottler(t *testing.T) {
	defer cluster.PanicHandler(t)

	t.Run("validating OK response from disabled throttler", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
	t.Run("enabling throttler with very low threshold", func(t *testing.T) {
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, true, false, unreasonablyLowThreshold.Seconds(), useDefaultQuery, false)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere with the new config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: unreasonablyLowThreshold.Seconds()}, throttlerEnabledTimeout)
		}
	})
	t.Run("validating pushback response from throttler", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
	t.Run("disabling throttler", func(t *testing.T) {
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, false, true, unreasonablyLowThreshold.Seconds(), useDefaultQuery, false)
		assert.NoError(t, err)

		// Wait for the throttler to be disabled everywhere.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, tablet, false, nil, throttlerEnabledTimeout)
		}
	})
	t.Run("validating OK response from disabled throttler, again", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
	t.Run("enabling throttler, again", func(t *testing.T) {
		// Enable the throttler again with the default query which also moves us back
		// to the default threshold.
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, true, false, 0, useDefaultQuery, true)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere again with the default config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, tablet, true, throttler.DefaultConfig, throttlerEnabledTimeout)
		}
	})
	t.Run("validating pushback response from throttler, again", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
	t.Run("setting high threshold", func(t *testing.T) {
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, false, false, extremelyHighThreshold.Seconds(), useDefaultQuery, true)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: extremelyHighThreshold.Seconds()}, throttlerEnabledTimeout)
		}
	})
	t.Run("validating OK response from throttler with high threshold", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
	t.Run("setting low threshold", func(t *testing.T) {
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, false, false, throttler.DefaultThreshold.Seconds(), useDefaultQuery, true)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, tablet, true, throttler.DefaultConfig, throttlerEnabledTimeout)
		}
	})
	t.Run("validating pushback response from throttler on low threshold", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
	t.Run("requesting heartbeats", func(t *testing.T) {
		respStatus := warmUpHeartbeat(t)
		assert.NotEqual(t, http.StatusOK, respStatus)
	})
	t.Run("validating OK response from throttler with low threshold, heartbeats running", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
	t.Run("validating OK response from throttler with low threshold, heartbeats running still", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
	t.Run("validating pushback response from throttler on low threshold once heartbeats go stale", func(t *testing.T) {
		time.Sleep(2 * onDemandHeartbeatDuration) // just... really wait long enough, make sure on-demand stops
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
}

func TestThrottlerAfterMetricsCollected(t *testing.T) {
	defer cluster.PanicHandler(t)

	// By this time metrics will have been collected. We expect no lag, and something like:
	// {"StatusCode":200,"Value":0.282278,"Threshold":1,"Message":""}
	t.Run("validating throttler OK", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
	t.Run("validating throttled apps", func(t *testing.T) {
		resp, body, err := throttledApps(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
		assert.Contains(t, body, "always-throttled-app")
	})
	t.Run("validating primary check self", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
	t.Run("validating replica check self", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
}

func TestLag(t *testing.T) {
	defer cluster.PanicHandler(t)
	// Temporarily disable VTOrc recoveries because we want to
	// STOP replication specifically in order to increase the
	// lag and we DO NOT want VTOrc to try and fix this.
	clusterInstance.DisableVTOrcRecoveries(t)
	defer clusterInstance.EnableVTOrcRecoveries(t)

	t.Run("stopping replication", func(t *testing.T) {
		err := clusterInstance.VtctlclientProcess.ExecuteCommand("StopReplication", replicaTablet.Alias)
		assert.NoError(t, err)
	})
	t.Run("accumulating lag, expecting throttler push back", func(t *testing.T) {
		time.Sleep(2 * throttler.DefaultThreshold)

		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusTooManyRequests, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
	t.Run("primary self-check should still be fine", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		// self (on primary) is unaffected by replication lag
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
	t.Run("replica self-check should show error", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusTooManyRequests, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
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
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
	t.Run("replica self-check should be fine", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
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

func TestCustomQuery(t *testing.T) {
	defer cluster.PanicHandler(t)

	t.Run("enabling throttler with custom query and threshold", func(t *testing.T) {
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, true, false, customThreshold.Seconds(), customQuery, false)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere with new custom config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, tablet, true, &throttler.Config{Query: customQuery, Threshold: customThreshold.Seconds()}, throttlerEnabledTimeout)
		}
	})
	t.Run("validating OK response from throttler with custom query", func(t *testing.T) {
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
	t.Run("test threads running", func(t *testing.T) {
		sleepDuration := 20 * time.Second
		var wg sync.WaitGroup
		for i := 0; i < int(customThreshold.Seconds()); i++ {
			// Generate different Sleep() calls, all at minimum sleepDuration.
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				vtgateExec(t, fmt.Sprintf("select sleep(%d)", int(sleepDuration.Seconds())+i), "")
			}(i)
		}
		t.Run("exceeds threshold", func(t *testing.T) {
			throttler.WaitForQueryResult(t, primaryTablet,
				"select if(variable_value > 5, 'true', 'false') as result from performance_schema.global_status where variable_name='threads_running'",
				"true", sleepDuration/3)
			throttler.WaitForValidData(t, primaryTablet, sleepDuration-(5*time.Second))
			// Now we should be reporting ~ customThreshold*2 threads_running, and we should
			// hit the threshold. For example:
			// {"StatusCode":429,"Value":6,"Threshold":5,"Message":"Threshold exceeded"}
			{
				resp, err := throttleCheck(primaryTablet, false)
				require.NoError(t, err)
				defer resp.Body.Close()
				assert.Equalf(t, http.StatusTooManyRequests, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
			}
			{
				resp, err := throttleCheckSelf(primaryTablet)
				require.NoError(t, err)
				defer resp.Body.Close()
				assert.Equalf(t, http.StatusTooManyRequests, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
			}
		})
		t.Run("wait for queries to terminate", func(t *testing.T) {
			wg.Wait()
			time.Sleep(1 * time.Second) // graceful time to let throttler read metrics
		})
		t.Run("restored below threshold", func(t *testing.T) {
			{
				resp, err := throttleCheck(primaryTablet, false)
				require.NoError(t, err)
				defer resp.Body.Close()
				assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
			}
			{
				resp, err := throttleCheckSelf(primaryTablet)
				require.NoError(t, err)
				defer resp.Body.Close()
				assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
			}
		})
	})
}

func TestRestoreDefaultQuery(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Validate going back from custom-query to default-query (replication lag) still works.
	t.Run("enabling throttler with default query and threshold", func(t *testing.T) {
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, true, false, throttler.DefaultThreshold.Seconds(), useDefaultQuery, false)
		assert.NoError(t, err)

		// Wait for the throttler to be up and running everywhere again with the default config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, tablet, true, throttler.DefaultConfig, throttlerEnabledTimeout)
		}
	})
	t.Run("validating OK response from throttler with default threshold, heartbeats running", func(t *testing.T) {
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
	})
	t.Run("validating pushback response from throttler on default threshold once heartbeats go stale", func(t *testing.T) {
		time.Sleep(2 * onDemandHeartbeatDuration) // just... really wait long enough, make sure on-demand stops
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
}
