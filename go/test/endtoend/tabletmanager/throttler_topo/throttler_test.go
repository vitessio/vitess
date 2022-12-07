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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	customQuery          = "show global status like 'threads_running'"
	customThreshold      = 5
)

const (
	throttlerThreshold        = 1 * time.Second // standard, tight threshold
	unreasonablyLowThreshold  = 1 * time.Millisecond
	extremelyHighThreshold    = 1 * time.Hour
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
			"--throttler-config-via-topo",
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

// updateThrottlerConfig runs vtctlclient UpdateThrottlerConfig
func updateThrottlerConfig(enable bool, disable bool, threshold float64, metricsQuery string, viaVtctldClient bool) (result string, err error) {
	args := []string{}
	if !viaVtctldClient {
		args = append(args, "--")
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
	if metricsQuery != "" {
		args = append(args, "--custom-query", metricsQuery)
		args = append(args, "--check-as-check-self")
	} else {
		args = append(args, "--check-as-check-shard")
	}
	args = append(args, keyspaceName)
	if viaVtctldClient {
		return clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput(args...)
	}
	return clusterInstance.VtctlclientProcess.ExecuteCommandWithOutput(args...)
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
	resp, err := httpClient.Get(fmt.Sprintf("http://localhost:%d/%s?s=%t", tablet.HTTPPort, checkAPIPath, skipRequestHeartbeats))
	return resp, err
}

func throttleCheckSelf(tablet *cluster.Vttablet) (*http.Response, error) {
	return httpClient.Get(fmt.Sprintf("http://localhost:%d/%s", tablet.HTTPPort, checkSelfAPIPath))
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
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("enabling throttler with low threshold", func(t *testing.T) {
		_, err := updateThrottlerConfig(true, false, unreasonablyLowThreshold.Seconds(), "", false)
		assert.NoError(t, err)
	})
	t.Run("validating pushback response from throttler", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
	t.Run("disabling throttler", func(t *testing.T) {
		_, err := updateThrottlerConfig(false, true, unreasonablyLowThreshold.Seconds(), "", false)
		assert.NoError(t, err)
	})
	t.Run("validating OK response from disabled throttler, again", func(t *testing.T) {
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("enabling throttler, again", func(t *testing.T) {
		_, err := updateThrottlerConfig(true, false, 0, "", true)
		assert.NoError(t, err)
	})
	t.Run("validating pushback response from throttler, again", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
	t.Run("setting high threshold", func(t *testing.T) {
		_, err := updateThrottlerConfig(false, false, extremelyHighThreshold.Seconds(), "", true)
		assert.NoError(t, err)
	})
	t.Run("validating OK response from throttler with high threshold", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
	t.Run("setting low threshold", func(t *testing.T) {
		_, err := updateThrottlerConfig(false, false, throttlerThreshold.Seconds(), "", true)
		assert.NoError(t, err)
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
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("validating OK response from throttler with low threshold, heartbeats running still", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("validating pushback response from throttler on low threshold once heartbeats go stale", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusTooManyRequests)
	})
}

func TestThrottlerAfterMetricsCollected(t *testing.T) {
	defer cluster.PanicHandler(t)

	// By this time metrics will have been collected. We expect no lag, and something like:
	// {"StatusCode":200,"Value":0.282278,"Threshold":1,"Message":""}
	//
	t.Run("validating throttler OK", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, http.StatusOK)
	})
	t.Run("validating throttled apps", func(t *testing.T) {
		resp, body, err := throttledApps(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, body, "always-throttled-app")
	})
	t.Run("validating primary check self", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("validating replica check self", func(t *testing.T) {
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

func TestCustomQuery(t *testing.T) {
	defer cluster.PanicHandler(t)

	t.Run("enabling throttler with low threshold", func(t *testing.T) {
		_, err := updateThrottlerConfig(true, false, float64(customThreshold), customQuery, false)
		assert.NoError(t, err)
		time.Sleep(applyConfigWait)
	})
	t.Run("validating OK response from throttler with custom query", func(t *testing.T) {
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()

		b, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode, "response: %v", string(b))
	})
	t.Run("test threads running", func(t *testing.T) {
		sleepDuration := 10 * time.Second
		var wg sync.WaitGroup
		for i := 0; i < customThreshold; i++ {
			// generate different Sleep() calls, all at minimum sleepDuration
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				vtgateExec(t, fmt.Sprintf("select sleep(%d)", int(sleepDuration.Seconds())+i), "")
			}(i)
		}
		t.Run("exceeds threshold", func(t *testing.T) {
			time.Sleep(sleepDuration / 2)
			// by this time we will have testThreshold+1 threads_running, and we should hit the threshold
			// {"StatusCode":429,"Value":2,"Threshold":2,"Message":"Threshold exceeded"}
			{
				resp, err := throttleCheck(primaryTablet, false)
				require.NoError(t, err)
				defer resp.Body.Close()

				b, err := io.ReadAll(resp.Body)
				assert.NoError(t, err)
				assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode, "response: %v", string(b))
			}
			{
				resp, err := throttleCheckSelf(primaryTablet)
				require.NoError(t, err)
				defer resp.Body.Close()

				b, err := io.ReadAll(resp.Body)
				assert.NoError(t, err)
				assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode, "response: %v", string(b))
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
				assert.Equal(t, http.StatusOK, resp.StatusCode)
			}
			{
				resp, err := throttleCheckSelf(primaryTablet)
				require.NoError(t, err)
				defer resp.Body.Close()
				assert.Equal(t, http.StatusOK, resp.StatusCode)
			}
		})
	})
}

func TestRestoreDefaultQuery(t *testing.T) {
	// validte going back from custom-query to default-query (replication lag) still works
	defer cluster.PanicHandler(t)

	t.Run("enabling throttler with standard threshold", func(t *testing.T) {
		_, err := updateThrottlerConfig(true, false, throttlerThreshold.Seconds(), "", false)
		assert.NoError(t, err)
	})
	t.Run("requesting heartbeats", func(t *testing.T) {
		_ = warmUpHeartbeat(t)
	})
	t.Run("validating OK response from throttler with low threshold, heartbeats running", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
	t.Run("validating pushback response from throttler on low threshold once heartbeats go stale", func(t *testing.T) {
		time.Sleep(2 * onDemandHeartbeatDuration) // just... really wait long enough, make sure on-demand stops
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	})
}
