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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/throttler"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	customQuery               = "show global status like 'threads_running'"
	customThreshold           = 5
	unreasonablyLowThreshold  = 1 * time.Millisecond
	extremelyHighThreshold    = 1 * time.Hour
	onDemandHeartbeatDuration = 5 * time.Second
	throttlerEnabledTimeout   = 60 * time.Second
	useDefaultQuery           = ""
	testAppName               = throttlerapp.TestingName
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
	statusAPIPath        = "throttler/status"
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

func throttleCheck(tablet *cluster.Vttablet, skipRequestHeartbeats bool) (*vtctldatapb.CheckThrottlerResponse, error) {
	flags := &throttle.CheckFlags{
		Scope:                 base.ShardScope,
		SkipRequestHeartbeats: skipRequestHeartbeats,
		MultiMetricsEnabled:   true,
	}
	resp, err := throttler.CheckThrottler(clusterInstance, tablet, testAppName, flags)
	return resp, err
}

func throttleCheckSelf(tablet *cluster.Vttablet) (*vtctldatapb.CheckThrottlerResponse, error) {
	flags := &throttle.CheckFlags{
		Scope:               base.SelfScope,
		MultiMetricsEnabled: true,
	}
	resp, err := throttler.CheckThrottler(clusterInstance, tablet, testAppName, flags)
	return resp, err
}

func throttleStatus(t *testing.T, tablet *cluster.Vttablet) string {
	resp, err := httpClient.Get(fmt.Sprintf("http://localhost:%d/%s", tablet.HTTPPort, statusAPIPath))
	require.NoError(t, err)
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

func warmUpHeartbeat(t *testing.T) tabletmanagerdatapb.CheckThrottlerResponseCode {
	//  because we run with -heartbeat_on_demand_duration=5s, the heartbeat is "cold" right now.
	// Let's warm it up.
	resp, err := throttleCheck(primaryTablet, false)
	require.NoError(t, err)

	time.Sleep(time.Second)
	return throttle.ResponseCodeFromStatus(resp.Check.ResponseCode, int(resp.Check.StatusCode))
}

// waitForThrottleCheckStatus waits for the tablet to return the provided HTTP code in a throttle check
func waitForThrottleCheckStatus(t *testing.T, tablet *cluster.Vttablet, wantCode tabletmanagerdatapb.CheckThrottlerResponseCode) bool {
	_ = warmUpHeartbeat(t)
	ctx, cancel := context.WithTimeout(context.Background(), onDemandHeartbeatDuration*4)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		resp, err := throttleCheck(tablet, true)
		require.NoError(t, err)

		if wantCode == resp.Check.ResponseCode {
			// Wait for any cached check values to be cleared and the new
			// status value to be in effect everywhere before returning.
			return true
		}
		select {
		case <-ctx.Done():
			return assert.EqualValues(t, wantCode, resp.Check.StatusCode, "response: %+v", resp)
		case <-ticker.C:
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
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("enabling throttler with very low threshold", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true, Threshold: unreasonablyLowThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere with the new config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: unreasonablyLowThreshold.Seconds()}, throttlerEnabledTimeout)
		}
	})
	t.Run("validating pushback response from throttler", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
	t.Run("disabling throttler", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Disable: true, Threshold: unreasonablyLowThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)

		// Wait for the throttler to be disabled everywhere.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, tablet, false, nil, throttlerEnabledTimeout)
		}
	})
	t.Run("validating OK response from disabled throttler, again", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("enabling throttler, again", func(t *testing.T) {
		// Enable the throttler again with the default query which also moves us back
		// to the default threshold.
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere again with the default config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, tablet, true, throttler.DefaultConfig, throttlerEnabledTimeout)
		}
	})
	t.Run("validating pushback response from throttler, again", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
	t.Run("setting high threshold", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: extremelyHighThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: extremelyHighThreshold.Seconds()}, throttlerEnabledTimeout)
		}
	})
	t.Run("validating OK response from throttler with high threshold", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("setting low threshold", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, tablet, true, throttler.DefaultConfig, throttlerEnabledTimeout)
		}
	})
	t.Run("validating pushback response from throttler on low threshold", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
	t.Run("requesting heartbeats", func(t *testing.T) {
		respStatus := warmUpHeartbeat(t)
		assert.NotEqual(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, respStatus)
	})
	t.Run("validating OK response from throttler with low threshold, heartbeats running", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		cluster.ValidateReplicationIsHealthy(t, replicaTablet)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		require.NotNil(t, resp)
		for _, metrics := range resp.Check.Metrics {
			assert.Equal(t, base.ShardScope.String(), metrics.Scope)
		}

		if !assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp) {
			rs, err := replicaTablet.VttabletProcess.QueryTablet("show replica status", keyspaceName, false)
			assert.NoError(t, err)
			t.Logf("Seconds_Behind_Source: %s", rs.Named().Row()["Seconds_Behind_Source"].ToString())
			t.Logf("throttler primary status: %+v", throttleStatus(t, primaryTablet))
			t.Logf("throttler replica status: %+v", throttleStatus(t, replicaTablet))
		}
		if !assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp) {
			rs, err := replicaTablet.VttabletProcess.QueryTablet("show replica status", keyspaceName, false)
			assert.NoError(t, err)
			t.Logf("Seconds_Behind_Source: %s", rs.Named().Row()["Seconds_Behind_Source"].ToString())
			t.Logf("throttler primary status: %+v", throttleStatus(t, primaryTablet))
			t.Logf("throttler replica status: %+v", throttleStatus(t, replicaTablet))
		}
	})

	t.Run("validating OK response from throttler with low threshold, heartbeats running still", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		cluster.ValidateReplicationIsHealthy(t, replicaTablet)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		require.NotNil(t, resp)
		for _, metrics := range resp.Check.Metrics {
			assert.Equal(t, base.ShardScope.String(), metrics.Scope)
		}
		if !assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp) {
			rs, err := replicaTablet.VttabletProcess.QueryTablet("show replica status", keyspaceName, false)
			assert.NoError(t, err)
			t.Logf("Seconds_Behind_Source: %s", rs.Named().Row()["Seconds_Behind_Source"].ToString())
			t.Logf("throttler primary status: %+v", throttleStatus(t, primaryTablet))
			t.Logf("throttler replica status: %+v", throttleStatus(t, replicaTablet))
		}
		if !assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp) {
			rs, err := replicaTablet.VttabletProcess.QueryTablet("show replica status", keyspaceName, false)
			assert.NoError(t, err)
			t.Logf("Seconds_Behind_Source: %s", rs.Named().Row()["Seconds_Behind_Source"].ToString())
			t.Logf("throttler primary status: %+v", throttleStatus(t, primaryTablet))
			t.Logf("throttler replica status: %+v", throttleStatus(t, replicaTablet))
		}
	})
	t.Run("validating pushback response from throttler on low threshold once heartbeats go stale", func(t *testing.T) {
		time.Sleep(2 * onDemandHeartbeatDuration) // just... really wait long enough, make sure on-demand stops
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
}

func TestThrottleViaApplySchema(t *testing.T) {
	defer cluster.PanicHandler(t)
	t.Run("throttling via ApplySchema", func(t *testing.T) {
		vtctlParams := &cluster.ApplySchemaParams{DDLStrategy: "online"}
		_, err := clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(
			keyspaceName, "alter vitess_migration throttle all", *vtctlParams,
		)
		assert.NoError(t, err)
	})
	t.Run("validate keyspace configuration after throttle", func(t *testing.T) {
		keyspace, err := clusterInstance.VtctldClientProcess.GetKeyspace(keyspaceName)
		require.NoError(t, err)
		require.NotNil(t, keyspace)
		require.NotNil(t, keyspace.Keyspace.ThrottlerConfig)
		require.NotNil(t, keyspace.Keyspace.ThrottlerConfig.ThrottledApps)
		require.NotEmpty(t, keyspace.Keyspace.ThrottlerConfig.ThrottledApps, "throttler config: %+v", keyspace.Keyspace.ThrottlerConfig)
		appRule, ok := keyspace.Keyspace.ThrottlerConfig.ThrottledApps[throttlerapp.OnlineDDLName.String()]
		require.True(t, ok, "throttled apps: %v", keyspace.Keyspace.ThrottlerConfig.ThrottledApps)
		require.NotNil(t, appRule)
		assert.Equal(t, throttlerapp.OnlineDDLName.String(), appRule.Name)
		assert.EqualValues(t, 1.0, appRule.Ratio)
		expireAt := time.Unix(appRule.ExpiresAt.Seconds, int64(appRule.ExpiresAt.Nanoseconds))
		assert.True(t, expireAt.After(time.Now()), "expected rule to expire in the future: %v", expireAt)
	})
	t.Run("unthrottling via ApplySchema", func(t *testing.T) {
		vtctlParams := &cluster.ApplySchemaParams{DDLStrategy: "online"}
		_, err := clusterInstance.VtctldClientProcess.ApplySchemaWithOutput(
			keyspaceName, "alter vitess_migration unthrottle all", *vtctlParams,
		)
		assert.NoError(t, err)
	})
	t.Run("validate keyspace configuration after unthrottle", func(t *testing.T) {
		keyspace, err := clusterInstance.VtctldClientProcess.GetKeyspace(keyspaceName)
		require.NoError(t, err)
		require.NotNil(t, keyspace)
		require.NotNil(t, keyspace.Keyspace.ThrottlerConfig)
		require.NotNil(t, keyspace.Keyspace.ThrottlerConfig.ThrottledApps)
		// ThrottledApps will actually be empty at this point, but more specifically we want to see that "online-ddl" is not there.
		appRule, ok := keyspace.Keyspace.ThrottlerConfig.ThrottledApps[throttlerapp.OnlineDDLName.String()]
		assert.True(t, ok, "app rule: %v", appRule)
	})
}

func TestThrottlerAfterMetricsCollected(t *testing.T) {
	defer cluster.PanicHandler(t)

	// By this time metrics will have been collected. We expect no lag, and something like:
	// {"StatusCode":200,"Value":0.282278,"Threshold":1,"Message":""}
	t.Run("validating throttler OK", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("validating throttled apps", func(t *testing.T) {
		resp, body, err := throttledApps(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equalf(t, http.StatusOK, resp.StatusCode, "Unexpected response from throttler: %s", getResponseBody(resp))
		assert.Contains(t, body, throttlerapp.TestingAlwaysThrottlerName)
	})
	t.Run("validating primary check self", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
		assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
	})
	t.Run("validating replica check self", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
		assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
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
		err := clusterInstance.VtctldClientProcess.ExecuteCommand("StopReplication", replicaTablet.Alias)
		assert.NoError(t, err)
	})
	t.Run("accumulating lag, expecting throttler push back", func(t *testing.T) {
		time.Sleep(2 * throttler.DefaultThreshold)
	})
	t.Run("requesting heartbeats while replication stopped", func(t *testing.T) {
		// By now on-demand heartbeats have stopped.
		_ = warmUpHeartbeat(t)
	})

	t.Run("expecting throttler push back", func(t *testing.T) {
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		assert.EqualValues(t, http.StatusTooManyRequests, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
		assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
	})
	t.Run("primary self-check should still be fine", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		require.NotNil(t, resp)
		for _, metrics := range resp.Check.Metrics {
			assert.Equal(t, base.SelfScope.String(), metrics.Scope)
		}
		// self (on primary) is unaffected by replication lag
		if !assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp) {
			t.Logf("throttler primary status: %+v", throttleStatus(t, primaryTablet))
			t.Logf("throttler replica status: %+v", throttleStatus(t, replicaTablet))
		}
		if !assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp) {
			t.Logf("throttler primary status: %+v", throttleStatus(t, primaryTablet))
			t.Logf("throttler replica status: %+v", throttleStatus(t, replicaTablet))
		}
	})
	t.Run("replica self-check should show error", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		require.NotNil(t, resp)
		for _, metrics := range resp.Check.Metrics {
			assert.Equal(t, base.SelfScope.String(), metrics.Scope)
		}
		assert.EqualValues(t, http.StatusTooManyRequests, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
		assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
	})
	t.Run("exempting test app", func(t *testing.T) {
		appRule := &topodatapb.ThrottledAppRule{
			Name:      testAppName.String(),
			ExpiresAt: protoutil.TimeToProto(time.Now().Add(time.Hour)),
			Exempt:    true,
		}
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, appRule, nil)
		assert.NoError(t, err)
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("unexempting test app", func(t *testing.T) {
		appRule := &topodatapb.ThrottledAppRule{
			Name:      testAppName.String(),
			ExpiresAt: protoutil.TimeToProto(time.Now()),
		}
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, appRule, nil)
		assert.NoError(t, err)
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
	t.Run("exempting all apps", func(t *testing.T) {
		appRule := &topodatapb.ThrottledAppRule{
			Name:      throttlerapp.AllName.String(),
			ExpiresAt: protoutil.TimeToProto(time.Now().Add(time.Hour)),
			Exempt:    true,
		}
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, appRule, nil)
		assert.NoError(t, err)
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("throttling test app", func(t *testing.T) {
		appRule := &topodatapb.ThrottledAppRule{
			Name:      testAppName.String(),
			Ratio:     throttle.DefaultThrottleRatio,
			ExpiresAt: protoutil.TimeToProto(time.Now().Add(time.Hour)),
		}
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, appRule, nil)
		assert.NoError(t, err)
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_APP_DENIED)
	})
	t.Run("unthrottling test app", func(t *testing.T) {
		appRule := &topodatapb.ThrottledAppRule{
			Name:      testAppName.String(),
			ExpiresAt: protoutil.TimeToProto(time.Now()),
		}
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, appRule, nil)
		assert.NoError(t, err)
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("unexempting all apps", func(t *testing.T) {
		appRule := &topodatapb.ThrottledAppRule{
			Name:      throttlerapp.AllName.String(),
			ExpiresAt: protoutil.TimeToProto(time.Now()),
		}
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, appRule, nil)
		assert.NoError(t, err)
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})

	t.Run("starting replication", func(t *testing.T) {
		err := clusterInstance.VtctldClientProcess.ExecuteCommand("StartReplication", replicaTablet.Alias)
		assert.NoError(t, err)
	})
	t.Run("expecting replication to catch up and throttler check to return OK", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("primary self-check should be fine", func(t *testing.T) {
		resp, err := throttleCheckSelf(primaryTablet)
		require.NoError(t, err)
		// self (on primary) is unaffected by replication lag
		assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
		assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
	})
	t.Run("replica self-check should be fine", func(t *testing.T) {
		resp, err := throttleCheckSelf(replicaTablet)
		require.NoError(t, err)
		assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
		assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
	})
}

func TestNoReplicas(t *testing.T) {
	defer cluster.PanicHandler(t)
	t.Run("changing replica to RDONLY", func(t *testing.T) {
		err := clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", replicaTablet.Alias, "RDONLY")
		assert.NoError(t, err)

		// This makes no REPLICA servers available. We expect something like:
		// {"StatusCode":200,"Value":0,"Threshold":1,"Message":""}
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("restoring to REPLICA", func(t *testing.T) {
		err := clusterInstance.VtctldClientProcess.ExecuteCommand("ChangeTabletType", replicaTablet.Alias, "REPLICA")
		assert.NoError(t, err)

		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
}

func TestCustomQuery(t *testing.T) {
	defer cluster.PanicHandler(t)

	t.Run("enabling throttler with custom query and threshold", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true, Threshold: customThreshold, CustomQuery: customQuery}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)

		// Wait for the throttler to be enabled everywhere with new custom config.
		expectConfig := &throttler.Config{Query: customQuery, Threshold: customThreshold}
		for _, ks := range clusterInstance.Keyspaces {
			for _, shard := range ks.Shards {
				for _, tablet := range shard.Vttablets {
					throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, tablet, true, expectConfig, throttlerEnabledTimeout)
				}
			}
		}
	})
	t.Run("validating OK response from throttler with custom query", func(t *testing.T) {
		throttler.WaitForValidData(t, primaryTablet, throttlerEnabledTimeout)
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
		assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
	})
	t.Run("test threads running", func(t *testing.T) {
		sleepDuration := 20 * time.Second
		var wg sync.WaitGroup
		t.Run("generate running queries", func(t *testing.T) {
			for i := 0; i < customThreshold+1; i++ {
				// Generate different Sleep() calls, all at minimum sleepDuration.
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					// Make sure to generate a different query in each goroutine, so that vtgate does not oversmart us
					// and optimizes connections/caching.
					query := fmt.Sprintf("select sleep(%d) + %d", int(sleepDuration.Seconds()), i)
					vtgateExec(t, query, "")
				}(i)
			}
		})
		t.Run("exceeds threshold", func(t *testing.T) {
			// Now we should be reporting ~ customThreshold+1 threads_running, and we should
			// hit the threshold. For example:
			// {"StatusCode":429,"Value":6,"Threshold":5,"Message":"Threshold exceeded"}
			waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
			{
				resp, err := throttleCheckSelf(primaryTablet)
				require.NoError(t, err)
				assert.EqualValues(t, http.StatusTooManyRequests, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
			}
		})
		t.Run("wait for queries to terminate", func(t *testing.T) {
			wg.Wait()
		})
		t.Run("restored below threshold", func(t *testing.T) {
			waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
			{
				resp, err := throttleCheckSelf(primaryTablet)
				require.NoError(t, err)
				assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
			}
		})
	})
}

func TestRestoreDefaultQuery(t *testing.T) {
	defer cluster.PanicHandler(t)

	// Validate going back from custom-query to default-query (replication lag) still works.
	t.Run("enabling throttler with default query and threshold", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Enable: true, Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)

		// Wait for the throttler to be up and running everywhere again with the default config.
		for _, tablet := range clusterInstance.Keyspaces[0].Shards[0].Vttablets {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, tablet, true, throttler.DefaultConfig, throttlerEnabledTimeout)
		}
	})
	t.Run("validating OK response from throttler with default threshold, heartbeats running", func(t *testing.T) {
		resp, err := throttleCheck(primaryTablet, false)
		require.NoError(t, err)
		assert.EqualValues(t, http.StatusOK, resp.Check.StatusCode, "Unexpected response from throttler: %+v", resp)
		assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, resp.Check.ResponseCode, "Unexpected response from throttler: %+v", resp)
	})
	t.Run("validating pushback response from throttler on default threshold once heartbeats go stale", func(t *testing.T) {
		time.Sleep(2 * onDemandHeartbeatDuration) // just... really wait long enough, make sure on-demand stops
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
}

func TestUpdateMetricThresholds(t *testing.T) {
	t.Run("validating pushback from throttler", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)
		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: throttler.DefaultThreshold.Seconds()}, throttlerEnabledTimeout)
		}
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
	t.Run("setting low general threshold, and high threshold for 'lag' metric", func(t *testing.T) {
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{MetricName: "lag", Threshold: extremelyHighThreshold.Seconds()}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
			assert.NoError(t, err)
		}
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: unreasonablyLowThreshold.Seconds()}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
			assert.NoError(t, err)
		}
		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: unreasonablyLowThreshold.Seconds()}, throttlerEnabledTimeout)
		}
	})
	t.Run("validating OK response from throttler thanks to high 'lag' threshold", func(t *testing.T) {
		// Note that the default threshold is extremely low, but gets overriden.
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK)
	})
	t.Run("removing explicit 'lag' threshold", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{MetricName: "lag", Threshold: 0}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)
	})
	t.Run("validating pushback from throttler again", func(t *testing.T) {
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
	t.Run("restoring standard threshold", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)
		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: throttler.DefaultThreshold.Seconds()}, throttlerEnabledTimeout)
		}
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
}

func TestUpdateAppCheckedMetrics(t *testing.T) {
	t.Run("ensure replica is not dormant", func(t *testing.T) {
		_, err := throttleCheck(replicaTablet, false)
		require.NoError(t, err)
	})
	t.Run("validating pushback from throttler", func(t *testing.T) {
		req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
		_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
		assert.NoError(t, err)
		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: throttler.DefaultThreshold.Seconds()}, throttlerEnabledTimeout)
		}
		waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
	})
	t.Run("assigning 'loadavg' metrics to 'test' app", func(t *testing.T) {
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{MetricName: "loadavg", Threshold: 7777}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
			assert.NoError(t, err)
		}
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{}
			appCheckedMetrics := map[string]*topodatapb.ThrottlerConfig_MetricNames{
				testAppName.String(): {Names: []string{"loadavg"}},
			}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, appCheckedMetrics)
			assert.NoError(t, err)
		}
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: unreasonablyLowThreshold.Seconds()}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
			assert.NoError(t, err)
		}
		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: unreasonablyLowThreshold.Seconds()}, throttlerEnabledTimeout)
		}
		t.Run("validating OK response from throttler since it's checking loadavg", func(t *testing.T) {
			if !waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_OK) {
				t.Logf("throttler primary status: %+v", throttleStatus(t, primaryTablet))
				t.Logf("throttler replica status: %+v", throttleStatus(t, replicaTablet))
			}
		})
	})
	t.Run("assigning 'loadavg,lag' metrics to 'test' app", func(t *testing.T) {
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{}
			appCheckedMetrics := map[string]*topodatapb.ThrottlerConfig_MetricNames{
				testAppName.String(): {Names: []string{"loadavg,lag"}},
			}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, appCheckedMetrics)
			assert.NoError(t, err)
		}
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: unreasonablyLowThreshold.Seconds()}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
			assert.NoError(t, err)
		}
		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: unreasonablyLowThreshold.Seconds()}, throttlerEnabledTimeout)
		}
		t.Run("validating pushback from throttler since lag is above threshold", func(t *testing.T) {
			waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
		})
	})
	t.Run("removing assignment from 'test' app and restoring defaults", func(t *testing.T) {
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{MetricName: "loadavg", Threshold: 0}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
			assert.NoError(t, err)
		}
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{}
			appCheckedMetrics := map[string]*topodatapb.ThrottlerConfig_MetricNames{
				testAppName.String(): {Names: []string{}},
			}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, appCheckedMetrics)
			assert.NoError(t, err)
		}
		{
			req := &vtctldatapb.UpdateThrottlerConfigRequest{Threshold: throttler.DefaultThreshold.Seconds()}
			_, err := throttler.UpdateThrottlerTopoConfig(clusterInstance, req, nil, nil)
			assert.NoError(t, err)
		}
		// Wait for the throttler to be enabled everywhere with new config.
		for _, tablet := range []cluster.Vttablet{*primaryTablet, *replicaTablet} {
			throttler.WaitForThrottlerStatusEnabled(t, &clusterInstance.VtctldClientProcess, &tablet, true, &throttler.Config{Query: throttler.DefaultQuery, Threshold: throttler.DefaultThreshold.Seconds()}, throttlerEnabledTimeout)
		}
		t.Run("validating error response from throttler since lag is still high", func(t *testing.T) {
			waitForThrottleCheckStatus(t, primaryTablet, tabletmanagerdatapb.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED)
		})
	})
}
