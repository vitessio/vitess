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

	httpClient       = base.SetupHTTPClient(time.Second)
	checkAPIPath     = "throttler/check"
	checkSelfAPIPath = "throttler/check-self"
	vtParams         mysql.ConnParams
)

const (
	testThreshold     = 5
	applyConfigWait   = 15 * time.Second // time after which we're sure the throttler has refreshed config and tablets
	statusWaitTimeout = 30 * time.Second
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
			"--throttle_metrics_query", "show global status like 'threads_running'",
			"--throttle_metrics_threshold", fmt.Sprintf("%d", testThreshold),
			"--throttle_check_as_check_self",
			"--heartbeat_enable",
			"--heartbeat_interval", "250ms",
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}

		if err = clusterInstance.StartUnshardedKeyspace(*keyspace, 0, false); err != nil {
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

		return m.Run()
	}()
	os.Exit(exitCode)
}

func throttleCheck(tablet *cluster.Vttablet) (*http.Response, error) {
	resp, err := httpClient.Get(fmt.Sprintf("http://localhost:%d/%s", tablet.HTTPPort, checkAPIPath))
	return resp, err
}

func waitForThrottlerStatus(tablet *cluster.Vttablet, status int) error {
	ctx, cancel := context.WithTimeout(context.Background(), statusWaitTimeout)
	defer cancel()
	tkr := time.NewTicker(100 * time.Millisecond)
	defer tkr.Stop()

	for {
		resp, _ := throttleCheck(tablet)
		seenStatus := resp.StatusCode
		resp.Body.Close()
		if seenStatus == status {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for expected throttler status %d after %v; last seen value: %d",
				status, statusWaitTimeout, seenStatus)
		case <-tkr.C:
		}
	}
}

func throttleCheckSelf(tablet *cluster.Vttablet) (*http.Response, error) {
	return httpClient.Head(fmt.Sprintf("http://localhost:%d/%s", tablet.HTTPPort, checkSelfAPIPath))
}

func TestThrottlerThresholdOK(t *testing.T) {
	defer cluster.PanicHandler(t)

	t.Run("immediately", func(t *testing.T) {
		// The tablet throttler can still be initializing so we wait for
		// the status to be OK.
		err := waitForThrottlerStatus(primaryTablet, http.StatusOK)
		require.NoError(t, err)
	})
	t.Run("after long wait", func(t *testing.T) {
		time.Sleep(applyConfigWait)
		resp, err := throttleCheck(primaryTablet)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestThreadsRunning(t *testing.T) {
	defer cluster.PanicHandler(t)

	sleepDuration := 10 * time.Second
	var wg sync.WaitGroup
	for i := 0; i < testThreshold; i++ {
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
			resp, err := throttleCheck(primaryTablet)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		}
		{
			resp, err := throttleCheckSelf(primaryTablet)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
		}
	})
	t.Run("wait for queries to terminate", func(t *testing.T) {
		wg.Wait()
	})
	t.Run("restored below threshold", func(t *testing.T) {
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
	})
}

func vtgateExec(t *testing.T, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
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
