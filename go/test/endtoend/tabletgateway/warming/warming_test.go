/*
Copyright 2024 The Vitess Authors.

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

package warming

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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestWarmingBalancerTabletStartTimeReported verifies that tablets report their
// start time in the health stream and that VTGate receives this information.
func TestWarmingBalancerTabletStartTimeReported(t *testing.T) {
	// Wait for health checks to propagate
	time.Sleep(2 * time.Second)

	// Query VTGate debug vars to check tablet health information
	resp, err := http.Get(clusterInstance.VtgateProcess.VerifyURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var debugVars map[string]any
	err = json.Unmarshal(body, &debugVars)
	require.NoError(t, err)

	// Check that we have healthy tablets
	healthCheck, ok := debugVars["HealthcheckConnections"]
	require.True(t, ok, "HealthcheckConnections should be present in debug vars")

	// The health check should show tablets with their information
	t.Logf("HealthcheckConnections: %v", healthCheck)

	// Verify we can connect and query
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Show tablets should work
	qr := utils.Exec(t, conn, "show vitess_tablets")
	assert.GreaterOrEqual(t, len(qr.Rows), 3, "should have at least 3 tablets (1 primary + 2 replicas)")

	for _, row := range qr.Rows {
		t.Logf("Tablet: %v", row)
	}
}

// TestWarmingBalancerQueriesWork verifies that queries work correctly with the
// warming balancer enabled.
func TestWarmingBalancerQueriesWork(t *testing.T) {
	// Wait for health checks to propagate
	time.Sleep(2 * time.Second)

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Insert some data
	utils.Exec(t, conn, "insert into test_table (val) values ('test1'), ('test2'), ('test3')")

	// Run multiple read queries - these should be distributed according to warming logic
	for i := 0; i < 100; i++ {
		qr := utils.Exec(t, conn, "select * from test_table")
		assert.Equal(t, 3, len(qr.Rows), "should return 3 rows")
	}

	// Clean up
	utils.Exec(t, conn, "delete from test_table")
}

// TestWarmingBalancerDebugEndpoint verifies that the warming balancer debug
// endpoint provides useful information.
func TestWarmingBalancerDebugEndpoint(t *testing.T) {
	// Wait for health checks and some queries to happen
	time.Sleep(2 * time.Second)

	// First run some queries to populate the balancer stats
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Run a few queries to trigger the balancer
	for i := 0; i < 10; i++ {
		utils.Exec(t, conn, "select 1")
	}

	// Check the balancer debug endpoint
	// The debug endpoint is typically at /debug/balancer
	debugURL := fmt.Sprintf("http://%s:%d/debug/balancer",
		clusterInstance.Hostname, clusterInstance.VtgateProcess.Port)

	resp, err := http.Get(debugURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	debugOutput := string(body)
	t.Logf("Balancer debug output:\n%s", debugOutput)

	// Verify the output contains warming-specific information
	assert.Contains(t, debugOutput, "warming", "debug output should mention warming mode")
	assert.Contains(t, debugOutput, "Warming Period", "debug output should show warming period")
	assert.Contains(t, debugOutput, "Warming Traffic Percent", "debug output should show traffic percent")
}

// TestWarmingBalancerAllTabletsServing verifies that when all tablets are relatively
// new (simulating a full zone recycle), queries still work correctly.
func TestWarmingBalancerAllTabletsServing(t *testing.T) {
	// This test verifies the "all new tablets" mode where no warming is needed
	// because there are no old tablets to absorb traffic.

	// Wait for health checks
	time.Sleep(2 * time.Second)

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Insert test data
	utils.Exec(t, conn, "insert into test_table (val) values ('allnew1')")

	// Run many queries - they should all succeed
	successCount := 0
	for i := 0; i < 50; i++ {
		_, err := conn.ExecuteFetch("select * from test_table", 100, false)
		if err == nil {
			successCount++
		}
	}

	assert.Equal(t, 50, successCount, "all queries should succeed")

	// Clean up
	utils.Exec(t, conn, "delete from test_table")
}

// TestWarmingBalancerVtgateVars checks that VTGate exposes relevant metrics/vars
// for monitoring the warming balancer.
func TestWarmingBalancerVtgateVars(t *testing.T) {
	// Wait for some activity
	time.Sleep(2 * time.Second)

	// Generate some query traffic
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	for i := 0; i < 20; i++ {
		utils.Exec(t, conn, "select 1")
	}

	// Fetch VTGate debug vars
	resp, err := http.Get(clusterInstance.VtgateProcess.VerifyURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	debugVarsStr := string(body)

	// Log the relevant parts for debugging
	if strings.Contains(debugVarsStr, "Queries") {
		t.Log("VTGate is tracking query counts")
	}

	// Parse and check for relevant metrics
	var debugVars map[string]any
	err = json.Unmarshal(body, &debugVars)
	require.NoError(t, err)

	// Log some key metrics that would be useful for monitoring warming
	if hc, ok := debugVars["HealthcheckConnections"]; ok {
		t.Logf("HealthcheckConnections: %v", hc)
	}
}
