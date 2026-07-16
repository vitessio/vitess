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

package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vitesst"
)

// dynamicConfig accumulates the configuration values the test has set so far, so each rewrite ships
// the complete configuration to VTOrc, matching what VTOrc persists back to the watched file.
var dynamicConfig = map[string]any{}

// TestDynamicConfigs tests the dyanamic configurations that VTOrc offers.
func TestDynamicConfigs(t *testing.T) {
	clusterInstance = setupCluster(t)
	ctx := t.Context()
	vtorc := clusterInstance.VTOrc()

	// Restart VTOrc without any flag overrides so that all the configurations can be tested.
	require.NoError(t, vtorc.RestartWithBuiltinConfig(ctx))

	// Call API with retry to ensure VTOrc is up
	status, resp := makeAPICallRetry(ctx, t, vtorc, "/debug/health", func(code int, response string) bool {
		return code != 200
	})
	// Verify when VTOrc is healthy, it has also run the first discovery.
	assert.Equal(t, 200, status)
	assert.Contains(t, resp, `"Healthy": true,`)

	t.Run("InstancePollTime", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"instance-poll-time": 5000000000`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "instance-poll-time", "10h")
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"instance-poll-time": "10h"`)
	})

	t.Run("PreventCrossCellFailover", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"prevent-cross-cell-failover": false`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "prevent-cross-cell-failover", true)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"prevent-cross-cell-failover": true`)
	})

	t.Run("ReasonableReplicationLag", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"reasonable-replication-lag": 10000000000`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "reasonable-replication-lag", "10h")
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"reasonable-replication-lag": "10h"`)
	})

	t.Run("AuditToBackend", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"audit-to-backend": false`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "audit-to-backend", true)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"audit-to-backend": true`)
	})

	t.Run("AuditToSyslog", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"audit-to-syslog": false`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "audit-to-syslog", true)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"audit-to-syslog": true`)
	})

	t.Run("AuditPurgeDuration", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"audit-purge-duration": 604800000000000`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "audit-purge-duration", "10h")
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"audit-purge-duration": "10h"`)
	})

	t.Run("WaitReplicasTimeout", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"wait-replicas-timeout": 30000000000`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "wait-replicas-timeout", "10h")
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"wait-replicas-timeout": "10h"`)
	})

	t.Run("TolerableReplicationLag", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"tolerable-replication-lag": 0`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "tolerable-replication-lag", "10h")
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"tolerable-replication-lag": "10h"`)
	})

	t.Run("TopoInformationRefreshDuration", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"topo-information-refresh-duration": 15000000000`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "topo-information-refresh-duration", "10h")
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"topo-information-refresh-duration": "10h"`)
	})

	t.Run("RecoveryPollDuration", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"recovery-poll-duration": 1000000000`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "recovery-poll-duration", "10h")
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"recovery-poll-duration": "10h"`)
	})

	t.Run("AllowEmergencyReparent", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"allow-emergency-reparent": true`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "allow-emergency-reparent", "false")
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"allow-emergency-reparent": "false"`)
	})

	t.Run("ChangeTabletsWithErrantGtidToDrained", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"change-tablets-with-errant-gtid-to-drained": false`)
		// Update configuration and verify the output.
		writeConfig(ctx, t, vtorc, "change-tablets-with-errant-gtid-to-drained", true)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"change-tablets-with-errant-gtid-to-drained": true`)
	})
}

// writeConfig records a configuration value and ships the complete accumulated configuration to VTOrc.
func writeConfig(ctx context.Context, t *testing.T, vtorc *vitesst.VTOrc, key string, value any) {
	t.Helper()
	dynamicConfig[key] = value
	content, err := json.Marshal(dynamicConfig)
	require.NoError(t, err)
	require.NoError(t, vtorc.WriteConfig(ctx, string(content)))
}

// waitForConfig waits for the expectedConfig to be present in the VTOrc configuration.
func waitForConfig(t *testing.T, vtorc *vitesst.VTOrc, expectedConfig string) {
	t.Helper()
	status, _ := makeAPICallRetry(t.Context(), t, vtorc, "/api/config", func(_ int, response string) bool {
		return !strings.Contains(response, expectedConfig)
	})
	require.EqualValues(t, 200, status)
}
