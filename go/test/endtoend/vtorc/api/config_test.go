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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtorc/utils"
)

// TestDynamicConfigs tests the dyanamic configurations that VTOrc offers.
func TestDynamicConfigs(t *testing.T) {
	utils.SetupVttabletsAndVTOrcs(t, clusterInfo, 2, 1, nil, cluster.VTOrcConfiguration{}, cluster.DefaultVtorcsByCell, "")
	vtorc := clusterInfo.ClusterInstance.VTOrcProcesses[0]

	// Restart VTOrc without any flag overrides so that all the configurations can be tested.
	err := vtorc.TearDown()
	require.NoError(t, err)
	vtorc.Config = cluster.VTOrcConfiguration{}
	vtorc.NoOverride = true
	err = vtorc.Setup()
	require.NoError(t, err)

	// Call API with retry to ensure VTOrc is up
	status, resp := utils.MakeAPICallRetry(t, vtorc, "/debug/health", func(code int, response string) bool {
		return code != 200
	})
	// Verify when VTOrc is healthy, it has also run the first discovery.
	assert.Equal(t, 200, status)
	assert.Contains(t, resp, `"Healthy": true,`)

	t.Run("InstancePollTime", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"instance-poll-time": 5000000000`)
		// Update configuration and verify the output.
		vtorc.Config.InstancePollTime = "10h"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"instance-poll-time": "10h"`)
	})

	t.Run("PreventCrossCellFailover", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"prevent-cross-cell-failover": false`)
		// Update configuration and verify the output.
		vtorc.Config.PreventCrossCellFailover = true
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"prevent-cross-cell-failover": true`)
	})

	t.Run("SnapshotTopologyInterval", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"snapshot-topology-interval": 0`)
		// Update configuration and verify the output.
		vtorc.Config.SnapshotTopologyInterval = "10h"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"snapshot-topology-interval": "10h"`)
	})

	t.Run("ReasonableReplicationLag", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"reasonable-replication-lag": 10000000000`)
		// Update configuration and verify the output.
		vtorc.Config.ReasonableReplicationLag = "10h"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"reasonable-replication-lag": "10h"`)
	})

	t.Run("AuditToBackend", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"audit-to-backend": false`)
		// Update configuration and verify the output.
		vtorc.Config.AuditToBackend = true
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"audit-to-backend": true`)
	})

	t.Run("AuditToSyslog", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"audit-to-syslog": false`)
		// Update configuration and verify the output.
		vtorc.Config.AuditToSyslog = true
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"audit-to-syslog": true`)
	})

	t.Run("AuditPurgeDuration", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"audit-purge-duration": 604800000000000`)
		// Update configuration and verify the output.
		vtorc.Config.AuditPurgeDuration = "10h"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"audit-purge-duration": "10h"`)
	})

	t.Run("WaitReplicasTimeout", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"wait-replicas-timeout": 30000000000`)
		// Update configuration and verify the output.
		vtorc.Config.WaitReplicasTimeout = "10h"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"wait-replicas-timeout": "10h"`)
	})

	t.Run("TolerableReplicationLag", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"tolerable-replication-lag": 0`)
		// Update configuration and verify the output.
		vtorc.Config.TolerableReplicationLag = "10h"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"tolerable-replication-lag": "10h"`)
	})

	t.Run("TopoInformationRefreshDuration", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"topo-information-refresh-duration": 15000000000`)
		// Update configuration and verify the output.
		vtorc.Config.TopoInformationRefreshDuration = "10h"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"topo-information-refresh-duration": "10h"`)
	})

	t.Run("RecoveryPollDuration", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"recovery-poll-duration": 1000000000`)
		// Update configuration and verify the output.
		vtorc.Config.RecoveryPollDuration = "10h"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"recovery-poll-duration": "10h"`)
	})

	t.Run("AllowEmergencyReparent", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"allow-emergency-reparent": true`)
		// Update configuration and verify the output.
		vtorc.Config.AllowEmergencyReparent = "false"
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"allow-emergency-reparent": "false"`)
	})

	t.Run("ChangeTabletsWithErrantGtidToDrained", func(t *testing.T) {
		// Get configuration and verify the output.
		waitForConfig(t, vtorc, `"change-tablets-with-errant-gtid-to-drained": false`)
		// Update configuration and verify the output.
		vtorc.Config.ChangeTabletsWithErrantGtidToDrained = true
		err := vtorc.RewriteConfiguration()
		assert.NoError(t, err)
		// Wait until the config has been updated and seen.
		waitForConfig(t, vtorc, `"change-tablets-with-errant-gtid-to-drained": true`)
	})
}

// waitForConfig waits for the expectedConfig to be present in the VTOrc configuration.
func waitForConfig(t *testing.T, vtorc *cluster.VTOrcProcess, expectedConfig string) {
	t.Helper()
	status, _ := utils.MakeAPICallRetry(t, vtorc, "/api/config", func(_ int, response string) bool {
		return !strings.Contains(response, expectedConfig)
	})
	require.EqualValues(t, 200, status)
}
