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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUpdateConfigValuesFromFlags(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		// Restore the changes we make to the Config parameter
		defer func() {
			Config = newConfiguration()
		}()
		defaultConfig := newConfiguration()
		UpdateConfigValuesFromFlags()
		require.Equal(t, defaultConfig, Config)
	})

	t.Run("override auditPurgeDuration", func(t *testing.T) {
		oldAuditPurgeDuration := auditPurgeDuration
		auditPurgeDuration = 8 * time.Hour * 24
		auditPurgeDuration += time.Second + 4*time.Minute
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			auditPurgeDuration = oldAuditPurgeDuration
		}()

		testConfig := newConfiguration()
		// auditPurgeDuration is supposed to be in multiples of days.
		// If it is not, then we round down to the nearest number of days.
		testConfig.AuditPurgeDays = 8
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override sqliteDataFile", func(t *testing.T) {
		oldSqliteDataFile := sqliteDataFile
		sqliteDataFile = "newVal"
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			sqliteDataFile = oldSqliteDataFile
		}()

		testConfig := newConfiguration()
		testConfig.SQLite3DataFile = "newVal"
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override instancePollTime", func(t *testing.T) {
		oldInstancePollTime := instancePollTime
		instancePollTime = 7 * time.Second
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			instancePollTime = oldInstancePollTime
		}()

		testConfig := newConfiguration()
		testConfig.InstancePollSeconds = 7
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override snapshotTopologyInterval", func(t *testing.T) {
		oldSnapshotTopologyInterval := snapshotTopologyInterval
		snapshotTopologyInterval = 1 * time.Hour
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			snapshotTopologyInterval = oldSnapshotTopologyInterval
		}()

		testConfig := newConfiguration()
		testConfig.SnapshotTopologiesIntervalHours = 1
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override reasonableReplicationLag", func(t *testing.T) {
		oldReasonableReplicationLag := reasonableReplicationLag
		reasonableReplicationLag = 15 * time.Second
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			reasonableReplicationLag = oldReasonableReplicationLag
		}()

		testConfig := newConfiguration()
		testConfig.ReasonableReplicationLagSeconds = 15
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override auditFileLocation", func(t *testing.T) {
		oldAuditFileLocation := auditFileLocation
		auditFileLocation = "newFile"
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			auditFileLocation = oldAuditFileLocation
		}()

		testConfig := newConfiguration()
		testConfig.AuditLogFile = "newFile"
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override auditToBackend", func(t *testing.T) {
		oldAuditToBackend := auditToBackend
		auditToBackend = true
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			auditToBackend = oldAuditToBackend
		}()

		testConfig := newConfiguration()
		testConfig.AuditToBackendDB = true
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override auditToSyslog", func(t *testing.T) {
		oldAuditToSyslog := auditToSyslog
		auditToSyslog = true
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			auditToSyslog = oldAuditToSyslog
		}()

		testConfig := newConfiguration()
		testConfig.AuditToSyslog = true
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override recoveryPeriodBlockDuration", func(t *testing.T) {
		oldRecoveryPeriodBlockDuration := recoveryPeriodBlockDuration
		recoveryPeriodBlockDuration = 5 * time.Minute
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			recoveryPeriodBlockDuration = oldRecoveryPeriodBlockDuration
		}()

		testConfig := newConfiguration()
		testConfig.RecoveryPeriodBlockSeconds = 300
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override preventCrossCellFailover", func(t *testing.T) {
		oldPreventCrossCellFailover := preventCrossCellFailover
		preventCrossCellFailover = true
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			preventCrossCellFailover = oldPreventCrossCellFailover
		}()

		testConfig := newConfiguration()
		testConfig.PreventCrossDataCenterPrimaryFailover = true
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override lockShardTimeout", func(t *testing.T) {
		oldLockShardTimeout := lockShardTimeout
		lockShardTimeout = 3 * time.Hour
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			lockShardTimeout = oldLockShardTimeout
		}()

		testConfig := newConfiguration()
		testConfig.LockShardTimeoutSeconds = 10800
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override waitReplicasTimeout", func(t *testing.T) {
		oldWaitReplicasTimeout := waitReplicasTimeout
		waitReplicasTimeout = 3*time.Minute + 4*time.Second
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			waitReplicasTimeout = oldWaitReplicasTimeout
		}()

		testConfig := newConfiguration()
		testConfig.WaitReplicasTimeoutSeconds = 184
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override topoInformationRefreshDuration", func(t *testing.T) {
		oldTopoInformationRefreshDuration := topoInformationRefreshDuration
		topoInformationRefreshDuration = 1 * time.Second
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			topoInformationRefreshDuration = oldTopoInformationRefreshDuration
		}()

		testConfig := newConfiguration()
		testConfig.TopoInformationRefreshSeconds = 1
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override recoveryPollDuration", func(t *testing.T) {
		oldRecoveryPollDuration := recoveryPollDuration
		recoveryPollDuration = 15 * time.Second
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			recoveryPollDuration = oldRecoveryPollDuration
		}()

		testConfig := newConfiguration()
		testConfig.RecoveryPollSeconds = 15
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})
}
