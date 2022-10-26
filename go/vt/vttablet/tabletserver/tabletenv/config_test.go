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

package tabletenv

import (
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/yaml2"
)

func TestConfigParse(t *testing.T) {
	cfg := TabletConfig{
		DB: &dbconfigs.DBConfigs{
			Socket: "a",
			App: dbconfigs.UserConfig{
				User: "b",
			},
			Dba: dbconfigs.UserConfig{
				User: "c",
			},
		},
		OltpReadPool: ConnPoolConfig{
			Size:               16,
			TimeoutSeconds:     10,
			IdleTimeoutSeconds: 20,
			PrefillParallelism: 30,
			MaxWaiters:         40,
		},
		RowStreamer: RowStreamerConfig{
			MaxInnoDBTrxHistLen: 1000,
			MaxMySQLReplLagSecs: 400,
		},
	}
	gotBytes, err := yaml2.Marshal(&cfg)
	require.NoError(t, err)
	wantBytes := `db:
  allprivs:
    password: '****'
  app:
    password: '****'
    user: b
  appdebug:
    password: '****'
  dba:
    password: '****'
    user: c
  filtered:
    password: '****'
  repl:
    password: '****'
  socket: a
gracePeriods: {}
healthcheck: {}
hotRowProtection: {}
olap: {}
olapReadPool: {}
oltp: {}
oltpReadPool:
  idleTimeoutSeconds: 20
  maxWaiters: 40
  prefillParallelism: 30
  size: 16
  timeoutSeconds: 10
replicationTracker: {}
rowStreamer:
  maxInnoDBTrxHistLen: 1000
  maxMySQLReplLagSecs: 400
txPool: {}
`
	assert.Equal(t, wantBytes, string(gotBytes))

	// Make sure things already set don't get overwritten,
	// and thing specified do overwrite.
	// OltpReadPool.TimeoutSeconds should not get overwritten.
	// DB.App.User should not get overwritten.
	// DB.Dba.User should get overwritten.
	inBytes := []byte(`db:
  socket: a
  dba:
    user: c
oltpReadPool:
  size: 16
  idleTimeoutSeconds: 20
  prefillParallelism: 30
  maxWaiters: 40
`)
	gotCfg := cfg
	gotCfg.DB = cfg.DB.Clone()
	gotCfg.DB.Dba = dbconfigs.UserConfig{}
	err = yaml2.Unmarshal(inBytes, &gotCfg)
	require.NoError(t, err)
	assert.Equal(t, cfg, gotCfg)
}

func TestDefaultConfig(t *testing.T) {
	gotBytes, err := yaml2.Marshal(NewDefaultConfig())
	require.NoError(t, err)
	want := `cacheResultFields: true
consolidator: enable
consolidatorStreamQuerySize: 2097152
consolidatorStreamTotalSize: 134217728
gracePeriods: {}
healthcheck:
  degradedThresholdSeconds: 30
  intervalSeconds: 20
  unhealthyThresholdSeconds: 7200
hotRowProtection:
  maxConcurrency: 5
  maxGlobalQueueSize: 1000
  maxQueueSize: 20
  mode: disable
messagePostponeParallelism: 4
olap:
  txTimeoutSeconds: 30
olapReadPool:
  idleTimeoutSeconds: 1800
  size: 200
oltp:
  maxRows: 10000
  queryTimeoutSeconds: 30
  txTimeoutSeconds: 30
oltpReadPool:
  idleTimeoutSeconds: 1800
  maxWaiters: 5000
  size: 16
queryCacheLFU: true
queryCacheMemory: 33554432
queryCacheSize: 5000
replicationTracker:
  heartbeatIntervalSeconds: 0.25
  mode: disable
rowStreamer:
  maxInnoDBTrxHistLen: 1000000
  maxMySQLReplLagSecs: 43200
schemaReloadIntervalSeconds: 1800
signalSchemaChangeReloadIntervalSeconds: 5
signalWhenSchemaChange: true
streamBufferSize: 32768
txPool:
  idleTimeoutSeconds: 1800
  maxWaiters: 5000
  size: 20
  timeoutSeconds: 1
`
	utils.MustMatch(t, want, string(gotBytes))
}

func TestClone(t *testing.T) {
	queryLogHandler = ""
	txLogHandler = ""

	cfg1 := &TabletConfig{
		OltpReadPool: ConnPoolConfig{
			Size:               16,
			TimeoutSeconds:     10,
			IdleTimeoutSeconds: 20,
			PrefillParallelism: 30,
			MaxWaiters:         40,
		},
		RowStreamer: RowStreamerConfig{
			MaxInnoDBTrxHistLen: 1000000,
			MaxMySQLReplLagSecs: 43200,
		},
	}
	cfg2 := cfg1.Clone()
	assert.Equal(t, cfg1, cfg2)
	cfg1.OltpReadPool.Size = 10
	assert.NotEqual(t, cfg1, cfg2)
}

func TestFlags(t *testing.T) {
	fs := pflag.NewFlagSet("TestFlags", pflag.ContinueOnError)
	registerTabletEnvFlags(fs)
	want := *NewDefaultConfig()
	want.DB = &dbconfigs.DBConfigs{}
	assert.Equal(t, want.DB, currentConfig.DB)
	assert.Equal(t, want, currentConfig)

	// Simple Init.
	Init()
	want.OlapReadPool.IdleTimeoutSeconds = 1800
	want.TxPool.IdleTimeoutSeconds = 1800
	want.HotRowProtection.Mode = Disable
	want.Consolidator = Enable
	want.Healthcheck.IntervalSeconds = 20
	want.Healthcheck.DegradedThresholdSeconds = 30
	want.Healthcheck.UnhealthyThresholdSeconds = 7200
	want.ReplicationTracker.HeartbeatIntervalSeconds = 1
	want.ReplicationTracker.Mode = Disable
	assert.Equal(t, want.DB, currentConfig.DB)
	assert.Equal(t, want, currentConfig)

	enableHotRowProtection = true
	enableHotRowProtectionDryRun = true
	Init()
	want.HotRowProtection.Mode = Dryrun
	assert.Equal(t, want, currentConfig)

	enableHotRowProtection = true
	enableHotRowProtectionDryRun = false
	Init()
	want.HotRowProtection.Mode = Enable
	assert.Equal(t, want, currentConfig)

	enableHotRowProtection = false
	enableHotRowProtectionDryRun = true
	Init()
	want.HotRowProtection.Mode = Disable
	assert.Equal(t, want, currentConfig)

	enableHotRowProtection = false
	enableHotRowProtectionDryRun = false
	Init()
	want.HotRowProtection.Mode = Disable
	assert.Equal(t, want, currentConfig)

	enableConsolidator = true
	enableConsolidatorReplicas = true
	Init()
	want.Consolidator = NotOnPrimary
	assert.Equal(t, want, currentConfig)

	enableConsolidator = true
	enableConsolidatorReplicas = false
	Init()
	want.Consolidator = Enable
	assert.Equal(t, want, currentConfig)

	enableConsolidator = false
	enableConsolidatorReplicas = true
	Init()
	want.Consolidator = NotOnPrimary
	assert.Equal(t, want, currentConfig)

	enableConsolidator = false
	enableConsolidatorReplicas = false
	Init()
	want.Consolidator = Disable
	assert.Equal(t, want, currentConfig)

	enableHeartbeat = true
	heartbeatInterval = 1 * time.Second
	currentConfig.ReplicationTracker.Mode = ""
	currentConfig.ReplicationTracker.HeartbeatIntervalSeconds = 0
	Init()
	want.ReplicationTracker.Mode = Heartbeat
	want.ReplicationTracker.HeartbeatIntervalSeconds = 1
	assert.Equal(t, want, currentConfig)

	enableHeartbeat = false
	heartbeatInterval = 1 * time.Second
	currentConfig.ReplicationTracker.Mode = ""
	currentConfig.ReplicationTracker.HeartbeatIntervalSeconds = 0
	Init()
	want.ReplicationTracker.Mode = Disable
	want.ReplicationTracker.HeartbeatIntervalSeconds = 1
	assert.Equal(t, want, currentConfig)

	enableReplicationReporter = true
	heartbeatInterval = 1 * time.Second
	currentConfig.ReplicationTracker.Mode = ""
	currentConfig.ReplicationTracker.HeartbeatIntervalSeconds = 0
	Init()
	want.ReplicationTracker.Mode = Polling
	want.ReplicationTracker.HeartbeatIntervalSeconds = 1
	assert.Equal(t, want, currentConfig)

	healthCheckInterval = 1 * time.Second
	currentConfig.Healthcheck.IntervalSeconds = 0
	Init()
	want.Healthcheck.IntervalSeconds = 1
	assert.Equal(t, want, currentConfig)

	degradedThreshold = 2 * time.Second
	currentConfig.Healthcheck.DegradedThresholdSeconds = 0
	Init()
	want.Healthcheck.DegradedThresholdSeconds = 2
	assert.Equal(t, want, currentConfig)

	unhealthyThreshold = 3 * time.Second
	currentConfig.Healthcheck.UnhealthyThresholdSeconds = 0
	Init()
	want.Healthcheck.UnhealthyThresholdSeconds = 3
	assert.Equal(t, want, currentConfig)

	transitionGracePeriod = 4 * time.Second
	currentConfig.GracePeriods.TransitionSeconds = 0
	Init()
	want.GracePeriods.TransitionSeconds = 4
	assert.Equal(t, want, currentConfig)

	currentConfig.SanitizeLogMessages = false
	Init()
	want.SanitizeLogMessages = false
	assert.Equal(t, want, currentConfig)

	currentConfig.SanitizeLogMessages = true
	Init()
	want.SanitizeLogMessages = true
	assert.Equal(t, want, currentConfig)
}
