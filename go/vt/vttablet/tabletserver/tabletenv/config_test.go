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

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/yaml2"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
			Size:        16,
			Timeout:     10 * time.Second,
			IdleTimeout: 20 * time.Second,
			MaxLifetime: 50 * time.Second,
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
  idleTimeoutSeconds: 20s
  maxLifetimeSeconds: 50s
  size: 16
  timeoutSeconds: 10s
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
  idleTimeoutSeconds: 20s
  maxLifetimeSeconds: 50s
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
	want := `consolidator: enable
consolidatorStreamQuerySize: 2097152
consolidatorStreamTotalSize: 134217728
gracePeriods:
  shutdownSeconds: 3s
healthcheck:
  degradedThresholdSeconds: 30s
  intervalSeconds: 20s
  unhealthyThresholdSeconds: 2h0m0s
hotRowProtection:
  maxConcurrency: 5
  maxGlobalQueueSize: 1000
  maxQueueSize: 20
  mode: disable
messagePostponeParallelism: 4
olap:
  txTimeoutSeconds: 30s
olapReadPool:
  idleTimeoutSeconds: 30m0s
  size: 200
oltp:
  maxRows: 10000
  queryTimeoutSeconds: 30s
  txTimeoutSeconds: 30s
oltpReadPool:
  idleTimeoutSeconds: 30m0s
  size: 16
queryCacheDoorkeeper: true
queryCacheMemory: 33554432
replicationTracker:
  heartbeatIntervalSeconds: 250ms
  mode: disable
rowStreamer:
  maxInnoDBTrxHistLen: 1000000
  maxMySQLReplLagSecs: 43200
schemaChangeReloadTimeout: 30s
schemaReloadIntervalSeconds: 30m0s
signalWhenSchemaChange: true
streamBufferSize: 32768
txPool:
  idleTimeoutSeconds: 30m0s
  size: 20
  timeoutSeconds: 1s
`
	utils.MustMatch(t, want, string(gotBytes))
}

func TestClone(t *testing.T) {
	queryLogHandler = ""
	txLogHandler = ""

	cfg1 := &TabletConfig{
		OltpReadPool: ConnPoolConfig{
			Size:        16,
			Timeout:     10 * time.Second,
			IdleTimeout: 20 * time.Second,
			MaxLifetime: 50 * time.Second,
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
	want.OlapReadPool.IdleTimeout = 30 * time.Minute
	want.TxPool.IdleTimeout = 30 * time.Minute
	want.HotRowProtection.Mode = Disable
	want.Consolidator = Enable
	want.Healthcheck.Interval = 20 * time.Second
	want.Healthcheck.DegradedThreshold = 30 * time.Second
	want.Healthcheck.UnhealthyThreshold = 2 * time.Hour
	want.ReplicationTracker.HeartbeatInterval = time.Second
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
	currentConfig.ReplicationTracker.HeartbeatInterval = 0
	Init()
	want.ReplicationTracker.Mode = Heartbeat
	want.ReplicationTracker.HeartbeatInterval = time.Second
	assert.Equal(t, want, currentConfig)

	enableHeartbeat = false
	heartbeatInterval = 1 * time.Second
	currentConfig.ReplicationTracker.Mode = ""
	currentConfig.ReplicationTracker.HeartbeatInterval = 0
	Init()
	want.ReplicationTracker.Mode = Disable
	want.ReplicationTracker.HeartbeatInterval = time.Second
	assert.Equal(t, want, currentConfig)

	enableReplicationReporter = true
	heartbeatInterval = 1 * time.Second
	currentConfig.ReplicationTracker.Mode = ""
	currentConfig.ReplicationTracker.HeartbeatInterval = 0
	Init()
	want.ReplicationTracker.Mode = Polling
	want.ReplicationTracker.HeartbeatInterval = time.Second
	assert.Equal(t, want, currentConfig)

	healthCheckInterval = time.Second
	currentConfig.Healthcheck.Interval = 0
	Init()
	want.Healthcheck.Interval = time.Second
	assert.Equal(t, want, currentConfig)

	degradedThreshold = 2 * time.Second
	currentConfig.Healthcheck.DegradedThreshold = 0
	Init()
	want.Healthcheck.DegradedThreshold = 2 * time.Second
	assert.Equal(t, want, currentConfig)

	unhealthyThreshold = 3 * time.Second
	currentConfig.Healthcheck.UnhealthyThreshold = 0
	Init()
	want.Healthcheck.UnhealthyThreshold = 3 * time.Second
	assert.Equal(t, want, currentConfig)

	transitionGracePeriod = 4 * time.Second
	currentConfig.GracePeriods.Transition = 0
	Init()
	want.GracePeriods.Transition = 4 * time.Second
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

func TestTxThrottlerConfigFlag(t *testing.T) {
	f := NewTxThrottlerConfigFlag()
	defaultMaxReplicationLagModuleConfig := throttler.DefaultMaxReplicationLagModuleConfig().Configuration

	{
		assert.Nil(t, f.Set(defaultMaxReplicationLagModuleConfig.String()))
		assert.Equal(t, defaultMaxReplicationLagModuleConfig.String(), f.String())
		assert.Equal(t, "string", f.Type())
	}
	{
		defaultMaxReplicationLagModuleConfig.TargetReplicationLagSec = 5
		assert.Nil(t, f.Set(defaultMaxReplicationLagModuleConfig.String()))
		assert.NotNil(t, f.Get())
		assert.Equal(t, int64(5), f.Get().TargetReplicationLagSec)
	}
	{
		assert.NotNil(t, f.Set("should not parse"))
	}
}

func TestVerifyTxThrottlerConfig(t *testing.T) {
	defaultMaxReplicationLagModuleConfig := throttler.DefaultMaxReplicationLagModuleConfig().Configuration
	invalidMaxReplicationLagModuleConfig := throttler.DefaultMaxReplicationLagModuleConfig().Configuration
	invalidMaxReplicationLagModuleConfig.TargetReplicationLagSec = -1

	type testConfig struct {
		Name              string
		ExpectedErrorCode vtrpcpb.Code
		//
		EnableTxThrottler           bool
		TxThrottlerConfig           *TxThrottlerConfigFlag
		TxThrottlerHealthCheckCells []string
		TxThrottlerTabletTypes      *topoproto.TabletTypeListFlag
		TxThrottlerDefaultPriority  int
	}

	tests := []testConfig{
		{
			// default (disabled)
			Name:              "default",
			EnableTxThrottler: false,
		},
		{
			// enabled with invalid throttler config
			Name:              "enabled invalid config",
			ExpectedErrorCode: vtrpcpb.Code_INVALID_ARGUMENT,
			EnableTxThrottler: true,
			TxThrottlerConfig: &TxThrottlerConfigFlag{invalidMaxReplicationLagModuleConfig},
		},
		{
			// enabled with good config (default/replica tablet type)
			Name:                        "enabled",
			EnableTxThrottler:           true,
			TxThrottlerConfig:           &TxThrottlerConfigFlag{defaultMaxReplicationLagModuleConfig},
			TxThrottlerHealthCheckCells: []string{"cell1"},
		},
		{
			// enabled + replica and rdonly tablet types
			Name:                        "enabled plus rdonly",
			EnableTxThrottler:           true,
			TxThrottlerConfig:           &TxThrottlerConfigFlag{defaultMaxReplicationLagModuleConfig},
			TxThrottlerHealthCheckCells: []string{"cell1"},
			TxThrottlerTabletTypes: &topoproto.TabletTypeListFlag{
				topodatapb.TabletType_REPLICA,
				topodatapb.TabletType_RDONLY,
			},
		},
		{
			// enabled without tablet types
			Name:                        "enabled without tablet types",
			ExpectedErrorCode:           vtrpcpb.Code_FAILED_PRECONDITION,
			EnableTxThrottler:           true,
			TxThrottlerConfig:           &TxThrottlerConfigFlag{defaultMaxReplicationLagModuleConfig},
			TxThrottlerHealthCheckCells: []string{"cell1"},
			TxThrottlerTabletTypes:      &topoproto.TabletTypeListFlag{},
		},
		{
			// enabled + disallowed tablet type
			Name:                        "enabled disallowed tablet type",
			ExpectedErrorCode:           vtrpcpb.Code_INVALID_ARGUMENT,
			EnableTxThrottler:           true,
			TxThrottlerConfig:           &TxThrottlerConfigFlag{defaultMaxReplicationLagModuleConfig},
			TxThrottlerHealthCheckCells: []string{"cell1"},
			TxThrottlerTabletTypes:      &topoproto.TabletTypeListFlag{topodatapb.TabletType_DRAINED},
		},
		{
			// enabled + disallowed priority
			Name:                        "enabled disallowed priority",
			ExpectedErrorCode:           vtrpcpb.Code_INVALID_ARGUMENT,
			EnableTxThrottler:           true,
			TxThrottlerConfig:           &TxThrottlerConfigFlag{defaultMaxReplicationLagModuleConfig},
			TxThrottlerDefaultPriority:  12345,
			TxThrottlerHealthCheckCells: []string{"cell1"},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			config := defaultConfig
			config.EnableTxThrottler = test.EnableTxThrottler
			if test.TxThrottlerConfig == nil {
				test.TxThrottlerConfig = NewTxThrottlerConfigFlag()
			}
			config.TxThrottlerConfig = test.TxThrottlerConfig
			config.TxThrottlerHealthCheckCells = test.TxThrottlerHealthCheckCells
			config.TxThrottlerDefaultPriority = test.TxThrottlerDefaultPriority
			if test.TxThrottlerTabletTypes != nil {
				config.TxThrottlerTabletTypes = test.TxThrottlerTabletTypes
			}

			err := config.verifyTxThrottlerConfig()
			if test.ExpectedErrorCode == vtrpcpb.Code_OK {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
				assert.Equal(t, test.ExpectedErrorCode, vterrors.Code(err))
			}
		})
	}
}

func TestVerifyUnmanagedTabletConfig(t *testing.T) {
	oldDisableActiveReparents := mysqlctl.DisableActiveReparents
	defer func() {
		mysqlctl.DisableActiveReparents = oldDisableActiveReparents
	}()

	config := defaultConfig

	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	config.DB = dbconfigs.NewTestDBConfigs(*params, *params, "")

	// By default, unmanaged mode should be false
	err := config.verifyUnmanagedTabletConfig()
	assert.Nil(t, err)

	config.Unmanaged = true
	err = config.verifyUnmanagedTabletConfig()
	assert.EqualError(t, err, "no connection parameters specified but unmanaged mode specified")

	config.DB.Socket = db.ConnParams().UnixSocket
	err = config.verifyUnmanagedTabletConfig()
	assert.EqualError(t, err, "database app user not specified")

	config.DB.App.User = "testUser"
	err = config.verifyUnmanagedTabletConfig()
	assert.EqualError(t, err, "database app user password not specified")

	config.DB.App.Password = "testPassword"
	err = config.verifyUnmanagedTabletConfig()
	assert.Nil(t, err)
}
