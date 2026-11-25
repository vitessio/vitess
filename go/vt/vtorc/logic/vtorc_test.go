/*
Copyright 2026 The Vitess Authors.

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

package logic

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/process"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/vttablet/tmclienttest"
)

func TestWaitForLocksRelease(t *testing.T) {
	oldShutdownWaitTime := shutdownWaitTime
	// Restore initial values
	defer func() {
		shutdownWaitTime = oldShutdownWaitTime
	}()

	t.Run("No locks to wait for", func(t *testing.T) {
		// Initially when shardsLockCounter is zero, waitForLocksRelease should run immediately
		timeSpent := waitForLocksReleaseAndGetTimeWaitedFor()
		assert.Less(t, timeSpent, 1*time.Second, "waitForLocksRelease should run immediately if there are no locks to wait for")
	})

	t.Run("Timeout from shutdownWaitTime", func(t *testing.T) {
		// Increment shardsLockCounter to simulate locking of a shard
		shardsLockCounter.Add(1)
		defer func() {
			// Restore the initial value
			shardsLockCounter.Store(0)
		}()
		shutdownWaitTime = 200 * time.Millisecond
		timeSpent := waitForLocksReleaseAndGetTimeWaitedFor()
		assert.Greater(t, timeSpent, 100*time.Millisecond, "waitForLocksRelease should timeout after 200 milliseconds and not before")
		assert.Less(t, timeSpent, 300*time.Millisecond, "waitForLocksRelease should timeout after 200 milliseconds and not take any longer")
	})

	t.Run("Successful wait for locks release", func(t *testing.T) {
		// Increment shardsLockCounter to simulate locking of a shard
		shardsLockCounter.Add(1)
		shutdownWaitTime = 500 * time.Millisecond
		// Release the locks after 200 milliseconds
		go func() {
			time.Sleep(200 * time.Millisecond)
			shardsLockCounter.Store(0)
		}()
		timeSpent := waitForLocksReleaseAndGetTimeWaitedFor()
		assert.Greater(t, timeSpent, 100*time.Millisecond, "waitForLocksRelease should wait for the locks and not return early")
		assert.Less(t, timeSpent, 300*time.Millisecond, "waitForLocksRelease should be successful after 200 milliseconds as all the locks are released")
	})
}

func waitForLocksReleaseAndGetTimeWaitedFor() time.Duration {
	start := time.Now()
	waitForLocksRelease()
	return time.Since(start)
}

func TestRefreshAllInformation(t *testing.T) {
	// Store the old flags and restore on test completion
	oldTs := ts
	defer func() {
		ts = oldTs
	}()

	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	// Verify in the beginning, we have the first DiscoveredOnce field false.
	_, discoveredOnce := process.HealthTest()
	require.False(t, discoveredOnce)

	// Create a memory topo-server and create the keyspace and shard records
	ts = memorytopo.NewServer(t.Context(), cell1)
	_, err := ts.GetOrCreateShard(t.Context(), keyspace, shard)
	require.NoError(t, err)

	// Test error
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel context to simulate timeout
	require.Error(t, refreshAllInformation(ctx))
	require.False(t, process.FirstDiscoveryCycleComplete.Load())
	_, discoveredOnce = process.HealthTest()
	require.False(t, discoveredOnce)

	// Test success
	ctx2 := t.Context()
	require.NoError(t, refreshAllInformation(ctx2))
	require.True(t, process.FirstDiscoveryCycleComplete.Load())
	_, discoveredOnce = process.HealthTest()
	require.True(t, discoveredOnce)
}

var (
	discoveryTMC         = &testutil.TabletManagerClient{}
	registerDiscoveryTMC sync.Once
)

// TestDiscoverInstanceLogs checks the level of the Discovered line, that the
// replica's last SQL error, which can quote row values, is under
// last_sql_error and never in a message, and that a tablet that was shut down,
// which has no instance and no error, is logged without instance fields or
// error attributes.
func TestDiscoverInstanceLogs(t *testing.T) {
	t.Cleanup(db.ClearVTOrcDatabase)
	inst.InitializeForgetAliasesCache()
	prevKeys := recentDiscoveryOperationKeys
	recentDiscoveryOperationKeys = cache.New(config.GetInstancePollTime(), time.Second)
	t.Cleanup(func() { recentDiscoveryOperationKeys = prevKeys })

	const protocol = "vtorc-discover-instance-test"
	registerDiscoveryTMC.Do(func() {
		tmclient.RegisterTabletManagerClientFactory(protocol, func() tmclient.TabletManagerClient { return discoveryTMC })
	})
	// Setting and resetting the protocol writes the flags that the timer of a
	// failed instance read reads. Wait for an earlier test's timer, and for
	// this test's before the reset.
	waitForLastAttemptedCheck()
	resetProtocol := tmclienttest.SetProtocol(t.Name(), protocol)
	inst.InitializeTMC()
	t.Cleanup(func() {
		waitForLastAttemptedCheck()
		resetProtocol()
		inst.InitializeTMC()
	})

	// The instance-is-nil line is logged once a minute per tablet, so the
	// shut down tablet gets a new UID on each run of the test.
	offset := newRunUIDOffset()
	saveTablet := func(uid uint32, shutdown bool) *topodatapb.Tablet {
		if shutdown {
			uid += offset
		}
		tablet := &topodatapb.Tablet{
			Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: uid},
			Hostname:      "localhost",
			Keyspace:      "ks",
			Shard:         "0",
			Type:          topodatapb.TabletType_REPLICA,
			MysqlHostname: "localhost",
			MysqlPort:     6709,
		}
		if shutdown {
			tablet.TabletShutdownTime = &vttimepb.Time{Seconds: 1}
		}
		require.NoError(t, inst.SaveTablet(tablet))
		return tablet
	}
	discoveryTMC.FullStatusResult = &replicationdatapb.FullStatus{
		TabletType: topodatapb.TabletType_REPLICA,
		ReplicationStatus: &replicationdatapb.Status{
			LastSqlError: "Duplicate entry 'row-value-sentinel' for key 'PRIMARY'",
		},
	}

	t.Run("discovered", func(t *testing.T) {
		tablet := saveTablet(100, false)
		logs := logtest.Capture(t, slog.LevelInfo)
		DiscoverInstance(tablet.Alias, true)

		record := logs.Record(t, "Discovered")
		assert.Equal(t, "INFO", record["level"])
		assert.Contains(t, record["last_sql_error"], "row-value-sentinel")
		for _, record := range logs.Records(t) {
			assert.NotContains(t, record["msg"], "row-value-sentinel")
		}
	})

	t.Run("shut down tablet", func(t *testing.T) {
		// VTOrc doesn't poll a tablet that was shut down: there is no
		// instance and no error.
		tablet := saveTablet(300, true)
		logs := logtest.Capture(t, slog.LevelInfo)
		DiscoverInstance(tablet.Alias, true)

		record := logs.Record(t, "Discovered")
		assert.Equal(t, topoproto.TabletAliasString(tablet.Alias), record["tablet"])
		assert.NotContains(t, record, "mysql_hostname")

		record = logs.Record(t, "DiscoverInstance: instance is nil")
		assert.Equal(t, topoproto.TabletAliasString(tablet.Alias), record["tablet"])
		assert.NotContains(t, record, "error")
		assert.NotContains(t, record, "error_code")
	})

	t.Run("routine poll logs only on state change", func(t *testing.T) {
		// Save the shard record. WriteInstance skips persisting database_instance
		// when the shard lookup fails.
		require.NoError(t, inst.SaveShard(topo.NewShardInfo("ks", "0", &topodatapb.Shard{}, nil)))

		prevFullStatus := discoveryTMC.FullStatusResult
		t.Cleanup(func() { discoveryTMC.FullStatusResult = prevFullStatus })

		makeStatus := func(pos, filePos string, lag uint32, readOnly bool) *replicationdatapb.FullStatus {
			return &replicationdatapb.FullStatus{
				ServerId:      100,
				ServerUuid:    "11111111-1111-1111-1111-111111111111",
				TabletType:    topodatapb.TabletType_REPLICA,
				Version:       "8.0.36",
				ReadOnly:      readOnly,
				LogBinEnabled: true,
				BinlogFormat:  "ROW",
				GtidMode:      "ON",
				PrimaryStatus: &replicationdatapb.PrimaryStatus{
					Position:     pos,
					FilePosition: filePos,
				},
				ReplicationStatus: &replicationdatapb.Status{
					SourceHost:            "primary-host",
					SourcePort:            3306,
					SourceUuid:            "22222222-2222-2222-2222-222222222222",
					IoState:               3,
					SqlState:              3,
					AutoPosition:          true,
					ReplicationLagSeconds: lag,
					FilePosition:          filePos,
					RelayLogFilePosition:  filePos,
					SourceUser:            "vt_repl",
				},
			}
		}

		// Age the cached row and clear the in-memory throttle. DiscoverInstance
		// skips polling instances that are still within the poll window.
		expireInstance := func(t *testing.T, alias *topodatapb.TabletAlias, lastCheckedOffset, lastSeenOffset string) {
			t.Helper()
			res, err := db.ExecVTOrc(
				`UPDATE database_instance SET last_checked = DATETIME('now', ?), last_seen = DATETIME('now', ?) WHERE alias = ?`,
				lastCheckedOffset,
				lastSeenOffset,
				topoproto.TabletAliasString(alias),
			)
			require.NoError(t, err)
			rows, err := res.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), rows)
			recentDiscoveryOperationKeys.Flush()
		}

		// Persist a valid cached baseline for a dedicated tablet and expire it so
		// the next poll exercises the cached comparison path.
		seedBaseline := func(t *testing.T, uid uint32, readOnly bool) *topodatapb.Tablet {
			t.Helper()
			tablet := saveTablet(uid, false)
			discoveryTMC.FullStatusResult = makeStatus(
				"MySQL56/11111111-1111-1111-1111-111111111111:1-10",
				"FilePos/mysql-bin.000001:100",
				0,
				readOnly,
			)
			DiscoverInstance(tablet.Alias, false)
			expireInstance(t, tablet.Alias, "-60 SECOND", "-60 SECOND")

			cached, found, err := inst.ReadInstance(tablet.Alias)
			require.NoError(t, err)
			require.True(t, found)
			require.False(t, cached.IsUpToDate)
			require.True(t, cached.IsLastCheckValid)
			return tablet
		}

		t.Run("first discovery logs at INFO", func(t *testing.T) {
			tablet := saveTablet(401, false)
			discoveryTMC.FullStatusResult = makeStatus(
				"MySQL56/11111111-1111-1111-1111-111111111111:1-10",
				"FilePos/mysql-bin.000001:100",
				0,
				true,
			)
			recentDiscoveryOperationKeys.Flush()
			logs := logtest.Capture(t, slog.LevelInfo)
			DiscoverInstance(tablet.Alias, false)
			record := logs.Record(t, "Discovered")
			assert.Equal(t, "INFO", record["level"])
			assert.Equal(t, false, record["force_discovery"])
		})

		t.Run("steady-state routine poll skips INFO", func(t *testing.T) {
			tablet := seedBaseline(t, 402, true)
			discoveryTMC.FullStatusResult = makeStatus(
				"MySQL56/11111111-1111-1111-1111-111111111111:1-25",
				"FilePos/mysql-bin.000001:500",
				1,
				true,
			)
			infoLogs := logtest.Capture(t, slog.LevelInfo)
			DiscoverInstance(tablet.Alias, false)
			assert.Empty(t, infoLogs.Records(t), "steady-state routine poll should not emit INFO logs")
		})

		t.Run("steady-state routine poll logs at DEBUG when enabled", func(t *testing.T) {
			tablet := seedBaseline(t, 403, true)
			discoveryTMC.FullStatusResult = makeStatus(
				"MySQL56/11111111-1111-1111-1111-111111111111:1-40",
				"FilePos/mysql-bin.000001:900",
				2,
				true,
			)
			debugLogs := logtest.Capture(t, slog.LevelDebug)
			DiscoverInstance(tablet.Alias, false)
			debugRecord := debugLogs.Record(t, "Discovered")
			assert.Equal(t, "DEBUG", debugRecord["level"])
		})

		t.Run("operational state change logs at INFO", func(t *testing.T) {
			tablet := seedBaseline(t, 404, true)
			discoveryTMC.FullStatusResult = makeStatus(
				"MySQL56/11111111-1111-1111-1111-111111111111:1-45",
				"FilePos/mysql-bin.000001:1000",
				0,
				false,
			)
			changeLogs := logtest.Capture(t, slog.LevelInfo)
			DiscoverInstance(tablet.Alias, false)
			changeRecord := changeLogs.Record(t, "Discovered")
			assert.Equal(t, "INFO", changeRecord["level"])
			assert.Equal(t, false, changeRecord["read_only"])
		})

		t.Run("forced discovery with unchanged state logs at INFO", func(t *testing.T) {
			tablet := seedBaseline(t, 405, true)
			forcedLogs := logtest.Capture(t, slog.LevelInfo)
			DiscoverInstance(tablet.Alias, true)
			forcedRecord := forcedLogs.Record(t, "Discovered")
			assert.Equal(t, "INFO", forcedRecord["level"])
			assert.Equal(t, true, forcedRecord["force_discovery"])
		})

		t.Run("recovery from invalid last check logs at INFO", func(t *testing.T) {
			tablet := seedBaseline(t, 406, true)
			expireInstance(t, tablet.Alias, "-10 SECOND", "-60 SECOND")

			cached, found, err := inst.ReadInstance(tablet.Alias)
			require.NoError(t, err)
			require.True(t, found)
			require.False(t, cached.IsLastCheckValid)

			recoveryLogs := logtest.Capture(t, slog.LevelInfo)
			DiscoverInstance(tablet.Alias, false)
			recoveryRecord := recoveryLogs.Record(t, "Discovered")
			assert.Equal(t, "INFO", recoveryRecord["level"])
		})
	})
}
