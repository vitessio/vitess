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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vterrors"
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

// discoveryTabletManagerClient is the tablet manager client that the inst
// package uses in TestDiscoverInstanceLogs. FullStatus fails with
// fullStatusErr when it is set.
type discoveryTabletManagerClient struct {
	*testutil.TabletManagerClient
	fullStatusErr error
}

func (c *discoveryTabletManagerClient) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	if c.fullStatusErr != nil {
		return nil, c.fullStatusErr
	}
	return c.TabletManagerClient.FullStatus(ctx, tablet)
}

var (
	discoveryTMC         = &discoveryTabletManagerClient{TabletManagerClient: &testutil.TabletManagerClient{}}
	registerDiscoveryTMC sync.Once
)

// TestDiscoverInstanceLogs checks the lines DiscoverInstance logs. A
// discovered instance is logged field by field, with the replica's last SQL
// error, which can quote row values, under last_sql_error and never in the
// message.
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
		discoveryTMC.fullStatusErr = nil
	})

	// Each case uses its own tablet: the instance-is-nil line is logged once a
	// minute per tablet. VTOrc logs it for a tablet it can't read, one that was
	// shut down or whose FullStatus fails, so those tablets also get new UIDs
	// on each run of the test.
	offset := newRunUIDOffset()
	saveTablet := func(uid uint32, shutdown bool) *topodatapb.Tablet {
		if shutdown || discoveryTMC.fullStatusErr != nil {
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
	// assertDurations checks that the durations are integer nanoseconds: each
	// step of a discovery takes more than a microsecond.
	assertDurations := func(t *testing.T, record map[string]any, keys ...string) {
		for _, key := range keys {
			assert.GreaterOrEqual(t, record[key], float64(time.Microsecond), key)
		}
	}

	t.Run("discovered", func(t *testing.T) {
		tablet := saveTablet(100, false)
		logs := logtest.Capture(t, slog.LevelInfo)
		DiscoverInstance(tablet.Alias, true)

		// The instance's fields are checked in TestInstanceLogAttrs.
		record := logs.Record(t, "Discovered")
		assert.Equal(t, "INFO", record["level"])
		assert.Equal(t, "zone1-0000000100", record["tablet"])
		assert.Equal(t, true, record["force_discovery"])
		assert.Equal(t, "REPLICA", record["current_tablet_type"])
		assert.Contains(t, record["last_sql_error"], "Duplicate entry 'row-value-sentinel'")
		for _, record := range logs.Records(t) {
			assert.NotContains(t, record["msg"], "row-value-sentinel")
		}
	})

	t.Run("discovery error", func(t *testing.T) {
		discoveryTMC.fullStatusErr = vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "tablet is down")
		t.Cleanup(func() { discoveryTMC.fullStatusErr = nil })
		tablet := saveTablet(200, false)
		logs := logtest.Capture(t, slog.LevelInfo)
		DiscoverInstance(tablet.Alias, true)

		record := logs.Record(t, "Failed to discover")
		assert.Equal(t, "ERROR", record["level"])
		assert.Equal(t, topoproto.TabletAliasString(tablet.Alias), record["tablet"])
		assert.Equal(t, "tablet is down", record["error"])
		assert.Equal(t, "UNAVAILABLE", record["error_code"])

		record = logs.Record(t, "DiscoverInstance: instance is nil")
		assert.Equal(t, "WARN", record["level"])
		assert.Equal(t, "tablet is down", record["error"])
		assert.Equal(t, "UNAVAILABLE", record["error_code"])
		assertDurations(t, record, "duration", "backend_duration", "instance_duration")
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

	t.Run("slow discovery", func(t *testing.T) {
		tablet := saveTablet(400, false)
		prevPollTime := config.GetInstancePollTime()
		config.SetInstancePollTime(time.Nanosecond)
		t.Cleanup(func() { config.SetInstancePollTime(prevPollTime) })
		logs := logtest.Capture(t, slog.LevelInfo)
		DiscoverInstance(tablet.Alias, true)

		record := logs.Record(t, "DiscoverInstance exceeded --instance-poll-time")
		assert.Equal(t, "WARN", record["level"])
		assert.Equal(t, "zone1-0000000400", record["tablet"])
		assertDurations(t, record, "duration")
	})
}
