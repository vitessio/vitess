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

package logic

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

func TestAnalysisEntriesHaveSameRecovery(t *testing.T) {
	tests := []struct {
		prevAnalysisCode inst.AnalysisCode
		newAnalysisCode  inst.AnalysisCode
		shouldBeEqual    bool
	}{
		{
			// DeadPrimary and DeadPrimaryAndSomeReplicas have the same recovery
			prevAnalysisCode: inst.DeadPrimary,
			newAnalysisCode:  inst.DeadPrimaryAndSomeReplicas,
			shouldBeEqual:    true,
		}, {
			// DeadPrimary and StalledDiskPrimary have the same recovery
			prevAnalysisCode: inst.DeadPrimary,
			newAnalysisCode:  inst.PrimaryDiskStalled,
			shouldBeEqual:    true,
		}, {
			// PrimarySemiSyncBlocked and PrimaryDiskStalled have the same recovery
			prevAnalysisCode: inst.PrimarySemiSyncBlocked,
			newAnalysisCode:  inst.PrimaryDiskStalled,
			shouldBeEqual:    true,
		}, {
			// DeadPrimary and PrimaryTabletDeleted are different recoveries.
			prevAnalysisCode: inst.DeadPrimary,
			newAnalysisCode:  inst.PrimaryTabletDeleted,
			shouldBeEqual:    false,
		}, {
			// same codes will always have same recovery
			prevAnalysisCode: inst.DeadPrimary,
			newAnalysisCode:  inst.DeadPrimary,
			shouldBeEqual:    true,
		}, {
			prevAnalysisCode: inst.PrimaryHasPrimary,
			newAnalysisCode:  inst.DeadPrimaryAndSomeReplicas,
			shouldBeEqual:    false,
		}, {
			prevAnalysisCode: inst.DeadPrimary,
			newAnalysisCode:  inst.PrimaryHasPrimary,
			shouldBeEqual:    false,
		}, {
			prevAnalysisCode: inst.LockedSemiSyncPrimary,
			newAnalysisCode:  inst.PrimaryHasPrimary,
			shouldBeEqual:    false,
		}, {
			prevAnalysisCode: inst.PrimaryIsReadOnly,
			newAnalysisCode:  inst.PrimarySemiSyncMustNotBeSet,
			shouldBeEqual:    true,
		}, {
			prevAnalysisCode: inst.PrimarySemiSyncMustBeSet,
			newAnalysisCode:  inst.PrimarySemiSyncMustNotBeSet,
			shouldBeEqual:    true,
		}, {
			prevAnalysisCode: inst.PrimaryCurrentTypeMismatch,
			newAnalysisCode:  inst.PrimarySemiSyncMustNotBeSet,
			shouldBeEqual:    true,
		}, {
			prevAnalysisCode: inst.PrimaryIsReadOnly,
			newAnalysisCode:  inst.DeadPrimary,
			shouldBeEqual:    false,
		}, {
			prevAnalysisCode: inst.NotConnectedToPrimary,
			newAnalysisCode:  inst.ConnectedToWrongPrimary,
			shouldBeEqual:    true,
		}, {
			prevAnalysisCode: inst.ConnectedToWrongPrimary,
			newAnalysisCode:  inst.ReplicaIsWritable,
			shouldBeEqual:    true,
		},
	}
	t.Parallel()
	for _, tt := range tests {
		t.Run(string(tt.prevAnalysisCode)+","+string(tt.newAnalysisCode), func(t *testing.T) {
			res := analysisEntriesHaveSameRecovery(&inst.DetectionAnalysis{Analysis: tt.prevAnalysisCode}, &inst.DetectionAnalysis{Analysis: tt.newAnalysisCode})
			require.Equal(t, tt.shouldBeEqual, res)
		})
	}
}

func TestElectNewPrimaryPanic(t *testing.T) {
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	oldTs := ts
	defer func() {
		ts = oldTs
		_, err = orcDb.Exec("delete from vitess_tablet")
		require.NoError(t, err)
	}()

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
		Hostname:      "localhost",
		MysqlHostname: "localhost",
		MysqlPort:     1200,
		Keyspace:      "ks",
		Shard:         "-",
		Type:          topodatapb.TabletType_REPLICA,
	}
	err = inst.SaveTablet(tablet)
	require.NoError(t, err)
	analysisEntry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: topoproto.TabletAliasString(tablet.Alias),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts = memorytopo.NewServer(ctx, "zone1")
	recoveryAttempted, _, err := electNewPrimary(context.Background(), analysisEntry, log.NewPrefixedLogger("prefix"))
	require.True(t, recoveryAttempted)
	require.Error(t, err)
}

func TestRecoveryRegistration(t *testing.T) {
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	oldTs := ts
	defer func() {
		ts = oldTs
		_, err = orcDb.Exec("delete from vitess_tablet")
		require.NoError(t, err)
	}()

	primary := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  1,
		},
		Hostname:      "localhost1",
		MysqlHostname: "localhost1",
		MysqlPort:     1200,
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_PRIMARY,
	}
	replica := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  2,
		},
		Hostname:      "localhost2",
		MysqlHostname: "localhost2",
		MysqlPort:     1200,
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
	}
	err = inst.SaveTablet(primary)
	require.NoError(t, err)
	err = inst.SaveTablet(replica)
	require.NoError(t, err)
	primaryAnalysisEntry := inst.DetectionAnalysis{
		AnalyzedInstanceAlias: topoproto.TabletAliasString(primary.Alias),
		Analysis:              inst.ReplicationStopped,
	}
	replicaAnalysisEntry := inst.DetectionAnalysis{
		AnalyzedInstanceAlias: topoproto.TabletAliasString(replica.Alias),
		Analysis:              inst.DeadPrimary,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts = memorytopo.NewServer(ctx, "zone1")
	tp, err := AttemptRecoveryRegistration(&replicaAnalysisEntry)
	require.NoError(t, err)

	// because there is another recovery in progress for this shard, this will fail.
	_, err = AttemptRecoveryRegistration(&primaryAnalysisEntry)
	require.ErrorContains(t, err, "Active recovery")

	// Lets say the recovery finishes after some time.
	err = resolveRecovery(tp, nil)
	require.NoError(t, err)

	// now this recovery registration should be successful.
	_, err = AttemptRecoveryRegistration(&primaryAnalysisEntry)
	require.NoError(t, err)
}

func TestGetCheckAndRecoverFunctionCode(t *testing.T) {
	tests := []struct {
		name                         string
		ersEnabled                   bool
		convertTabletWithErrantGTIDs bool
		analysisCode                 inst.AnalysisCode
		wantRecoveryFunction         recoveryFunction
	}{
		{
			name:                 "DeadPrimary with ERS enabled",
			ersEnabled:           true,
			analysisCode:         inst.DeadPrimary,
			wantRecoveryFunction: recoverDeadPrimaryFunc,
		}, {
			name:                 "DeadPrimary with ERS disabled",
			ersEnabled:           false,
			analysisCode:         inst.DeadPrimary,
			wantRecoveryFunction: noRecoveryFunc,
		}, {
			name:                 "StalledDiskPrimary with ERS enabled",
			ersEnabled:           true,
			analysisCode:         inst.PrimaryDiskStalled,
			wantRecoveryFunction: recoverDeadPrimaryFunc,
		}, {
			name:                 "StalledDiskPrimary with ERS disabled",
			ersEnabled:           false,
			analysisCode:         inst.PrimaryDiskStalled,
			wantRecoveryFunction: noRecoveryFunc,
		}, {
			name:                 "PrimarySemiSyncBlocked with ERS enabled",
			ersEnabled:           true,
			analysisCode:         inst.PrimarySemiSyncBlocked,
			wantRecoveryFunction: recoverDeadPrimaryFunc,
		}, {
			name:                 "PrimarySemiSyncBlocked with ERS disabled",
			ersEnabled:           false,
			analysisCode:         inst.PrimarySemiSyncBlocked,
			wantRecoveryFunction: noRecoveryFunc,
		}, {
			name:                 "PrimaryTabletDeleted with ERS enabled",
			ersEnabled:           true,
			analysisCode:         inst.PrimaryTabletDeleted,
			wantRecoveryFunction: recoverPrimaryTabletDeletedFunc,
		}, {
			name:                 "PrimaryTabletDeleted with ERS disabled",
			ersEnabled:           false,
			analysisCode:         inst.PrimaryTabletDeleted,
			wantRecoveryFunction: noRecoveryFunc,
		}, {
			name:                 "PrimaryHasPrimary",
			ersEnabled:           false,
			analysisCode:         inst.PrimaryHasPrimary,
			wantRecoveryFunction: recoverPrimaryHasPrimaryFunc,
		}, {
			name:                 "ClusterHasNoPrimary",
			ersEnabled:           false,
			analysisCode:         inst.ClusterHasNoPrimary,
			wantRecoveryFunction: electNewPrimaryFunc,
		}, {
			name:                 "ReplicationStopped",
			ersEnabled:           false,
			analysisCode:         inst.ReplicationStopped,
			wantRecoveryFunction: fixReplicaFunc,
		}, {
			name:                 "PrimarySemiSyncMustBeSet",
			ersEnabled:           false,
			analysisCode:         inst.PrimarySemiSyncMustBeSet,
			wantRecoveryFunction: fixPrimaryFunc,
		}, {
			name:                         "ErrantGTIDDetected",
			ersEnabled:                   false,
			convertTabletWithErrantGTIDs: true,
			analysisCode:                 inst.ErrantGTIDDetected,
			wantRecoveryFunction:         recoverErrantGTIDDetectedFunc,
		}, {
			name:                         "ErrantGTIDDetected with --change-tablets-with-errant-gtid-to-drained false",
			ersEnabled:                   false,
			convertTabletWithErrantGTIDs: false,
			analysisCode:                 inst.ErrantGTIDDetected,
			wantRecoveryFunction:         noRecoveryFunc,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prevVal := config.ERSEnabled()
			config.SetERSEnabled(tt.ersEnabled)
			defer config.SetERSEnabled(prevVal)

			convertErrantVal := config.ConvertTabletWithErrantGTIDs()
			config.SetConvertTabletWithErrantGTIDs(tt.convertTabletWithErrantGTIDs)
			defer config.SetConvertTabletWithErrantGTIDs(convertErrantVal)

			gotFunc := getCheckAndRecoverFunctionCode(tt.analysisCode, "")
			require.EqualValues(t, tt.wantRecoveryFunction, gotFunc)
		})
	}
}

func TestRecoverShardAnalyses(t *testing.T) {
	// DeadPrimary and PrimaryHasPrimary have detectionAnalysisPriorityShardWideAction,
	// so they require ordered execution. ReplicationStopped and ReplicaIsWritable are
	// medium priority with no shard-wide action or before/after dependencies,
	// so they run concurrently.
	analyses := []*inst.DetectionAnalysis{
		{Analysis: inst.ReplicationStopped, AnalyzedInstanceAlias: "replica1"},
		{Analysis: inst.DeadPrimary, AnalyzedInstanceAlias: "primary1"},
		{Analysis: inst.ReplicaIsWritable, AnalyzedInstanceAlias: "replica2"},
		{Analysis: inst.PrimaryHasPrimary, AnalyzedInstanceAlias: "primary2"},
	}

	var mu sync.Mutex
	var order []inst.AnalysisCode
	recoverFunc := func(entry *inst.DetectionAnalysis) error {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, entry.Analysis)
		return nil
	}

	recoverShardAnalyses(analyses, recoverFunc)

	require.Len(t, order, 4)
	// Ordered recoveries must come first, in their original order.
	require.Equal(t, inst.DeadPrimary, order[0])
	require.Equal(t, inst.PrimaryHasPrimary, order[1])
	// Concurrent recoveries come after, in any order.
	require.ElementsMatch(t, []inst.AnalysisCode{inst.ReplicationStopped, inst.ReplicaIsWritable}, order[2:])
}

// TestReconcileStaleTopoPrimary verifies that reconcileStaleTopoPrimary updates the topology record of a
// stale primary tablet to REPLICA, regardless of whether the best-effort demotion RPC to the tablet succeeds.
func TestReconcileStaleTopoPrimary(t *testing.T) {
	tests := []struct {
		name string

		// demotePrimaryErr is whether the DemotePrimary RPC should return an error.
		demotePrimaryErr error

		// demotePrimaryDelay is the delay the DemotePrimary RPC should take before returning.
		demotePrimaryDelay time.Duration

		// topoAlreadyReplica seeds the stale tablet in topo as REPLICA with no primary term.
		topoAlreadyReplica bool
	}{
		{
			name: "tablet reachable, demotion succeeds",
		},
		{
			name:               "tablet unreachable, demotion times out",
			demotePrimaryDelay: time.Second,
		},
		{
			name:             "tablet reachable, demotion fails",
			demotePrimaryErr: errors.New("injected demote error"),
		},
		{
			name:               "topo already replica, no update needed",
			demotePrimaryErr:   errors.New("injected demote error"),
			topoAlreadyReplica: true,
		},
	}

	oldRemoteOperationTimeout := topo.RemoteOperationTimeout
	topo.RemoteOperationTimeout = 25 * time.Millisecond
	t.Cleanup(func() {
		topo.RemoteOperationTimeout = oldRemoteOperationTimeout
	})

	orcDB, err := db.OpenVTOrc()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, orcDB.Close())
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, table := range []string{"topology_recovery_steps", "topology_recovery", "recovery_detection", "vitess_tablet", "vitess_keyspace"} {
				_, err = orcDB.Exec("delete from " + table)
				require.NoError(t, err)
			}

			const (
				keyspace = "ks"
				shard    = "0"
			)

			primaryTablet := &topodatapb.Tablet{
				Alias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
				Hostname:             "primary",
				MysqlHostname:        "primary",
				MysqlPort:            3306,
				Keyspace:             keyspace,
				Shard:                shard,
				Type:                 topodatapb.TabletType_PRIMARY,
				PrimaryTermStartTime: &vttime.Time{Seconds: 1000},
				PortMap:              map[string]int32{"vt": 15100, "grpc": 15101},
			}

			staleTablet := &topodatapb.Tablet{
				Alias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
				Hostname:             "stale-primary",
				MysqlHostname:        "stale-primary",
				MysqlPort:            3306,
				Keyspace:             keyspace,
				Shard:                shard,
				Type:                 topodatapb.TabletType_PRIMARY,
				PrimaryTermStartTime: &vttime.Time{Seconds: 500},
				PortMap:              map[string]int32{"vt": 15200, "grpc": 15201},
			}

			require.NoError(t, inst.SaveTablet(primaryTablet))
			require.NoError(t, inst.SaveTablet(staleTablet))

			keyspaceInfo := &topo.KeyspaceInfo{
				Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone},
			}
			keyspaceInfo.SetKeyspaceName(keyspace)
			require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

			ctx := t.Context()

			oldTS := ts
			oldTMC := tmc
			t.Cleanup(func() {
				ts = oldTS
				tmc = oldTMC
			})

			ts = memorytopo.NewServer(ctx, "zone1")
			require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
			require.NoError(t, ts.CreateShard(ctx, keyspace, shard))
			require.NoError(t, ts.CreateTablet(ctx, primaryTablet))

			if tt.topoAlreadyReplica {
				staleTablet.Type = topodatapb.TabletType_REPLICA
				staleTablet.PrimaryTermStartTime = nil
			}

			require.NoError(t, ts.CreateTablet(ctx, staleTablet))

			staleAlias := topoproto.TabletAliasString(staleTablet.Alias)
			tmc = &testutil.TabletManagerClient{
				DemotePrimaryDelays: map[string]time.Duration{
					staleAlias: tt.demotePrimaryDelay,
				},
				DemotePrimaryResults: map[string]struct {
					Status *replicationdatapb.PrimaryStatus
					Error  error
				}{
					staleAlias: {
						Status: &replicationdatapb.PrimaryStatus{},
						Error:  tt.demotePrimaryErr,
					},
				},
				SetReplicationSourceResults: map[string]error{
					staleAlias: nil,
				},
			}

			analysisEntry := &inst.DetectionAnalysis{
				Analysis:              inst.StaleTopoPrimary,
				AnalyzedInstanceAlias: topoproto.TabletAliasString(staleTablet.Alias),
				AnalyzedKeyspace:      keyspace,
				AnalyzedShard:         shard,
			}

			logger := log.NewPrefixedLogger("test-stale-primary")
			start := time.Now()
			attempted, topologyRecovery, err := reconcileStaleTopoPrimary(ctx, analysisEntry, logger)

			require.True(t, attempted, "recovery must be attempted")
			require.NoError(t, err, "topo update must succeed")
			require.NotNil(t, topologyRecovery, "topology recovery record must be returned")
			require.Less(t, time.Since(start), time.Second, "recovery should not wait for the demotion delay to elapse")

			updatedTablet, err := ts.GetTablet(ctx, staleTablet.Alias)
			require.NoError(t, err)
			require.Equal(t, topodatapb.TabletType_REPLICA, updatedTablet.Type, "stale primary must be updated to REPLICA in topo")

			activeRecoveries, err := ReadActiveClusterRecoveries(keyspace, shard)
			require.NoError(t, err)
			require.Empty(t, activeRecoveries, "recovery row must be resolved after reconcileStaleTopoPrimary returns")
		})
	}
}
