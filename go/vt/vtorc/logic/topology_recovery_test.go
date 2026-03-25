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
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/test"
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
	keyspace := "ks1"
	shard := "-"
	tests := []struct {
		name                         string
		ersEnabled                   bool
		convertTabletWithErrantGTIDs bool
		analysisEntry                *inst.DetectionAnalysis
		wantRecoveryFunction         recoveryFunction
		wantRecoverySkipCode         RecoverySkipCode
	}{
		{
			name:       "DeadPrimary with ERS enabled",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.DeadPrimary,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
		}, {
			name:       "DeadPrimary with ERS disabled",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.DeadPrimary,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
			wantRecoverySkipCode: RecoverySkipERSDisabled,
		}, {
			name:       "StalledDiskPrimary with ERS enabled",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimaryDiskStalled,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
		}, {
			name:       "StalledDiskPrimary with ERS disabled",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimaryDiskStalled,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
			wantRecoverySkipCode: RecoverySkipERSDisabled,
		}, {
			name:       "PrimarySemiSyncBlocked with ERS enabled",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimarySemiSyncBlocked,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
		}, {
			name:       "PrimarySemiSyncBlocked with ERS disabled",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimarySemiSyncBlocked,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
			wantRecoverySkipCode: RecoverySkipERSDisabled,
		}, {
			name:       "PrimaryTabletDeleted with ERS enabled",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimaryTabletDeleted,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverPrimaryTabletDeletedFunc,
		}, {
			name:       "PrimaryTabletDeleted with ERS disabled",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimaryTabletDeleted,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverPrimaryTabletDeletedFunc,
			wantRecoverySkipCode: RecoverySkipERSDisabled,
		}, {
			name:       "PrimaryHasPrimary",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimaryHasPrimary,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverPrimaryHasPrimaryFunc,
		}, {
			name:       "ClusterHasNoPrimary",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.ClusterHasNoPrimary,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: electNewPrimaryFunc,
		}, {
			name:       "ReplicationStopped",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.ReplicationStopped,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: fixReplicaFunc,
		}, {
			name:       "PrimarySemiSyncMustBeSet",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimarySemiSyncMustBeSet,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: fixPrimaryFunc,
		}, {
			name:                         "ErrantGTIDDetected",
			ersEnabled:                   false,
			convertTabletWithErrantGTIDs: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.ErrantGTIDDetected,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverErrantGTIDDetectedFunc,
		}, {
			name:                         "ErrantGTIDDetected with --change-tablets-with-errant-gtid-to-drained false",
			ersEnabled:                   false,
			convertTabletWithErrantGTIDs: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.ErrantGTIDDetected,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverErrantGTIDDetectedFunc,
			wantRecoverySkipCode: RecoverySkipNoRecoveryAction,
		}, {
			name:       "DeadPrimary with global ERS enabled and keyspace ERS disabled",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.DeadPrimary,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
				AnalyzedKeyspaceEmergencyReparentDisabled: true,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
			wantRecoverySkipCode: RecoverySkipERSDisabled,
		}, {
			name:       "DeadPrimary with global+keyspace ERS enabled and shard ERS disabled",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:                               inst.DeadPrimary,
				AnalyzedKeyspace:                       keyspace,
				AnalyzedShard:                          shard,
				AnalyzedShardEmergencyReparentDisabled: true,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
			wantRecoverySkipCode: RecoverySkipERSDisabled,
		}, {
			name:       "UnreachablePrimary",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:                               inst.UnreachablePrimary,
				AnalyzedKeyspace:                       keyspace,
				AnalyzedShard:                          shard,
				AnalyzedShardEmergencyReparentDisabled: true,
			},
			wantRecoveryFunction: restartArbitraryDirectReplicaFunc,
		}, {
			name:       "UnreachablePrimaryWithBrokenReplicas",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:                               inst.UnreachablePrimaryWithBrokenReplicas,
				AnalyzedKeyspace:                       keyspace,
				AnalyzedShard:                          shard,
				AnalyzedShardEmergencyReparentDisabled: true,
			},
			wantRecoveryFunction: restartAllDirectReplicasFunc,
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

			gotFunc, recoverySkipCode := getCheckAndRecoverFunctionCode(tt.analysisEntry)
			require.EqualValues(t, tt.wantRecoveryFunction, gotFunc)
			require.EqualValues(t, tt.wantRecoverySkipCode.String(), recoverySkipCode.String())
		})
	}
}

func TestRecheckPrimaryHealth(t *testing.T) {
	tests := []struct {
		name    string
		info    []*test.InfoForRecoveryAnalysis
		wantErr string
	}{
		{
			name: "analysis change",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy:              "none",
				LastCheckValid:                0,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 0,
			}},
			wantErr: "aborting ReplicationStopped, primary mitigation is required",
		},
		{
			name: "analysis did not change",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:              policy.DurabilityNone,
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}, {
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: policy.DurabilityNone,
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
				},
				LastCheckValid:     1,
				ReadOnly:           1,
				ReplicationStopped: 1,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// reset vtorc db after every test
			oldDB := db.Db
			defer func() {
				db.Db = oldDB
			}()

			var rowMaps []sqlutils.RowMap
			for _, analysis := range tt.info {
				analysis.SetValuesFromTabletInfo()
				rowMaps = append(rowMaps, analysis.ConvertToRowMap())
			}

			// set replication analysis in Vtorc DB.
			db.Db = test.NewTestDB([][]sqlutils.RowMap{rowMaps})

			err := recheckPrimaryHealth(&inst.DetectionAnalysis{
				AnalyzedInstanceAlias: "zon1-0000000100",
				Analysis:              inst.ReplicationStopped,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}, []string{"ks", "0", ""}, func(s string, b bool) {
				// the implementation for DiscoverInstance is not required because we are mocking the db response.
			})

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
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

// TestRestartDirectReplicasTimeout verifies that restartDirectReplicas does not block forever if an RPC hangs.
func TestRestartDirectReplicasTimeout(t *testing.T) {
	// Ensure the configuration is marked as loaded so that the background
	// initializeInstanceDao goroutine can proceed and initialize the
	// forgetAliases cache (needed by inst.WriteInstance).
	config.MarkConfigurationLoaded()
	time.Sleep(100 * time.Millisecond)

	orcDB, err := db.OpenVTOrc()
	require.NoError(t, err)

	synctest.Test(t, func(t *testing.T) {
		for _, table := range []string{"topology_recovery_steps", "topology_recovery", "recovery_detection", "vitess_tablet", "vitess_keyspace", "database_instance"} {
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
			PrimaryTermStartTime: &vttimepb.Time{Seconds: 1000},
			PortMap:              map[string]int32{"vt": 15100, "grpc": 15101},
		}

		replicaTablet := &topodatapb.Tablet{
			Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
			Hostname:      "replica",
			MysqlHostname: "replica",
			MysqlPort:     3306,
			Keyspace:      keyspace,
			Shard:         shard,
			Type:          topodatapb.TabletType_REPLICA,
			PortMap:       map[string]int32{"vt": 15200, "grpc": 15201},
		}

		require.NoError(t, inst.SaveTablet(primaryTablet))
		require.NoError(t, inst.SaveTablet(replicaTablet))

		keyspaceInfo := &topo.KeyspaceInfo{
			Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone},
		}
		keyspaceInfo.SetKeyspaceName(keyspace)
		require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

		require.NoError(t, inst.WriteInstance(&inst.Instance{
			InstanceAlias:    topoproto.TabletAliasString(replicaTablet.Alias),
			Hostname:         "replica",
			Port:             3306,
			SourceHost:       "primary",
			SourcePort:       3306,
			ReplicationDepth: 1,
		}, true, nil))

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
		require.NoError(t, ts.CreateTablet(ctx, replicaTablet))

		urgentOperations.Flush()

		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		// Simulate a replication RPC that never returns on its own. The call only unblocks
		// when the passed context is canceled.
		mockTMC := tmcmock.NewMockTabletManagerClient(mockController)
		mockTMC.EXPECT().
			StopReplication(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, _ *topodatapb.Tablet) error {
				<-ctx.Done()
				return ctx.Err()
			}).
			Times(1)

		tmc = mockTMC

		analysisEntry := &inst.DetectionAnalysis{
			Analysis:              inst.UnreachablePrimary,
			AnalyzedInstanceAlias: topoproto.TabletAliasString(primaryTablet.Alias),
			AnalyzedKeyspace:      keyspace,
			AnalyzedShard:         shard,
		}

		logger := log.NewPrefixedLogger("test-restart-replicas-hang")

		type restartDirectReplicasResult struct {
			attempted        bool
			topologyRecovery *TopologyRecovery
			err              error
		}

		ctx, cancel := context.WithCancel(ctx)
		t.Cleanup(func() {
			cancel()
			synctest.Wait()
		})

		// Run the recovery in a separate goroutine and collect its result.
		resultCh := make(chan restartDirectReplicasResult, 1)
		go func() {
			attempted, topologyRecovery, err := restartDirectReplicas(ctx, analysisEntry, 0, logger)
			resultCh <- restartDirectReplicasResult{
				attempted:        attempted,
				topologyRecovery: topologyRecovery,
				err:              err,
			}
		}()

		// Let the recovery goroutine reach a blocked state before advancing fake time (in this case,
		// hanging on the StopReplication RPC).
		synctest.Wait()

		// Move fake time just beyond the expected RPC timeout boundary.
		time.Sleep(topo.RemoteOperationTimeout + time.Nanosecond)
		synctest.Wait()

		// The recovery should now have returned with context.DeadlineExceeded.
		select {
		case result := <-resultCh:
			require.True(t, result.attempted, "recovery must be attempted")
			require.NotNil(t, result.topologyRecovery, "topology recovery record must be returned")
			require.ErrorIs(t, result.err, context.DeadlineExceeded, "restartDirectReplicas must timeout and return when a replication RPC hangs indefinitely")
		default:
			require.FailNowf(t, "restartDirectReplicas did not return", "expected timeout after %s when a replication RPC hangs indefinitely", topo.RemoteOperationTimeout)
		}

		activeRecoveries, err := ReadActiveClusterRecoveries(keyspace, shard)
		require.NoError(t, err)
		require.Empty(t, activeRecoveries, "recovery row must be resolved after restartDirectReplicas returns")
	})
}
