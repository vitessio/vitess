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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/sys/unix"

	cache "github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/test"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
	tmcmock "vitess.io/vitess/go/vt/vttablet/tmclient/mock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

type writerFunc func([]byte) (int, error)

func (wf writerFunc) Write(p []byte) (int, error) {
	return wf(p)
}

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
	orcDb, fromCache, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	defer func() {
		if !fromCache {
			require.NoError(t, orcDb.Close())
		}
	}()
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
		AnalyzedInstanceAlias: tablet.Alias,
	}
	ctx := t.Context()

	ts = memorytopo.NewServer(ctx, "zone1")
	recoveryAttempted, _, err := electNewPrimary(t.Context(), analysisEntry, log.NewPrefixedLogger("prefix"))
	require.True(t, recoveryAttempted)
	require.Error(t, err)
}

func TestRecoveryRegistration(t *testing.T) {
	orcDb, fromCache, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	defer func() {
		if !fromCache {
			require.NoError(t, orcDb.Close())
		}
	}()
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
		AnalyzedInstanceAlias: primary.Alias,
		Analysis:              inst.ReplicationStopped,
	}
	replicaAnalysisEntry := inst.DetectionAnalysis{
		AnalyzedInstanceAlias: replica.Alias,
		Analysis:              inst.DeadPrimary,
	}
	ctx := t.Context()

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
			name:       "IncapacitatedPrimary",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:               inst.IncapacitatedPrimary,
				AnalyzedKeyspace:       keyspace,
				AnalyzedShard:          shard,
				PrimaryHealthUnhealthy: true,
				LastCheckValid:         true,
			},
			wantRecoveryFunction: recoverIncapacitatedPrimaryFunc,
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
			name:       "PrimaryTabletUnreachableByQuorum with ERS enabled",
			ersEnabled: true,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimaryTabletUnreachableByQuorum,
				AnalyzedKeyspace: keyspace,
				AnalyzedShard:    shard,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
		}, {
			name:       "PrimaryTabletUnreachableByQuorum with ERS disabled",
			ersEnabled: false,
			analysisEntry: &inst.DetectionAnalysis{
				Analysis:         inst.PrimaryTabletUnreachableByQuorum,
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
			require.Equal(t, tt.wantRecoveryFunction, gotFunc)
			require.Equal(t, tt.wantRecoverySkipCode.String(), recoverySkipCode.String())
		})
	}
}

func TestRecheckPrimaryHealth(t *testing.T) {
	tests := []struct {
		name          string
		info          []*test.InfoForRecoveryAnalysis
		analysis      inst.AnalysisCode
		analyzedAlias *topodatapb.TabletAlias
		wantErr       string
	}{
		{
			name: "analysis change",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
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
			// PrimarySemiSyncBlocked on the primary, acker replica has
			// ReplicationStopped. GetDetectionAnalysis preserves the
			// acker's analysis (via declaresBefore), so checkIfAlreadyFixed
			// finds it and returns alreadyFixed=false → proceed.
			name: "acker ReplicationStopped preserved despite shard-wide PrimarySemiSyncBlocked",
			info: []*test.InfoForRecoveryAnalysis{
				{
					TabletInfo: &topodatapb.Tablet{
						Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
						Hostname:      "localhost",
						Keyspace:      "ks",
						Shard:         "0",
						Type:          topodatapb.TabletType_PRIMARY,
						MysqlHostname: "localhost",
						MysqlPort:     6708,
					},
					DurabilityPolicy:                   policy.DurabilitySemiSync,
					LastCheckValid:                     1,
					CountReplicas:                      1,
					CountValidReplicas:                 1,
					CountValidReplicatingReplicas:      0,
					CountValidOracleGTIDReplicas:       1,
					CountLoggingReplicas:               1,
					IsPrimary:                          1,
					CurrentTabletType:                  int(topodatapb.TabletType_PRIMARY),
					SemiSyncPrimaryEnabled:             1,
					SemiSyncPrimaryStatus:              1,
					SemiSyncBlocked:                    1,
					SemiSyncPrimaryWaitForReplicaCount: 1,
					SemiSyncPrimaryClients:             0,
					CountSemiSyncReplicasEnabled:       1,
				},
				{
					TabletInfo: &topodatapb.Tablet{
						Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
						Hostname:      "localhost",
						Keyspace:      "ks",
						Shard:         "0",
						Type:          topodatapb.TabletType_REPLICA,
						MysqlHostname: "localhost",
						MysqlPort:     6709,
					},
					DurabilityPolicy: policy.DurabilitySemiSync,
					PrimaryTabletInfo: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
					},
					LastCheckValid:         1,
					ReadOnly:               1,
					ReplicationStopped:     1,
					SemiSyncReplicaEnabled: 1,
				},
			},
		},
		{
			// PrimaryIsReadOnly on a primary that is also detected as
			// PrimarySemiSyncBlocked. GetDetectionAnalysis preserves the
			// primary read-only analysis (via declaresBefore), so
			// checkIfAlreadyFixed finds it and recovery proceeds.
			name:          "PrimaryIsReadOnly preserved despite shard-wide PrimarySemiSyncBlocked",
			analysis:      inst.PrimaryIsReadOnly,
			analyzedAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
			info: []*test.InfoForRecoveryAnalysis{
				{
					TabletInfo: &topodatapb.Tablet{
						Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
						Hostname:      "localhost",
						Keyspace:      "ks",
						Shard:         "0",
						Type:          topodatapb.TabletType_PRIMARY,
						MysqlHostname: "localhost",
						MysqlPort:     6708,
					},
					DurabilityPolicy:                   policy.DurabilitySemiSync,
					LastCheckValid:                     1,
					CountReplicas:                      1,
					CountValidReplicas:                 1,
					CountValidReplicatingReplicas:      1,
					CountValidOracleGTIDReplicas:       1,
					CountLoggingReplicas:               1,
					IsPrimary:                          1,
					ReadOnly:                           1,
					CurrentTabletType:                  int(topodatapb.TabletType_PRIMARY),
					SemiSyncPrimaryEnabled:             1,
					SemiSyncPrimaryStatus:              1,
					SemiSyncBlocked:                    1,
					SemiSyncPrimaryWaitForReplicaCount: 1,
					SemiSyncPrimaryClients:             0,
					CountSemiSyncReplicasEnabled:       1,
				},
				{
					TabletInfo: &topodatapb.Tablet{
						Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
						Hostname:      "localhost",
						Keyspace:      "ks",
						Shard:         "0",
						Type:          topodatapb.TabletType_REPLICA,
						MysqlHostname: "localhost",
						MysqlPort:     6709,
					},
					DurabilityPolicy: policy.DurabilitySemiSync,
					PrimaryTabletInfo: &topodatapb.Tablet{
						Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
					},
					LastCheckValid:         1,
					ReadOnly:               1,
					SemiSyncReplicaEnabled: 1,
				},
			},
		},
		{
			name: "analysis did not change",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
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
					Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: policy.DurabilityNone,
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
				},
				LastCheckValid:     1,
				ReadOnly:           1,
				ReplicationStopped: 1,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldDB := db.Db
			defer func() {
				db.Db = oldDB
			}()

			var rowMaps []sqlutils.RowMap
			for _, analysis := range tt.info {
				analysis.SetValuesFromTabletInfo()
				rowMaps = append(rowMaps, analysis.ConvertToRowMap())
			}
			db.Db = test.NewTestDB([][]sqlutils.RowMap{rowMaps})

			analysis := tt.analysis
			if analysis == "" {
				analysis = inst.ReplicationStopped
			}
			analyzedAlias := tt.analyzedAlias
			if analyzedAlias == nil {
				analyzedAlias = &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
			}

			err := recheckPrimaryHealth(&inst.DetectionAnalysis{
				AnalyzedInstanceAlias: analyzedAlias,
				Analysis:              analysis,
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			}, []string{"ks", "0", ""}, func(*topodatapb.TabletAlias, bool) {
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

func TestShardWideRecoveryIgnoredTablets(t *testing.T) {
	primaryAlias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}

	tests := []struct {
		name        string
		analysis    inst.AnalysisCode
		wantIgnored bool
	}{
		{
			name:        "DeadPrimary skips primary refresh",
			analysis:    inst.DeadPrimary,
			wantIgnored: true,
		},
		{
			name:        "DeadPrimaryAndSomeReplicas skips primary refresh",
			analysis:    inst.DeadPrimaryAndSomeReplicas,
			wantIgnored: true,
		},
		{
			name:        "PrimarySemiSyncBlocked does NOT skip primary refresh",
			analysis:    inst.PrimarySemiSyncBlocked,
			wantIgnored: false,
		},
		{
			name:        "PrimaryDiskStalled does NOT skip primary refresh",
			analysis:    inst.PrimaryDiskStalled,
			wantIgnored: false,
		},
		{
			// The quorum case is specifically a vttablet crash with mysqld
			// still up: the vttablet may restart between detection and
			// recovery, so the primary must be refreshed under the shard
			// lock for checkIfAlreadyFixed to abort on a recovered primary.
			name:        "PrimaryTabletUnreachableByQuorum does NOT skip primary refresh",
			analysis:    inst.PrimaryTabletUnreachableByQuorum,
			wantIgnored: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &inst.DetectionAnalysis{
				Analysis:              tt.analysis,
				AnalyzedInstanceAlias: primaryAlias,
			}
			ignored := shardWideRecoveryIgnoredTablets(recoverDeadPrimaryFunc, entry)
			if tt.wantIgnored {
				require.Len(t, ignored, 1)
				assert.True(t, topoproto.TabletAliasEqual(ignored[0], primaryAlias))
			} else {
				assert.Empty(t, ignored)
			}
		})
	}
}

func TestRecoverShardAnalyses(t *testing.T) {
	// DeadPrimary and PrimaryHasPrimary have detectionAnalysisPriorityShardWideAction,
	// so they require ordered execution. ReplicationStopped requires ordered execution
	// because it declares BeforeAnalyses: [PrimarySemiSyncBlocked]. ReplicaIsWritable
	// has no dependencies, so it runs concurrently.
	analyses := []*inst.DetectionAnalysis{
		{Analysis: inst.ReplicationStopped, AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 1}},
		{Analysis: inst.DeadPrimary, AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 2}},
		{Analysis: inst.ReplicaIsWritable, AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 3}},
		{Analysis: inst.PrimaryHasPrimary, AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 4}},
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
	require.Equal(t, inst.ReplicationStopped, order[0])
	require.Equal(t, inst.DeadPrimary, order[1])
	require.Equal(t, inst.PrimaryHasPrimary, order[2])
	// Concurrent recoveries come after.
	require.Equal(t, inst.ReplicaIsWritable, order[3])
}

func TestRecoverIncapacitatedPrimary(t *testing.T) {
	tests := []struct {
		name        string
		analysis    *inst.DetectionAnalysis
		pingOK      bool
		wantAttempt bool
		setupDB     bool
		rows        int
		prsFails    bool
	}{
		{
			name: "reachable ping (prs failure)",
			analysis: &inst.DetectionAnalysis{
				Analysis:              inst.IncapacitatedPrimary,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
				LastCheckValid:        true,
			},
			pingOK:      true,
			wantAttempt: true,
			setupDB:     true,
			rows:        3,
			prsFails:    true,
		},
		{
			name: "reachable ping (ers fallback)",
			analysis: &inst.DetectionAnalysis{
				Analysis:              inst.IncapacitatedPrimary,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
				LastCheckValid:        false,
			},
			pingOK:      true,
			wantAttempt: true,
			setupDB:     true,
			rows:        3,
			prsFails:    true,
		},
		{
			name: "reachable ping (prs ok)",
			analysis: &inst.DetectionAnalysis{
				Analysis:              inst.IncapacitatedPrimary,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
				LastCheckValid:        true,
			},
			pingOK:      true,
			wantAttempt: true,
			setupDB:     true,
			rows:        3,
		},
		{
			name: "unreachable ping",
			analysis: &inst.DetectionAnalysis{
				Analysis:              inst.IncapacitatedPrimary,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
			},
			pingOK:      false,
			wantAttempt: false,
			setupDB:     true,
			rows:        3,
		},
	}

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prevERS := config.ERSEnabled()
			config.SetERSEnabled(true)
			defer config.SetERSEnabled(prevERS)

			logger := log.NewPrefixedLogger("test")

			keyspace := fmt.Sprintf("ks_incap_%d", idx)
			shard := strconv.Itoa(idx)
			analysis := *tt.analysis
			analysis.AnalyzedKeyspace = keyspace
			analysis.AnalyzedShard = shard

			oldTs := ts
			oldTmc := tmc
			defer func() {
				ts = oldTs
				tmc = oldTmc
			}()

			type stderrCapture struct {
				mu  sync.Mutex
				buf bytes.Buffer
			}
			capture := &stderrCapture{}
			captureWrite := func(p []byte) (int, error) {
				capture.mu.Lock()
				defer capture.mu.Unlock()
				return capture.buf.Write(p)
			}

			var restoreStderr func()
			if tt.prsFails {
				oldStderr := os.Stderr
				r, w, err := os.Pipe()
				require.NoError(t, err)
				oldFD, err := unix.Dup(int(os.Stderr.Fd()))
				require.NoError(t, err)
				require.NoError(t, unix.Dup2(int(w.Fd()), int(os.Stderr.Fd())))
				os.Stderr = w
				done := make(chan struct{})
				go func() {
					_, _ = io.Copy(writerFunc(captureWrite), r)
					_ = r.Close()
					close(done)
				}()
				restoreStderr = func() {
					log.Flush()
					_ = w.Close()
					os.Stderr = oldStderr
					_ = unix.Dup2(oldFD, int(os.Stderr.Fd()))
					_ = unix.Close(oldFD)
					<-done
				}
			}

			if tt.setupDB {
				orcDb, fromCache, err := db.OpenVTOrcWithCache()
				require.NoError(t, err)
				defer func() {
					if !fromCache {
						require.NoError(t, orcDb.Close())
					}
				}()
				_, err = orcDb.Exec("delete from topology_recovery_steps")
				require.NoError(t, err)
				_, err = orcDb.Exec("delete from topology_recovery")
				require.NoError(t, err)
				_, err = orcDb.Exec("delete from recovery_detection")
				require.NoError(t, err)
				_, err = orcDb.Exec("delete from vitess_tablet")
				require.NoError(t, err)

				primaryTablet := &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					MysqlHostname: "localhost",
					MysqlPort:     6709,
					Keyspace:      keyspace,
					Shard:         shard,
					Type:          topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttimepb.Time{
						Seconds: 1,
					},
					PortMap: map[string]int32{
						"vt":   15000,
						"grpc": 16000,
					},
				}
				require.NoError(t, inst.SaveTablet(primaryTablet))
				require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					MysqlHostname: "localhost",
					MysqlPort:     6710,
					Keyspace:      keyspace,
					Shard:         shard,
					Type:          topodatapb.TabletType_REPLICA,
					PrimaryTermStartTime: &vttimepb.Time{
						Seconds: 1,
					},
					PortMap: map[string]int32{
						"vt":   15001,
						"grpc": 16001,
					},
				}))

				ctx := t.Context()
				ts = memorytopo.NewServer(ctx, "zon1")
				err = ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone})
				require.NoError(t, err)
				err = ts.CreateShard(ctx, keyspace, shard)
				require.NoError(t, err)
				err = ts.CreateTablet(ctx, primaryTablet)
				require.NoError(t, err)
				err = ts.CreateTablet(ctx, &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					MysqlHostname: "localhost",
					MysqlPort:     6710,
					Keyspace:      keyspace,
					Shard:         shard,
					Type:          topodatapb.TabletType_REPLICA,
					PortMap: map[string]int32{
						"vt":   15001,
						"grpc": 16001,
					},
				})
				require.NoError(t, err)

				tmc = &testutil.TabletManagerClient{}
				pingErr := error(nil)
				if !tt.pingOK {
					pingErr = errors.New("ping failed")
				}
				tmc.(*testutil.TabletManagerClient).PingResults = map[string]error{
					"zon1-0000000100": pingErr,
				}
				fullStatusPosition := replication.EncodePosition(replication.MustParsePosition("MySQL56", "16b1039f-22b6-11ed-b765-0a43f95f28a3:1"))
				tmc.(*testutil.TabletManagerClient).FullStatusResult = &replicationdatapb.FullStatus{
					PrimaryStatus: &replicationdatapb.PrimaryStatus{Position: fullStatusPosition},
					ReplicationStatus: &replicationdatapb.Status{
						Position: fullStatusPosition,
					},
				}
				pos := replication.EncodePosition(replication.MustParsePosition("MySQL56", "16b1039f-22b6-11ed-b765-0a43f95f28a3:1"))
				tmc.(*testutil.TabletManagerClient).StopReplicationAndGetStatusResults = map[string]struct {
					StopStatus *replicationdatapb.StopReplicationStatus
					Error      error
				}{
					"zon1-0000000100": {StopStatus: &replicationdatapb.StopReplicationStatus{Before: &replicationdatapb.Status{Position: pos, RelayLogPosition: pos}, After: &replicationdatapb.Status{Position: pos, RelayLogPosition: pos}}, Error: nil},
					"zon1-0000000101": {StopStatus: &replicationdatapb.StopReplicationStatus{Before: &replicationdatapb.Status{Position: pos, RelayLogPosition: pos}, After: &replicationdatapb.Status{Position: pos, RelayLogPosition: pos}}, Error: nil},
				}
				tmc.(*testutil.TabletManagerClient).WaitForPositionResults = map[string]map[string]error{
					"zon1-0000000100": {pos: nil},
					"zon1-0000000101": {pos: nil},
				}
				tmc.(*testutil.TabletManagerClient).PrimaryPositionResults = map[string]struct {
					Position string
					Error    error
				}{
					"zon1-0000000100": {Position: "pos", Error: nil},
				}
				if tt.prsFails {
					tmc.(*testutil.TabletManagerClient).DemotePrimaryResults = map[string]struct {
						Status *replicationdatapb.PrimaryStatus
						Error  error
					}{
						"zon1-0000000100": {Status: nil, Error: errors.New("prs failed")},
					}
				} else {
					tmc.(*testutil.TabletManagerClient).DemotePrimaryResults = map[string]struct {
						Status *replicationdatapb.PrimaryStatus
						Error  error
					}{
						"zon1-0000000100": {Status: &replicationdatapb.PrimaryStatus{Position: "pos"}, Error: nil},
					}
				}
				tmc.(*testutil.TabletManagerClient).InitPrimaryResults = map[string]struct {
					Result string
					Error  error
				}{
					"zon1-0000000100": {Result: "pos", Error: nil},
					"zon1-0000000101": {Result: "pos", Error: nil},
				}
				tmc.(*testutil.TabletManagerClient).SetReplicationSourceResults = map[string]error{
					"zon1-0000000100": nil,
				}
				tmc.(*testutil.TabletManagerClient).PopulateReparentJournalResults = map[string]error{
					"zon1-0000000100": nil,
					"zon1-0000000101": nil,
				}
				tmc.(*testutil.TabletManagerClient).ReadReparentJournalInfoResults = map[string]int32{
					"zon1-0000000100": 1,
					"zon1-0000000101": 1,
				}
				tmc.(*testutil.TabletManagerClient).PromoteReplicaResults = map[string]struct {
					Result string
					Error  error
				}{
					"zon1-0000000100": {Result: "pos", Error: nil},
					"zon1-0000000101": {Result: "pos", Error: nil},
				}
			}

			attempted, topologyRecovery, err := recoverIncapacitatedPrimary(t.Context(), &analysis, logger)
			if restoreStderr != nil {
				log.Flush()
				require.Eventually(t, func() bool {
					err := db.QueryVTOrc("select message from topology_recovery_steps where message like 'ERS - %'", nil, func(_ sqlutils.RowMap) error {
						return nil
					})
					return err == nil
				}, 2*time.Second, 10*time.Millisecond)
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantAttempt, attempted)
			if tt.wantAttempt {
				require.NotNil(t, topologyRecovery)
			} else {
				require.Nil(t, topologyRecovery)
			}
			if restoreStderr != nil {
				restoreStderr()
			}
		})
	}
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
			demotePrimaryDelay: 30 * time.Second,
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

	orcDB, cached, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	if !cached {
		t.Cleanup(func() {
			require.NoError(t, orcDB.Close())
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Clean tables from prior runs.
				for _, table := range []string{"topology_recovery_steps", "topology_recovery", "recovery_detection", "vitess_tablet", "vitess_keyspace"} {
					_, err = orcDB.Exec("delete from " + table)
					require.NoError(t, err)
				}

				const (
					keyspace = "ks"
					shard    = "0"
				)

				// The real primary, has the newer PrimaryTermStartTime.
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

				// The stale primary, has an older PrimaryTermStartTime
				staleTablet := &topodatapb.Tablet{
					Alias:                &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
					Hostname:             "stale-primary",
					MysqlHostname:        "stale-primary",
					MysqlPort:            3306,
					Keyspace:             keyspace,
					Shard:                shard,
					Type:                 topodatapb.TabletType_PRIMARY,
					PrimaryTermStartTime: &vttimepb.Time{Seconds: 500},
					PortMap:              map[string]int32{"vt": 15200, "grpc": 15201},
				}

				// Populate the VTOrc DB with the tablet records.
				require.NoError(t, inst.SaveTablet(primaryTablet))
				require.NoError(t, inst.SaveTablet(staleTablet))

				// Store the durability policy so GetDurabilityPolicy succeeds.
				keyspaceInfo := &topo.KeyspaceInfo{
					Keyspace: &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone},
				}
				keyspaceInfo.SetKeyspaceName(keyspace)
				require.NoError(t, inst.SaveKeyspace(keyspaceInfo))

				// Wire up memorytopo with the same tablets.
				ctx := t.Context()

				oldTS := ts
				oldTMC := tmc
				defer func() {
					ts = oldTS
					tmc = oldTMC
				}()

				ts = memorytopo.NewServer(ctx, "zone1")
				require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
				require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

				if tt.topoAlreadyReplica {
					staleTablet.Type = topodatapb.TabletType_REPLICA
					staleTablet.PrimaryTermStartTime = nil
				}

				require.NoError(t, ts.CreateTablet(ctx, primaryTablet))
				require.NoError(t, ts.CreateTablet(ctx, staleTablet))

				mockController := gomock.NewController(t)
				t.Cleanup(mockController.Finish)

				mockTMC := tmcmock.NewMockTabletManagerClient(mockController)
				mockTMC.EXPECT().
					DemotePrimary(gomock.Any(), gomock.Any(), true).
					DoAndReturn(func(ctx context.Context, _ *topodatapb.Tablet, _ bool) (*replicationdatapb.PrimaryStatus, error) {
						if tt.demotePrimaryDelay > 0 {
							<-ctx.Done()
							return nil, ctx.Err()
						}

						if tt.demotePrimaryErr != nil {
							return nil, tt.demotePrimaryErr
						}

						return &replicationdatapb.PrimaryStatus{}, nil
					}).
					Times(1)

				mockTMC.EXPECT().
					SetReplicationSource(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil).
					AnyTimes()

				tmc = mockTMC

				analysisEntry := &inst.DetectionAnalysis{
					Analysis:              inst.StaleTopoPrimary,
					AnalyzedInstanceAlias: staleTablet.Alias,
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard,
				}

				logger := log.NewPrefixedLogger("test-stale-primary")
				attempted, topologyRecovery, err := reconcileStaleTopoPrimary(ctx, analysisEntry, logger)

				require.True(t, attempted, "recovery must be attempted")
				require.NoError(t, err, "topo update must succeed")
				require.NotNil(t, topologyRecovery, "topology recovery record must be returned")

				// Verify that the stale tablet's topo record was changed to REPLICA.
				updatedTablet, err := ts.GetTablet(ctx, staleTablet.Alias)
				require.NoError(t, err)
				require.Equal(t, topodatapb.TabletType_REPLICA, updatedTablet.Type, "stale primary must be updated to REPLICA in topo")

				// Verify that the recovery row has been resolved.
				activeRecoveries, err := ReadActiveClusterRecoveries(keyspace, shard)
				require.NoError(t, err)
				require.Empty(t, activeRecoveries, "recovery row must be resolved after reconcileStaleTopoPrimary returns")
			})
		})
	}
}

// TestReconcileStaleTopoPrimaryTopoTimeout verifies that reconcileStaleTopoPrimary
// returns when the topology type change blocks until the remote operation timeout.
func TestReconcileStaleTopoPrimaryTopoTimeout(t *testing.T) {
	orcDB, fromCache, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	if !fromCache {
		t.Cleanup(func() {
			_ = orcDB.Close()
		})
	}

	synctest.Test(t, func(t *testing.T) {
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
			PrimaryTermStartTime: &vttimepb.Time{Seconds: 1000},
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
			PrimaryTermStartTime: &vttimepb.Time{Seconds: 500},
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

		seededTS, topoFactory := memorytopo.NewServerAndFactory(ctx, "zone1")
		t.Cleanup(seededTS.Close)

		require.NoError(t, seededTS.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
		require.NoError(t, seededTS.CreateShard(ctx, keyspace, shard))
		require.NoError(t, seededTS.CreateTablet(ctx, primaryTablet))
		require.NoError(t, seededTS.CreateTablet(ctx, staleTablet))

		require.NoError(t, seededTS.UpdateCellInfoFields(ctx, "zone1", func(ci *topodatapb.CellInfo) error {
			ci.ServerAddress = memorytopo.UnreachableServerAddr
			return nil
		}))

		blockedTS, err := topo.NewWithFactory(topoFactory, "", "")
		require.NoError(t, err)
		t.Cleanup(blockedTS.Close)

		oldTS := ts
		oldTMC := tmc
		oldRemoteOpTimeout := topo.RemoteOperationTimeout

		t.Cleanup(func() {
			ts = oldTS
			tmc = oldTMC
			topo.RemoteOperationTimeout = oldRemoteOpTimeout
		})

		ts = blockedTS
		topo.RemoteOperationTimeout = 100 * time.Millisecond

		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockTMC := tmcmock.NewMockTabletManagerClient(mockController)
		mockTMC.EXPECT().
			DemotePrimary(gomock.Any(), gomock.Any(), true).
			Return(&replicationdatapb.PrimaryStatus{}, nil).
			Times(1)

		mockTMC.EXPECT().
			SetReplicationSource(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil).
			Times(1)

		tmc = mockTMC

		analysisEntry := &inst.DetectionAnalysis{
			Analysis:              inst.StaleTopoPrimary,
			AnalyzedInstanceAlias: staleTablet.Alias,
			AnalyzedKeyspace:      keyspace,
			AnalyzedShard:         shard,
		}

		logger := log.NewPrefixedLogger("test-stale-primary-topo-timeout")

		type reconcileResult struct {
			attempted        bool
			topologyRecovery *TopologyRecovery
			err              error
		}

		recoveryCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(func() {
			cancel()
			synctest.Wait()
		})

		resultCh := make(chan reconcileResult, 1)
		go func() {
			attempted, topologyRecovery, err := reconcileStaleTopoPrimary(recoveryCtx, analysisEntry, logger)
			resultCh <- reconcileResult{
				attempted:        attempted,
				topologyRecovery: topologyRecovery,
				err:              err,
			}
		}()

		synctest.Wait()

		time.Sleep(topo.RemoteOperationTimeout + time.Nanosecond)
		synctest.Wait()

		select {
		case result := <-resultCh:
			require.True(t, result.attempted, "recovery must be attempted")
			require.NotNil(t, result.topologyRecovery, "topology recovery record must be returned")
			require.ErrorContains(t, result.err, "failed to set tablet type to REPLICA in topology")
			require.ErrorContains(t, result.err, context.DeadlineExceeded.Error(), "reconcileStaleTopoPrimary must timeout and return when the topo type change blocks indefinitely")
		default:
			require.FailNowf(t, "reconcileStaleTopoPrimary did not return", "expected timeout after %s when the topo type change hangs indefinitely", topo.RemoteOperationTimeout)
		}

		activeRecoveries, err := ReadActiveClusterRecoveries(keyspace, shard)
		require.NoError(t, err)
		require.Empty(t, activeRecoveries, "recovery row must be resolved after reconcileStaleTopoPrimary returns")
	})
}

// TestRestartDirectReplicasTimeout verifies that restartDirectReplicas does not block forever if an RPC hangs.
func TestRestartDirectReplicasTimeout(t *testing.T) {
	orcDB, fromCache, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	if !fromCache {
		t.Cleanup(func() {
			_ = orcDB.Close()
		})
	}

	inst.InitializeForgetAliasesCache()

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
			InstanceAlias:    replicaTablet.Alias,
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
			AnalyzedInstanceAlias: primaryTablet.Alias,
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

func TestAllCellsDenied(t *testing.T) {
	tests := []struct {
		name          string
		shardCells    []string
		deniedCells   []string
		wantAllDenied bool
	}{
		{"all cells denied", []string{"z1", "z2"}, []string{"z1", "z2"}, true},
		{"superset denied", []string{"z1", "z2"}, []string{"z1", "z2", "z3"}, true},
		{"partial denied", []string{"z1", "z2"}, []string{"z1"}, false},
		{"none denied", []string{"z1", "z2"}, []string{"z3"}, false},
		{"empty denied list", []string{"z1"}, []string{}, false},
		{"empty shard cells", []string{}, []string{"z1"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantAllDenied, allCellsDenied(tt.shardCells, tt.deniedCells))
		})
	}
}

// TestCellsNoRecoveryGateSkip verifies that executeCheckAndRecoverFunction returns
// nil without attempting recovery when the cell gate fires. The skip cases are
// distinguishable at the unit level because: (a) without a gate skip the function
// proceeds to LockShard + actual recovery, (b) the skip path returns nil without
// touching the topology_recovery table. The proceed path is covered by the e2e
// test TestDownPrimary_CellsNoRecovery.
func TestCellsNoRecoveryGateSkip(t *testing.T) {
	tests := []struct {
		name         string
		analysis     inst.DetectionAnalysis
		cellsToSet   []string
		shardTablets []struct {
			cell string
			uid  uint32
		}
		wantSkip bool
	}{
		{
			name: "tablet-level skip when analyzed cell is in deny list",
			analysis: inst.DetectionAnalysis{
				Analysis:              inst.ConnectedToWrongPrimary,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
				AnalyzedCell:          "zone1",
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			},
			cellsToSet: []string{"zone1"},
			wantSkip:   true,
		},
		{
			name: "shard-level NOT skipped when only some shard cells are denied",
			analysis: inst.DetectionAnalysis{
				Analysis:              inst.ClusterHasNoPrimary,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
				AnalyzedCell:          "zone1",
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			},
			cellsToSet: []string{"zone1"},
			shardTablets: []struct {
				cell string
				uid  uint32
			}{
				{"zone1", 100},
				{"zone2", 200},
			},
			wantSkip: false,
		},
		{
			name: "shard-level skip when all shard cells are in deny list",
			analysis: inst.DetectionAnalysis{
				Analysis:              inst.ClusterHasNoPrimary,
				AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
				AnalyzedCell:          "zone1",
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
			},
			cellsToSet: []string{"zone1", "zone2"},
			shardTablets: []struct {
				cell string
				uid  uint32
			}{
				{"zone1", 100},
				{"zone2", 200},
			},
			wantSkip: true,
		},
	}

	db.ClearVTOrcDatabase()
	defer db.ClearVTOrcDatabase()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orcDb, _, err := db.OpenVTOrcWithCache()
			require.NoError(t, err)
			for _, tbl := range []string{"topology_recovery_steps", "topology_recovery", "recovery_detection", "vitess_tablet", "global_recovery_disable"} {
				_, err = orcDb.Exec("DELETE FROM " + tbl)
				require.NoError(t, err)
			}

			for _, tab := range tt.shardTablets {
				require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
					Alias:    &topodatapb.TabletAlias{Cell: tab.cell, Uid: tab.uid},
					Keyspace: tt.analysis.AnalyzedKeyspace,
					Shard:    tt.analysis.AnalyzedShard,
				}))
			}

			ctx := t.Context()
			oldTs := ts
			ts = memorytopo.NewServer(ctx, "zone1", "zone2")
			require.NoError(t, ts.CreateKeyspace(ctx, tt.analysis.AnalyzedKeyspace, &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
			require.NoError(t, ts.CreateShard(ctx, tt.analysis.AnalyzedKeyspace, tt.analysis.AnalyzedShard))
			// Also register tablets in the in-process topology so getShardTabletsByCell
			// (used by the post-lock gate) can read them. inst.SaveTablet only writes to
			// SQLite, which the gate no longer consults.
			for _, tab := range tt.shardTablets {
				require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
					Alias:    &topodatapb.TabletAlias{Cell: tab.cell, Uid: tab.uid},
					Keyspace: tt.analysis.AnalyzedKeyspace,
					Shard:    tt.analysis.AnalyzedShard,
					Type:     topodatapb.TabletType_REPLICA,
				}))
			}
			defer func() { ts = oldTs }()

			// forceRefreshAllTabletsInShard calls DiscoverInstance for each topo tablet,
			// which requires recentDiscoveryOperationKeys to be non-nil. The var is
			// normally set by OpenTabletDiscovery; initialize it here for sub-tests that
			// register tablets in topo so DiscoverInstance exits cleanly (no MySQL in
			// tests means it logs a discovery failure and returns, not crashes).
			if len(tt.shardTablets) > 0 {
				prevKeys := recentDiscoveryOperationKeys
				recentDiscoveryOperationKeys = cache.New(config.GetInstancePollTime(), time.Second)
				defer func() { recentDiscoveryOperationKeys = prevKeys }()
			}

			prev := cellsNoRecovery
			cellsNoRecovery = tt.cellsToSet
			defer func() { cellsNoRecovery = prev }()

			prevValidated := cellsNoRecoveryValidated.Swap(true)
			defer func() { cellsNoRecoveryValidated.Store(prevValidated) }()

			analysis := tt.analysis

			checkAndRecoverFunctionCode, _ := getCheckAndRecoverFunctionCode(&analysis)
			recoveryName := getRecoverFunctionName(checkAndRecoverFunctionCode)
			counterKey := strings.Join([]string{recoveryName, analysis.AnalyzedKeyspace, analysis.AnalyzedShard, RecoverySkipCellNoRecovery.String()}, ".")
			skipsBefore := recoveriesSkippedCounter.Counts()[counterKey]

			require.NoError(t, executeCheckAndRecoverFunction(&analysis))

			skipsAfter := recoveriesSkippedCounter.Counts()[counterKey]
			if tt.wantSkip {
				require.Equal(t, skipsBefore+1, skipsAfter, "CellNoRecovery skip counter must be incremented exactly once")

				var recoveryRows int
				require.NoError(t, orcDb.QueryRow("SELECT COUNT(*) FROM topology_recovery").Scan(&recoveryRows))
				require.Zero(t, recoveryRows, "no topology_recovery row should exist when recovery is skipped by the cell gate")

				var detectionRows int
				require.NoError(t, orcDb.QueryRow("SELECT COUNT(*) FROM recovery_detection").Scan(&detectionRows))
				require.Equal(t, 1, detectionRows, "detection must be recorded even when recovery is skipped")
			} else {
				require.Equal(t, skipsBefore, skipsAfter, "CellNoRecovery skip counter must NOT increment when recovery is allowed")
			}
		})
	}
}

func TestInsertRecoveryDetectionNewIncident(t *testing.T) {
	orcDb, fromCache, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	defer func() {
		if !fromCache {
			require.NoError(t, orcDb.Close())
		}
	}()
	defer func() {
		_, err = orcDb.Exec("DELETE FROM recovery_detection")
		require.NoError(t, err)
	}()

	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	analysis := inst.ReplicationStopped

	entry1 := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		Analysis:              analysis,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(entry1))
	require.NotZero(t, entry1.RecoveryId)

	// Same ongoing incident: should reuse detection_id and refresh timestamp.
	entry2 := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		Analysis:              analysis,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(entry2))
	require.Equal(t, entry1.RecoveryId, entry2.RecoveryId, "same ongoing incident should reuse detection_id")

	// Pin the detection_timestamp to a known past value so the UPSERT's
	// DATETIME('now') is distinguishable even when the test runs sub-second.
	_, err = orcDb.Exec(
		`UPDATE recovery_detection SET detection_timestamp = '2026-01-01 00:00:00'
		 WHERE detection_id = ?`, entry1.RecoveryId)
	require.NoError(t, err)
	var pinnedTimestamp string
	require.NoError(t, orcDb.QueryRow(
		`SELECT detection_timestamp FROM recovery_detection WHERE detection_id = ?`,
		entry1.RecoveryId).Scan(&pinnedTimestamp))

	// Recurring failure: UPSERT refreshes detection_timestamp on every poll.
	entry3 := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		Analysis:              analysis,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(entry3))
	require.Equal(t, entry1.RecoveryId, entry3.RecoveryId, "UPSERT reuses the same detection_id")

	var refreshedTimestamp string
	require.NoError(t, orcDb.QueryRow(
		`SELECT detection_timestamp FROM recovery_detection WHERE detection_id = ?`,
		entry3.RecoveryId).Scan(&refreshedTimestamp))
	require.NotEqual(t, pinnedTimestamp, refreshedTimestamp,
		"each poll should refresh detection_timestamp")
}

func TestExpireRecoveryDetectionActiveIncidentSurvives(t *testing.T) {
	orcDb, fromCache, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	defer func() {
		if !fromCache {
			require.NoError(t, orcDb.Close())
		}
	}()
	defer func() {
		_, err = orcDb.Exec("DELETE FROM recovery_detection")
		require.NoError(t, err)
	}()

	oldVal := config.GetAuditPurgeDays()
	config.SetAuditPurgeDays(10)
	defer config.SetAuditPurgeDays(oldVal)

	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	entry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		Analysis:              inst.ReplicationStopped,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}

	// Insert the initial detection.
	require.NoError(t, InsertRecoveryDetection(entry))
	firstID := entry.RecoveryId
	require.NotZero(t, firstID)

	// Simulate the UPSERT refreshing detection_timestamp on every poll,
	// which keeps the row recent even across the retention window.
	require.NoError(t, InsertRecoveryDetection(entry))
	require.Equal(t, firstID, entry.RecoveryId, "detection_id must be stable across polls")

	// Run expiry — the row's detection_timestamp is fresh (just updated),
	// so it must survive even with a short retention period.
	require.NoError(t, ExpireRecoveryDetectionHistory())

	var remaining int
	require.NoError(t, orcDb.QueryRow("SELECT COUNT(*) FROM recovery_detection").Scan(&remaining))
	require.Equal(t, 1, remaining, "actively-polled detection must survive expiry")

	// Verify the detection_id is unchanged.
	var survivingID int64
	require.NoError(t, orcDb.QueryRow(
		"SELECT detection_id FROM recovery_detection").Scan(&survivingID))
	require.Equal(t, firstID, survivingID, "stable detection_id must be preserved")
}

// TestCheckIfAlreadyFixedReturnsCellFromRefreshedEntry is a regression test for the
// PrimaryTabletDeleted cache-generation skew. refreshAllInformation runs
// RefreshAllKeyspacesAndShards and refreshAllTablets concurrently, so vitess_shard and
// vitess_tablet may not form a single consistent snapshot. For PrimaryTabletDeleted,
// AnalyzedCell comes from vitess_shard.primary_alias.Cell, not from the surviving replica's
// cell. A pre-lock analysis may therefore see AnalyzedCell="zone1" (allowed) while the
// shard record already records the deleted primary as being in "zone2" (denied). This test
// verifies that checkIfAlreadyFixed returns the refreshed analysis entry (with the updated
// AnalyzedCell from the current vitess_shard record) so the caller can re-evaluate the
// cells-no-recovery policy before proceeding with ERS.
func TestCheckIfAlreadyFixedReturnsCellFromRefreshedEntry(t *testing.T) {
	db.ClearVTOrcDatabase()
	defer db.ClearVTOrcDatabase()

	orcDb, _, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)

	// Populate vitess_keyspace (durability_policy required by GetDetectionAnalysis).
	_, err = orcDb.Exec(
		`INSERT OR REPLACE INTO vitess_keyspace (keyspace, keyspace_type, durability_policy, disable_emergency_reparent) VALUES ('ks', 0, 'semi_sync', 0)`,
	)
	require.NoError(t, err)

	// Populate vitess_shard: primary_alias points to zone2 (the deleted primary's cell),
	// with a non-zero primary_timestamp so PrimaryTabletDeleted can fire.
	_, err = orcDb.Exec(
		`INSERT OR REPLACE INTO vitess_shard (keyspace, shard, primary_alias, primary_timestamp, disable_emergency_reparent) VALUES ('ks', '0', 'zone2-0000000200', '2022-12-28 07:23:25 +0000 UTC', 0)`,
	)
	require.NoError(t, err)

	// Save the surviving replica (zone1-0000000100) to vitess_tablet. No primary tablet is
	// saved, simulating the deleted-primary scenario.
	require.NoError(t, inst.SaveTablet(&topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
		MysqlHostname: "localhost",
		MysqlPort:     6711,
	}))

	// Insert a minimal database_instance row so the surviving replica is not flagged
	// is_invalid (NULL alias in the LEFT JOIN → is_invalid=1 → PrimaryTabletDeleted skipped).
	_, err = orcDb.Exec(`INSERT OR REPLACE INTO database_instance
		(alias, hostname, port, tablet_type, cell,
		 server_id, version, binlog_format, log_bin, log_replica_updates,
		 binary_log_file, binary_log_pos,
		 source_host, source_port, replica_net_timeout, heartbeat_interval,
		 replica_sql_running, replica_io_running,
		 source_log_file, read_source_log_pos, relay_source_log_file, exec_source_log_pos,
		 last_checked, last_seen)
		VALUES ('zone1-0000000100', 'localhost', 6711, 2, 'zone1',
		        100, '8.0.31', 'ROW', 1, 1,
		        'bin.000001', 0,
		        '', 0, 8, 4.0,
		        1, 1,
		        '', 0, '', 0,
		        DATETIME('now'), DATETIME('now'))`)
	require.NoError(t, err)

	// Initial analysis: AnalyzedCell is "zone1" — the stale pre-refresh snapshot. This
	// reflects the race window where vitess_tablet recorded the primary deletion while
	// vitess_shard.primary_alias.Cell still pointed to zone1 instead of zone2.
	initial := &inst.DetectionAnalysis{
		Analysis:              inst.PrimaryTabletDeleted,
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		AnalyzedCell:          "zone1",
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}

	alreadyFixed, refreshedEntry, err := checkIfAlreadyFixed(initial)
	require.NoError(t, err)
	require.False(t, alreadyFixed, "PrimaryTabletDeleted is still active; must not be marked already fixed")
	require.NotNil(t, refreshedEntry, "a matched entry must be returned when the problem is still present")
	// The refreshed entry's AnalyzedCell must come from vitess_shard.primary_alias.Cell
	// (zone2), not from the surviving replica's cell (zone1). This is the cell that
	// --cells-no-recovery must evaluate against.
	require.Equal(t, "zone2", refreshedEntry.AnalyzedCell,
		"refreshed AnalyzedCell must reflect vitess_shard.primary_alias.Cell (zone2), not the stale pre-refresh value (zone1)")
	require.True(t, topoproto.TabletAliasEqual(initial.AnalyzedInstanceAlias, refreshedEntry.AnalyzedInstanceAlias),
		"AnalyzedInstanceAlias must remain stable across refresh (surviving replica {zone1, 100} in both snapshots)")
}

// TestCellsNoRecoveryGateFiresAfterPostLockCellShift is a full integration test of the
// post-lock cell gate added for PR #20022. It exercises the TOCTOU scenario where:
//
//   - refreshAllInformation runs RefreshAllKeyspacesAndShards and refreshAllTablets
//     concurrently, so vitess_shard and vitess_tablet may not form a consistent snapshot.
//   - The pre-lock analysis sees AnalyzedCell="zone1" (allowed), because vitess_shard
//     recorded the deleted primary in zone1 at the time the analysis was built.
//   - After the shard lock, RefreshKeyspaceAndShard updates vitess_shard so that
//     primary_alias.Cell is "zone2" (denied).
//   - checkIfAlreadyFixed matches on (AnalyzedInstanceAlias, recovery function) and
//     returns the refreshed analysis entry with AnalyzedCell="zone2".
//   - The post-checkIfAlreadyFixed gate detects the cell shift and suppresses ERS.
//
// This test complements TestCheckIfAlreadyFixedReturnsCellFromRefreshedEntry, which only
// verifies the checkIfAlreadyFixed API. Here we verify the full
// executeCheckAndRecoverFunction path to ensure the returned refreshed entry is actually
// used to suppress recovery.
func TestCellsNoRecoveryGateFiresAfterPostLockCellShift(t *testing.T) {
	db.ClearVTOrcDatabase()
	defer db.ClearVTOrcDatabase()

	orcDb, _, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)

	ctx := t.Context()
	oldTs := ts
	ts = memorytopo.NewServer(ctx, "zone1", "zone2")
	defer func() { ts = oldTs }()

	// Create keyspace and shard in memorytopo. PrimaryAlias points to zone2 (the deleted
	// primary's cell — the cell that will be in the deny list).
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: policy.DurabilityNone}))
	require.NoError(t, ts.CreateShard(ctx, "ks", "0"))
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = &topodatapb.TabletAlias{Cell: "zone2", Uid: 200}
		si.PrimaryTermStartTime = &vttimepb.Time{Seconds: 1672212205}
		return nil
	})
	require.NoError(t, err)

	// Register the surviving replica in memorytopo with MySQL hostname/port matching
	// the pre-inserted database_instance row, so the JOIN in GetDetectionAnalysis succeeds
	// after forceRefreshAllTabletsInShard overwrites vitess_tablet with topo data.
	require.NoError(t, ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_REPLICA,
		MysqlHostname: "localhost",
		MysqlPort:     6711,
	}))

	// Pre-insert database_instance for the surviving replica. forceRefreshAllTabletsInShard
	// calls DiscoverInstance which fails without real MySQL and does not touch
	// database_instance, so this row survives and keeps is_invalid=false for
	// GetDetectionAnalysis.
	_, err = orcDb.Exec(`INSERT OR REPLACE INTO database_instance
		(alias, hostname, port, tablet_type, cell,
		 server_id, version, binlog_format, log_bin, log_replica_updates,
		 binary_log_file, binary_log_pos,
		 source_host, source_port, replica_net_timeout, heartbeat_interval,
		 replica_sql_running, replica_io_running,
		 source_log_file, read_source_log_pos, relay_source_log_file, exec_source_log_pos,
		 last_checked, last_seen)
		VALUES ('zone1-0000000100', 'localhost', 6711, 2, 'zone1',
		        100, '8.0.31', 'ROW', 1, 1,
		        'bin.000001', 0,
		        '', 0, 8, 4.0,
		        1, 1,
		        '', 0, '', 0,
		        DATETIME('now'), DATETIME('now'))`)
	require.NoError(t, err)

	// Initialize recentDiscoveryOperationKeys so DiscoverInstance invocations within
	// forceRefreshAllTabletsInShard do not panic on a nil cache.
	prevKeys := recentDiscoveryOperationKeys
	recentDiscoveryOperationKeys = cache.New(config.GetInstancePollTime(), time.Second)
	defer func() { recentDiscoveryOperationKeys = prevKeys }()

	prev := cellsNoRecovery
	cellsNoRecovery = []string{"zone2"} // zone2 is denied
	defer func() { cellsNoRecovery = prev }()

	prevValidated := cellsNoRecoveryValidated.Swap(true)
	defer func() { cellsNoRecoveryValidated.Store(prevValidated) }()

	// Construct the initial (stale) analysis: AnalyzedCell="zone1" reflects the
	// pre-refresh snapshot where vitess_shard.primary_alias.Cell was still zone1.
	analysis := &inst.DetectionAnalysis{
		Analysis:              inst.PrimaryTabletDeleted,
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		AnalyzedCell:          "zone1", // stale; zone2 is the actual cell after topo refresh
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}

	checkAndRecoverFunctionCode, _ := getCheckAndRecoverFunctionCode(analysis)
	recoveryName := getRecoverFunctionName(checkAndRecoverFunctionCode)
	counterKey := strings.Join([]string{recoveryName, analysis.AnalyzedKeyspace, analysis.AnalyzedShard, RecoverySkipCellNoRecovery.String()}, ".")
	skipsBefore := recoveriesSkippedCounter.Counts()[counterKey]

	require.NoError(t, executeCheckAndRecoverFunction(analysis))

	skipsAfter := recoveriesSkippedCounter.Counts()[counterKey]
	require.Equal(t, skipsBefore+1, skipsAfter,
		"CellNoRecovery skip counter must increment: post-lock refresh showed AnalyzedCell shifted from zone1 (allowed) to zone2 (denied)")

	var recoveryRows int
	require.NoError(t, orcDb.QueryRow("SELECT COUNT(*) FROM topology_recovery").Scan(&recoveryRows))
	require.Zero(t, recoveryRows, "no topology_recovery row must exist when recovery is suppressed by the post-lock cell gate")

	var detectionRows int
	require.NoError(t, orcDb.QueryRow("SELECT COUNT(*) FROM recovery_detection").Scan(&detectionRows))
	require.Equal(t, 1, detectionRows, "detection must be recorded even when recovery is suppressed by the post-lock cell gate")
}

// TestResolveRecoveryIncidentBoundary covers the two-act incident boundary scenario:
//
// Act 1 — failed recovery: the detection row must be preserved so retry attempts share the
// same detection_id (the incident is still active).
//
// Act 2 — successful recovery: resolveRecovery establishes the incident boundary by deleting
// the detection row. A subsequent recurrence of the same failure must receive a strictly
// greater detection_id (AUTOINCREMENT).
func TestResolveRecoveryIncidentBoundary(t *testing.T) {
	orcDb, _, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	defer func() {
		_, _ = orcDb.Exec("DELETE FROM recovery_detection")
		_, _ = orcDb.Exec("DELETE FROM topology_recovery")
	}()

	alias := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
	entry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		Analysis:              inst.DeadPrimary,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(entry))
	firstID := entry.RecoveryId
	require.NotZero(t, firstID, "detection row must have been inserted")

	// Act 1: failed recovery attempt — detection row must survive.
	failedRecovery, err := AttemptRecoveryRegistration(entry)
	require.NoError(t, err)
	require.NotNil(t, failedRecovery)

	require.NoError(t, resolveRecovery(failedRecovery, nil)) // nil successor → IsSuccessful=false

	var remaining int
	require.NoError(t, orcDb.QueryRow("SELECT COUNT(*) FROM recovery_detection").Scan(&remaining))
	require.Equal(t, 1, remaining, "failed recovery must NOT delete the detection row — incident is still active")

	// A retry attempt re-inserts (UPSERT) and must return the same detection_id.
	retryEntry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		Analysis:              inst.DeadPrimary,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(retryEntry))
	require.Equal(t, firstID, retryEntry.RecoveryId,
		"retry attempt must share the same detection_id — incident is ongoing")

	// Act 2: successful recovery — detection row must be deleted, establishing the incident boundary.
	successEntry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		Analysis:              inst.DeadPrimary,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
		RecoveryId:            firstID,
	}
	successRecovery, err := AttemptRecoveryRegistration(successEntry)
	require.NoError(t, err)
	require.NotNil(t, successRecovery)

	promotedReplica := &inst.Instance{InstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}}
	require.NoError(t, resolveRecovery(successRecovery, promotedReplica)) // non-nil → IsSuccessful=true

	require.NoError(t, orcDb.QueryRow("SELECT COUNT(*) FROM recovery_detection").Scan(&remaining))
	require.Zero(t, remaining, "successful recovery must delete the detection row — incident boundary established")

	// Recurrence after the boundary must produce a strictly greater detection_id.
	// detection_id is INTEGER PRIMARY KEY AUTOINCREMENT: even after deletion, the next
	// insert gets max_ever + 1, never a reused value.
	recurrenceEntry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: alias,
		Analysis:              inst.DeadPrimary,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(recurrenceEntry))
	require.Greater(t, recurrenceEntry.RecoveryId, firstID,
		"recurrence after incident boundary must receive a fresh detection_id strictly greater than the prior incident's")
}

// TestDeleteResolvedDetectionEstablishesIncidentBoundary verifies the deleteResolvedDetection
// helper directly: it removes the row and a re-insertion gets a strictly greater AUTOINCREMENT id.
func TestDeleteResolvedDetectionEstablishesIncidentBoundary(t *testing.T) {
	orcDb, _, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	defer func() {
		_, _ = orcDb.Exec("DELETE FROM recovery_detection")
	}()

	entry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Analysis:              inst.ReplicationStopped,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(entry))
	firstID := entry.RecoveryId
	require.NotZero(t, firstID)

	deleteResolvedDetection(firstID)

	var remaining int
	require.NoError(t, orcDb.QueryRow("SELECT COUNT(*) FROM recovery_detection").Scan(&remaining))
	require.Zero(t, remaining, "deleteResolvedDetection must remove the detection row")

	reEntry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Analysis:              inst.ReplicationStopped,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(reEntry))
	require.Greater(t, reEntry.RecoveryId, firstID,
		"AUTOINCREMENT must assign a fresh detection_id strictly greater than the deleted one")
}

// TestAlreadyFixedDoesNotDeleteDetectionRow verifies that checkIfAlreadyFixed returning true
// does NOT delete the detection row. GetDetectionAnalysis can return empty for two reasons:
// the problem was genuinely resolved, or it was suppressed by a shard-wide ordered action.
// We cannot distinguish the two cases, so the detection row must be left intact to expire
// naturally — deleting on suppression would create a false incident boundary.
//
// We test this directly via checkIfAlreadyFixed (same package) rather than through
// executeCheckAndRecoverFunction to avoid the complexity of setting up the full refresh
// path for each analysis type.
func TestAlreadyFixedDoesNotDeleteDetectionRow(t *testing.T) {
	orcDb, _, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	defer func() {
		_, _ = orcDb.Exec("DELETE FROM recovery_detection")
	}()

	// Insert a detection row.
	entry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Analysis:              inst.ReplicationStopped,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(entry))
	firstID := entry.RecoveryId
	require.NotZero(t, firstID)

	// Mock the DB so GetDetectionAnalysis returns no entries — simulating either genuine
	// resolution or suppression by a shard-wide action. checkIfAlreadyFixed returns
	// alreadyFixed=true in both cases.
	oldDB := db.Db
	defer func() { db.Db = oldDB }()
	db.Db = test.NewTestDB([][]sqlutils.RowMap{{}})

	alreadyFixed, _, err := checkIfAlreadyFixed(entry)
	require.NoError(t, err)
	require.True(t, alreadyFixed, "empty GetDetectionAnalysis must be treated as already fixed")

	// Restore the real DB before querying.
	db.Db = oldDB

	// The detection row must still exist. The alreadyFixed=true path no longer deletes it
	// because suppression is indistinguishable from resolution at this point.
	var remaining int
	require.NoError(t, orcDb.QueryRow(
		"SELECT COUNT(*) FROM recovery_detection WHERE detection_id = ?", firstID).Scan(&remaining))
	require.Equal(t, 1, remaining,
		"alreadyFixed=true must NOT delete the detection row — suppression and genuine resolution are indistinguishable")

	// A re-insert (UPSERT) must return the same detection_id: the row was preserved,
	// so there is no incident boundary between the suppressed analysis and a recurrence.
	reEntry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Analysis:              inst.ReplicationStopped,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(reEntry))
	require.Equal(t, firstID, reEntry.RecoveryId,
		"recurrence after suppression must share the same detection_id — no false incident boundary")
}

// TestAttemptRecoveryRegistrationStoresDetectionID verifies that writeTopologyRecovery correctly
// stores analysisEntry.RecoveryId (the detection FK) in topology_recovery.detection_id. There are
// 6 SQL placeholders and 6 Go args; a regression would silently store the alias string in the
// detection_id column (SQLite drops extra args without error) and ReadRecentRecoveries would
// return DetectionID=0 for every row.
func TestAttemptRecoveryRegistrationStoresDetectionID(t *testing.T) {
	orcDb, fromCache, err := db.OpenVTOrcWithCache()
	require.NoError(t, err)
	defer func() {
		if !fromCache {
			require.NoError(t, orcDb.Close())
		}
	}()
	defer func() {
		_, _ = orcDb.Exec("DELETE FROM topology_recovery")
		_, _ = orcDb.Exec("DELETE FROM recovery_detection")
	}()

	entry := &inst.DetectionAnalysis{
		AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
		Analysis:              inst.ReplicationStopped,
		AnalyzedKeyspace:      "ks",
		AnalyzedShard:         "0",
	}
	require.NoError(t, InsertRecoveryDetection(entry))
	require.NotZero(t, entry.RecoveryId, "detection row must have been inserted")

	recovery, err := AttemptRecoveryRegistration(entry)
	require.NoError(t, err)
	require.NotNil(t, recovery)

	recoveries, err := ReadRecentRecoveries(0)
	require.NoError(t, err)
	require.Len(t, recoveries, 1)
	require.Equal(t, entry.RecoveryId, recoveries[0].DetectionID,
		"topology_recovery.detection_id must store the integer RecoveryId, not the alias string")
}
