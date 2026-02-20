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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/test"
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vttimepb "vitess.io/vitess/go/vt/proto/vttime"
)

type writerFunc func([]byte) (int, error)

func (wf writerFunc) Write(p []byte) (int, error) {
	return wf(p)
}

func seedTestAnalysisRow(row sqlutils.RowMap) error {
	_ = row
	return nil
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
		AnalyzedInstanceAlias: topoproto.TabletAliasString(tablet.Alias),
	}
	ctx := t.Context()

	ts = memorytopo.NewServer(ctx, "zone1")
	recoveryAttempted, _, err := electNewPrimary(context.Background(), analysisEntry, log.NewPrefixedLogger("prefix"))
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
		AnalyzedInstanceAlias: topoproto.TabletAliasString(primary.Alias),
		Analysis:              inst.ReplicationStopped,
	}
	replicaAnalysisEntry := inst.DetectionAnalysis{
		AnalyzedInstanceAlias: topoproto.TabletAliasString(replica.Alias),
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

func TestRecoverIncapacitatedPrimary(t *testing.T) {
	tests := []struct {
		name        string
		analysis    *inst.DetectionAnalysis
		probeOK     bool
		wantAttempt bool
		setupDB     bool
		rows        int
		prsFails    bool
	}{
		{
			name: "reachable healthz (prs failure)",
			analysis: &inst.DetectionAnalysis{
				Analysis:              inst.IncapacitatedPrimary,
				AnalyzedInstanceAlias: "zon1-0000000100",
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
				LastCheckValid:        true,
			},
			probeOK:     true,
			wantAttempt: true,
			setupDB:     true,
			rows:        3,
			prsFails:    true,
		},
		{
			name: "reachable healthz (ers fallback)",
			analysis: &inst.DetectionAnalysis{
				Analysis:              inst.IncapacitatedPrimary,
				AnalyzedInstanceAlias: "zon1-0000000100",
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
				LastCheckValid:        false,
			},
			probeOK:     true,
			wantAttempt: true,
			setupDB:     true,
			rows:        3,
			prsFails:    true,
		},
		{
			name: "reachable healthz (prs ok)",
			analysis: &inst.DetectionAnalysis{
				Analysis:              inst.IncapacitatedPrimary,
				AnalyzedInstanceAlias: "zon1-0000000100",
				AnalyzedKeyspace:      "ks",
				AnalyzedShard:         "0",
				LastCheckValid:        true,
			},
			probeOK:     true,
			wantAttempt: true,
			setupDB:     true,
			rows:        3,
		},
		{
			name: "unreachable healthz",
			analysis: &inst.DetectionAnalysis{
				Analysis:              inst.IncapacitatedPrimary,
				AnalyzedInstanceAlias: "zon1-0000000100",
			},
			probeOK:     false,
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

			oldProbe := healthzProbe
			healthzProbe = func(_ *topodatapb.Tablet) (bool, error) {
				return tt.probeOK, nil
			}
			defer func() {
				healthzProbe = oldProbe
			}()

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
				oldFD, err := syscall.Dup(int(os.Stderr.Fd()))
				require.NoError(t, err)
				require.NoError(t, syscall.Dup2(int(w.Fd()), int(os.Stderr.Fd())))
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
					_ = syscall.Dup2(oldFD, int(os.Stderr.Fd()))
					_ = syscall.Close(oldFD)
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
						"vt": 15000,
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
						"vt": 15001,
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
						"vt": 15001,
					},
				})
				require.NoError(t, err)

				tmc = &testutil.TabletManagerClient{}
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

			attempted, topologyRecovery, err := recoverIncapacitatedPrimary(context.Background(), &analysis, logger)
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
