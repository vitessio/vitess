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
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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
	emergencyOperationGracefulPeriodMap = cache.New(time.Second*5, time.Millisecond*500)
	t.Parallel()
	for _, tt := range tests {
		t.Run(string(tt.prevAnalysisCode)+","+string(tt.newAnalysisCode), func(t *testing.T) {
			res := analysisEntriesHaveSameRecovery(inst.ReplicationAnalysis{Analysis: tt.prevAnalysisCode}, inst.ReplicationAnalysis{Analysis: tt.newAnalysisCode})
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
	analysisEntry := inst.ReplicationAnalysis{
		AnalyzedInstanceKey: inst.InstanceKey{
			Hostname: tablet.MysqlHostname,
			Port:     int(tablet.MysqlPort),
		},
	}
	ts = memorytopo.NewServer("zone1")
	recoveryAttempted, _, err := electNewPrimary(context.Background(), analysisEntry, nil, false, false)
	require.True(t, recoveryAttempted)
	require.Error(t, err)
}

func TestDifferentAnalysescHaveDifferentCooldowns(t *testing.T) {
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
	primaryAnalysisEntry := inst.ReplicationAnalysis{
		AnalyzedInstanceKey: inst.InstanceKey{
			Hostname: primary.MysqlHostname,
			Port:     int(primary.MysqlPort),
		},
		Analysis: inst.ReplicationStopped,
	}
	replicaAnalysisEntry := inst.ReplicationAnalysis{
		AnalyzedInstanceKey: inst.InstanceKey{
			Hostname: replica.MysqlHostname,
			Port:     int(replica.MysqlPort),
		},
		Analysis: inst.DeadPrimary,
	}
	ts = memorytopo.NewServer("zone1")
	_, err = AttemptRecoveryRegistration(&replicaAnalysisEntry, false, true)
	require.Nil(t, err)

	// even though this is another recovery on the same cluster, allow it to go through
	// because the analysis is different (ReplicationStopped vs DeadPrimary)
	_, err = AttemptRecoveryRegistration(&primaryAnalysisEntry, true, true)
	require.Nil(t, err)
}

func TestGetCheckAndRecoverFunctionCode(t *testing.T) {
	tests := []struct {
		name                 string
		ersEnabled           bool
		analysisCode         inst.AnalysisCode
		analyzedInstanceKey  *inst.InstanceKey
		wantRecoveryFunction recoveryFunction
	}{
		{
			name:         "DeadPrimary with ERS enabled",
			ersEnabled:   true,
			analysisCode: inst.DeadPrimary,
			analyzedInstanceKey: &inst.InstanceKey{
				Hostname: hostname,
				Port:     1,
			},
			wantRecoveryFunction: recoverDeadPrimaryFunc,
		}, {
			name:         "DeadPrimary with ERS disabled",
			ersEnabled:   false,
			analysisCode: inst.DeadPrimary,
			analyzedInstanceKey: &inst.InstanceKey{
				Hostname: hostname,
				Port:     1,
			},
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
		},
	}

	// Needed for the test to work
	oldMap := emergencyOperationGracefulPeriodMap
	emergencyOperationGracefulPeriodMap = cache.New(time.Second*5, time.Millisecond*500)
	defer func() {
		emergencyOperationGracefulPeriodMap = oldMap
	}()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prevVal := config.ERSEnabled()
			config.SetERSEnabled(tt.ersEnabled)
			defer config.SetERSEnabled(prevVal)

			gotFunc := getCheckAndRecoverFunctionCode(tt.analysisCode, tt.analyzedInstanceKey)
			require.EqualValues(t, tt.wantRecoveryFunction, gotFunc)
		})
	}
}
