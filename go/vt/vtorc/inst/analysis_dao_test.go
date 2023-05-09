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

package inst

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/test"
)

func TestGetReplicationAnalysis(t *testing.T) {
	tests := []struct {
		name           string
		info           []*test.InfoForRecoveryAnalysis
		codeWanted     AnalysisCode
		shardWanted    string
		keyspaceWanted string
		wantErr        string
	}{
		{
			name: "ClusterHasNoPrimary",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: "none",
				LastCheckValid:   1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ClusterHasNoPrimary,
		}, {
			name: "DeadPrimary",
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
				IsPrimary:                     1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimary,
		}, {
			name: "DeadPrimaryWithoutReplicas",
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
				DurabilityPolicy: "none",
				LastCheckValid:   0,
				CountReplicas:    0,
				IsPrimary:        1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimaryWithoutReplicas,
		}, {
			name: "DeadPrimaryAndReplicas",
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
				DurabilityPolicy: "none",
				LastCheckValid:   0,
				CountReplicas:    3,
				IsPrimary:        1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimaryAndReplicas,
		}, {
			name: "DeadPrimaryAndSomeReplicas",
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
				CountValidReplicas:            2,
				CountValidReplicatingReplicas: 0,
				IsPrimary:                     1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimaryAndSomeReplicas,
		}, {
			name: "PrimaryHasPrimary",
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
				DurabilityPolicy:   "none",
				LastCheckValid:     1,
				CountReplicas:      4,
				CountValidReplicas: 4,
				IsPrimary:          0,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimaryHasPrimary,
		}, {
			name: "PrimaryIsReadOnly",
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
				DurabilityPolicy:   "none",
				LastCheckValid:     1,
				CountReplicas:      4,
				CountValidReplicas: 4,
				IsPrimary:          1,
				ReadOnly:           1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimaryIsReadOnly,
		}, {
			name: "PrimarySemiSyncMustNotBeSet",
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
				DurabilityPolicy:       "none",
				LastCheckValid:         1,
				CountReplicas:          4,
				CountValidReplicas:     4,
				IsPrimary:              1,
				SemiSyncPrimaryEnabled: 1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimarySemiSyncMustNotBeSet,
		}, {
			name: "PrimarySemiSyncMustBeSet",
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
				DurabilityPolicy:       "semi_sync",
				LastCheckValid:         1,
				CountReplicas:          4,
				CountValidReplicas:     4,
				IsPrimary:              1,
				SemiSyncPrimaryEnabled: 0,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimarySemiSyncMustBeSet,
		}, {
			name: "NotConnectedToPrimary",
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
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
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
				LastCheckValid: 1,
				ReadOnly:       1,
				IsPrimary:      1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     NotConnectedToPrimary,
		}, {
			name: "ReplicaIsWritable",
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
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
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
				DurabilityPolicy: "none",
				SourceHost:       "localhost",
				SourcePort:       6708,
				LastCheckValid:   1,
				ReadOnly:         0,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicaIsWritable,
		}, {
			name: "ConnectedToWrongPrimary",
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
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
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
				DurabilityPolicy: "none",
				SourceHost:       "localhost",
				SourcePort:       6706,
				LastCheckValid:   1,
				ReadOnly:         1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ConnectedToWrongPrimary,
		}, {
			name: "ReplicationStopped",
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
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
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
				DurabilityPolicy:   "none",
				SourceHost:         "localhost",
				SourcePort:         6708,
				LastCheckValid:     1,
				ReadOnly:           1,
				ReplicationStopped: 1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicationStopped,
		},
		{
			name: "ReplicaSemiSyncMustBeSet",
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
				DurabilityPolicy:              "semi_sync",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
				SemiSyncPrimaryEnabled:        1,
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
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:       "semi_sync",
				SourceHost:             "localhost",
				SourcePort:             6708,
				LastCheckValid:         1,
				ReadOnly:               1,
				SemiSyncReplicaEnabled: 0,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicaSemiSyncMustBeSet,
		}, {
			name: "ReplicaSemiSyncMustNotBeSet",
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
				DurabilityPolicy:              "none",
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
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
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_PRIMARY,
					MysqlHostname: "localhost",
					MysqlPort:     6708,
				},
				DurabilityPolicy:       "none",
				SourceHost:             "localhost",
				SourcePort:             6708,
				LastCheckValid:         1,
				ReadOnly:               1,
				SemiSyncReplicaEnabled: 1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicaSemiSyncMustNotBeSet,
		}, {
			name: "SnapshotKeyspace",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				// Snapshot Keyspace
				KeyspaceType:     1,
				DurabilityPolicy: "none",
				LastCheckValid:   1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     NoProblem,
		}, {
			name: "EmptyDurabilityPolicy",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				LastCheckValid: 1,
			}},
			// We will ignore these keyspaces too until the durability policy is set in the topo server
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     NoProblem,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rowMaps []sqlutils.RowMap
			for _, analysis := range tt.info {
				analysis.SetValuesFromTabletInfo()
				rowMaps = append(rowMaps, analysis.ConvertToRowMap())
			}
			db.Db = test.NewTestDB([][]sqlutils.RowMap{rowMaps})

			got, err := GetReplicationAnalysis("", &ReplicationAnalysisHints{})
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.codeWanted == NoProblem {
				require.Len(t, got, 0)
				return
			}
			require.Len(t, got, 1)
			require.Equal(t, tt.codeWanted, got[0].Analysis)
			require.Equal(t, tt.keyspaceWanted, got[0].AnalyzedKeyspace)
			require.Equal(t, tt.shardWanted, got[0].AnalyzedShard)
		})
	}
}
