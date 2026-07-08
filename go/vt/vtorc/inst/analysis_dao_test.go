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
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/test"
)

// The initialSQL is a set of insert commands copied from a dump of an actual running VTOrc instances. The relevant insert commands are here.
// This is a dump taken from a test running 4 tablets, zone1-101 is the primary, zone1-100 is a replica, zone1-112 is a rdonly and zone2-200 is a cross-cell replica.
var initialSQL = []string{
	`INSERT INTO database_instance VALUES('zone1-0000000112','localhost',6747,3,'zone1','2022-12-28 07:26:04','2022-12-28 07:26:04',213696377,'8.0.31','ROW',1,1,'vt-0000000112-bin.000001',15963,'localhost',6714,8,4.0,1,1,'vt-0000000101-bin.000001',15583,'vt-0000000101-bin.000001',15583,0,0,1,'','',1,'vt-0000000112-relay-bin.000002',15815,1,0,0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a5138-8680-11ed-9240-92a06c3be3c2','2022-12-28 07:26:04','',1,0,0,'Homebrew','8.0','FULL',10816929,0,0,'ON',1,'729a4cc4-8680-11ed-a104-47706090afbd','','729a4cc4-8680-11ed-a104-47706090afbd,729a5138-8680-11ed-9240-92a06c3be3c2',1,1,1000000000000000000,1,0,0,0,false,false,false);`,
	`INSERT INTO database_instance VALUES('zone1-0000000100','localhost',6711,2,'zone1','2022-12-28 07:26:04','2022-12-28 07:26:04',1094500338,'8.0.31','ROW',1,1,'vt-0000000100-bin.000001',15963,'localhost',6714,8,4.0,1,1,'vt-0000000101-bin.000001',15583,'vt-0000000101-bin.000001',15583,0,0,1,'','',1,'vt-0000000100-relay-bin.000002',15815,1,0,0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a5138-8680-11ed-acf8-d6b0ef9f4eaa','2022-12-28 07:26:04','',1,0,0,'Homebrew','8.0','FULL',10103920,0,1,'ON',1,'729a4cc4-8680-11ed-a104-47706090afbd','','729a4cc4-8680-11ed-a104-47706090afbd,729a5138-8680-11ed-acf8-d6b0ef9f4eaa',1,1,1000000000000000000,1,0,1,0,false,false,false);`,
	`INSERT INTO database_instance VALUES('zone1-0000000101','localhost',6714,1,'zone1','2022-12-28 07:26:04','2022-12-28 07:26:04',390954723,'8.0.31','ROW',1,1,'vt-0000000101-bin.000001',15583,'',0,0,0,0,0,'',0,'',0,NULL,NULL,0,'','',0,'',0,0,0,0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a4cc4-8680-11ed-a104-47706090afbd','2022-12-28 07:26:04','',0,0,0,'Homebrew','8.0','FULL',11366095,1,1,'ON',1,'','','729a4cc4-8680-11ed-a104-47706090afbd',-1,-1,1000000000000000000,1,1,0,2,false,false,false);`,
	`INSERT INTO database_instance VALUES('zone2-0000000200','localhost',6756,2,'zone2','2022-12-28 07:26:05','2022-12-28 07:26:05',444286571,'8.0.31','ROW',1,1,'vt-0000000200-bin.000001',15963,'localhost',6714,8,4.0,1,1,'vt-0000000101-bin.000001',15583,'vt-0000000101-bin.000001',15583,0,0,1,'','',1,'vt-0000000200-relay-bin.000002',15815,1,0,0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a497c-8680-11ed-8ad4-3f51d747db75','2022-12-28 07:26:05','',1,0,0,'Homebrew','8.0','FULL',10443112,0,1,'ON',1,'729a4cc4-8680-11ed-a104-47706090afbd','','729a4cc4-8680-11ed-a104-47706090afbd,729a497c-8680-11ed-8ad4-3f51d747db75',1,1,1000000000000000000,1,0,1,0,false,false,false);`,
	`INSERT INTO vitess_tablet VALUES('zone1-0000000100','localhost',6711,'ks','0','zone1',2,'0001-01-01 00:00:00 +0000 UTC',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3130307d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a363731307d20706f72745f6d61703a7b6b65793a227674222076616c75653a363730397d206b657973706163653a226b73222073686172643a22302220747970653a5245504c494341206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a363731312064625f7365727665725f76657273696f6e3a22382e302e3331222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
	`INSERT INTO vitess_tablet VALUES('zone1-0000000101','localhost',6714,'ks','0','zone1',1,'2022-12-28 07:23:25.129898 +0000 UTC',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3130317d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a363731337d20706f72745f6d61703a7b6b65793a227674222076616c75653a363731327d206b657973706163653a226b73222073686172643a22302220747970653a5052494d415259206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a36373134207072696d6172795f7465726d5f73746172745f74696d653a7b7365636f6e64733a31363732323132323035206e616e6f7365636f6e64733a3132393839383030307d2064625f7365727665725f76657273696f6e3a22382e302e3331222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
	`INSERT INTO vitess_tablet VALUES('zone1-0000000112','localhost',6747,'ks','0','zone1',3,'0001-01-01 00:00:00 +0000 UTC',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3131327d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a363734367d20706f72745f6d61703a7b6b65793a227674222076616c75653a363734357d206b657973706163653a226b73222073686172643a22302220747970653a52444f4e4c59206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a363734372064625f7365727665725f76657273696f6e3a22382e302e3331222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
	`INSERT INTO vitess_tablet VALUES('zone2-0000000200','localhost',6756,'ks','0','zone2',2,'0001-01-01 00:00:00 +0000 UTC',X'616c6961733a7b63656c6c3a227a6f6e653222207569643a3230307d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a363735357d20706f72745f6d61703a7b6b65793a227674222076616c75653a363735347d206b657973706163653a226b73222073686172643a22302220747970653a5245504c494341206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a363735362064625f7365727665725f76657273696f6e3a22382e302e3331222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
	`INSERT INTO vitess_shard VALUES('ks','0','zone1-0000000101','2025-06-25 23:48:57.306096 +0000 UTC',0);`,
	`INSERT INTO vitess_keyspace VALUES('ks',0,'semi_sync',0);`,
}

// TestGetDetectionAnalysisDecision tests the code of GetDetectionAnalysis decision-making. It doesn't check the SQL query
// run by it. It only checks the analysis part after the rows have been read. This tests fakes the db and explicitly returns the
// rows that are specified in the test.
func TestGetDetectionAnalysisDecision(t *testing.T) {
	resetPrimaryHealthState()
	tests := []struct {
		name           string
		info           []*test.InfoForRecoveryAnalysis
		codeWanted     AnalysisCode
		shardWanted    string
		keyspaceWanted string
		preFunc        func(t *testing.T)
		wantErr        string
	}{
		{
			name: "ClusterHasNoPrimary",
			info: []*test.InfoForRecoveryAnalysis{{
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
				LastCheckValid:   1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ClusterHasNoPrimary,
		},
		{
			name: "PrimaryTabletDeleted",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				ShardPrimaryTermTimestamp: "2022-12-28 07:23:25.129898 +0000 UTC",
				DurabilityPolicy:          policy.DurabilityNone,
				LastCheckValid:            1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimaryTabletDeleted,
		},
		{
			name: "StalledDiskPrimary",
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
				IsPrimary:                     1,
				IsStalledDisk:                 1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			preFunc: func(t *testing.T) {
				config.SetStalledDiskPrimaryRecovery(true)
				t.Cleanup(func() { config.SetStalledDiskPrimaryRecovery(false) })
			},
			codeWanted: PrimaryDiskStalled,
		},
		{
			// With stalled-disk recovery disabled, a stalled-disk flag —
			// possibly preserved from a prior poll — must not mask DeadPrimary,
			// or the shard would be stuck with no recovery at all.
			name: "StalledDiskPrimaryFallsBackToDeadPrimaryWhenRecoveryDisabled",
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
				IsPrimary:                     1,
				IsStalledDisk:                 1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimary,
		},
		{
			name: "FullDiskPrimary",
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
				IsPrimary:                     1,
				IsFullDisk:                    1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			preFunc: func(t *testing.T) {
				config.SetFullDiskPrimaryRecovery(true)
				t.Cleanup(func() { config.SetFullDiskPrimaryRecovery(false) })
			},
			codeWanted: PrimaryDiskFull,
		},
		{
			// With full-disk recovery disabled, a full-disk flag — possibly
			// preserved from a prior poll — must not mask DeadPrimary, or the
			// shard would be stuck with no recovery at all.
			name: "FullDiskPrimaryFallsBackToDeadPrimaryWhenRecoveryDisabled",
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
				IsPrimary:                     1,
				IsFullDisk:                    1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimary,
		},
		{
			name: "FullDiskReplica",
			info: []*test.InfoForRecoveryAnalysis{
				{
					TabletInfo: &topodatapb.Tablet{
						Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
						Hostname:      "primary",
						Keyspace:      "ks",
						Shard:         "0",
						Type:          topodatapb.TabletType_PRIMARY,
						MysqlHostname: "primary",
						MysqlPort:     6709,
					},
					DurabilityPolicy:  "none",
					LastCheckValid:    1,
					IsPrimary:         1,
					CurrentTabletType: int(topodatapb.TabletType_PRIMARY),
				},
				{
					TabletInfo: &topodatapb.Tablet{
						Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
						Hostname:      "replica",
						Keyspace:      "ks",
						Shard:         "0",
						Type:          topodatapb.TabletType_REPLICA,
						MysqlHostname: "replica",
						MysqlPort:     6710,
					},
					DurabilityPolicy:  "none",
					LastCheckValid:    0,
					IsFullDisk:        1,
					CurrentTabletType: int(topodatapb.TabletType_REPLICA),
				},
			},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicaDiskFull,
		},
		{
			name: "PrimarySemiSyncBlocked",
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
				DurabilityPolicy:                   "semi_sync",
				LastCheckValid:                     1,
				CountReplicas:                      4,
				CountValidReplicas:                 4,
				CountValidReplicatingReplicas:      4,
				IsPrimary:                          1,
				SemiSyncPrimaryEnabled:             1,
				SemiSyncPrimaryStatus:              1,
				SemiSyncPrimaryWaitForReplicaCount: 2,
				CountSemiSyncReplicasEnabled:       2,
				SemiSyncPrimaryClients:             0,
				SemiSyncBlocked:                    1,
				CurrentTabletType:                  int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimarySemiSyncBlocked,
		},
		{
			name: "LockedSemiSync",
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
				DurabilityPolicy:                   "semi_sync",
				LastCheckValid:                     1,
				CountReplicas:                      4,
				CountValidReplicas:                 4,
				CountValidReplicatingReplicas:      4,
				IsPrimary:                          1,
				SemiSyncPrimaryEnabled:             1,
				SemiSyncPrimaryStatus:              1,
				SemiSyncPrimaryWaitForReplicaCount: 2,
				CountSemiSyncReplicasEnabled:       1,
				SemiSyncPrimaryClients:             1,
				SemiSyncBlocked:                    1,
				CurrentTabletType:                  int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     LockedSemiSyncPrimaryHypothesis,
		},
		{
			name: "DeadPrimary",
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
				DurabilityPolicy:              policy.DurabilityNone,
				LastCheckValid:                0,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 0,
				IsPrimary:                     1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimary,
		},
		{
			name: "IncapacitatedPrimaryHealthWindow",
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
				DurabilityPolicy:              policy.DurabilityNone,
				LastCheckValid:                0,
				CountReplicas:                 2,
				CountValidReplicas:            2,
				CountValidReplicatingReplicas: 2,
				IsPrimary:                     1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			preFunc: func(t *testing.T) {
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, true)
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, false)
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, false)
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, true)
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, false)
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, false)
			},
			codeWanted: IncapacitatedPrimary,
		},
		{
			name: "DeadPrimaryHealthUnhealthyIgnored",
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
				DurabilityPolicy:              policy.DurabilityNone,
				LastCheckValid:                0,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 0,
				IsPrimary:                     1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			preFunc: func(t *testing.T) {
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, true)
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, false)
				RecordPrimaryHealthCheck(&topodatapb.TabletAlias{Cell: "zon1", Uid: 100}, false)
			},
			codeWanted: DeadPrimary,
		},
		{
			name: "DeadPrimaryWithoutReplicas",
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
				DurabilityPolicy:  policy.DurabilityNone,
				LastCheckValid:    0,
				CountReplicas:     0,
				IsPrimary:         1,
				CurrentTabletType: int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimaryWithoutReplicas,
		},
		{
			name: "DeadPrimaryAndReplicas",
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
				DurabilityPolicy:  policy.DurabilityNone,
				LastCheckValid:    0,
				CountReplicas:     3,
				IsPrimary:         1,
				CurrentTabletType: int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimaryAndReplicas,
		},
		{
			name: "DeadPrimaryAndSomeReplicas",
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
				DurabilityPolicy:              policy.DurabilityNone,
				LastCheckValid:                0,
				CountReplicas:                 4,
				CountValidReplicas:            2,
				CountValidReplicatingReplicas: 0,
				IsPrimary:                     1,
				CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimaryAndSomeReplicas,
		},
		{
			name: "PrimaryHasPrimary",
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
				DurabilityPolicy:   policy.DurabilityNone,
				LastCheckValid:     1,
				CountReplicas:      4,
				CountValidReplicas: 4,
				IsPrimary:          0,
				CurrentTabletType:  int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimaryHasPrimary,
		},
		{
			name: "PrimaryIsReadOnly",
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
				DurabilityPolicy:   policy.DurabilityNone,
				LastCheckValid:     1,
				CountReplicas:      4,
				CountValidReplicas: 4,
				IsPrimary:          1,
				ReadOnly:           1,
				CurrentTabletType:  int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimaryIsReadOnly,
		},
		{
			name: "PrimaryCurrentTypeMismatch",
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
				DurabilityPolicy:   policy.DurabilityNone,
				LastCheckValid:     1,
				CountReplicas:      4,
				CountValidReplicas: 4,
				IsPrimary:          1,
				CurrentTabletType:  int(topodatapb.TabletType_REPLICA),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimaryCurrentTypeMismatch,
		},
		{
			name: "Unknown tablet type shouldn't run the mismatch recovery analysis",
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
				CurrentTabletType:             int(topodatapb.TabletType_UNKNOWN),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     NoProblem,
		},
		{
			name: "PrimarySemiSyncMustNotBeSet",
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
				DurabilityPolicy:       policy.DurabilityNone,
				LastCheckValid:         1,
				CountReplicas:          4,
				CountValidReplicas:     4,
				IsPrimary:              1,
				SemiSyncPrimaryEnabled: 1,
				CurrentTabletType:      int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimarySemiSyncMustNotBeSet,
		},
		{
			name: "PrimarySemiSyncMustBeSet",
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
				DurabilityPolicy:                      policy.DurabilitySemiSync,
				LastCheckValid:                        1,
				CountReplicas:                         4,
				CountValidReplicas:                    4,
				CountValidReplicatingReplicas:         4,
				CountValidSemiSyncReplicatingReplicas: 1,
				IsPrimary:                             1,
				SemiSyncPrimaryEnabled:                0,
				CurrentTabletType:                     int(topodatapb.TabletType_PRIMARY),
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     PrimarySemiSyncMustBeSet,
		},
		{
			name: "NotConnectedToPrimary",
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
				LastCheckValid: 1,
				ReadOnly:       1,
				IsPrimary:      1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     NotConnectedToPrimary,
		},
		{
			name: "ReplicaIsWritable",
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
				LastCheckValid: 1,
				ReadOnly:       0,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicaIsWritable,
		},
		{
			name: "ConnectedToWrongPrimary",
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
					Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102},
				},
				LastCheckValid: 1,
				ReadOnly:       1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ConnectedToWrongPrimary,
		},
		{
			name: "ReplicationStopped",
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
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicationStopped,
		},
		{
			name: "No recoveries on drained tablets",
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
					Type:          topodatapb.TabletType_DRAINED,
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
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     NoProblem,
		},
		{
			name: "ReplicaMisconfigured",
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
				LastCheckValid:    1,
				ReadOnly:          1,
				ReplicaNetTimeout: 30,
				HeartbeatInterval: 30,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicaMisconfigured,
		},
		{
			name: "ReplicaSemiSyncMustBeSet",
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
				DurabilityPolicy:              policy.DurabilitySemiSync,
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
				SemiSyncPrimaryEnabled:        1,
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
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
				},
				DurabilityPolicy:       policy.DurabilitySemiSync,
				LastCheckValid:         1,
				ReadOnly:               1,
				SemiSyncReplicaEnabled: 0,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicaSemiSyncMustBeSet,
		},
		{
			name: "ReplicaSemiSyncMustNotBeSet",
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
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
				},
				DurabilityPolicy:       policy.DurabilityNone,
				LastCheckValid:         1,
				ReadOnly:               1,
				SemiSyncReplicaEnabled: 1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ReplicaSemiSyncMustNotBeSet,
		},
		{
			name: "SnapshotKeyspace",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				// Snapshot Keyspace
				KeyspaceType:     1,
				DurabilityPolicy: policy.DurabilityNone,
				LastCheckValid:   1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     NoProblem,
		},
		{
			name: "EmptyDurabilityPolicy",
			info: []*test.InfoForRecoveryAnalysis{{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
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
		{
			// If the database_instance table for a tablet is empty (discovery of MySQL information hasn't happened yet or failed)
			// then we shouldn't run a failure fix on it until the discovery succeeds
			name: "Empty database_instance table",
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
				DurabilityPolicy:              policy.DurabilitySemiSync,
				LastCheckValid:                1,
				CountReplicas:                 4,
				CountValidReplicas:            4,
				CountValidReplicatingReplicas: 3,
				CountValidOracleGTIDReplicas:  4,
				CountLoggingReplicas:          2,
				IsPrimary:                     1,
				SemiSyncPrimaryEnabled:        1,
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
				IsInvalid:        1,
				DurabilityPolicy: policy.DurabilitySemiSync,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     InvalidReplica,
		},
		{
			name: "DeadPrimary when VTOrc is starting up",
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
				DurabilityPolicy: policy.DurabilityNone,
				IsInvalid:        1,
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
				LastCheckValid:     1,
				ReplicationStopped: 1,
			}, {
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 103},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6710,
				},
				LastCheckValid:     1,
				ReplicationStopped: 1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     DeadPrimary,
		},
		{
			name: "Invalid Primary",
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
				DurabilityPolicy: policy.DurabilityNone,
				IsInvalid:        1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     InvalidPrimary,
		},
		{
			name: "ErrantGTID",
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
				ErrantGTID:       "some errant GTID",
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
				},
				LastCheckValid: 1,
				ReadOnly:       1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     ErrantGTIDDetected,
		},
		{
			name: "ErrantGTID on a non-replica",
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
					Type:          topodatapb.TabletType_DRAINED,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				DurabilityPolicy: policy.DurabilityNone,
				ErrantGTID:       "some errant GTID",
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
				},
				LastCheckValid: 1,
				ReadOnly:       1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     NoProblem,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetPrimaryHealthState()
			if tt.preFunc != nil {
				tt.preFunc(t)
			}
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

			got, err := GetDetectionAnalysis("", "", &DetectionAnalysisHints{})
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.codeWanted == NoProblem {
				require.Empty(t, got)
				return
			}
			require.Len(t, got, 1)
			require.Equal(t, tt.codeWanted, got[0].Analysis)
			require.Equal(t, tt.keyspaceWanted, got[0].AnalyzedKeyspace)
			require.Equal(t, tt.shardWanted, got[0].AnalyzedShard)
		})
	}
}

// fullDiskShardRows returns rows for a shard whose primary and only replica
// both report a full disk and a failed last check.
func fullDiskShardRows() []sqlutils.RowMap {
	rows := []sqlutils.RowMap{}
	for _, analysis := range []*test.InfoForRecoveryAnalysis{
		{
			TabletInfo: &topodatapb.Tablet{
				Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
				Hostname:      "primary",
				Keyspace:      "ks",
				Shard:         "0",
				Type:          topodatapb.TabletType_PRIMARY,
				MysqlHostname: "primary",
				MysqlPort:     6709,
			},
			DurabilityPolicy:              "none",
			LastCheckValid:                0,
			CountReplicas:                 1,
			CountValidReplicas:            1,
			CountValidReplicatingReplicas: 0,
			IsPrimary:                     1,
			IsFullDisk:                    1,
			CurrentTabletType:             int(topodatapb.TabletType_PRIMARY),
		},
		{
			TabletInfo: &topodatapb.Tablet{
				Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
				Hostname:      "replica",
				Keyspace:      "ks",
				Shard:         "0",
				Type:          topodatapb.TabletType_REPLICA,
				MysqlHostname: "replica",
				MysqlPort:     6710,
			},
			DurabilityPolicy:  "none",
			LastCheckValid:    0,
			IsFullDisk:        1,
			CurrentTabletType: int(topodatapb.TabletType_REPLICA),
		},
	} {
		analysis.SetValuesFromTabletInfo()
		rows = append(rows, analysis.ConvertToRowMap())
	}
	return rows
}

func TestGetDetectionAnalysisDecisionDiskFull(t *testing.T) {
	oldDB := db.Db
	defer func() {
		db.Db = oldDB
	}()

	config.SetFullDiskPrimaryRecovery(true)
	t.Cleanup(func() { config.SetFullDiskPrimaryRecovery(false) })

	db.Db = test.NewTestDB([][]sqlutils.RowMap{fullDiskShardRows()})

	got, err := GetDetectionAnalysis("", "", &DetectionAnalysisHints{})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.ElementsMatch(t, []AnalysisCode{PrimaryDiskFull, ReplicaDiskFull}, []AnalysisCode{got[0].Analysis, got[1].Analysis})
}

// TestGetDetectionAnalysisDecisionDiskFullRecoveryDisabled is a regression
// test for a full-disk primary when --enable-primary-disk-full-recovery is
// off (the default): the full-disk flag — possibly preserved from a prior
// poll after the tablet became unreachable — must not produce PrimaryDiskFull
// (whose recovery would be skipped), but fall back to plain DeadPrimary so
// the ordinary ERS recovery can run.
func TestGetDetectionAnalysisDecisionDiskFullRecoveryDisabled(t *testing.T) {
	oldDB := db.Db
	defer func() {
		db.Db = oldDB
	}()

	db.Db = test.NewTestDB([][]sqlutils.RowMap{fullDiskShardRows()})

	got, err := GetDetectionAnalysis("", "", &DetectionAnalysisHints{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, DeadPrimary, got[0].Analysis)
}

// TestStalePrimary tests that an old primary that remains writable and is of tablet type PRIMARY
// in the topo is demoted to a read-only replica by VTOrc.
func TestStalePrimary(t *testing.T) {
	oldDB := db.Db
	defer func() {
		db.Db = oldDB
	}()

	currentPrimaryTimestamp := time.Now().UTC().Truncate(time.Microsecond)
	stalePrimaryTimestamp := currentPrimaryTimestamp.Add(-1 * time.Minute)
	shardPrimaryTermTimestamp := currentPrimaryTimestamp.Format(sqlutils.DateTimeFormat)

	// We set up a real primary and replica, and then a stale primary running as REPLICA but with
	// tablet type PRIMARY in the topology.
	info := []*test.InfoForRecoveryAnalysis{
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
			IsPrimary:                          1,
			SemiSyncPrimaryEnabled:             1,
			SemiSyncPrimaryStatus:              1,
			SemiSyncPrimaryWaitForReplicaCount: 1,
			SemiSyncPrimaryClients:             1,
			CurrentTabletType:                  int(topodatapb.TabletType_PRIMARY),
			PrimaryTimestamp:                   &currentPrimaryTimestamp,
			ShardPrimaryTermTimestamp:          shardPrimaryTermTimestamp,
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
			LastCheckValid:            1,
			ReadOnly:                  1,
			SemiSyncReplicaEnabled:    1,
			ShardPrimaryTermTimestamp: shardPrimaryTermTimestamp,
		},
		{
			TabletInfo: &topodatapb.Tablet{
				Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 102},
				Hostname:      "localhost",
				Keyspace:      "ks",
				Shard:         "0",
				Type:          topodatapb.TabletType_PRIMARY,
				MysqlHostname: "localhost",
				MysqlPort:     6710,
			},
			DurabilityPolicy:                   policy.DurabilitySemiSync,
			LastCheckValid:                     1,
			IsPrimary:                          1,
			ReadOnly:                           0,
			SemiSyncPrimaryEnabled:             1,
			SemiSyncPrimaryStatus:              1,
			SemiSyncPrimaryWaitForReplicaCount: 2,
			SemiSyncPrimaryClients:             1,
			CurrentTabletType:                  int(topodatapb.TabletType_REPLICA),
			PrimaryTimestamp:                   &stalePrimaryTimestamp,
		},
	}

	rowMaps := make([]sqlutils.RowMap, 0, len(info))
	for _, analysis := range info {
		analysis.SetValuesFromTabletInfo()
		rowMaps = append(rowMaps, analysis.ConvertToRowMap())
	}
	db.Db = test.NewTestDB([][]sqlutils.RowMap{rowMaps, rowMaps})

	// Each sampling should yield the placeholder analysis that represents the future recovery behavior once
	// the demotion logic is implemented, which makes this test fail until the actual fix is in place.
	for range 2 {
		got, err := GetDetectionAnalysis("", "", &DetectionAnalysisHints{})
		require.NoError(t, err, "expected detection analysis to run without error")
		require.Len(t, got, 1, "expected exactly one analysis entry for the shard")
		require.Equal(t, AnalysisCode("StaleTopoPrimary"), got[0].Analysis, "expected stale primary analysis")
		require.Equal(t, "ks", got[0].AnalyzedKeyspace, "expected analysis to target keyspace ks")
		require.Equal(t, "0", got[0].AnalyzedShard, "expected analysis to target shard 0")
	}
}

// TestGetDetectionAnalysis tests the entire GetDetectionAnalysis. It inserts data into the database and runs the function.
// The database is not faked. This is intended to give more test coverage. This test is more comprehensive but more expensive than TestGetDetectionAnalysisDecision.
// This test is somewhere between a unit test, and an end-to-end test. It is specifically useful for testing situations which are hard to come by in end-to-end test, but require
// real-world data to test specifically.
func TestGetDetectionAnalysis(t *testing.T) {
	defer resetPrimaryHealthState()
	// The test is intended to be used as follows. The initial data is stored into the database. Following this, some specific queries are run that each individual test specifies to get the desired state.
	tests := []struct {
		name           string
		sql            []string
		codeWanted     AnalysisCode
		shardWanted    string
		keyspaceWanted string
	}{
		{
			name:       "No additions",
			sql:        nil,
			codeWanted: NoProblem,
		}, {
			name: "Removing Primary Tablet's Vitess record",
			sql: []string{
				// This query removes the primary tablet's vitess_tablet record
				`delete from vitess_tablet where port = 6714`,
			},
			codeWanted:     PrimaryTabletDeleted,
			keyspaceWanted: "ks",
			shardWanted:    "0",
		}, {
			name: "Removing Primary Tablet's MySQL record",
			sql: []string{
				// This query removes the primary tablet's database_instance record
				`delete from database_instance where port = 6714`,
			},
			// As long as we have the vitess record stating that this tablet is the primary
			// It would be incorrect to run a PRS.
			// We should still flag this tablet as Invalid.
			codeWanted:     InvalidPrimary,
			keyspaceWanted: "ks",
			shardWanted:    "0",
		}, {
			name: "Removing Replica Tablet's MySQL record",
			sql: []string{
				// This query removes the replica tablet's database_instance record
				`delete from database_instance where port = 6711`,
			},
			// As long as we don't have the MySQL information, we shouldn't do anything.
			// We should wait for the MySQL information to be refreshed once.
			// This situation only happens when we haven't been able to read the MySQL information even once for this tablet.
			// So it is likely a new tablet.
			codeWanted:     InvalidReplica,
			keyspaceWanted: "ks",
			shardWanted:    "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each test should clear the database. The easiest way to do that is to run all the initialization commands again.
			defer func() {
				db.ClearVTOrcDatabase()
			}()

			for _, query := range append(initialSQL, tt.sql...) {
				_, err := db.ExecVTOrc(query)
				require.NoError(t, err)
			}

			got, err := GetDetectionAnalysis("", "", &DetectionAnalysisHints{})
			require.NoError(t, err)
			if tt.codeWanted == NoProblem {
				require.Empty(t, got)
				return
			}
			require.Len(t, got, 1)
			require.Equal(t, tt.codeWanted, got[0].Analysis)
			require.Equal(t, tt.keyspaceWanted, got[0].AnalyzedKeyspace)
			require.Equal(t, tt.shardWanted, got[0].AnalyzedShard)
		})
	}
}

// TestGetDetectionAnalysisShardEligibleObservers verifies that shard_eligible_observers — the
// expected observer count the quorum gate feeds to EvaluatePrimaryQuorum — counts only the shard's
// REPLICA/RDONLY tablets (the shard-peer health voters). A SPARE (or any non-REPLICA/RDONLY) tablet
// in the shard must not inflate it: topo.IsReplicaType(SPARE) is true, so an IsReplicaType-based
// count would include it and could wrongly block an ERS even when every actual observer agrees the
// primary is down. The count is deliberately NOT gated on the (dynamic) quorum-ERS flag: the
// consumers re-read the flag later in the cycle, so a flag flip mid-cycle must still see the real
// denominator rather than a baked-in 0 that would degenerate the strict-majority gate.
func TestGetDetectionAnalysisShardEligibleObservers(t *testing.T) {
	defer resetPrimaryHealthState()
	defer db.ClearVTOrcDatabase()

	// Seed the standard ks/0 topology (1 PRIMARY + 2 REPLICA + 1 RDONLY = 3 eligible observers) and
	// drop the primary's MySQL record so it surfaces as an InvalidPrimary row we can read.
	for _, query := range append(initialSQL, `delete from database_instance where port = 6714`) {
		_, err := db.ExecVTOrc(query)
		require.NoError(t, err)
	}
	// Add a SPARE tablet to the same shard. It must NOT be counted as an eligible observer.
	require.NoError(t, SaveTablet(&topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "zone1", Uid: 399},
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_SPARE,
		Hostname:      "localhost",
		MysqlHostname: "localhost",
		MysqlPort:     6799,
	}))

	// primaryEligibleObservers runs the real analysis query and returns the ks/0 primary row's
	// ShardEligibleObservers count.
	primaryEligibleObservers := func(t *testing.T) uint {
		t.Helper()
		got, err := GetDetectionAnalysis("", "", &DetectionAnalysisHints{})
		require.NoError(t, err)
		var primaryAnalysis *DetectionAnalysis
		for _, a := range got {
			if a.AnalyzedKeyspace == "ks" && a.AnalyzedShard == "0" && a.TabletType == topodatapb.TabletType_PRIMARY {
				primaryAnalysis = a
				break
			}
		}
		require.NotNil(t, primaryAnalysis, "expected an analysis row for the ks/0 primary")
		return primaryAnalysis.ShardEligibleObservers
	}

	// Even with quorum ERS disabled (the default), the count is computed: the flag is dynamic and
	// its consumers re-read it later in the analysis cycle, so the denominator must never be a
	// baked-in 0 just because the flag was off when the query was built.
	assert.Equal(t, uint(3), primaryEligibleObservers(t),
		"shard_eligible_observers must be computed regardless of the quorum-ERS flag")

	// With quorum ERS enabled, only the shard's 2 REPLICA + 1 RDONLY tablets are eligible observers;
	// the SPARE and PRIMARY must be excluded.
	config.SetERSOnTabletUnreachable(true)
	t.Cleanup(func() { config.SetERSOnTabletUnreachable(false) })
	assert.Equal(t, uint(3), primaryEligibleObservers(t),
		"only the shard's 2 REPLICA + 1 RDONLY tablets are eligible observers; the SPARE and PRIMARY must be excluded")

	// ShardEligibleObserverCount — the standalone helper the /api/shard-tablet-health-quorum endpoint uses to source
	// the same expected observer count without an analysis row — must agree with the analysis path so
	// the endpoint's verdict matches the actionable ERS decision. It is not feature-gated.
	count, err := ShardEligibleObserverCount("ks", "0")
	require.NoError(t, err)
	assert.Equal(t, 3, count,
		"ShardEligibleObserverCount must match the analysis path: 2 REPLICA + 1 RDONLY, excluding the SPARE and PRIMARY")
}

// TestAuditInstanceAnalysisInChangelog tests the functionality of the auditInstanceAnalysisInChangelog function
// and verifies that we write the correct number of times to the database.
func TestAuditInstanceAnalysisInChangelog(t *testing.T) {
	tests := []struct {
		name            string
		cacheExpiration time.Duration
	}{
		{
			name:            "Long expiration",
			cacheExpiration: 2 * time.Minute,
		}, {
			name:            "Very short expiration",
			cacheExpiration: 100 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the cache for the test to use.
			oldRecentInstantAnalysisCache := recentInstantAnalysis
			oldAnalysisChangeWriteCounter := analysisChangeWriteCounter

			recentInstantAnalysis = cache.New(tt.cacheExpiration, 100*time.Millisecond)
			before := analysisChangeWriteCounter.Get()

			defer func() {
				// Set the old values back.
				recentInstantAnalysis = oldRecentInstantAnalysisCache
				analysisChangeWriteCounter = oldAnalysisChangeWriteCounter
				// Each test should clear the database. The easiest way to do that is to run all the initialization commands again.
				db.ClearVTOrcDatabase()
			}()

			updates := []struct {
				tabletAlias             *topodatapb.TabletAlias
				analysisCode            AnalysisCode
				writeCounterExpectation int64
				wantErr                 string
			}{
				{
					// Store a new analysis for the zone1-100 tablet.
					tabletAlias:             &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					analysisCode:            ReplicationStopped,
					writeCounterExpectation: 1,
				}, {
					// Write the same analysis, no new write should happen.
					tabletAlias:             &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					analysisCode:            ReplicationStopped,
					writeCounterExpectation: 1,
				}, {
					// Change the analysis. This should trigger an update.
					tabletAlias:             &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					analysisCode:            ReplicaSemiSyncMustBeSet,
					writeCounterExpectation: 2,
				},
			}

			for _, upd := range updates {
				// We sleep 200 milliseconds to make sure that the cache has had time to update.
				// It should be able to delete entries if the expiration is less than 200 milliseconds.
				time.Sleep(200 * time.Millisecond)
				err := auditInstanceAnalysisInChangelog(upd.tabletAlias, upd.analysisCode)
				if upd.wantErr != "" {
					require.EqualError(t, err, upd.wantErr)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, upd.writeCounterExpectation, analysisChangeWriteCounter.Get()-before)
			}
		})
	}
}

// TestPostProcessAnalyses tests the functionality of the postProcessAnalyses function.
func TestPostProcessAnalyses(t *testing.T) {
	keyspace := "ks"
	shard0 := "0"
	shard80 := "80-"
	clusters := map[string]*clusterAnalysis{
		getKeyspaceShardName(keyspace, shard0): {
			totalTablets: 4,
		},
		getKeyspaceShardName(keyspace, shard80): {
			totalTablets: 3,
		},
	}

	// Shared fixtures for the QuorumDetail cases below. The quorum matcher records QuorumDetail as a
	// side effect while matching; postProcessAnalyses folds it into the winning quorum analysis and
	// drops it from any other analysis that won instead.
	quorumDescription := "Primary vttablet is unreachable by VTOrc and confirmed down by a quorum of the shard's replicas"
	quorumDetail := &QuorumResult{
		PrimaryAlias: "zone1-0000000100", Keyspace: keyspace, Shard: shard0,
		Down: true, DownVotes: 1, TotalObservers: 1, Fraction: 1, MinObservers: 1,
		Observers: []ObserverVote{{Alias: "zone1-0000000101", Vote: voteDown, ConsecutiveFailures: 5, Fresh: true}},
	}

	// Cold-start fixtures: VTOrc has never reached the primary's vttablet (no instance row), so
	// the analysis is InvalidPrimary while the reachable replicas' fresh shard-peer reports can
	// still confirm the primary down. postProcessAnalyses upgrades such an analysis to
	// PrimaryTabletUnreachableByQuorum so the recovery can run; with absent quorum data or the
	// feature disabled it must stay InvalidPrimary (fail closed).
	// invalidPrimaryAnalysis builds the cold-start primary analysis. CountReplicas is left 0 (for a
	// real InvalidPrimary it is joined through the primary's database_instance row, which VTOrc never
	// created); ShardEligibleObservers carries the shard's REPLICA/RDONLY count from the analysis
	// query, which is the expected observer population the quorum gate must use.
	invalidPrimaryAnalysis := func(shardEligibleObservers uint) *DetectionAnalysis {
		return &DetectionAnalysis{
			Analysis:               InvalidPrimary,
			Description:            "VTOrc hasn't been able to reach the primary even once since restart/shutdown",
			AnalyzedInstanceAlias:  &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
			AnalyzedKeyspace:       keyspace,
			AnalyzedShard:          shard0,
			TabletType:             topodatapb.TabletType_PRIMARY,
			ShardEligibleObservers: shardEligibleObservers,
		}
	}
	// shutdownInvalidPrimaryAnalysis is an InvalidPrimary whose vttablet was gracefully shut down
	// (TabletShutdownTime stamped), so the quorum path must fail closed and leave it InvalidPrimary.
	shutdownInvalidPrimaryAnalysis := func(shardEligibleObservers uint) *DetectionAnalysis {
		a := invalidPrimaryAnalysis(shardEligibleObservers)
		a.IsTabletShutdown = true
		return a
	}
	upgradedQuorumDetail := &QuorumResult{
		PrimaryAlias: "zone1-0000000100", Keyspace: keyspace, Shard: shard0,
		Down: true, DownVotes: 2, TotalObservers: 2, EligibleObservers: 2, ExpectedObservers: 2, Fraction: 1, MinObservers: 1,
		Observers: []ObserverVote{
			{Alias: "zone1-0000000101", TabletType: "REPLICA", Vote: voteDown, ConsecutiveFailures: 5, Fresh: true},
			{Alias: "zone1-0000000102", TabletType: "REPLICA", Vote: voteDown, ConsecutiveFailures: 5, Fresh: true},
		},
	}
	seedQuorumDown := func(t *testing.T) {
		t.Helper()
		resetShardPeerHealth()
		t.Cleanup(resetShardPeerHealth)
		now := time.Now()
		primary := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
		RecordShardPeerHealth(&topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, topodatapb.TabletType_REPLICA, keyspace, shard0, reportFor(primary, 5, 0, now), now)
		RecordShardPeerHealth(&topodatapb.TabletAlias{Cell: "zone1", Uid: 102}, topodatapb.TabletType_REPLICA, keyspace, shard0, reportFor(primary, 5, 0, now), now)
	}
	enableERSOnTabletUnreachable := func(t *testing.T) {
		t.Helper()
		config.SetERSOnTabletUnreachable(true)
		t.Cleanup(func() { config.SetERSOnTabletUnreachable(false) })
	}

	tests := []struct {
		name     string
		prep     func(t *testing.T)
		analyses []*DetectionAnalysis
		want     []*DetectionAnalysis
	}{
		{
			name: "No processing needed",
			analyses: []*DetectionAnalysis{
				{
					Analysis:         ReplicationStopped,
					AnalyzedKeyspace: keyspace,
					AnalyzedShard:    shard0,
					TabletType:       topodatapb.TabletType_REPLICA,
					LastCheckValid:   true,
				}, {
					Analysis:         ReplicaSemiSyncMustBeSet,
					AnalyzedKeyspace: keyspace,
					AnalyzedShard:    shard0,
					LastCheckValid:   true,
					TabletType:       topodatapb.TabletType_REPLICA,
				}, {
					Analysis:         PrimaryHasPrimary,
					AnalyzedKeyspace: keyspace,
					AnalyzedShard:    shard0,
					LastCheckValid:   true,
					TabletType:       topodatapb.TabletType_REPLICA,
				},
			},
		},
		{
			name: "Conversion of InvalidPrimary to DeadPrimary",
			analyses: []*DetectionAnalysis{
				{
					Analysis:              InvalidPrimary,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_PRIMARY,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 202},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard80,
					TabletType:            topodatapb.TabletType_RDONLY,
				}, {
					Analysis:              ConnectedToWrongPrimary,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_REPLICA,
					ReplicationStopped:    true,
				}, {
					Analysis:              ReplicationStopped,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_RDONLY,
					ReplicationStopped:    true,
				}, {
					Analysis:              InvalidReplica,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 108},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_REPLICA,
					LastCheckValid:        false,
				}, {
					Analysis:              NoProblem,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 302},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard80,
					LastCheckValid:        true,
					TabletType:            topodatapb.TabletType_REPLICA,
				},
			},
			want: []*DetectionAnalysis{
				{
					Analysis:              DeadPrimary,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_PRIMARY,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 202},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard80,
					TabletType:            topodatapb.TabletType_RDONLY,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 302},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard80,
					TabletType:            topodatapb.TabletType_REPLICA,
				},
			},
		},
		{
			name: "Unable to convert InvalidPrimary to DeadPrimary",
			analyses: []*DetectionAnalysis{
				{
					Analysis:              InvalidPrimary,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_PRIMARY,
				}, {
					Analysis:              NoProblem,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 202},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard80,
					LastCheckValid:        true,
					TabletType:            topodatapb.TabletType_RDONLY,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_REPLICA,
				}, {
					Analysis:              ReplicationStopped,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 102},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_RDONLY,
					ReplicationStopped:    true,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 302},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard80,
					TabletType:            topodatapb.TabletType_REPLICA,
				},
			},
		},
		{
			name: "QuorumDetail dropped from a non-quorum analysis",
			analyses: []*DetectionAnalysis{
				{
					Analysis:              DeadPrimary,
					Description:           "Primary cannot be reached by vtorc and none of its replicas is replicating",
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_PRIMARY,
					QuorumDetail: &QuorumResult{
						PrimaryAlias: "zone1-0000000100", Keyspace: keyspace, Shard: shard0,
						Down: true, DownVotes: 2, TotalObservers: 2,
					},
				},
			},
			want: []*DetectionAnalysis{
				{
					Analysis:              DeadPrimary,
					Description:           "Primary cannot be reached by vtorc and none of its replicas is replicating",
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_PRIMARY,
				},
			},
		},
		{
			name: "QuorumDetail summary folded into quorum analysis description",
			analyses: []*DetectionAnalysis{
				{
					Analysis:              PrimaryTabletUnreachableByQuorum,
					Description:           quorumDescription,
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_PRIMARY,
					QuorumDetail:          quorumDetail,
				},
			},
			want: []*DetectionAnalysis{
				{
					Analysis:              PrimaryTabletUnreachableByQuorum,
					Description:           quorumDescription + " [" + quorumDetail.Summary() + "]",
					AnalyzedInstanceAlias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					AnalyzedKeyspace:      keyspace,
					AnalyzedShard:         shard0,
					TabletType:            topodatapb.TabletType_PRIMARY,
					QuorumDetail:          quorumDetail,
				},
			},
		},
		{
			name: "InvalidPrimary upgraded to PrimaryTabletUnreachableByQuorum when a fresh quorum confirms the primary down",
			prep: func(t *testing.T) {
				enableERSOnTabletUnreachable(t)
				seedQuorumDown(t)
			},
			// The shard has two eligible (REPLICA/RDONLY) observers (ShardEligibleObservers, from the
			// analysis query); both report the primary down. The healthy replicas are NoProblem and so
			// are absent from the analysis result — only the InvalidPrimary primary is present.
			analyses: []*DetectionAnalysis{invalidPrimaryAnalysis(2)},
			want: []*DetectionAnalysis{
				{
					Analysis:               PrimaryTabletUnreachableByQuorum,
					Description:            quorumDescription + " [" + upgradedQuorumDetail.Summary() + "]",
					AnalyzedInstanceAlias:  &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
					AnalyzedKeyspace:       keyspace,
					AnalyzedShard:          shard0,
					TabletType:             topodatapb.TabletType_PRIMARY,
					ShardEligibleObservers: 2,
					QuorumDetail:           upgradedQuorumDetail,
				},
			},
		},
		{
			// Regression for the cold-start minority hole: the shard has three eligible observers
			// (ShardEligibleObservers) but only one fresh down report. The expected count must come
			// from the shard's REPLICA/RDONLY population, NOT from the analysis result — the healthy
			// replicas are NoProblem and dropped before postProcessAnalyses, so a result-derived count
			// would be 0 and let this single report become a 1/1 quorum. With the real count of 3, one
			// report is a minority (1 of 3) and must NOT upgrade to a quorum failover.
			name: "InvalidPrimary stays when only a minority of the shard's observers report down",
			prep: func(t *testing.T) {
				enableERSOnTabletUnreachable(t)
				resetShardPeerHealth()
				t.Cleanup(resetShardPeerHealth)
				now := time.Now()
				primary := &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}
				RecordShardPeerHealth(&topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, topodatapb.TabletType_REPLICA, keyspace, shard0, reportFor(primary, 5, 0, now), now)
			},
			analyses: []*DetectionAnalysis{invalidPrimaryAnalysis(3)},
			want:     []*DetectionAnalysis{invalidPrimaryAnalysis(3)},
		},
		{
			name: "InvalidPrimary stays without quorum data (fail closed)",
			prep: func(t *testing.T) {
				enableERSOnTabletUnreachable(t)
				resetShardPeerHealth()
			},
			analyses: []*DetectionAnalysis{invalidPrimaryAnalysis(2)},
			want:     []*DetectionAnalysis{invalidPrimaryAnalysis(2)},
		},
		{
			name: "InvalidPrimary stays when quorum ERS is disabled",
			prep: func(t *testing.T) {
				seedQuorumDown(t)
			},
			analyses: []*DetectionAnalysis{invalidPrimaryAnalysis(2)},
			want:     []*DetectionAnalysis{invalidPrimaryAnalysis(2)},
		},
		{
			// A gracefully shut down primary must NOT be upgraded to a quorum failover even though a
			// fresh quorum of its shard peers reports its vttablet down — the shutdown was an operator
			// action, not a crash. Without the TabletShutdownTime guard this would become a
			// PrimaryTabletUnreachableByQuorum and run ERS.
			name: "InvalidPrimary stays when the primary was intentionally shut down",
			prep: func(t *testing.T) {
				enableERSOnTabletUnreachable(t)
				seedQuorumDown(t)
			},
			analyses: []*DetectionAnalysis{shutdownInvalidPrimaryAnalysis(2)},
			want:     []*DetectionAnalysis{shutdownInvalidPrimaryAnalysis(2)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.prep != nil {
				tt.prep(t)
			}
			if tt.want == nil {
				tt.want = tt.analyses
			}
			result := postProcessAnalyses(tt.analyses, clusters)
			// The quorum upgrade evaluates at time.Now(); zero the timestamp so expected
			// QuorumResult fixtures compare deterministically.
			for _, analysis := range result {
				if analysis.QuorumDetail != nil {
					analysis.QuorumDetail.EvaluatedAt = time.Time{}
				}
			}
			require.ElementsMatch(t, tt.want, result)
		})
	}
}

func TestDeclaresBefore(t *testing.T) {
	tests := []struct {
		name     string
		problem  *DetectionAnalysisProblem
		code     AnalysisCode
		expected bool
	}{
		{
			name:     "ReplicationStopped declares before PrimarySemiSyncBlocked",
			problem:  GetDetectionAnalysisProblem(ReplicationStopped),
			code:     PrimarySemiSyncBlocked,
			expected: true,
		},
		{
			name:     "ReplicationStopped does not declare before DeadPrimary",
			problem:  GetDetectionAnalysisProblem(ReplicationStopped),
			code:     DeadPrimary,
			expected: false,
		},
		{
			name:     "PrimaryIsReadOnly declares before PrimarySemiSyncBlocked",
			problem:  GetDetectionAnalysisProblem(PrimaryIsReadOnly),
			code:     PrimarySemiSyncBlocked,
			expected: true,
		},
		{
			name:     "PrimaryIsReadOnly does not declare before PrimaryDiskStalled",
			problem:  GetDetectionAnalysisProblem(PrimaryIsReadOnly),
			code:     PrimaryDiskStalled,
			expected: false,
		},
		{
			name:     "ReplicationStopped does not declare before PrimaryDiskStalled",
			problem:  GetDetectionAnalysisProblem(ReplicationStopped),
			code:     PrimaryDiskStalled,
			expected: false,
		},
		{
			name:     "problem with no BeforeAnalyses",
			problem:  GetDetectionAnalysisProblem(NotConnectedToPrimary),
			code:     PrimarySemiSyncBlocked,
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, declaresBefore(tt.problem, tt.code))
		})
	}
}

func TestDeclaresAfter(t *testing.T) {
	tests := []struct {
		name             string
		shardWideProblem *DetectionAnalysisProblem
		code             AnalysisCode
		expected         bool
	}{
		{
			name: "shard-wide problem with AfterAnalyses referencing suppressed code",
			shardWideProblem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{
					Analysis:    PrimarySemiSyncBlocked,
					Description: "test shard-wide",
					Priority:    detectionAnalysisPriorityShardWideAction,
				},
				AfterAnalyses: []AnalysisCode{ReplicationStopped},
			},
			code:     ReplicationStopped,
			expected: true,
		},
		{
			name: "shard-wide problem with AfterAnalyses not referencing suppressed code",
			shardWideProblem: &DetectionAnalysisProblem{
				Meta: &DetectionAnalysisProblemMeta{
					Analysis:    PrimarySemiSyncBlocked,
					Description: "test shard-wide",
					Priority:    detectionAnalysisPriorityShardWideAction,
				},
				AfterAnalyses: []AnalysisCode{ReplicationStopped},
			},
			code:     NotConnectedToPrimary,
			expected: false,
		},
		{
			name:             "shard-wide problem with no AfterAnalyses",
			shardWideProblem: GetDetectionAnalysisProblem(PrimarySemiSyncBlocked),
			code:             ReplicationStopped,
			expected:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, declaresAfter(tt.shardWideProblem, tt.code))
		})
	}
}
