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
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/test"
)

var (
	// The initialSQL is a set of insert commands copied from a dump of an actual running VTOrc instances. The relevant insert commands are here.
	// This is a dump taken from a test running 4 tablets, zone1-101 is the primary, zone1-100 is a replica, zone1-112 is a rdonly and zone2-200 is a cross-cell replica.
	initialSQL = []string{
		`INSERT INTO database_instance VALUES('zone1-0000000112','localhost',6747,'2022-12-28 07:26:04','2022-12-28 07:26:04',213696377,'8.0.31','ROW',1,1,'vt-0000000112-bin.000001',15963,'localhost',6714,1,1,'vt-0000000101-bin.000001',15583,'vt-0000000101-bin.000001',15583,0,0,1,'','',1,0,'vt-0000000112-relay-bin.000002',15815,0,1,0,'zone1','',0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a5138-8680-11ed-9240-92a06c3be3c2','2022-12-28 07:26:04','',1,0,0,'Homebrew','8.0','FULL',10816929,0,0,'ON',1,'729a4cc4-8680-11ed-a104-47706090afbd','','729a4cc4-8680-11ed-a104-47706090afbd,729a5138-8680-11ed-9240-92a06c3be3c2',1,1,'',1000000000000000000,1,0,0,0);`,
		`INSERT INTO database_instance VALUES('zone1-0000000100','localhost',6711,'2022-12-28 07:26:04','2022-12-28 07:26:04',1094500338,'8.0.31','ROW',1,1,'vt-0000000100-bin.000001',15963,'localhost',6714,1,1,'vt-0000000101-bin.000001',15583,'vt-0000000101-bin.000001',15583,0,0,1,'','',1,0,'vt-0000000100-relay-bin.000002',15815,0,1,0,'zone1','',0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a5138-8680-11ed-acf8-d6b0ef9f4eaa','2022-12-28 07:26:04','',1,0,0,'Homebrew','8.0','FULL',10103920,0,1,'ON',1,'729a4cc4-8680-11ed-a104-47706090afbd','','729a4cc4-8680-11ed-a104-47706090afbd,729a5138-8680-11ed-acf8-d6b0ef9f4eaa',1,1,'',1000000000000000000,1,0,1,0);`,
		`INSERT INTO database_instance VALUES('zone1-0000000101','localhost',6714,'2022-12-28 07:26:04','2022-12-28 07:26:04',390954723,'8.0.31','ROW',1,1,'vt-0000000101-bin.000001',15583,'',0,0,0,'',0,'',0,NULL,NULL,0,'','',0,0,'',0,0,0,0,'zone1','',0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a4cc4-8680-11ed-a104-47706090afbd','2022-12-28 07:26:04','',0,0,0,'Homebrew','8.0','FULL',11366095,1,1,'ON',1,'','','729a4cc4-8680-11ed-a104-47706090afbd',-1,-1,'',1000000000000000000,1,1,0,2);`,
		`INSERT INTO database_instance VALUES('zone2-0000000200','localhost',6756,'2022-12-28 07:26:05','2022-12-28 07:26:05',444286571,'8.0.31','ROW',1,1,'vt-0000000200-bin.000001',15963,'localhost',6714,1,1,'vt-0000000101-bin.000001',15583,'vt-0000000101-bin.000001',15583,0,0,1,'','',1,0,'vt-0000000200-relay-bin.000002',15815,0,1,0,'zone2','',0,0,0,1,'729a4cc4-8680-11ed-a104-47706090afbd:1-54','729a497c-8680-11ed-8ad4-3f51d747db75','2022-12-28 07:26:05','',1,0,0,'Homebrew','8.0','FULL',10443112,0,1,'ON',1,'729a4cc4-8680-11ed-a104-47706090afbd','','729a4cc4-8680-11ed-a104-47706090afbd,729a497c-8680-11ed-8ad4-3f51d747db75',1,1,'',1000000000000000000,1,0,1,0);`,
		`INSERT INTO vitess_tablet VALUES('zone1-0000000100','localhost',6711,'ks','0','zone1',2,'0001-01-01 00:00:00+00:00',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3130307d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a363731307d20706f72745f6d61703a7b6b65793a227674222076616c75653a363730397d206b657973706163653a226b73222073686172643a22302220747970653a5245504c494341206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a363731312064625f7365727665725f76657273696f6e3a22382e302e3331222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
		`INSERT INTO vitess_tablet VALUES('zone1-0000000101','localhost',6714,'ks','0','zone1',1,'2022-12-28 07:23:25.129898+00:00',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3130317d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a363731337d20706f72745f6d61703a7b6b65793a227674222076616c75653a363731327d206b657973706163653a226b73222073686172643a22302220747970653a5052494d415259206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a36373134207072696d6172795f7465726d5f73746172745f74696d653a7b7365636f6e64733a31363732323132323035206e616e6f7365636f6e64733a3132393839383030307d2064625f7365727665725f76657273696f6e3a22382e302e3331222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
		`INSERT INTO vitess_tablet VALUES('zone1-0000000112','localhost',6747,'ks','0','zone1',3,'0001-01-01 00:00:00+00:00',X'616c6961733a7b63656c6c3a227a6f6e653122207569643a3131327d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a363734367d20706f72745f6d61703a7b6b65793a227674222076616c75653a363734357d206b657973706163653a226b73222073686172643a22302220747970653a52444f4e4c59206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a363734372064625f7365727665725f76657273696f6e3a22382e302e3331222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
		`INSERT INTO vitess_tablet VALUES('zone2-0000000200','localhost',6756,'ks','0','zone2',2,'0001-01-01 00:00:00+00:00',X'616c6961733a7b63656c6c3a227a6f6e653222207569643a3230307d20686f73746e616d653a226c6f63616c686f73742220706f72745f6d61703a7b6b65793a2267727063222076616c75653a363735357d20706f72745f6d61703a7b6b65793a227674222076616c75653a363735347d206b657973706163653a226b73222073686172643a22302220747970653a5245504c494341206d7973716c5f686f73746e616d653a226c6f63616c686f737422206d7973716c5f706f72743a363735362064625f7365727665725f76657273696f6e3a22382e302e3331222064656661756c745f636f6e6e5f636f6c6c6174696f6e3a3435');`,
		`INSERT INTO vitess_keyspace VALUES('ks',0,'semi_sync');`,
	}
)

// TestGetReplicationAnalysisDecision tests the code of GetReplicationAnalysis decision-making. It doesn't check the SQL query
// run by it. It only checks the analysis part after the rows have been read. This tests fakes the db and explicitly returns the
// rows that are specified in the test.
func TestGetReplicationAnalysisDecision(t *testing.T) {
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
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
				},
				LastCheckValid: 1,
				ReadOnly:       0,
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
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 102},
				},
				LastCheckValid: 1,
				ReadOnly:       1,
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
				DurabilityPolicy: "none",
				PrimaryTabletInfo: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
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
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
				},
				DurabilityPolicy:       "semi_sync",
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
					Alias: &topodatapb.TabletAlias{Cell: "zon1", Uid: 101},
				},
				DurabilityPolicy:       "none",
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
		}, {
			// If the database_instance table for a tablet is empty (discovery of MySQL information hasn't happened yet or failed)
			// then we shouldn't run a failure fix on it until the discovery succeeds
			name: "Empty database_instance table",
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
				IsInvalid:        1,
				DurabilityPolicy: "semi_sync",
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     InvalidReplica,
		}, {
			name: "DeadPrimary when VTOrc is starting up",
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
				DurabilityPolicy: "none",
				IsInvalid:        1,
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
				LastCheckValid:     1,
				ReplicationStopped: 1,
			}, {
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 103},
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
		}, {
			name: "Invalid Primary",
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
				DurabilityPolicy: "none",
				IsInvalid:        1,
			}},
			keyspaceWanted: "ks",
			shardWanted:    "0",
			codeWanted:     InvalidPrimary,
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

			got, err := GetReplicationAnalysis("", "", &ReplicationAnalysisHints{})
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

// TestGetReplicationAnalysis tests the entire GetReplicationAnalysis. It inserts data into the database and runs the function.
// The database is not faked. This is intended to give more test coverage. This test is more comprehensive but more expensive than TestGetReplicationAnalysisDecision.
// This test is somewhere between a unit test, and an end-to-end test. It is specifically useful for testing situations which are hard to come by in end-to-end test, but require
// real-world data to test specifically.
func TestGetReplicationAnalysis(t *testing.T) {
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
			codeWanted:     ClusterHasNoPrimary,
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

			got, err := GetReplicationAnalysis("", "", &ReplicationAnalysisHints{})
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
			analysisChangeWriteCounter = metrics.NewCounter()

			defer func() {
				// Set the old values back.
				recentInstantAnalysis = oldRecentInstantAnalysisCache
				analysisChangeWriteCounter = oldAnalysisChangeWriteCounter
				// Each test should clear the database. The easiest way to do that is to run all the initialization commands again.
				db.ClearVTOrcDatabase()
			}()

			updates := []struct {
				tabletAlias             string
				analysisCode            AnalysisCode
				writeCounterExpectation int
				wantErr                 string
			}{
				{
					// Store a new analysis for the zone1-100 tablet.
					tabletAlias:             "zone1-100",
					analysisCode:            ReplicationStopped,
					writeCounterExpectation: 1,
				}, {
					// Write the same analysis, no new write should happen.
					tabletAlias:             "zone1-100",
					analysisCode:            ReplicationStopped,
					writeCounterExpectation: 1,
				}, {
					// Change the analysis. This should trigger an update.
					tabletAlias:             "zone1-100",
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
				require.EqualValues(t, upd.writeCounterExpectation, analysisChangeWriteCounter.Count())
			}
		})
	}
}

// TestPostProcessAnalyses tests the functionality of the postProcessAnalyses function.
func TestPostProcessAnalyses(t *testing.T) {
	ks0 := ClusterInfo{
		Keyspace:       "ks",
		Shard:          "0",
		CountInstances: 4,
	}
	ks80 := ClusterInfo{
		Keyspace:       "ks",
		Shard:          "80-",
		CountInstances: 3,
	}
	clusters := map[string]*clusterAnalysis{
		getKeyspaceShardName(ks0.Keyspace, ks0.Shard): {
			totalTablets: int(ks0.CountInstances),
		},
		getKeyspaceShardName(ks80.Keyspace, ks80.Shard): {
			totalTablets: int(ks80.CountInstances),
		},
	}

	tests := []struct {
		name     string
		analyses []*ReplicationAnalysis
		want     []*ReplicationAnalysis
	}{
		{
			name: "No processing needed",
			analyses: []*ReplicationAnalysis{
				{
					Analysis:       ReplicationStopped,
					TabletType:     topodatapb.TabletType_REPLICA,
					LastCheckValid: true,
					ClusterDetails: ks0,
				}, {
					Analysis:       ReplicaSemiSyncMustBeSet,
					LastCheckValid: true,
					TabletType:     topodatapb.TabletType_REPLICA,
					ClusterDetails: ks0,
				}, {
					Analysis:       PrimaryHasPrimary,
					LastCheckValid: true,
					TabletType:     topodatapb.TabletType_REPLICA,
					ClusterDetails: ks0,
				},
			},
		}, {
			name: "Conversion of InvalidPrimary to DeadPrimary",
			analyses: []*ReplicationAnalysis{
				{
					Analysis:              InvalidPrimary,
					AnalyzedInstanceAlias: "zone1-100",
					TabletType:            topodatapb.TabletType_PRIMARY,
					ClusterDetails:        ks0,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: "zone1-202",
					TabletType:            topodatapb.TabletType_RDONLY,
					ClusterDetails:        ks80,
				}, {
					Analysis:              ConnectedToWrongPrimary,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: "zone1-101",
					TabletType:            topodatapb.TabletType_REPLICA,
					ReplicationStopped:    true,
					ClusterDetails:        ks0,
				}, {
					Analysis:              ReplicationStopped,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: "zone1-102",
					TabletType:            topodatapb.TabletType_RDONLY,
					ReplicationStopped:    true,
					ClusterDetails:        ks0,
				}, {
					Analysis:              InvalidReplica,
					AnalyzedInstanceAlias: "zone1-108",
					TabletType:            topodatapb.TabletType_REPLICA,
					LastCheckValid:        false,
					ClusterDetails:        ks0,
				}, {
					Analysis:              NoProblem,
					AnalyzedInstanceAlias: "zone1-302",
					LastCheckValid:        true,
					TabletType:            topodatapb.TabletType_REPLICA,
					ClusterDetails:        ks80,
				},
			},
			want: []*ReplicationAnalysis{
				{
					Analysis:              DeadPrimary,
					AnalyzedInstanceAlias: "zone1-100",
					TabletType:            topodatapb.TabletType_PRIMARY,
					ClusterDetails:        ks0,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: "zone1-202",
					TabletType:            topodatapb.TabletType_RDONLY,
					ClusterDetails:        ks80,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: "zone1-302",
					TabletType:            topodatapb.TabletType_REPLICA,
					ClusterDetails:        ks80,
				},
			},
		},
		{
			name: "Unable to convert InvalidPrimary to DeadPrimary",
			analyses: []*ReplicationAnalysis{
				{
					Analysis:              InvalidPrimary,
					AnalyzedInstanceAlias: "zone1-100",
					TabletType:            topodatapb.TabletType_PRIMARY,
					ClusterDetails:        ks0,
				}, {
					Analysis:              NoProblem,
					AnalyzedInstanceAlias: "zone1-202",
					LastCheckValid:        true,
					TabletType:            topodatapb.TabletType_RDONLY,
					ClusterDetails:        ks80,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: "zone1-101",
					TabletType:            topodatapb.TabletType_REPLICA,
					ClusterDetails:        ks0,
				}, {
					Analysis:              ReplicationStopped,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: "zone1-102",
					TabletType:            topodatapb.TabletType_RDONLY,
					ReplicationStopped:    true,
					ClusterDetails:        ks0,
				}, {
					Analysis:              NoProblem,
					LastCheckValid:        true,
					AnalyzedInstanceAlias: "zone1-302",
					TabletType:            topodatapb.TabletType_REPLICA,
					ClusterDetails:        ks80,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.want == nil {
				tt.want = tt.analyses
			}
			result := postProcessAnalyses(tt.analyses, clusters)
			require.ElementsMatch(t, tt.want, result)
		})
	}
}
