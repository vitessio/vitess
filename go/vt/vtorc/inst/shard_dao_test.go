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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/protoutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtorcdatapb "vitess.io/vitess/go/vt/proto/vtorcdata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/db"
)

func TestSaveReadAndDeleteShard(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	timeToUse := time.Date(2023, 7, 24, 5, 0, 5, 1000, time.UTC)
	tests := []struct {
		name                   string
		keyspaceName           string
		shardName              string
		shard                  *topodatapb.Shard
		primaryAliasWanted     string
		primaryTimestampWanted time.Time
		err                    string
	}{
		{
			name:         "Success",
			keyspaceName: "ks1",
			shardName:    "80-",
			shard: &topodatapb.Shard{
				PrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  301,
				},
				PrimaryTermStartTime: protoutil.TimeToProto(timeToUse.Add(1 * time.Hour)),
			},
			primaryTimestampWanted: timeToUse.Add(1 * time.Hour).UTC(),
			primaryAliasWanted:     "zone1-0000000301",
		}, {
			name:         "Success with empty primary alias",
			keyspaceName: "ks1",
			shardName:    "-",
			shard: &topodatapb.Shard{
				PrimaryTermStartTime: protoutil.TimeToProto(timeToUse),
			},
			primaryTimestampWanted: timeToUse.UTC(),
			primaryAliasWanted:     "",
		}, {
			name:         "Success with empty primary term start time",
			keyspaceName: "ks1",
			shardName:    "80-",
			shard: &topodatapb.Shard{
				PrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  301,
				},
			},
			primaryTimestampWanted: time.Time{},
			primaryAliasWanted:     "zone1-0000000301",
		},
		{
			name:         "No shard found",
			keyspaceName: "ks1",
			shardName:    "-80",
			err:          ErrShardNotFound.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shard != nil {
				shardInfo := topo.NewShardInfo(tt.keyspaceName, tt.shardName, tt.shard, nil)
				err := SaveShard(shardInfo)
				require.NoError(t, err)
			}

			// ReadShardPrimaryInformation
			shardPrimaryAlias, primaryTimestamp, err := ReadShardPrimaryInformation(tt.keyspaceName, tt.shardName)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tt.primaryAliasWanted, shardPrimaryAlias)
			require.EqualValues(t, tt.primaryTimestampWanted, primaryTimestamp)

			// ReadShardNames
			shardNames, err := ReadShardNames(tt.keyspaceName)
			require.NoError(t, err)
			require.Equal(t, []string{tt.shardName}, shardNames)

			// DeleteShard
			require.NoError(t, DeleteShard(tt.keyspaceName, tt.shardName))
			_, _, err = ReadShardPrimaryInformation(tt.keyspaceName, tt.shardName)
			require.EqualError(t, err, ErrShardNotFound.Error())
		})
	}
}

func TestReadKeyspaceShardStats(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	var uid uint32
	for _, shard := range []string{"-40", "40-80", "80-c0", "c0-"} {
		for i := 0; i < 100; i++ {
			require.NoError(t, SaveTablet(&topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: "cell1",
					Uid:  uid,
				},
				Keyspace: "test",
				Shard:    shard,
			}))
			uid++
		}
	}

	t.Run("no_ers_disabled", func(t *testing.T) {
		shardStats, err := ReadKeyspaceShardStats()
		require.NoError(t, err)
		require.Equal(t, []ShardStats{
			{
				Keyspace:                 "test",
				Shard:                    "-40",
				TabletCount:              100,
				DisableEmergencyReparent: false,
			},
			{
				Keyspace:                 "test",
				Shard:                    "40-80",
				TabletCount:              100,
				DisableEmergencyReparent: false,
			},
			{
				Keyspace:                 "test",
				Shard:                    "80-c0",
				TabletCount:              100,
				DisableEmergencyReparent: false,
			},
			{
				Keyspace:                 "test",
				Shard:                    "c0-",
				TabletCount:              100,
				DisableEmergencyReparent: false,
			},
		}, shardStats)
	})

	t.Run("single_shard_ers_disabled", func(t *testing.T) {
		keyspaceInfo := &topo.KeyspaceInfo{
			Keyspace: &topodatapb.Keyspace{
				KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: policy.DurabilityNone,
			},
		}
		keyspaceInfo.SetKeyspaceName("test")
		require.NoError(t, SaveKeyspace(keyspaceInfo))

		shardInfo := topo.NewShardInfo("test", "-40", &topodatapb.Shard{
			VtorcState: &vtorcdatapb.Shard{
				DisableEmergencyReparent: true,
			},
		}, nil)
		require.NoError(t, SaveShard(shardInfo))

		shardStats, err := ReadKeyspaceShardStats()
		require.NoError(t, err)
		require.Equal(t, []ShardStats{
			{
				Keyspace:                 "test",
				Shard:                    "-40",
				TabletCount:              100,
				DisableEmergencyReparent: true,
			},
			{
				Keyspace:                 "test",
				Shard:                    "40-80",
				TabletCount:              100,
				DisableEmergencyReparent: false,
			},
			{
				Keyspace:                 "test",
				Shard:                    "80-c0",
				TabletCount:              100,
				DisableEmergencyReparent: false,
			},
			{
				Keyspace:                 "test",
				Shard:                    "c0-",
				TabletCount:              100,
				DisableEmergencyReparent: false,
			},
		}, shardStats)
	})

	t.Run("full_keyspace_ers_disabled", func(t *testing.T) {
		keyspaceInfo := &topo.KeyspaceInfo{
			Keyspace: &topodatapb.Keyspace{
				KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: policy.DurabilityNone,
				VtorcState: &vtorcdatapb.Keyspace{
					DisableEmergencyReparent: true,
				},
			},
		}
		keyspaceInfo.SetKeyspaceName("test")
		require.NoError(t, SaveKeyspace(keyspaceInfo))

		shardStats, err := ReadKeyspaceShardStats()
		require.NoError(t, err)
		require.Equal(t, []ShardStats{
			{
				Keyspace:                 "test",
				Shard:                    "-40",
				TabletCount:              100,
				DisableEmergencyReparent: true,
			},
			{
				Keyspace:                 "test",
				Shard:                    "40-80",
				TabletCount:              100,
				DisableEmergencyReparent: true,
			},
			{
				Keyspace:                 "test",
				Shard:                    "80-c0",
				TabletCount:              100,
				DisableEmergencyReparent: true,
			},
			{
				Keyspace:                 "test",
				Shard:                    "c0-",
				TabletCount:              100,
				DisableEmergencyReparent: true,
			},
		}, shardStats)
	})
}
