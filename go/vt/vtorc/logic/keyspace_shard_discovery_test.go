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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

var (
	keyspaceDurabilityNone = &topodatapb.Keyspace{
		KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
		DurabilityPolicy: "none",
	}
	keyspaceDurabilitySemiSync = &topodatapb.Keyspace{
		KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
		DurabilityPolicy: "semi_sync",
	}
	keyspaceDurabilityTest = &topodatapb.Keyspace{
		KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
		DurabilityPolicy: "test",
	}
	keyspaceSnapshot = &topodatapb.Keyspace{
		KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
	}
)

func TestRefreshAllKeyspaces(t *testing.T) {
	// Store the old flags and restore on test completion
	oldTs := ts
	oldClustersToWatch := clustersToWatch
	defer func() {
		ts = oldTs
		clustersToWatch = oldClustersToWatch
	}()

	db.ClearVTOrcDatabase()
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	ts = memorytopo.NewServer("zone1")
	keyspaceNames := []string{"ks1", "ks2", "ks3", "ks4"}
	keyspaces := []*topodatapb.Keyspace{keyspaceDurabilityNone, keyspaceDurabilitySemiSync, keyspaceSnapshot, keyspaceDurabilityTest}

	// Create 4 keyspaces
	for i, keyspace := range keyspaces {
		err := ts.CreateKeyspace(context.Background(), keyspaceNames[i], keyspace)
		require.NoError(t, err)
		for idx, shardName := range []string{"-80", "80-"} {
			err = ts.CreateShard(context.Background(), keyspaceNames[i], shardName)
			require.NoError(t, err)
			_, err = ts.UpdateShardFields(context.Background(), keyspaceNames[i], shardName, func(si *topo.ShardInfo) error {
				si.PrimaryAlias = &topodatapb.TabletAlias{
					Cell: fmt.Sprintf("zone_%v", keyspaceNames[i]),
					Uid:  uint32(100 + idx),
				}
				return nil
			})
			require.NoError(t, err)
		}
	}

	// Set clusters to watch to only watch ks1 and ks3
	onlyKs1and3 := []string{"ks1/-80", "ks3/-80", "ks3/80-"}
	clustersToWatch = onlyKs1and3
	RefreshAllKeyspacesAndShards()

	// Verify that we only have ks1 and ks3 in vtorc's db.
	verifyKeyspaceInfo(t, "ks1", keyspaceDurabilityNone, "")
	verifyPrimaryAlias(t, "ks1", "-80", "zone_ks1-0000000100", "")
	verifyKeyspaceInfo(t, "ks2", nil, "keyspace not found")
	verifyPrimaryAlias(t, "ks2", "80-", "", "shard not found")
	verifyKeyspaceInfo(t, "ks3", keyspaceSnapshot, "")
	verifyPrimaryAlias(t, "ks3", "80-", "zone_ks3-0000000101", "")
	verifyKeyspaceInfo(t, "ks4", nil, "keyspace not found")

	// Set clusters to watch to watch all keyspaces
	clustersToWatch = nil
	// Change the durability policy of ks1
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "ks1", "semi_sync")
	RefreshAllKeyspacesAndShards()

	// Verify that all the keyspaces are correctly reloaded
	verifyKeyspaceInfo(t, "ks1", keyspaceDurabilitySemiSync, "")
	verifyPrimaryAlias(t, "ks1", "-80", "zone_ks1-0000000100", "")
	verifyKeyspaceInfo(t, "ks2", keyspaceDurabilitySemiSync, "")
	verifyPrimaryAlias(t, "ks2", "80-", "zone_ks2-0000000101", "")
	verifyKeyspaceInfo(t, "ks3", keyspaceSnapshot, "")
	verifyPrimaryAlias(t, "ks3", "80-", "zone_ks3-0000000101", "")
	verifyKeyspaceInfo(t, "ks4", keyspaceDurabilityTest, "")
	verifyPrimaryAlias(t, "ks4", "80-", "zone_ks4-0000000101", "")

}

func TestRefreshKeyspace(t *testing.T) {
	// Store the old flags and restore on test completion
	oldTs := ts
	defer func() {
		ts = oldTs
	}()

	defer func() {
		db.ClearVTOrcDatabase()
	}()

	tests := []struct {
		name           string
		keyspaceName   string
		keyspace       *topodatapb.Keyspace
		ts             *topo.Server
		keyspaceWanted *topodatapb.Keyspace
		err            string
	}{
		{
			name:         "Success with keyspaceType and durability",
			keyspaceName: "ks1",
			ts:           memorytopo.NewServer("zone1"),
			keyspace: &topodatapb.Keyspace{
				KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: "semi_sync",
			},
			keyspaceWanted: nil,
			err:            "",
		}, {
			name:         "Success with keyspaceType and no durability",
			keyspaceName: "ks2",
			ts:           memorytopo.NewServer("zone1"),
			keyspace: &topodatapb.Keyspace{
				KeyspaceType: topodatapb.KeyspaceType_NORMAL,
			},
			keyspaceWanted: nil,
			err:            "",
		}, {
			name:         "Success with snapshot keyspaceType",
			keyspaceName: "ks3",
			ts:           memorytopo.NewServer("zone1"),
			keyspace: &topodatapb.Keyspace{
				KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
			},
			keyspaceWanted: nil,
			err:            "",
		}, {
			name:         "Success with fields that are not stored",
			keyspaceName: "ks4",
			ts:           memorytopo.NewServer("zone1"),
			keyspace: &topodatapb.Keyspace{
				KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: "none",
				BaseKeyspace:     "baseKeyspace",
			},
			keyspaceWanted: &topodatapb.Keyspace{
				KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: "none",
			},
			err: "",
		}, {
			name:           "No keyspace found",
			keyspaceName:   "ks5",
			ts:             memorytopo.NewServer("zone1"),
			keyspace:       nil,
			keyspaceWanted: nil,
			err:            "node doesn't exist: keyspaces/ks5/Keyspace",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.keyspaceWanted == nil {
				tt.keyspaceWanted = tt.keyspace
			}

			ts = tt.ts
			if tt.keyspace != nil {
				err := ts.CreateKeyspace(context.Background(), tt.keyspaceName, tt.keyspace)
				require.NoError(t, err)
			}

			err := refreshKeyspace(tt.keyspaceName)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
			} else {
				require.NoError(t, err)
				verifyKeyspaceInfo(t, tt.keyspaceName, tt.keyspaceWanted, "")
			}
		})
	}
}

// verifyKeyspaceInfo verifies that the keyspace information read from the vtorc database
// is the same as the one provided or reading it gives the same error as expected
func verifyKeyspaceInfo(t *testing.T, keyspaceName string, keyspace *topodatapb.Keyspace, errString string) {
	t.Helper()
	ksInfo, err := inst.ReadKeyspace(keyspaceName)
	if errString != "" {
		assert.EqualError(t, err, errString)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, keyspaceName, ksInfo.KeyspaceName())
		assert.True(t, topotools.KeyspaceEquality(keyspace, ksInfo.Keyspace))
	}
}

func TestRefreshShard(t *testing.T) {
	// Store the old flags and restore on test completion
	oldTs := ts
	defer func() {
		ts = oldTs
	}()

	defer func() {
		db.ClearVTOrcDatabase()
	}()

	tests := []struct {
		name               string
		keyspaceName       string
		shardName          string
		shard              *topodatapb.Shard
		ts                 *topo.Server
		primaryAliasWanted string
		err                string
	}{
		{
			name:         "Success with primaryAlias",
			keyspaceName: "ks1",
			shardName:    "0",
			ts:           memorytopo.NewServer("zone1"),
			shard: &topodatapb.Shard{
				PrimaryAlias: &topodatapb.TabletAlias{
					Cell: "zone1",
					Uid:  302,
				},
			},
			primaryAliasWanted: "zone1-0000000302",
			err:                "",
		}, {
			name:               "Success with empty primaryAlias",
			keyspaceName:       "ks1",
			shardName:          "-80",
			ts:                 memorytopo.NewServer("zone1"),
			shard:              &topodatapb.Shard{},
			primaryAliasWanted: "",
			err:                "",
		}, {
			name:         "No shard found",
			keyspaceName: "ks2",
			shardName:    "-",
			ts:           memorytopo.NewServer("zone1"),
			err:          "node doesn't exist: keyspaces/ks2/shards/-/Shard",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts = tt.ts
			if tt.shard != nil {
				_, err := ts.GetOrCreateShard(context.Background(), tt.keyspaceName, tt.shardName)
				require.NoError(t, err)
				_, err = ts.UpdateShardFields(context.Background(), tt.keyspaceName, tt.shardName, func(info *topo.ShardInfo) error {
					info.PrimaryAlias = tt.shard.PrimaryAlias
					return nil
				})
				require.NoError(t, err)
			}

			err := refreshShard(tt.keyspaceName, tt.shardName)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
			} else {
				require.NoError(t, err)
				verifyPrimaryAlias(t, tt.keyspaceName, tt.shardName, tt.primaryAliasWanted, "")
			}
		})
	}
}

// verifyPrimaryAlias verifies the correct primary alias is stored in the database for the given keyspace shard.
func verifyPrimaryAlias(t *testing.T, keyspaceName, shardName string, primaryAliasWanted string, errString string) {
	primaryAlias, _, err := inst.ReadShardPrimaryInformation(keyspaceName, shardName)
	if errString != "" {
		require.ErrorContains(t, err, errString)
		return
	}
	require.NoError(t, err)
	require.Equal(t, primaryAliasWanted, primaryAlias)
}
