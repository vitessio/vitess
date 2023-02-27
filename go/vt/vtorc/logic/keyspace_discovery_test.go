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

	// Open the vtorc
	// After the test completes delete everything from the vitess_keyspace table
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from vitess_keyspace")
		require.NoError(t, err)
	}()

	ts = memorytopo.NewServer("zone1")
	keyspaceNames := []string{"ks1", "ks2", "ks3", "ks4"}
	keyspaces := []*topodatapb.Keyspace{keyspaceDurabilityNone, keyspaceDurabilitySemiSync, keyspaceSnapshot, keyspaceDurabilityTest}

	// Create 4 keyspaces
	for i, keyspace := range keyspaces {
		err := ts.CreateKeyspace(context.Background(), keyspaceNames[i], keyspace)
		require.NoError(t, err)
	}

	// Set clusters to watch to only watch ks1 and ks3
	onlyKs1and3 := []string{"ks1/-", "ks3/-80", "ks3/80-"}
	clustersToWatch = onlyKs1and3
	RefreshAllKeyspaces()

	// Verify that we only have ks1 and ks3 in vtorc's db.
	verifyKeyspaceInfo(t, "ks1", keyspaceDurabilityNone, "")
	verifyKeyspaceInfo(t, "ks2", nil, "keyspace not found")
	verifyKeyspaceInfo(t, "ks3", keyspaceSnapshot, "")
	verifyKeyspaceInfo(t, "ks4", nil, "keyspace not found")

	// Set clusters to watch to watch all keyspaces
	clustersToWatch = nil
	// Change the durability policy of ks1
	reparenttestutil.SetKeyspaceDurability(context.Background(), t, ts, "ks1", "semi_sync")
	RefreshAllKeyspaces()

	// Verify that all the keyspaces are correctly reloaded
	verifyKeyspaceInfo(t, "ks1", keyspaceDurabilitySemiSync, "")
	verifyKeyspaceInfo(t, "ks2", keyspaceDurabilitySemiSync, "")
	verifyKeyspaceInfo(t, "ks3", keyspaceSnapshot, "")
	verifyKeyspaceInfo(t, "ks4", keyspaceDurabilityTest, "")
}

func TestRefreshKeyspace(t *testing.T) {
	// Store the old flags and restore on test completion
	oldTs := ts
	defer func() {
		ts = oldTs
	}()

	// Open the vtorc
	// After the test completes delete everything from the vitess_keyspace table
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from vitess_keyspace")
		require.NoError(t, err)
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

			err := RefreshKeyspace(tt.keyspaceName)
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
		assert.Equal(t, keyspaceName, ksInfo.KeyspaceName())
		assert.True(t, topotools.KeyspaceEquality(keyspace, ksInfo.Keyspace))
	}
}
