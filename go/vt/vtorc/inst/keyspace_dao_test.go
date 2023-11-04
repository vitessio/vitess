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
	_ "modernc.org/sqlite"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtorc/db"
)

func TestSaveAndReadKeyspace(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	tests := []struct {
		name                  string
		keyspaceName          string
		keyspace              *topodatapb.Keyspace
		keyspaceWanted        *topodatapb.Keyspace
		err                   string
		errInDurabilityPolicy string
		semiSyncAckersWanted  int
	}{
		{
			name:         "Success with keyspaceType and durability",
			keyspaceName: "ks1",
			keyspace: &topodatapb.Keyspace{
				KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: "semi_sync",
			},
			keyspaceWanted:       nil,
			semiSyncAckersWanted: 1,
		}, {
			name:         "Success with keyspaceType and no durability",
			keyspaceName: "ks2",
			keyspace: &topodatapb.Keyspace{
				KeyspaceType: topodatapb.KeyspaceType_NORMAL,
			},
			keyspaceWanted:        nil,
			errInDurabilityPolicy: "durability policy  not found",
		}, {
			name:         "Success with snapshot keyspaceType",
			keyspaceName: "ks3",
			keyspace: &topodatapb.Keyspace{
				KeyspaceType: topodatapb.KeyspaceType_SNAPSHOT,
			},
			keyspaceWanted: nil,
		}, {
			name:         "Success with fields that are not stored",
			keyspaceName: "ks4",
			keyspace: &topodatapb.Keyspace{
				KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: "none",
				BaseKeyspace:     "baseKeyspace",
			},
			keyspaceWanted: &topodatapb.Keyspace{
				KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
				DurabilityPolicy: "none",
			},
			semiSyncAckersWanted: 0,
		}, {
			name:           "No keyspace found",
			keyspaceName:   "ks5",
			keyspace:       nil,
			keyspaceWanted: nil,
			err:            ErrKeyspaceNotFound.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.keyspaceWanted == nil {
				tt.keyspaceWanted = tt.keyspace
			}

			if tt.keyspace != nil {
				keyspaceInfo := &topo.KeyspaceInfo{
					Keyspace: tt.keyspace,
				}
				keyspaceInfo.SetKeyspaceName(tt.keyspaceName)
				err := SaveKeyspace(keyspaceInfo)
				require.NoError(t, err)
			}

			readKeyspaceInfo, err := ReadKeyspace(tt.keyspaceName)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.True(t, topotools.KeyspaceEquality(tt.keyspaceWanted, readKeyspaceInfo.Keyspace))
			require.Equal(t, tt.keyspaceName, readKeyspaceInfo.KeyspaceName())
			if tt.keyspace.KeyspaceType == topodatapb.KeyspaceType_SNAPSHOT {
				return
			}
			durabilityPolicy, err := GetDurabilityPolicy(tt.keyspaceName)
			if tt.errInDurabilityPolicy != "" {
				require.EqualError(t, err, tt.errInDurabilityPolicy)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tt.semiSyncAckersWanted, reparentutil.SemiSyncAckers(durabilityPolicy, nil))
		})
	}
}
