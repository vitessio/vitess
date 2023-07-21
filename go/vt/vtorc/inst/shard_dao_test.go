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
	"vitess.io/vitess/go/vt/vtorc/db"
)

func TestSaveAndReadShard(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	tests := []struct {
		name               string
		keyspaceName       string
		shardName          string
		shard              *topodatapb.Shard
		primaryAliasWanted string
		err                string
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
			},
			primaryAliasWanted: "zone1-0000000301",
		}, {
			name:               "Success with empty primary alias",
			keyspaceName:       "ks1",
			shardName:          "-",
			shard:              &topodatapb.Shard{},
			primaryAliasWanted: "",
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

			shardPrimaryAlias, err := ReadShardPrimaryAlias(tt.keyspaceName, tt.shardName)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tt.primaryAliasWanted, shardPrimaryAlias)
		})
	}
}
