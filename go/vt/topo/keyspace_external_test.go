/*
Copyright 2023 The Vitess Authors.

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

package topo_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

func TestServerFindAllShardsInKeyspace(t *testing.T) {
	tests := []struct {
		name   string
		shards int
		cfg    *topo.FindAllShardsInKeyspaceConfig
	}{
		{
			name:   "unsharded",
			shards: 1,
			// Make sure the defaults apply as expected.
			cfg: nil,
		},
		{
			name:   "sharded",
			shards: 32,
			cfg:    &topo.FindAllShardsInKeyspaceConfig{Concurrency: 8},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ts := memorytopo.NewServer(ctx)
			defer ts.Close()

			// Create an ephemeral keyspace and generate shard records within
			// the keyspace to fetch later.
			const keyspace = "keyspace"
			require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))

			shards, err := key.GenerateShardRanges(tt.shards)
			require.NoError(t, err)

			for _, s := range shards {
				require.NoError(t, ts.CreateShard(ctx, keyspace, s))
			}

			// Verify that we return a complete list of shards and that each
			// key range is present in the output.
			out, err := ts.FindAllShardsInKeyspace(ctx, keyspace, tt.cfg)
			require.NoError(t, err)
			require.Len(t, out, tt.shards)

			for _, s := range shards {
				if _, ok := out[s]; !ok {
					t.Errorf("shard %q was not found", s)
				}
			}
		})
	}
}
