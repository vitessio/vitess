// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
)

// SetSourceShards is a utility function to override the SourceShards fields
// on a Shard.
func (wr *Wrangler) SetSourceShards(ctx context.Context, keyspace, shard string, sources []*topodatapb.TabletAlias, tables []string) error {
	// Read the source tablets.
	sourceTablets, err := wr.ts.GetTabletMap(ctx, sources)
	if err != nil {
		return err
	}

	// Insert their KeyRange in the SourceShards array.
	// We use a linear 0-based id, that matches what worker/split_clone.go
	// inserts into _vt.blp_checkpoint.
	// We want to guarantee sourceShards[i] is using sources[i],
	// So iterating over the sourceTablets map would be a bad idea.
	sourceShards := make([]*topodatapb.Shard_SourceShard, len(sourceTablets))
	for i, alias := range sources {
		ti := sourceTablets[*alias]
		sourceShards[i] = &topodatapb.Shard_SourceShard{
			Uid:      uint32(i),
			Keyspace: ti.Keyspace,
			Shard:    ti.Shard,
			KeyRange: ti.KeyRange,
			Tables:   tables,
		}
	}

	// Update the shard with the new source shards.
	_, err = wr.ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		// If the shard already has sources, maybe it's already been restored,
		// so let's be safe and abort right here.
		if len(si.SourceShards) > 0 {
			return fmt.Errorf("Shard %v/%v already has SourceShards, not overwriting them (full record: %v)", keyspace, shard, *si.Shard)
		}

		si.SourceShards = sourceShards
		return nil
	})
	return err
}
