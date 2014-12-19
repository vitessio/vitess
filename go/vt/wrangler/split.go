// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/topo"
)

// SetSourceShards is a utility function to override the SourceShards fields
// on a Shard.
func (wr *Wrangler) SetSourceShards(keyspace, shard string, sources []topo.TabletAlias, tables []string) error {
	// read the shard
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		return err
	}

	// If the shard already has sources, maybe it's already been restored,
	// so let's be safe and abort right here.
	if len(shardInfo.SourceShards) > 0 {
		return fmt.Errorf("Shard %v/%v already has SourceShards, not overwriting them", keyspace, shard)
	}

	// read the source tablets
	sourceTablets, err := topo.GetTabletMap(wr.ctx, wr.TopoServer(), sources)
	if err != nil {
		return err
	}

	// Insert their KeyRange in the SourceShards array.
	// We use a linear 0-based id, that matches what mysqlctld/split.go
	// inserts into _vt.blp_checkpoint.
	shardInfo.SourceShards = make([]topo.SourceShard, len(sourceTablets))
	i := 0
	for _, ti := range sourceTablets {
		shardInfo.SourceShards[i] = topo.SourceShard{
			Uid:      uint32(i),
			Keyspace: ti.Keyspace,
			Shard:    ti.Shard,
			KeyRange: ti.KeyRange,
			Tables:   tables,
		}
		i++
	}

	// and write the shard
	if err = topo.UpdateShard(wr.ctx, wr.ts, shardInfo); err != nil {
		return err
	}

	return nil
}
