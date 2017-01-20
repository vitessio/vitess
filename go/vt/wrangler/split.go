// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
)

const (
	// DefaultWaitForFilteredReplicationMaxDelay is the default maximum delay value used in WaitForFilteredReplication.
	DefaultWaitForFilteredReplicationMaxDelay = 30 * time.Second
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

// WaitForFilteredReplication will wait until the Filtered Replication process has finished.
func (wr *Wrangler) WaitForFilteredReplication(ctx context.Context, keyspace, shard string, maxDelay time.Duration) error {
	shardInfo, err := wr.TopoServer().GetShard(ctx, keyspace, shard)
	if err != nil {
		return err
	}
	if len(shardInfo.SourceShards) == 0 {
		return fmt.Errorf("shard %v/%v has no source shard", keyspace, shard)
	}
	if !shardInfo.HasMaster() {
		return fmt.Errorf("shard %v/%v has no master", keyspace, shard)
	}
	alias := shardInfo.MasterAlias
	tabletInfo, err := wr.TopoServer().GetTablet(ctx, alias)
	if err != nil {
		return err
	}

	// Always run an explicit healthcheck first to make sure we don't see any outdated values.
	// This is especially true for tests and automation where there is no pause of multiple seconds
	// between commands and the periodic healthcheck did not run again yet.
	if err := wr.TabletManagerClient().RunHealthCheck(ctx, tabletInfo.Tablet); err != nil {
		return fmt.Errorf("failed to run explicit healthcheck on tablet: %v err: %v", tabletInfo, err)
	}

	conn, err := tabletconn.GetDialer()(tabletInfo.Tablet, 30*time.Second)
	if err != nil {
		return fmt.Errorf("cannot connect to tablet %v: %v", alias, err)
	}

	stream, err := conn.StreamHealth(ctx)
	if err != nil {
		return fmt.Errorf("could not stream health records from tablet: %v err: %v", alias, err)
	}
	var lastSeenDelay int
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context was done before filtered replication did catch up. Last seen delay: %v context Error: %v", lastSeenDelay, ctx.Err())
		default:
		}

		shr, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream ended early: %v", err)
		}
		stats := shr.RealtimeStats
		if stats == nil {
			return fmt.Errorf("health record does not include RealtimeStats message. tablet: %v health record: %v", alias, shr)
		}
		if stats.HealthError != "" {
			return fmt.Errorf("tablet is not healthy. tablet: %v health record: %v", alias, shr)
		}
		if stats.BinlogPlayersCount == 0 {
			return fmt.Errorf("no filtered replication running on tablet: %v health record: %v", alias, shr)
		}

		delaySecs := stats.SecondsBehindMasterFilteredReplication
		lastSeenDelay := time.Duration(delaySecs) * time.Second
		if lastSeenDelay < 0 {
			return fmt.Errorf("last seen delay should never be negative. tablet: %v delay: %v", alias, lastSeenDelay)
		}
		if lastSeenDelay <= maxDelay {
			wr.Logger().Printf("Filtered replication on tablet: %v has caught up. Last seen delay: %.1f seconds\n", alias, lastSeenDelay.Seconds())
			return nil
		}
		wr.Logger().Printf("Waiting for filtered replication to catch up on tablet: %v Last seen delay: %.1f seconds\n", alias, lastSeenDelay.Seconds())
	}
}
