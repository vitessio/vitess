// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/trace"
	"github.com/youtube/vitess/go/vt/logutil"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// ShardReplicationInfo is the companion structure for ShardReplication.
type ShardReplicationInfo struct {
	*pb.ShardReplication
	cell     string
	keyspace string
	shard    string
}

// NewShardReplicationInfo is for topo.Server implementations to
// create the structure
func NewShardReplicationInfo(sr *pb.ShardReplication, cell, keyspace, shard string) *ShardReplicationInfo {
	return &ShardReplicationInfo{
		ShardReplication: sr,
		cell:             cell,
		keyspace:         keyspace,
		shard:            shard,
	}
}

// Cell returns the cell for a ShardReplicationInfo
func (sri *ShardReplicationInfo) Cell() string {
	return sri.cell
}

// Keyspace returns the keyspace for a ShardReplicationInfo
func (sri *ShardReplicationInfo) Keyspace() string {
	return sri.keyspace
}

// Shard returns the shard for a ShardReplicationInfo
func (sri *ShardReplicationInfo) Shard() string {
	return sri.shard
}

// GetReplicationLink find a link for a given tablet.
func (sri *ShardReplicationInfo) GetReplicationLink(tabletAlias *pb.TabletAlias) (*pb.ShardReplication_ReplicationLink, error) {
	for _, rl := range sri.ReplicationLinks {
		if *rl.TabletAlias == *tabletAlias {
			return rl, nil
		}
	}
	return nil, ErrNoNode
}

// UpdateShardReplicationRecord is a low level function to add / update an
// entry to the ShardReplication object.
func UpdateShardReplicationRecord(ctx context.Context, ts Server, keyspace, shard string, tabletAlias TabletAlias) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateShardReplicationFields")
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("tablet", tabletAlias.String())
	defer span.Finish()

	return ts.UpdateShardReplicationFields(ctx, tabletAlias.Cell, keyspace, shard, func(sr *pb.ShardReplication) error {
		// not very efficient, but easy to read
		links := make([]*pb.ShardReplication_ReplicationLink, 0, len(sr.ReplicationLinks)+1)
		found := false
		for _, link := range sr.ReplicationLinks {
			if ProtoToTabletAlias(link.TabletAlias) == tabletAlias {
				if found {
					log.Warningf("Found a second ReplicationLink for tablet %v, deleting it", tabletAlias)
					continue
				}
				found = true
			}
			links = append(links, link)
		}
		if !found {
			links = append(links, &pb.ShardReplication_ReplicationLink{TabletAlias: TabletAliasToProto(tabletAlias)})
		}
		sr.ReplicationLinks = links
		return nil
	})
}

// RemoveShardReplicationRecord is a low level function to remove an
// entry from the ShardReplication object.
func RemoveShardReplicationRecord(ctx context.Context, ts Server, cell, keyspace, shard string, tabletAlias TabletAlias) error {
	err := ts.UpdateShardReplicationFields(ctx, cell, keyspace, shard, func(sr *pb.ShardReplication) error {
		links := make([]*pb.ShardReplication_ReplicationLink, 0, len(sr.ReplicationLinks))
		for _, link := range sr.ReplicationLinks {
			if ProtoToTabletAlias(link.TabletAlias) != tabletAlias {
				links = append(links, link)
			}
		}
		sr.ReplicationLinks = links
		return nil
	})
	return err
}

// FixShardReplication will fix the first problem it encounters within
// a ShardReplication object
func FixShardReplication(ctx context.Context, ts Server, logger logutil.Logger, cell, keyspace, shard string) error {
	sri, err := ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil {
		return err
	}

	for _, rl := range sri.ReplicationLinks {
		ti, err := ts.GetTablet(ctx, ProtoToTabletAlias(rl.TabletAlias))
		if err == ErrNoNode {
			logger.Warningf("Tablet %v is in the replication graph, but does not exist, removing it", rl.TabletAlias)
			return RemoveShardReplicationRecord(ctx, ts, cell, keyspace, shard, ProtoToTabletAlias(rl.TabletAlias))
		}
		if err != nil {
			// unknown error, we probably don't want to continue
			return err
		}

		if ti.Type == TYPE_SCRAP {
			logger.Warningf("Tablet %v is in the replication graph, but is scrapped, removing it", rl.TabletAlias)
			return RemoveShardReplicationRecord(ctx, ts, cell, keyspace, shard, ProtoToTabletAlias(rl.TabletAlias))
		}

		logger.Infof("Keeping tablet %v in the replication graph", rl.TabletAlias)
	}

	logger.Infof("All entries in replication graph are valid")
	return nil
}
