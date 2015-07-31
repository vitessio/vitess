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

// GetShardReplicationNode finds a node for a given tablet.
func (sri *ShardReplicationInfo) GetShardReplicationNode(tabletAlias *pb.TabletAlias) (*pb.ShardReplication_Node, error) {
	for _, rl := range sri.Nodes {
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
		nodes := make([]*pb.ShardReplication_Node, 0, len(sr.Nodes)+1)
		found := false
		for _, node := range sr.Nodes {
			if ProtoToTabletAlias(node.TabletAlias) == tabletAlias {
				if found {
					log.Warningf("Found a second ShardReplication_Node for tablet %v, deleting it", tabletAlias)
					continue
				}
				found = true
			}
			nodes = append(nodes, node)
		}
		if !found {
			nodes = append(nodes, &pb.ShardReplication_Node{TabletAlias: TabletAliasToProto(tabletAlias)})
		}
		sr.Nodes = nodes
		return nil
	})
}

// RemoveShardReplicationRecord is a low level function to remove an
// entry from the ShardReplication object.
func RemoveShardReplicationRecord(ctx context.Context, ts Server, cell, keyspace, shard string, tabletAlias TabletAlias) error {
	err := ts.UpdateShardReplicationFields(ctx, cell, keyspace, shard, func(sr *pb.ShardReplication) error {
		nodes := make([]*pb.ShardReplication_Node, 0, len(sr.Nodes))
		for _, node := range sr.Nodes {
			if ProtoToTabletAlias(node.TabletAlias) != tabletAlias {
				nodes = append(nodes, node)
			}
		}
		sr.Nodes = nodes
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

	for _, node := range sri.Nodes {
		ti, err := ts.GetTablet(ctx, ProtoToTabletAlias(node.TabletAlias))
		if err == ErrNoNode {
			logger.Warningf("Tablet %v is in the replication graph, but does not exist, removing it", node.TabletAlias)
			return RemoveShardReplicationRecord(ctx, ts, cell, keyspace, shard, ProtoToTabletAlias(node.TabletAlias))
		}
		if err != nil {
			// unknown error, we probably don't want to continue
			return err
		}

		if ti.Type == TYPE_SCRAP {
			logger.Warningf("Tablet %v is in the replication graph, but is scrapped, removing it", node.TabletAlias)
			return RemoveShardReplicationRecord(ctx, ts, cell, keyspace, shard, ProtoToTabletAlias(node.TabletAlias))
		}

		logger.Infof("Keeping tablet %v in the replication graph", node.TabletAlias)
	}

	logger.Infof("All entries in replication graph are valid")
	return nil
}
