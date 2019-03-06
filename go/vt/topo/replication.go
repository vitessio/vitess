/*
Copyright 2017 Google Inc.

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

package topo

import (
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// ShardReplicationInfo is the companion structure for ShardReplication.
type ShardReplicationInfo struct {
	*topodatapb.ShardReplication
	cell     string
	keyspace string
	shard    string
}

// NewShardReplicationInfo is for topo.Server implementations to
// create the structure
func NewShardReplicationInfo(sr *topodatapb.ShardReplication, cell, keyspace, shard string) *ShardReplicationInfo {
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
func (sri *ShardReplicationInfo) GetShardReplicationNode(tabletAlias *topodatapb.TabletAlias) (*topodatapb.ShardReplication_Node, error) {
	for _, rl := range sri.Nodes {
		if proto.Equal(rl.TabletAlias, tabletAlias) {
			return rl, nil
		}
	}
	return nil, NewError(NoNode, tabletAlias.String())
}

// UpdateShardReplicationRecord is a low level function to add / update an
// entry to the ShardReplication object.
func UpdateShardReplicationRecord(ctx context.Context, ts *Server, keyspace, shard string, tabletAlias *topodatapb.TabletAlias) error {
	span := trace.NewSpanFromContext(ctx)
	span.StartClient("TopoServer.UpdateShardReplicationFields")
	span.Annotate("keyspace", keyspace)
	span.Annotate("shard", shard)
	span.Annotate("tablet", topoproto.TabletAliasString(tabletAlias))
	defer span.Finish()

	return ts.UpdateShardReplicationFields(ctx, tabletAlias.Cell, keyspace, shard, func(sr *topodatapb.ShardReplication) error {
		// Not very efficient, but easy to read, and allows us
		// to remove duplicate entries if any.
		nodes := make([]*topodatapb.ShardReplication_Node, 0, len(sr.Nodes)+1)
		found := false
		modified := false
		for _, node := range sr.Nodes {
			if proto.Equal(node.TabletAlias, tabletAlias) {
				if found {
					log.Warningf("Found a second ShardReplication_Node for tablet %v, deleting it", tabletAlias)
					modified = true
					continue
				}
				found = true
			}
			nodes = append(nodes, node)
		}
		if !found {
			nodes = append(nodes, &topodatapb.ShardReplication_Node{TabletAlias: tabletAlias})
			modified = true
		}
		if !modified {
			return NewError(NoUpdateNeeded, tabletAlias.String())
		}
		sr.Nodes = nodes
		return nil
	})
}

// RemoveShardReplicationRecord is a low level function to remove an
// entry from the ShardReplication object.
func RemoveShardReplicationRecord(ctx context.Context, ts *Server, cell, keyspace, shard string, tabletAlias *topodatapb.TabletAlias) error {
	err := ts.UpdateShardReplicationFields(ctx, cell, keyspace, shard, func(sr *topodatapb.ShardReplication) error {
		nodes := make([]*topodatapb.ShardReplication_Node, 0, len(sr.Nodes))
		for _, node := range sr.Nodes {
			if !proto.Equal(node.TabletAlias, tabletAlias) {
				nodes = append(nodes, node)
			}
		}
		sr.Nodes = nodes
		return nil
	})
	return err
}

// FixShardReplication will fix the first problem it encounters within
// a ShardReplication object.
func FixShardReplication(ctx context.Context, ts *Server, logger logutil.Logger, cell, keyspace, shard string) error {
	sri, err := ts.GetShardReplication(ctx, cell, keyspace, shard)
	if err != nil {
		return err
	}

	for _, node := range sri.Nodes {
		ti, err := ts.GetTablet(ctx, node.TabletAlias)
		if IsErrType(err, NoNode) {
			logger.Warningf("Tablet %v is in the replication graph, but does not exist, removing it", node.TabletAlias)
			return RemoveShardReplicationRecord(ctx, ts, cell, keyspace, shard, node.TabletAlias)
		}
		if err != nil {
			// unknown error, we probably don't want to continue
			return err
		}

		if ti.Keyspace != keyspace || ti.Shard != shard || ti.Alias.Cell != cell {
			logger.Warningf("Tablet '%v' is in the replication graph, but has wrong keyspace/shard/cell, removing it", ti.Tablet)
			return RemoveShardReplicationRecord(ctx, ts, cell, keyspace, shard, node.TabletAlias)
		}

		logger.Infof("Keeping tablet %v in the replication graph", node.TabletAlias)
	}

	logger.Infof("All entries in replication graph are valid")
	return nil
}

// UpdateShardReplicationFields updates the fields inside a topo.ShardReplication object.
func (ts *Server) UpdateShardReplicationFields(ctx context.Context, cell, keyspace, shard string, update func(*topodatapb.ShardReplication) error) error {
	nodePath := path.Join(KeyspacesPath, keyspace, ShardsPath, shard, ShardReplicationFile)

	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	for {
		data, version, err := conn.Get(ctx, nodePath)
		sr := &topodatapb.ShardReplication{}
		switch {
		case IsErrType(err, NoNode):
			// Empty node, version is nil
		case err == nil:
			// Use any data we got.
			if err = proto.Unmarshal(data, sr); err != nil {
				return vterrors.Wrap(err, "bad ShardReplication data")
			}
		default:
			return err
		}

		err = update(sr)
		switch {
		case IsErrType(err, NoUpdateNeeded):
			return nil
		case err == nil:
			// keep going
		default:
			return err
		}

		// marshall and save
		data, err = proto.Marshal(sr)
		if err != nil {
			return err
		}
		if version == nil {
			// We have to create, and we catch NodeExists.
			_, err = conn.Create(ctx, nodePath, data)
			if IsErrType(err, NodeExists) {
				// Node was created by another process, try
				// again.
				continue
			}
			return err
		}

		// We have to update, and we catch ErrBadVersion.
		_, err = conn.Update(ctx, nodePath, data, version)
		if IsErrType(err, BadVersion) {
			// Node was updated by another process, try again.
			continue
		}
		return err
	}
}

// GetShardReplication returns the ShardReplicationInfo object.
func (ts *Server) GetShardReplication(ctx context.Context, cell, keyspace, shard string) (*ShardReplicationInfo, error) {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}

	nodePath := path.Join(KeyspacesPath, keyspace, ShardsPath, shard, ShardReplicationFile)
	data, _, err := conn.Get(ctx, nodePath)
	if err != nil {
		return nil, err
	}

	sr := &topodatapb.ShardReplication{}
	if err = proto.Unmarshal(data, sr); err != nil {
		return nil, vterrors.Wrap(err, "bad ShardReplication data")
	}

	return NewShardReplicationInfo(sr, cell, keyspace, shard), nil
}

// DeleteShardReplication deletes a ShardReplication object.
func (ts *Server) DeleteShardReplication(ctx context.Context, cell, keyspace, shard string) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := path.Join(KeyspacesPath, keyspace, ShardsPath, shard, ShardReplicationFile)
	return conn.Delete(ctx, nodePath, nil)
}

// DeleteKeyspaceReplication deletes all the ShardReplication objects for a cell/keyspace.
func (ts *Server) DeleteKeyspaceReplication(ctx context.Context, cell, keyspace string) error {
	conn, err := ts.ConnForCell(ctx, cell)
	if err != nil {
		return err
	}

	nodePath := path.Join(KeyspacesPath, keyspace)
	return conn.Delete(ctx, nodePath, nil)
}
