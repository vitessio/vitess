// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
)

// GetSrvTabletTypesPerShard implements topo.Server.
func (s *Server) GetSrvTabletTypesPerShard(cellName, keyspace, shard string) ([]topo.TabletType, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(srvShardDirPath(keyspace, shard), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	tabletTypes := make([]topo.TabletType, 0, len(resp.Node.Nodes))
	for _, n := range resp.Node.Nodes {
		tabletTypes = append(tabletTypes, topo.TabletType(path.Base(n.Key)))
	}
	return tabletTypes, nil
}

// UpdateEndPoints implements topo.Server.
func (s *Server) UpdateEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data := jscfg.ToJson(addrs)

	_, err = cell.Set(endPointsFilePath(keyspace, shard, string(tabletType)), data, 0 /* ttl */)
	return convertError(err)
}

// updateEndPoints updates the EndPoints file only if the version matches.
func (s *Server) updateEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints, version int64) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data := jscfg.ToJson(addrs)

	_, err = cell.CompareAndSwap(endPointsFilePath(keyspace, shard, string(tabletType)), data, 0, /* ttl */
		"" /* prevValue */, uint64(version))
	return convertError(err)
}

// GetEndPoints implements topo.Server.
func (s *Server) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	value, _, err := s.getEndPoints(cell, keyspace, shard, tabletType)
	return value, err
}

func (s *Server) getEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, int64, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, -1, err
	}

	resp, err := cell.Get(endPointsFilePath(keyspace, shard, string(tabletType)), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, -1, convertError(err)
	}
	if resp.Node == nil {
		return nil, -1, ErrBadResponse
	}

	value := &topo.EndPoints{}
	if resp.Node.Value != "" {
		if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
			return nil, -1, fmt.Errorf("bad end points data (%v): %q", err, resp.Node.Value)
		}
	}
	return value, int64(resp.Node.ModifiedIndex), nil
}

// DeleteEndPoints implements topo.Server.
func (s *Server) DeleteEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(endPointsDirPath(keyspace, shard, string(tabletType)), true /* recursive */)
	return convertError(err)
}

// UpdateSrvShard implements topo.Server.
func (s *Server) UpdateSrvShard(cellName, keyspace, shard string, srvShard *topo.SrvShard) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data := jscfg.ToJson(srvShard)

	_, err = cell.Set(srvShardFilePath(keyspace, shard), data, 0 /* ttl */)
	return convertError(err)
}

// GetSrvShard implements topo.Server.
func (s *Server) GetSrvShard(cellName, keyspace, shard string) (*topo.SrvShard, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(srvShardFilePath(keyspace, shard), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	value := topo.NewSrvShard(int64(resp.Node.ModifiedIndex))
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad serving shard data (%v): %q", err, resp.Node.Value)
	}
	return value, nil
}

// DeleteSrvShard implements topo.Server.
func (s *Server) DeleteSrvShard(cellName, keyspace, shard string) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(srvShardDirPath(keyspace, shard), true /* recursive */)
	return convertError(err)
}

// UpdateSrvKeyspace implements topo.Server.
func (s *Server) UpdateSrvKeyspace(cellName, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data := jscfg.ToJson(srvKeyspace)

	_, err = cell.Set(srvKeyspaceFilePath(keyspace), data, 0 /* ttl */)
	return convertError(err)
}

// GetSrvKeyspace implements topo.Server.
func (s *Server) GetSrvKeyspace(cellName, keyspace string) (*topo.SrvKeyspace, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(srvKeyspaceFilePath(keyspace), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	value := topo.NewSrvKeyspace(int64(resp.Node.ModifiedIndex))
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad serving keyspace data (%v): %q", err, resp.Node.Value)
	}
	return value, nil
}

// GetSrvKeyspaceNames implements topo.Server.
func (s *Server) GetSrvKeyspaceNames(cellName string) ([]string, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(servingDirPath, true /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	return getNodeNames(resp)
}

// UpdateTabletEndpoint implements topo.Server.
func (s *Server) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.EndPoint) error {
	for {
		addrs, version, err := s.getEndPoints(cell, keyspace, shard, tabletType)
		if err == topo.ErrNoNode {
			// It's ok if the EndPoints file doesn't exist yet. See topo.Server.
			return nil
		}
		if err != nil {
			return err
		}

		// Update or add the record for the specified tablet.
		found := false
		for i, ep := range addrs.Entries {
			if ep.Uid == addr.Uid {
				found = true
				addrs.Entries[i] = *addr
				break
			}
		}
		if !found {
			addrs.Entries = append(addrs.Entries, *addr)
		}

		// Update the record
		err = s.updateEndPoints(cell, keyspace, shard, tabletType, addrs, version)
		if err != topo.ErrBadVersion {
			return err
		}
	}
}

// WatchEndPoints is part of the topo.Server interface
func (s *Server) WatchEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (<-chan *topo.EndPoints, chan<- struct{}, error) {
	return nil, nil, fmt.Errorf("NYI")
}
