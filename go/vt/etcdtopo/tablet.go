// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"

	"github.com/coreos/go-etcd/etcd"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// CreateTablet implements topo.Server.
func (s *Server) CreateTablet(ctx context.Context, tablet *topodatapb.Tablet) error {
	cell, err := s.getCell(tablet.Alias.Cell)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(tablet, "", "  ")
	if err != nil {
		return err
	}
	_, err = cell.Create(tabletFilePath(tablet.Alias), string(data), 0 /* ttl */)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// UpdateTablet implements topo.Server.
func (s *Server) UpdateTablet(ctx context.Context, tablet *topodatapb.Tablet, existingVersion int64) (int64, error) {
	cell, err := s.getCell(tablet.Alias.Cell)
	if err != nil {
		return -1, err
	}

	data, err := json.MarshalIndent(tablet, "", "  ")
	if err != nil {
		return -1, err
	}
	var resp *etcd.Response
	if existingVersion == -1 {
		// Set unconditionally.
		resp, err = cell.Set(tabletFilePath(tablet.Alias), string(data), 0 /* ttl */)
	} else {
		resp, err = cell.CompareAndSwap(tabletFilePath(tablet.Alias),
			string(data), 0 /* ttl */, "" /* prevValue */, uint64(existingVersion))
	}
	if err != nil {
		return -1, convertError(err)
	}
	if resp.Node == nil {
		return -1, ErrBadResponse
	}
	return int64(resp.Node.ModifiedIndex), nil
}

// DeleteTablet implements topo.Server.
func (s *Server) DeleteTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	cell, err := s.getCell(tabletAlias.Cell)
	if err != nil {
		return err
	}

	if _, err = cell.Delete(tabletDirPath(tabletAlias), true /* recursive */); err != nil {
		return convertError(err)
	}
	return nil
}

// ValidateTablet implements topo.Server.
func (s *Server) ValidateTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error {
	_, _, err := s.GetTablet(ctx, tabletAlias)
	return err
}

// GetTablet implements topo.Server.
func (s *Server) GetTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) (*topodatapb.Tablet, int64, error) {
	cell, err := s.getCell(tabletAlias.Cell)
	if err != nil {
		return nil, 0, err
	}

	resp, err := cell.Get(tabletFilePath(tabletAlias), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, 0, convertError(err)
	}
	if resp.Node == nil {
		return nil, 0, ErrBadResponse
	}

	value := &topodatapb.Tablet{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, 0, fmt.Errorf("bad tablet data (%v): %q", err, resp.Node.Value)
	}

	return value, int64(resp.Node.ModifiedIndex), nil
}

// GetTabletsByCell implements topo.Server.
func (s *Server) GetTabletsByCell(ctx context.Context, cellName string) ([]*topodatapb.TabletAlias, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(tabletsDirPath, false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}

	nodes, err := getNodeNames(resp)
	if err != nil {
		return nil, err
	}

	tablets := make([]*topodatapb.TabletAlias, 0, len(nodes))
	for _, node := range nodes {
		tabletAlias, err := topoproto.ParseTabletAlias(node)
		if err != nil {
			return nil, err
		}
		tablets = append(tablets, tabletAlias)
	}
	return tablets, nil
}
