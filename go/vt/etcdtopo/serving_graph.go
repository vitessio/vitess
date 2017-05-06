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

package etcdtopo

import (
	"encoding/json"
	"fmt"

	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	"github.com/youtube/vitess/go/vt/topo"
)

// GetSrvKeyspaceNames implements topo.Server.
func (s *Server) GetSrvKeyspaceNames(ctx context.Context, cellName string) ([]string, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(servingDirPath, true /* sort */, false /* recursive */)
	if err != nil {
		err = convertError(err)
		if err == topo.ErrNoNode {
			return nil, nil
		}
		return nil, err
	}
	return getNodeNames(resp)
}

// UpdateSrvKeyspace implements topo.Server.
func (s *Server) UpdateSrvKeyspace(ctx context.Context, cellName, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(srvKeyspace, "", "  ")
	if err != nil {
		return err
	}

	_, err = cell.Set(srvKeyspaceFilePath(keyspace), string(data), 0 /* ttl */)
	return convertError(err)
}

// DeleteSrvKeyspace implements topo.Server.
func (s *Server) DeleteSrvKeyspace(ctx context.Context, cellName, keyspace string) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(srvKeyspaceDirPath(keyspace), true /* recursive */)
	return convertError(err)
}

// GetSrvKeyspace implements topo.Server.
func (s *Server) GetSrvKeyspace(ctx context.Context, cellName, keyspace string) (*topodatapb.SrvKeyspace, error) {
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

	value := &topodatapb.SrvKeyspace{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad serving keyspace data (%v): %q", err, resp.Node.Value)
	}
	return value, nil
}

// UpdateSrvVSchema implements topo.Server.
func (s *Server) UpdateSrvVSchema(ctx context.Context, cellName string, srvVSchema *vschemapb.SrvVSchema) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(srvVSchema, "", "  ")
	if err != nil {
		return err
	}

	_, err = cell.Set(srvVSchemaFilePath(), string(data), 0 /* ttl */)
	return convertError(err)
}

// GetSrvVSchema implements topo.Server.
func (s *Server) GetSrvVSchema(ctx context.Context, cellName string) (*vschemapb.SrvVSchema, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(srvVSchemaFilePath(), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	value := &vschemapb.SrvVSchema{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad serving vschema data (%v): %q", err, resp.Node.Value)
	}
	return value, nil
}
