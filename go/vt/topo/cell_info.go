/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package topo

import (
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file provides the utility methods to save / retrieve CellInfo
// in the topology Backend.
//
// CellInfo records are not meant to be changed while the system is
// running.  In a running system, a CellInfo can be added, and
// topology server implementations should be able to read them to
// access the cells upon demand. Topology server implementations can
// also read the available CellInfo at startup to build a list of
// available cells, if necessary. A CellInfo can only be removed if no
// Shard record references the corresponding cell in its Cells list.

const (
	cellsPath = "cells"
)

func pathForCellInfo(cell string) string {
	return path.Join(cellsPath, cell, CellInfoFile)
}

// GetCellInfoNames returns the names of the existing cells. They are
// sorted by name.
func (ts Server) GetCellInfoNames(ctx context.Context) ([]string, error) {
	entries, err := ts.ListDir(ctx, GlobalCell, cellsPath)
	switch err {
	case ErrNoNode:
		return nil, nil
	case nil:
		return entries, nil
	default:
		return nil, err
	}
}

// GetCellInfo reads a CellInfo from the Backend.
func (ts Server) GetCellInfo(ctx context.Context, cell string) (*topodatapb.CellInfo, error) {
	// Read the file.
	filePath := pathForCellInfo(cell)
	contents, _, err := ts.Get(ctx, GlobalCell, filePath)
	if err != nil {
		return nil, err
	}

	// Unpack the contents.
	ci := &topodatapb.CellInfo{}
	if err := proto.Unmarshal(contents, ci); err != nil {
		return nil, err
	}
	return ci, nil
}

// CreateCellInfo creates a new CellInfo with the provided content.
func (ts Server) CreateCellInfo(ctx context.Context, cell string, ci *topodatapb.CellInfo) error {
	// Pack the content.
	contents, err := proto.Marshal(ci)
	if err != nil {
		return err
	}

	// Save it.
	filePath := pathForCellInfo(cell)
	_, err = ts.Create(ctx, GlobalCell, filePath, contents)
	return err
}

// UpdateCellInfoFields is a high level helper method to read a CellInfo
// object, update its fields, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil is returned.
func (ts Server) UpdateCellInfoFields(ctx context.Context, cell string, update func(*topodatapb.CellInfo) error) error {
	filePath := pathForCellInfo(cell)
	for {
		ci := &topodatapb.CellInfo{}

		// Read the file, unpack the contents.
		contents, version, err := ts.Get(ctx, GlobalCell, filePath)
		switch err {
		case nil:
			if err := proto.Unmarshal(contents, ci); err != nil {
				return err
			}
		case ErrNoNode:
			// Nothing to do.
		default:
			return err
		}

		// Call update method.
		if err = update(ci); err != nil {
			if err == ErrNoUpdateNeeded {
				return nil
			}
			return err
		}

		// Pack and save.
		contents, err = proto.Marshal(ci)
		if err != nil {
			return err
		}
		if _, err = ts.Update(ctx, GlobalCell, filePath, contents, version); err != ErrBadVersion {
			// This includes the 'err=nil' case.
			return err
		}
	}
}

// DeleteCellInfo deletes the specified CellInfo.
// We first make sure no Shard record points to the cell.
func (ts Server) DeleteCellInfo(ctx context.Context, cell string) error {
	// Get all keyspaces.
	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil {
		return fmt.Errorf("GetKeyspaces() failed: %v", err)
	}

	// For each keyspace, make sure no shard points at the cell.
	for _, keyspace := range keyspaces {
		shards, err := ts.FindAllShardsInKeyspace(ctx, keyspace)
		if err != nil {
			return fmt.Errorf("FindAllShardsInKeyspace(%v) failed: %v", keyspace, err)
		}

		for shard, si := range shards {
			if si.HasCell(cell) {
				return fmt.Errorf("cell %v is used by shard %v/%v, cannot remove it. Use 'vtctl RemoveShardCell' to remove unused cells in a Shard", cell, keyspace, shard)
			}
		}
	}

	filePath := pathForCellInfo(cell)
	return ts.Delete(ctx, GlobalCell, filePath, nil)
}
