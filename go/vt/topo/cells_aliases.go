/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"path"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file provides the utility methods to save / retrieve CellsAliases
// in the topology server.
//
// CellsAliases records are not meant to be changed while the system is
// running.  In a running system, a CellsAlias can be added, and
// topology server implementations should be able to read them to
// access the cells upon demand.

func pathForCellsAlias(alias string) string {
	return path.Join(CellsAliasesPath, alias, CellsAliasFile)
}

// GetCellsAliases returns the names of the existing cells. They are
// sorted by name.
func (ts *Server) GetCellsAliases(ctx context.Context, strongRead bool) (ret map[string]*topodatapb.CellsAlias, err error) {
	conn := ts.globalCell
	if !strongRead {
		conn = ts.globalReadOnlyCell
	}

	entries, err := ts.globalCell.ListDir(ctx, CellsAliasesPath, false /*full*/)
	switch {
	case IsErrType(err, NoNode):
		return nil, nil
	case err == nil:
		aliases := DirEntriesToStringArray(entries)
		ret = make(map[string]*topodatapb.CellsAlias, len(aliases))
		for _, alias := range aliases {
			aliasPath := pathForCellsAlias(alias)
			contents, _, err := conn.Get(ctx, aliasPath)
			if err != nil {
				return nil, err
			}

			// Unpack the contents.
			cellsAlias := &topodatapb.CellsAlias{}
			if err := proto.Unmarshal(contents, cellsAlias); err != nil {
				return nil, err
			}

			ret[alias] = cellsAlias
		}
		return ret, nil
	default:
		return nil, err
	}
}

// DeleteCellsAlias deletes the specified CellsAlias
func (ts *Server) DeleteCellsAlias(ctx context.Context, alias string) error {
	ts.clearCellAliasesCache()

	filePath := pathForCellsAlias(alias)
	return ts.globalCell.Delete(ctx, filePath, nil)
}

// CreateCellsAlias creates a new CellInfo with the provided content.
func (ts *Server) CreateCellsAlias(ctx context.Context, alias string, cellsAlias *topodatapb.CellsAlias) error {
	currentAliases, err := ts.GetCellsAliases(ctx, true)
	if err != nil {
		return err
	}

	if existingAlias := overlappingAlias(currentAliases, alias, cellsAlias); existingAlias != "" {
		return fmt.Errorf("cells alias %v overlaps with existing alias %v", alias, existingAlias)
	}

	ts.clearCellAliasesCache()

	// Pack the content.
	contents, err := proto.Marshal(cellsAlias)
	if err != nil {
		return err
	}

	// Save it.
	filePath := pathForCellsAlias(alias)
	_, err = ts.globalCell.Create(ctx, filePath, contents)
	return err
}

// UpdateCellsAlias updates cells for a given alias
func (ts *Server) UpdateCellsAlias(ctx context.Context, alias string, update func(*topodatapb.CellsAlias) error) error {
	ts.clearCellAliasesCache()

	filePath := pathForCellsAlias(alias)
	for {
		cellsAlias := &topodatapb.CellsAlias{}

		// Read the file, unpack the contents.
		contents, version, err := ts.globalCell.Get(ctx, filePath)
		switch {
		case err == nil:
			if err := proto.Unmarshal(contents, cellsAlias); err != nil {
				return err
			}
		case IsErrType(err, NoNode):
			// Nothing to do.
		default:
			return err
		}

		// Call update method.
		if err = update(cellsAlias); err != nil {
			if IsErrType(err, NoUpdateNeeded) {
				return nil
			}
			return err
		}

		currentAliases, err := ts.GetCellsAliases(ctx, true)
		if err != nil {
			return err
		}

		if existingAlias := overlappingAlias(currentAliases, alias, cellsAlias); existingAlias != "" {
			return fmt.Errorf("cells alias %v overlaps with existing alias %v", alias, existingAlias)
		}

		// Pack and save.
		contents, err = proto.Marshal(cellsAlias)
		if err != nil {
			return err
		}
		if _, err = ts.globalCell.Update(ctx, filePath, contents, version); !IsErrType(err, BadVersion) {
			// This includes the 'err=nil' case.
			return err
		}
	}
}

// overlappingAlias returns the first overlapping alias, if any.
// If no alias overlaps, it returns an empty string.
func overlappingAlias(currentAliases map[string]*topodatapb.CellsAlias, newAliasName string, newAlias *topodatapb.CellsAlias) string {
	for name, alias := range currentAliases {
		// Skip the alias we're checking against. It's allowed to overlap with itself.
		if name == newAliasName {
			continue
		}

		for _, cell := range alias.Cells {
			if InCellList(cell, newAlias.Cells) {
				return name
			}
		}
	}
	return ""
}
