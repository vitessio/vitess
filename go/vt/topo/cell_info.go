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
	"path"
	"strings"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file provides the utility methods to save / retrieve CellInfo
// in the topology server.
//
// CellInfo records are not meant to be changed while the system is
// running.  In a running system, a CellInfo can be added, and
// topology server implementations should be able to read them to
// access the cells upon demand. Topology server implementations can
// also read the available CellInfo at startup to build a list of
// available cells, if necessary. A CellInfo can only be removed if no
// Shard record references the corresponding cell in its Cells list.

func pathForCellInfo(cell string) string {
	return path.Join(CellsPath, cell, CellInfoFile)
}

// GetCellInfoNames returns the names of the existing cells. They are
// sorted by name.
func (ts *Server) GetCellInfoNames(ctx context.Context) ([]string, error) {
	entries, err := ts.globalCell.ListDir(ctx, CellsPath, false /*full*/)
	switch {
	case IsErrType(err, NoNode):
		return nil, nil
	case err == nil:
		return DirEntriesToStringArray(entries), nil
	default:
		return nil, err
	}
}

// GetCellInfo reads a CellInfo from the global Conn.
func (ts *Server) GetCellInfo(ctx context.Context, cell string, strongRead bool) (*topodatapb.CellInfo, error) {
	conn := ts.globalCell
	if !strongRead {
		conn = ts.globalReadOnlyCell
	}
	// Read the file.
	filePath := pathForCellInfo(cell)
	contents, _, err := conn.Get(ctx, filePath)
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
func (ts *Server) CreateCellInfo(ctx context.Context, cell string, ci *topodatapb.CellInfo) error {
	// Pack the content.
	contents, err := proto.Marshal(ci)
	if err != nil {
		return err
	}

	// Save it.
	filePath := pathForCellInfo(cell)
	_, err = ts.globalCell.Create(ctx, filePath, contents)
	return err
}

// UpdateCellInfoFields is a high level helper method to read a CellInfo
// object, update its fields, and then write it back. If the write fails due to
// a version mismatch, it will re-read the record and retry the update.
// If the update method returns ErrNoUpdateNeeded, nothing is written,
// and nil is returned.
func (ts *Server) UpdateCellInfoFields(ctx context.Context, cell string, update func(*topodatapb.CellInfo) error) error {
	filePath := pathForCellInfo(cell)
	for {
		ci := &topodatapb.CellInfo{}

		// Read the file, unpack the contents.
		contents, version, err := ts.globalCell.Get(ctx, filePath)
		switch {
		case err == nil:
			if err := proto.Unmarshal(contents, ci); err != nil {
				return err
			}
		case IsErrType(err, NoNode):
			// Nothing to do.
		default:
			return err
		}

		// Call update method.
		if err = update(ci); err != nil {
			if IsErrType(err, NoUpdateNeeded) {
				return nil
			}
			return err
		}

		// Pack and save.
		contents, err = proto.Marshal(ci)
		if err != nil {
			return err
		}
		if _, err = ts.globalCell.Update(ctx, filePath, contents, version); !IsErrType(err, BadVersion) {
			// This includes the 'err=nil' case.
			return err
		}
	}
}

// DeleteCellInfo deletes the specified CellInfo.
// We first try to make sure no Shard record points to the cell,
// but we'll continue regardless if 'force' is true.
func (ts *Server) DeleteCellInfo(ctx context.Context, cell string, force bool) error {
	srvKeyspaces, err := ts.GetSrvKeyspaceNames(ctx, cell)
	switch {
	case err == nil:
		if len(srvKeyspaces) != 0 && !force {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cell %v has serving keyspaces. Before deleting, delete keyspace with DeleteKeyspace, or use -force to continue anyway.", cell)
		}
	case IsErrType(err, NoNode):
		// Nothing to do.
	default:
		if !force {
			return vterrors.Wrap(err, "can't list SrvKeyspace entries in the cell; use -force flag to continue anyway (e.g. if cell-local topo was already permanently shut down)")
		}
	}

	filePath := pathForCellInfo(cell)
	return ts.globalCell.Delete(ctx, filePath, nil)
}

// GetKnownCells returns the list of known cells.
// For now, it just lists the 'cells' directory in the global topology server.
// TODO(alainjobart) once the cell map is migrated to this generic
// package, we can do better than this.
func (ts *Server) GetKnownCells(ctx context.Context) ([]string, error) {
	// Note we use the global read-only cell here, as the result
	// is not time sensitive.
	entries, err := ts.globalReadOnlyCell.ListDir(ctx, CellsPath, false /*full*/)
	if err != nil {
		return nil, err
	}
	return DirEntriesToStringArray(entries), nil
}

// ExpandCells takes a comma-separated list of cells and returns an array of cell names
// Aliases are expanded and an empty string returns all cells
func (ts *Server) ExpandCells(ctx context.Context, cells string) ([]string, error) {
	var err error
	var outputCells []string
	inputCells := strings.Split(cells, ",")
	if cells == "" {
		inputCells, err = ts.GetCellInfoNames(ctx)
		if err != nil {
			return nil, err
		}
	}

	for _, cell := range inputCells {
		cell2 := strings.TrimSpace(cell)
		shortCtx, cancel := context.WithTimeout(ctx, *RemoteOperationTimeout)
		defer cancel()
		_, err := ts.GetCellInfo(shortCtx, cell2, false)
		if err != nil {
			// not a valid cell, check whether it is a cell alias
			shortCtx, cancel := context.WithTimeout(ctx, *RemoteOperationTimeout)
			defer cancel()
			alias, err2 := ts.GetCellsAlias(shortCtx, cell2, false)
			// if we get an error, either cellAlias doesn't exist or it isn't a cell alias at all. Ignore and continue
			if err2 == nil {
				outputCells = append(outputCells, alias.Cells...)
			}
			if err != nil {
				return nil, err
			}
		} else {
			// valid cell, add it to our list
			outputCells = append(outputCells, cell2)
		}
	}
	return outputCells, nil
}
