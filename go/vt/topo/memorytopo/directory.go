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

package memorytopo

import (
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/topo"
)

// ListDir is part of the topo.Conn interface.
func (c *Conn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return nil, c.factory.err
	}

	isRoot := false
	if dirPath == "" || dirPath == "/" {
		isRoot = true
	}

	// Get the node to list.
	n := c.factory.nodeByPath(c.cell, dirPath)
	if n == nil {
		return nil, topo.NewError(topo.NoNode, dirPath)
	}

	// Check it's a directory.
	if !n.isDirectory() {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "node %v in cell %v is not a directory", dirPath, c.cell)
	}

	result := make([]topo.DirEntry, 0, len(n.children))
	for name, child := range n.children {
		e := topo.DirEntry{
			Name: name,
		}
		if full {
			e.Type = topo.TypeFile
			if child.isDirectory() {
				e.Type = topo.TypeDirectory
			}
			if isRoot && name == electionsPath {
				e.Ephemeral = true
			}
		}
		result = append(result, e)
	}
	topo.DirEntriesSortByName(result)
	return result, nil
}
