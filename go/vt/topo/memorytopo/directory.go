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

package memorytopo

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// ListDir is part of the topo.Conn interface.
func (c *Conn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	isRoot := false
	if dirPath == "" || dirPath == "/" {
		isRoot = true
	}

	// Get the node to list.
	n := c.factory.nodeByPath(c.cell, dirPath)
	if n == nil {
		return nil, topo.ErrNoNode
	}

	// Check it's a directory.
	if !n.isDirectory() {
		return nil, fmt.Errorf("node %v in cell %v is not a directory", dirPath, c.cell)
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
