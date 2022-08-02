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
	"context"
	"fmt"
	"path"
	"strings"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/topo"
)

// Create is part of topo.Conn interface.
func (c *Conn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	if err := c.dial(ctx); err != nil {
		return nil, err
	}

	if contents == nil {
		contents = []byte{}
	}

	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return nil, c.factory.err
	}

	// Get the parent dir.
	dir, file := path.Split(filePath)
	p := c.factory.getOrCreatePath(c.cell, dir)
	if p == nil {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "trying to create file %v in cell %v in a path that contains files", filePath, c.cell)
	}

	// Check the file doesn't already exist.
	if _, ok := p.children[file]; ok {
		return nil, topo.NewError(topo.NodeExists, file)
	}

	// Create the file.
	n := c.factory.newFile(file, contents, p)
	p.children[file] = n

	n.propagateRecursiveWatch(&topo.WatchDataRecursive{
		Path: filePath,
		WatchData: topo.WatchData{
			Contents: n.contents,
			Version:  NodeVersion(n.version),
		},
	})

	return NodeVersion(n.version), nil
}

// Update is part of topo.Conn interface.
func (c *Conn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if err := c.dial(ctx); err != nil {
		return nil, err
	}

	if contents == nil {
		contents = []byte{}
	}

	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return nil, c.factory.err
	}

	// Get the parent dir, we'll need it in case of creation.
	dir, file := path.Split(filePath)
	p := c.factory.nodeByPath(c.cell, dir)
	if p == nil {
		// Parent doesn't exist, let's create it if we need to.
		if version != nil {
			return nil, topo.NewError(topo.NoNode, filePath)
		}
		p = c.factory.getOrCreatePath(c.cell, dir)
		if p == nil {
			return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "trying to create file %v in cell %v in a path that contains files", filePath, c.cell)
		}
	}

	// Get the existing file.
	n, ok := p.children[file]
	if !ok {
		// File doesn't exist, see if we need to create it.
		if version != nil {
			return nil, topo.NewError(topo.NoNode, filePath)
		}
		n = c.factory.newFile(file, contents, p)
		p.children[file] = n
		return NodeVersion(n.version), nil
	}

	// Check if it's a directory.
	if n.isDirectory() {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Update(%v, %v) failed: it's a directory", c.cell, filePath)
	}

	// Check the version.
	if version != nil && n.version != uint64(version.(NodeVersion)) {
		return nil, topo.NewError(topo.BadVersion, filePath)
	}

	// Now we can update.
	n.version = c.factory.getNextVersion()
	n.contents = contents

	// Call the watches
	for _, w := range n.watches {
		if w.contents != nil {
			w.contents <- &topo.WatchData{
				Contents: n.contents,
				Version:  NodeVersion(n.version),
			}
		}
	}

	n.propagateRecursiveWatch(&topo.WatchDataRecursive{
		Path: filePath,
		WatchData: topo.WatchData{
			Contents: n.contents,
			Version:  NodeVersion(n.version),
		},
	})

	return NodeVersion(n.version), nil
}

// Get is part of topo.Conn interface.
func (c *Conn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	if err := c.dial(ctx); err != nil {
		return nil, nil, err
	}

	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return nil, nil, c.factory.err
	}

	// Get the node.
	n := c.factory.nodeByPath(c.cell, filePath)
	if n == nil {
		return nil, nil, topo.NewError(topo.NoNode, filePath)
	}
	if n.contents == nil {
		// it's a directory
		return nil, nil, fmt.Errorf("cannot Get() directory %v in cell %v", filePath, c.cell)
	}
	return n.contents, NodeVersion(n.version), nil
}

// List is part of the topo.Conn interface.
func (c *Conn) List(ctx context.Context, filePathPrefix string) ([]topo.KVInfo, error) {
	if err := c.dial(ctx); err != nil {
		return nil, err
	}

	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return nil, c.factory.err
	}

	dir, file := path.Split(filePathPrefix)
	// Get the node to list.
	n := c.factory.nodeByPath(c.cell, dir)
	if n == nil {
		return []topo.KVInfo{}, topo.NewError(topo.NoNode, filePathPrefix)
	}

	var result []topo.KVInfo
	for name, child := range n.children {
		if !strings.HasPrefix(name, file) {
			continue
		}
		if child.isDirectory() {
			result = append(result, gatherChildren(child, path.Join(dir, name))...)
		} else {
			result = append(result, topo.KVInfo{
				Key:     []byte(path.Join(dir, name)),
				Value:   child.contents,
				Version: NodeVersion(child.version),
			})
		}
	}

	if len(result) == 0 {
		return []topo.KVInfo{}, topo.NewError(topo.NoNode, filePathPrefix)
	}

	return result, nil
}

func gatherChildren(n *node, dirPath string) []topo.KVInfo {
	var result []topo.KVInfo
	for name, child := range n.children {
		if child.isDirectory() {
			result = append(result, gatherChildren(child, path.Join(dirPath, name))...)
		} else {
			result = append(result, topo.KVInfo{
				Key:     []byte(path.Join(dirPath, name)),
				Value:   child.contents,
				Version: NodeVersion(child.version),
			})
		}
	}
	return result
}

// Delete is part of topo.Conn interface.
func (c *Conn) Delete(ctx context.Context, filePath string, version topo.Version) error {
	if err := c.dial(ctx); err != nil {
		return err
	}

	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return c.factory.err
	}

	// Get the parent dir.
	dir, file := path.Split(filePath)
	p := c.factory.nodeByPath(c.cell, dir)
	if p == nil {
		return topo.NewError(topo.NoNode, filePath)
	}

	// Get the existing file.
	n, ok := p.children[file]
	if !ok {
		return topo.NewError(topo.NoNode, filePath)
	}

	// Check if it's a directory.
	if n.isDirectory() {
		//lint:ignore ST1005 Delete is a function name
		return fmt.Errorf("delete(%v, %v) failed: it's a directory", c.cell, filePath)
	}

	// Check the version.
	if version != nil && n.version != uint64(version.(NodeVersion)) {
		return topo.NewError(topo.BadVersion, filePath)
	}

	// Now we can delete.
	c.factory.recursiveDelete(n)

	// Call the watches
	for _, w := range n.watches {
		if w.contents != nil {
			w.contents <- &topo.WatchData{
				Err: topo.NewError(topo.NoNode, filePath),
			}
			close(w.contents)
		}
		if w.lock != nil {
			close(w.lock)
		}
	}

	n.propagateRecursiveWatch(&topo.WatchDataRecursive{
		Path: filePath,
		WatchData: topo.WatchData{
			Err: topo.NewError(topo.NoNode, filePath),
		},
	})

	return nil
}
