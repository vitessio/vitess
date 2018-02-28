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
	"path"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
)

// Create is part of topo.Conn interface.
func (c *Conn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
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
		return nil, fmt.Errorf("trying to create file %v in cell %v in a path that contains files", filePath, c.cell)
	}

	// Check the file doesn't already exist.
	if _, ok := p.children[file]; ok {
		return nil, topo.ErrNodeExists
	}

	// Create the file.
	n := c.factory.newFile(file, contents, p)
	p.children[file] = n
	return NodeVersion(n.version), nil
}

// Update is part of topo.Conn interface.
func (c *Conn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
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
			return nil, topo.ErrNoNode
		}
		p = c.factory.getOrCreatePath(c.cell, dir)
		if p == nil {
			return nil, fmt.Errorf("trying to create file %v in cell %v in a path that contains files", filePath, c.cell)
		}
	}

	// Get the existing file.
	n, ok := p.children[file]
	if !ok {
		// File doesn't exist, see if we need to create it.
		if version != nil {
			return nil, topo.ErrNoNode
		}
		n = c.factory.newFile(file, contents, p)
		p.children[file] = n
		return NodeVersion(n.version), nil
	}

	// Check if it's a directory.
	if n.isDirectory() {
		return nil, fmt.Errorf("Update(%v, %v) failed: it's a directory", c.cell, filePath)
	}

	// Check the version.
	if version != nil && n.version != uint64(version.(NodeVersion)) {
		return nil, topo.ErrBadVersion
	}

	// Now we can update.
	n.version = c.factory.getNextVersion()
	n.contents = contents

	// Call the watches
	for _, w := range n.watches {
		w <- &topo.WatchData{
			Contents: n.contents,
			Version:  NodeVersion(n.version),
		}
	}

	return NodeVersion(n.version), nil
}

// Get is part of topo.Conn interface.
func (c *Conn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return nil, nil, c.factory.err
	}

	// Get the node.
	n := c.factory.nodeByPath(c.cell, filePath)
	if n == nil {
		return nil, nil, topo.ErrNoNode
	}
	if n.contents == nil {
		// it's a directory
		return nil, nil, fmt.Errorf("cannot Get() directory %v in cell %v", filePath, c.cell)
	}
	return n.contents, NodeVersion(n.version), nil
}

// Delete is part of topo.Conn interface.
func (c *Conn) Delete(ctx context.Context, filePath string, version topo.Version) error {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return c.factory.err
	}

	// Get the parent dir.
	dir, file := path.Split(filePath)
	p := c.factory.nodeByPath(c.cell, dir)
	if p == nil {
		return topo.ErrNoNode
	}

	// Get the existing file.
	n, ok := p.children[file]
	if !ok {
		return topo.ErrNoNode
	}

	// Check if it's a directory.
	if n.isDirectory() {
		return fmt.Errorf("Delete(%v, %v) failed: it's a directory", c.cell, filePath)
	}

	// Check the version.
	if version != nil && n.version != uint64(version.(NodeVersion)) {
		return topo.ErrBadVersion
	}

	// Now we can delete.
	c.factory.recursiveDelete(n)

	// Call the watches
	for _, w := range n.watches {
		w <- &topo.WatchData{
			Err: topo.ErrNoNode,
		}
		close(w)
	}

	return nil
}
