package memorytopo

import (
	"fmt"
	"path"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// Create is part of topo.Backend interface.
func (mt *MemoryTopo) Create(ctx context.Context, cell, filePath string, contents []byte) (topo.Version, error) {
	if contents == nil {
		contents = []byte{}
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Get the parent dir.
	dir, file := path.Split(filePath)
	p := mt.getOrCreatePath(cell, dir)
	if p == nil {
		return nil, fmt.Errorf("trying to create file %v in cell %v in a path that contains files", filePath, cell)
	}

	// Check the file doesn't already exist.
	if _, ok := p.children[file]; ok {
		return nil, topo.ErrNodeExists
	}

	// Create the file.
	n := mt.newFile(file, contents, p)
	p.children[file] = n
	return NodeVersion(n.version), nil
}

// Update is part of topo.Backend interface.
func (mt *MemoryTopo) Update(ctx context.Context, cell, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if contents == nil {
		contents = []byte{}
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Get the parent dir, we'll need it in case of creation.
	dir, file := path.Split(filePath)
	p := mt.nodeByPath(cell, dir)
	if p == nil {
		// Parent doesn't exist, let's create it if we need to.
		if version != nil {
			return nil, topo.ErrNoNode
		}
		p = mt.getOrCreatePath(cell, dir)
		if p == nil {
			return nil, fmt.Errorf("trying to create file %v in cell %v in a path that contains files", filePath, cell)
		}
	}

	// Get the existing file.
	n, ok := p.children[file]
	if !ok {
		// File doesn't exist, see if we need to create it.
		if version != nil {
			return nil, topo.ErrNoNode
		}
		n = mt.newFile(file, contents, p)
		p.children[file] = n
		return NodeVersion(n.version), nil
	}

	// Check if it's a directory.
	if n.isDirectory() {
		return nil, fmt.Errorf("Update(%v,%v) failed: it's a directory", cell, filePath)
	}

	// Check the version.
	if version != nil && n.version != uint64(version.(NodeVersion)) {
		return nil, topo.ErrBadVersion
	}

	// Now we can update.
	n.version = mt.getNextVersion()
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

// Get is part of topo.Backend interface.
func (mt *MemoryTopo) Get(ctx context.Context, cell, filePath string) ([]byte, topo.Version, error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Get the node.
	n := mt.nodeByPath(cell, filePath)
	if n == nil {
		return nil, nil, topo.ErrNoNode
	}
	if n.contents == nil {
		// it's a directory
		return nil, nil, fmt.Errorf("cannot Get() directory %v in cell %v", filePath, cell)
	}
	return n.contents, NodeVersion(n.version), nil
}

// Delete is part of topo.Backend interface.
func (mt *MemoryTopo) Delete(ctx context.Context, cell, filePath string, version topo.Version) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Get the parent dir.
	dir, file := path.Split(filePath)
	p := mt.nodeByPath(cell, dir)
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
		return fmt.Errorf("Delete(%v,%v) failed: it's a directory", cell, filePath)
	}

	// Check the version.
	if version != nil && n.version != uint64(version.(NodeVersion)) {
		return topo.ErrBadVersion
	}

	// Now we can delete.
	mt.recursiveDelete(n)

	// Call the watches
	for _, w := range n.watches {
		w <- &topo.WatchData{
			Err: topo.ErrNoNode,
		}
		close(w)
	}

	return nil
}
