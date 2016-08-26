// Package memorytopo contains an implementation of the topo.Backend
// API based on an in-process memory map.
//
// At the moment, it is actually a topo.Impl implementation as well,
// based on faketopo. As we convert more and more code to the new
// file-based topo.Backend APIs, this will grow. Eventually, the topo.Impl
// interface will be retired.
package memorytopo

import (
	"fmt"
	"path"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/test/faketopo"
)

var (
	nextWatchIndex = 0
)

// MemoryTopo is a memory-based implementation of topo.Backend.
type MemoryTopo struct {
	faketopo.FakeTopo

	// mu protects the entire map. One root node per cell.
	mu    sync.Mutex
	cells *node
}

// node contains a directory or a file entry.
// Exactly one of contents or children is not nil.
type node struct {
	name     string
	version  uint64
	contents []byte
	children map[string]*node

	watches map[int]chan *topo.WatchData
}

func newFile(name string, contents []byte) *node {
	return &node{
		name:     name,
		version:  1,
		contents: contents,
		watches:  make(map[int]chan *topo.WatchData),
	}
}

func newDirectory(name string) *node {
	return &node{
		name:     name,
		version:  1,
		children: make(map[string]*node),
	}
}

func (n *node) isFile() bool {
	return n.contents != nil
}

func (n *node) isDirectory() bool {
	return n.children != nil
}

// NodeVersion is the local topo.Version implementation
type NodeVersion uint64

func (v NodeVersion) String() string {
	return fmt.Sprintf("%v", uint64(v))
}

// NewMemoryTopo returns a new MemoryTopo for all the cells.
func NewMemoryTopo(cells []string) *MemoryTopo {
	result := &MemoryTopo{
		cells: newDirectory(""),
	}
	for _, cell := range cells {
		result.cells.children[cell] = newDirectory(cell)
	}
	return result
}

func (mt *MemoryTopo) nodeByPath(cell, filePath string) *node {
	parts := strings.Split(filePath, "/")
	parts[0] = cell
	n := mt.cells
	for _, part := range parts {
		if part == "" {
			// Skip empty parts, usually happens at the end.
			continue
		}
		if n.children == nil {
			// This is a file.
			return nil
		}
		child, ok := n.children[part]
		if !ok {
			// Path doesn't exist.
			return nil
		}
		n = child
	}
	return n
}

// MkDir will eventually be part of topo.Backend interface.
func (mt *MemoryTopo) MkDir(ctx context.Context, cell string, dirPath string) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Get the parent dir.
	dir, file := path.Split(dirPath)
	n := mt.nodeByPath(cell, dir)
	if n == nil {
		return topo.ErrNoNode
	}

	// Check node doesn't exist.
	if _, ok := n.children[file]; ok {
		return topo.ErrNodeExists
	}

	n.children[file] = newDirectory(file)
	return nil
}

// Update will eventually be part of topo.Backend interface.
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
		return nil, topo.ErrNoNode
	}

	// Get the existing file.
	n, ok := p.children[file]
	if !ok {
		// File doesn't exist, see if we need to create it.
		if version != nil {
			return nil, topo.ErrNoNode
		}
		n = newFile(file, contents)
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
	n.version++
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

// Delete will eventually be part of topo.Backend interface.
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
	delete(p.children, file)

	// Call the watches
	for _, w := range n.watches {
		w <- &topo.WatchData{
			Err: topo.ErrNoNode,
		}
		close(w)
	}

	return nil
}

// Watch is part of the topo.Backend interface.
func (mt *MemoryTopo) Watch(ctx context.Context, cell string, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	n := mt.nodeByPath(cell, filePath)
	if n == nil {
		return &topo.WatchData{Err: topo.ErrNoNode}, nil, nil
	}
	if n.contents == nil {
		// it's a directory
		return &topo.WatchData{Err: fmt.Errorf("cannot watch directory %v in cell %v", filePath, cell)}, nil, nil
	}
	current := &topo.WatchData{
		Contents: n.contents,
		Version:  NodeVersion(n.version),
	}

	notifications := make(chan *topo.WatchData, 100)
	watchIndex := nextWatchIndex
	nextWatchIndex++
	n.watches[watchIndex] = notifications

	cancel := func() {
		// This function can be called at any point, so we first need
		// to make sure the watch is still valid.
		mt.mu.Lock()
		defer mt.mu.Unlock()

		n := mt.nodeByPath(cell, filePath)
		if n == nil {
			return
		}

		if w, ok := n.watches[watchIndex]; ok {
			delete(n.watches, watchIndex)
			w <- &topo.WatchData{Err: topo.ErrInterrupted}
			close(w)
		}
	}
	return current, notifications, cancel
}

var _ topo.Impl = (*MemoryTopo)(nil) // compile-time interface check
