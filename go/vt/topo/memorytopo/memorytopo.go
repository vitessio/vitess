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
	"sort"
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
// It takes a file-system like approach, with directories at each level
// being an actual directory node. This is meant to be closer to
// file-system like servers, like ZooKeeper or Chubby. The fake etcd
// version is closer to a node-based fake.
type MemoryTopo struct {
	faketopo.FakeTopo

	// mu protects the following fields.
	mu sync.Mutex
	// cells is the toplevel node that has one child per cell.
	cells *node
	// generation is used to generate unique incrementing version
	// numbers.  We want a global counter so when creating a file,
	// then deleting it, then re-creating it, we don't restart the
	// version at 1.
	generation uint64
}

// node contains a directory or a file entry.
// Exactly one of contents or children is not nil.
type node struct {
	name     string
	version  uint64
	contents []byte
	children map[string]*node
	// parent is a pointer to the parent node.
	// It is set to nil in toplevel and cell node.
	parent *node

	watches map[int]chan *topo.WatchData
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
	result := &MemoryTopo{}
	result.cells = result.newDirectory("", nil)
	for _, cell := range cells {
		result.cells.children[cell] = result.newDirectory(cell, nil)
	}
	return result
}

func (mt *MemoryTopo) getNextVersion() uint64 {
	mt.generation++
	return mt.generation
}

func (mt *MemoryTopo) newFile(name string, contents []byte, parent *node) *node {
	return &node{
		name:     name,
		version:  mt.getNextVersion(),
		contents: contents,
		parent:   parent,
		watches:  make(map[int]chan *topo.WatchData),
	}
}

func (mt *MemoryTopo) newDirectory(name string, parent *node) *node {
	return &node{
		name:     name,
		version:  mt.getNextVersion(),
		children: make(map[string]*node),
		parent:   parent,
	}
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

func (mt *MemoryTopo) getOrCreatePath(cell, filePath string) *node {
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
			// Path doesn't exist, create it.
			child = mt.newDirectory(part, n)
			n.children[part] = child
		}
		n = child
	}
	return n
}

// recursiveDelete deletes a node and its parent directory if empty.
func (mt *MemoryTopo) recursiveDelete(n *node) {
	parent := n.parent
	if parent == nil {
		return
	}
	delete(parent.children, n.name)
	if len(parent.children) == 0 {
		mt.recursiveDelete(parent)
	}
}

// ListDir is part of the topo.Backend interface.
func (mt *MemoryTopo) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Get the node to list.
	n := mt.nodeByPath(cell, dirPath)
	if n == nil {
		return nil, topo.ErrNoNode
	}

	// Check it's a directory.
	if !n.isDirectory() {
		return nil, fmt.Errorf("node %v in cell %v is not a directory", dirPath, cell)
	}

	var result []string
	for n := range n.children {
		result = append(result, n)
	}
	sort.Strings(result)
	return result, nil
}

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
		return nil, topo.ErrNoNode
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

// GetKnownCells is part of the topo.Server interface.
func (mt *MemoryTopo) GetKnownCells(ctx context.Context) ([]string, error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	var result []string
	for c := range mt.cells.children {
		if c != "global" {
			result = append(result, c)
		}
	}
	return result, nil
}

var _ topo.Impl = (*MemoryTopo)(nil) // compile-time interface check
