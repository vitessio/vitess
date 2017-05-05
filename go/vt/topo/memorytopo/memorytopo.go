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

// Package memorytopo contains an implementation of the topo.Backend
// API based on an in-process memory map.
//
// It also contains the plumbing to make it a topo.Impl as well.
// Eventually we will ove the difference to go/vt/topo.
package memorytopo

import (
	"sort"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// Path components
	keyspacesPath = "keyspaces"
	shardsPath    = "shards"
	tabletsPath   = "tablets"
	electionsPath = "elections"
)

var (
	nextWatchIndex = 0
)

// MemoryTopo is a memory-based implementation of topo.Backend.  It
// takes a file-system like approach, with directories at each level
// being an actual directory node. This is meant to be closer to
// file-system like servers, like ZooKeeper or Chubby. etcd or Consul
// implementations would be closer to a node-based implementation.
type MemoryTopo struct {
	// mu protects the following fields.
	mu sync.Mutex
	// cells is the toplevel map that has one entry per cell.
	cells map[string]*node
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

	// watches is a map of all watches for this node.
	watches map[int]chan *topo.WatchData

	// lock is nil when the node is not locked.
	// otherwise it has a channel that is closed by unlock.
	lock chan struct{}

	// lockContents is the contents of the locks.
	// For regular locks, it has the contents that was passed in.
	// For master election, it has the id of the election leader.
	lockContents string
}

func (n *node) isDirectory() bool {
	return n.children != nil
}

// New returns a new MemoryTopo for all the cells. It will create one
// cell for each parameter passed in.  It will log.Fatal out in case
// of a problem.
func New(cells ...string) *MemoryTopo {
	mt := &MemoryTopo{
		cells: make(map[string]*node),
	}
	mt.cells[topo.GlobalCell] = mt.newDirectory(topo.GlobalCell, nil)

	ctx := context.Background()
	ts := topo.Server{Impl: mt}
	for _, cell := range cells {
		if err := ts.CreateCellInfo(ctx, cell, &topodatapb.CellInfo{
			Root: "/",
		}); err != nil {
			log.Fatalf("ts.CreateCellInfo(%v) failed: %v", cell, err)
		}
		mt.cells[cell] = mt.newDirectory(cell, nil)
	}
	return mt
}

// NewServer returns a topo.Server based on a MemoryTopo.
func NewServer(cells ...string) topo.Server {
	return topo.Server{Impl: New(cells...)}
}

// Close is part of the topo.Impl interface.
func (mt *MemoryTopo) Close() {
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
	n, ok := mt.cells[cell]
	if !ok {
		return nil
	}

	parts := strings.Split(filePath, "/")
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
	n, ok := mt.cells[cell]
	if !ok {
		return nil
	}

	parts := strings.Split(filePath, "/")
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

// GetKnownCells is part of the topo.Server interface.
func (mt *MemoryTopo) GetKnownCells(ctx context.Context) ([]string, error) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	var result []string
	for c := range mt.cells {
		if c != topo.GlobalCell {
			result = append(result, c)
		}
	}
	sort.Strings(result)
	return result, nil
}
