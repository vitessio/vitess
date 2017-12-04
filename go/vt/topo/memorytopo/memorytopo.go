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

// Package memorytopo contains an implementation of the topo.Factory /
// topo.Conn interfaces based on an in-memory tree of data.
// It is constructed with an immutable set of cells.
package memorytopo

import (
	"math/rand"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

const (
	// Path components
	electionsPath = "elections"
)

var (
	nextWatchIndex = 0
)

// Factory is a memory-based implementation of topo.Factory.  It
// takes a file-system like approach, with directories at each level
// being an actual directory node. This is meant to be closer to
// file-system like servers, like ZooKeeper or Chubby. etcd or Consul
// implementations would be closer to a node-based implementation.
//
// It contains a single tree of nodes. Each cell topo.Conn will use
// a sub-directory in that tree.
type Factory struct {
	// mu protects the following fields.
	mu sync.Mutex
	// cells is the toplevel map that has one entry per cell.
	cells map[string]*node
	// generation is used to generate unique incrementing version
	// numbers.  We want a global counter so when creating a file,
	// then deleting it, then re-creating it, we don't restart the
	// version at 1. It is initialized with a random number,
	// so if we have two implementations, the numbers won't match.
	generation uint64
}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
func (f *Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create is part of the topo.Factory interface.
func (f *Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.cells[cell]; !ok {
		return nil, topo.ErrNoNode
	}
	return &Conn{
		factory: f,
		cell:    cell,
	}, nil
}

// Conn implements the topo.Conn interface. It remembers the cell, and
// points at the Factory that has all the data.
type Conn struct {
	factory *Factory
	cell    string
}

// Close is part of the topo.Conn interface.
// It nils out factory, so any subsequent call will panic.
func (c *Conn) Close() {
	c.factory = nil
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

// NewServer returns a new MemoryTopo for all the cells. It will create one
// cell for each parameter passed in.  It will log.Exit out in case
// of a problem.
func NewServer(cells ...string) *topo.Server {
	f := &Factory{
		cells:      make(map[string]*node),
		generation: uint64(rand.Int63n(2 ^ 60)),
	}
	f.cells[topo.GlobalCell] = f.newDirectory(topo.GlobalCell, nil)

	ctx := context.Background()
	ts, err := topo.NewWithFactory(f, "" /*serverAddress*/, "" /*root*/)
	if err != nil {
		log.Exitf("topo.NewWithFactory() failed: %v", err)
	}
	for _, cell := range cells {
		f.cells[cell] = f.newDirectory(cell, nil)
		if err := ts.CreateCellInfo(ctx, cell, &topodatapb.CellInfo{}); err != nil {
			log.Exitf("ts.CreateCellInfo(%v) failed: %v", cell, err)
		}
	}
	return ts
}

func (f *Factory) getNextVersion() uint64 {
	f.generation++
	return f.generation
}

func (f *Factory) newFile(name string, contents []byte, parent *node) *node {
	return &node{
		name:     name,
		version:  f.getNextVersion(),
		contents: contents,
		parent:   parent,
		watches:  make(map[int]chan *topo.WatchData),
	}
}

func (f *Factory) newDirectory(name string, parent *node) *node {
	return &node{
		name:     name,
		version:  f.getNextVersion(),
		children: make(map[string]*node),
		parent:   parent,
	}
}

func (f *Factory) nodeByPath(cell, filePath string) *node {
	n, ok := f.cells[cell]
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

func (f *Factory) getOrCreatePath(cell, filePath string) *node {
	n, ok := f.cells[cell]
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
			child = f.newDirectory(part, n)
			n.children[part] = child
		}
		n = child
	}
	return n
}

// recursiveDelete deletes a node and its parent directory if empty.
func (f *Factory) recursiveDelete(n *node) {
	parent := n.parent
	if parent == nil {
		return
	}
	delete(parent.children, n.name)
	if len(parent.children) == 0 {
		f.recursiveDelete(parent)
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
