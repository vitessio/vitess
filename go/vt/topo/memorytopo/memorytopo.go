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

// Package memorytopo contains an implementation of the topo.Factory /
// topo.Conn interfaces based on an in-memory tree of data.
// It is constructed with an immutable set of cells.
package memorytopo

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	// Path components
	electionsPath = "elections"
)

var ErrConnectionClosed = errors.New("connection closed")

const (
	// UnreachableServerAddr is a sentinel value for CellInfo.ServerAddr.
	// If a memorytopo topo.Conn is created with this serverAddr then every
	// method on that Conn which takes a context will simply block until the
	// context finishes, and return ctx.Err(), in order to simulate an
	// unreachable local cell for testing.
	UnreachableServerAddr = "unreachable"
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
	// err is used for testing purposes to force queries / watches
	// to return the given error
	err error
	// listErr is used for testing purposed to fake errors from
	// calls to List.
	listErr error
	// callstats allows us to keep track of how many topo.Conn calls
	// we make (Create, Get, Update, Delete, List, ListDir, etc).
	callstats *stats.CountersWithMultiLabels
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
		return nil, topo.NewError(topo.NoNode, cell)
	}
	return &Conn{
		factory:    f,
		cell:       cell,
		serverAddr: serverAddr,
	}, nil
}

// SetError forces the given error to be returned from all calls and propagates
// the error to all active watches.
func (f *Factory) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.err = err
	if err != nil {
		for _, node := range f.cells {
			node.PropagateWatchError(err)
		}
	}
}

func (f *Factory) GetCallStats() *stats.CountersWithMultiLabels {
	return f.callstats
}

// Lock blocks all requests to the topo and is exposed to allow tests to
// simulate an unresponsive topo server
func (f *Factory) Lock() {
	f.mu.Lock()
}

// Unlock unblocks all requests to the topo and is exposed to allow tests to
// simulate an unresponsive topo server
func (f *Factory) Unlock() {
	f.mu.Unlock()
}

// Conn implements the topo.Conn interface. It remembers the cell and serverAddr,
// and points at the Factory that has all the data.
type Conn struct {
	factory    *Factory
	cell       string
	serverAddr string
	closed     atomic.Bool
}

// dial returns immediately, unless the Conn points to the sentinel
// UnreachableServerAddr, in which case it will block until the context expires.
func (c *Conn) dial(ctx context.Context) error {
	if c.closed.Load() {
		return ErrConnectionClosed
	}
	if c.serverAddr == UnreachableServerAddr {
		<-ctx.Done()
	}

	return ctx.Err()
}

// Close is part of the topo.Conn interface.
func (c *Conn) Close() {
	c.factory.callstats.Add([]string{"Close"}, 1)
	c.closed.Store(true)
}

type watch struct {
	contents  chan *topo.WatchData
	recursive chan *topo.WatchDataRecursive
	lock      chan string
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
	watches map[int]watch

	// lock is nil when the node is not locked.
	// otherwise it has a channel that is closed by unlock.
	lock chan struct{}

	// lockContents is the contents of the locks.
	// For regular locks, it has the contents that was passed in.
	// For primary election, it has the id of the election leader.
	lockContents string
}

func (n *node) isDirectory() bool {
	return n.children != nil
}

func (n *node) recurseContents(callback func(n *node)) {
	if n.isDirectory() {
		for _, child := range n.children {
			child.recurseContents(callback)
		}
	} else {
		callback(n)
	}
}

func (n *node) propagateRecursiveWatch(ev *topo.WatchDataRecursive) {
	for parent := n.parent; parent != nil; parent = parent.parent {
		for _, w := range parent.watches {
			if w.recursive != nil {
				w.recursive <- ev
			}
		}
	}
}

var (
	nextWatchIndex   = 0
	nextWatchIndexMu sync.Mutex
)

func (n *node) addWatch(w watch) int {
	nextWatchIndexMu.Lock()
	defer nextWatchIndexMu.Unlock()
	watchIndex := nextWatchIndex
	nextWatchIndex++
	n.watches[watchIndex] = w
	return watchIndex
}

// PropagateWatchError propagates the given error to all watches on this node
// and recursively applies to all children
func (n *node) PropagateWatchError(err error) {
	for _, ch := range n.watches {
		if ch.contents == nil {
			continue
		}
		ch.contents <- &topo.WatchData{
			Err: err,
		}
	}

	for _, c := range n.children {
		c.PropagateWatchError(err)
	}
}

// NewServerAndFactory returns a new MemoryTopo and the backing factory for all
// the cells. It will create one cell for each parameter passed in.  It will log.Exit out
// in case of a problem.
func NewServerAndFactory(ctx context.Context, cells ...string) (*topo.Server, *Factory) {
	f := &Factory{
		cells:      make(map[string]*node),
		generation: uint64(rand.Int63n(1 << 60)),
		callstats:  stats.NewCountersWithMultiLabels("", "", []string{"Call"}),
	}
	f.cells[topo.GlobalCell] = f.newDirectory(topo.GlobalCell, nil)

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
	return ts, f
}

// NewServer returns the new server
func NewServer(ctx context.Context, cells ...string) *topo.Server {
	server, _ := NewServerAndFactory(ctx, cells...)
	return server
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
		watches:  make(map[int]watch),
	}
}

func (f *Factory) newDirectory(name string, parent *node) *node {
	return &node{
		name:     name,
		version:  f.getNextVersion(),
		children: make(map[string]*node),
		parent:   parent,
		watches:  make(map[int]watch),
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

func (f *Factory) SetListError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listErr = err
}
