// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk2topo

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"
)

// fakeConn is a pretty complete mock implementation of a Zookeper
// Conn object. All operations work as expected with the exceptions of
// zk.Conn.ACL and zk.Conn.SetACL. zk.Conn.SetACL will succeed, but it
// is a noop (and the ACLs won't be respected). zk.Conn.ACL will
// panic. It is OK to access the connection from multiple goroutines,
// but the locking is very naive (every operation locks the whole
// connection).

// fakeConn implements the Conn interface, backed by a map.
type fakeConn struct {
	mu           sync.Mutex
	root         *node
	zxid         int64
	existWatches map[string][]chan zk.Event
}

// ConnectFake returns a fake connection.
// It ignores addr.
func ConnectFake(addr string) Conn {
	return newFakeConn()
}

// newFakeConn returns a fake Conn implementation. Data is stored in
// memory, and there's a global connection lock for concurrent access.
func newFakeConn() *fakeConn {
	return &fakeConn{
		root: &node{
			name:     "/",
			children: make(map[string]*node),
		},
		existWatches: make(map[string][]chan zk.Event)}
}

func (conn *fakeConn) Get(ctx context.Context, zkPath string) (data []byte, stat *zk.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	node, _, rest, err := conn.getNode(zkPath, "get")
	if err != nil {
		return nil, nil, err
	}
	if len(rest) != 0 {
		return nil, nil, zk.ErrNoNode
	}
	return node.content, node.statCopy(), nil
}

func (conn *fakeConn) GetW(ctx context.Context, zkPath string) (data []byte, stat *zk.Stat, watch <-chan zk.Event, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	node, _, rest, err := conn.getNode(zkPath, "getw")
	if err != nil {
		return nil, nil, nil, err
	}

	if len(rest) != 0 {
		return nil, nil, nil, zk.ErrNoNode
	}
	c := make(chan zk.Event, 1)
	node.changeWatches = append(node.changeWatches, c)
	return node.content, node.statCopy(), c, nil
}

func (conn *fakeConn) Children(ctx context.Context, zkPath string) (children []string, stat *zk.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "children")
	if err != nil {
		return nil, nil, err
	}
	if len(rest) != 0 {
		return nil, nil, zk.ErrNoNode
	}

	for name := range node.children {
		children = append(children, name)
	}
	return children, node.statCopy(), nil
}

func (conn *fakeConn) ChildrenW(ctx context.Context, zkPath string) (children []string, stat *zk.Stat, watch <-chan zk.Event, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "childrenw")
	if err != nil {
		return nil, nil, nil, err
	}

	if len(rest) != 0 {
		return nil, nil, nil, zk.ErrNoNode
	}
	c := make(chan zk.Event, 1)
	node.childrenWatches = append(node.childrenWatches, c)
	for name := range node.children {
		children = append(children, name)
	}
	return children, node.statCopy(), c, nil
}

func (conn *fakeConn) Exists(ctx context.Context, zkPath string) (exists bool, stat *zk.Stat, err error) {
	// FIXME(szopa): if the path is bad, Op will be "get."
	_, stat, err = conn.Get(ctx, zkPath)
	if err == zk.ErrNoNode {
		return false, nil, nil
	}
	return true, stat, err
}

func (conn *fakeConn) ExistsW(ctx context.Context, zkPath string) (exists bool, stat *zk.Stat, watch <-chan zk.Event, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	node, _, rest, err := conn.getNode(zkPath, "existsw")
	if err != nil {
		return false, nil, nil, err
	}

	c := make(chan zk.Event, 1)
	if len(rest) != 0 {
		watches, ok := conn.existWatches[zkPath]
		if !ok {
			watches = make([]chan zk.Event, 0)
			conn.existWatches[zkPath] = watches
		}
		conn.existWatches[zkPath] = append(watches, c)
		return false, nil, c, nil
	}
	node.existWatches = append(node.existWatches, c)
	return true, node.statCopy(), c, nil

}

func (conn *fakeConn) Create(ctx context.Context, zkPath string, value []byte, flags int32, aclv []zk.ACL) (zkPathCreated string, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	parent, _, rest, err := conn.getNode(zkPath, "create")
	if err != nil {
		return "", err
	}
	if len(rest) == 0 {
		return "", zk.ErrNodeExists
	}

	if len(rest) > 1 {
		return "", zk.ErrNoNode
	}

	zxid := conn.getZxid()
	name := rest[0]
	if (flags & zk.FlagSequence) != 0 {
		sequence := parent.nextSequence()
		name += sequence
		zkPath = zkPath + sequence
	}

	parent.children[name] = &node{
		acl:          aclv,
		children:     make(map[string]*node),
		existWatches: make([]chan zk.Event, 0),
		name:         name,
		content:      value,
		stat: zk.Stat{
			Mtime: ZkTime(time.Now()),
			Ctime: ZkTime(time.Now()),
			Czxid: zxid,
			Mzxid: zxid,
		},
	}
	event := zk.Event{
		Type:  zk.EventNodeCreated,
		Path:  zkPath,
		State: zk.StateConnected,
	}
	if watches, ok := conn.existWatches[zkPath]; ok {
		delete(conn.existWatches, zkPath)
		for _, watch := range watches {
			watch <- event
		}
	}
	childrenEvent := zk.Event{
		Type:  zk.EventNodeChildrenChanged,
		Path:  zkPath,
		State: zk.StateConnected,
	}
	for _, watch := range parent.childrenWatches {
		watch <- childrenEvent
		close(watch)
	}
	parent.childrenWatches = nil

	parent.stat.Cversion++

	return zkPath, nil
}

func (conn *fakeConn) Set(ctx context.Context, zkPath string, value []byte, version int32) (stat *zk.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "set")
	if err != nil {
		return nil, err
	}

	if len(rest) != 0 {
		return nil, zk.ErrNoNode
	}

	if version != -1 && node.stat.Version != version {
		return nil, zk.ErrBadVersion
	}
	node.content = value
	node.stat.Version++
	for _, watch := range node.changeWatches {
		watch <- zk.Event{
			Type:  zk.EventNodeDataChanged,
			Path:  zkPath,
			State: zk.StateConnected,
		}
	}
	node.changeWatches = nil
	return node.statCopy(), nil
}

func (conn *fakeConn) Delete(ctx context.Context, zkPath string, version int32) (err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, parent, rest, err := conn.getNode(zkPath, "delete")
	if err != nil {
		return err
	}

	if len(rest) > 0 {
		return zk.ErrNoNode
	}
	if len(node.children) > 0 {
		return zk.ErrNotEmpty
	}
	if version != -1 && node.stat.Version != version {
		return zk.ErrBadVersion
	}
	delete(parent.children, node.name)
	event := zk.Event{
		Type:  zk.EventNodeDeleted,
		Path:  zkPath,
		State: zk.StateConnected,
	}
	for _, watch := range node.existWatches {
		watch <- event
	}
	for _, watch := range node.changeWatches {
		watch <- event
	}
	node.existWatches = nil
	node.changeWatches = nil
	childrenEvent := zk.Event{
		Type:  zk.EventNodeChildrenChanged,
		Path:  zkPath,
		State: zk.StateConnected,
	}
	for _, watch := range parent.childrenWatches {
		watch <- childrenEvent
	}
	return nil
}

func (conn *fakeConn) GetACL(ctx context.Context, zkPath string) (acl []zk.ACL, stat *zk.Stat, err error) {
	panic("not implemented")
}

func (conn *fakeConn) SetACL(ctx context.Context, zkPath string, aclv []zk.ACL, version int32) (err error) {
	return nil
}

func (conn *fakeConn) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	for _, watches := range conn.existWatches {
		for _, c := range watches {
			close(c)
		}
	}
	conn.root.closeAllWatches()
	return nil
}

func (conn *fakeConn) getNode(zkPath string, op string) (node *node, parent *node, rest []string, err error) {
	// We have two edge cases that contradict each-other in the logic below:
	// - zkPath=/
	// - zkPath=/path/to/locks/
	// To make this simpler, we hard-code '/'.
	if zkPath == "/" {
		return conn.root, nil, nil, nil
	}

	// Make sure the path starts with '/'.
	parts := strings.Split(zkPath, "/")
	if parts[0] != "" {
		return nil, nil, nil, zk.ErrInvalidPath
	}
	elements := parts[1:]
	parent = nil
	current := conn.root
	for i, el := range elements {
		candidateParent := current
		candidate, ok := current.children[el]
		if !ok {
			return current, parent, elements[i:], nil
		}
		current, parent = candidate, candidateParent
	}
	return current, parent, []string{}, nil
}

type node struct {
	name    string
	content []byte
	stat    zk.Stat

	acl      []zk.ACL
	children map[string]*node
	sequence int

	existWatches    []chan zk.Event
	changeWatches   []chan zk.Event
	childrenWatches []chan zk.Event
}

func (n *node) statCopy() *zk.Stat {
	result := n.stat
	result.NumChildren = int32(len(n.children))
	return &result
}

func (n *node) closeAllWatches() {
	for _, c := range n.existWatches {
		close(c)
	}
	for _, c := range n.changeWatches {
		close(c)
	}
	for _, c := range n.childrenWatches {
		close(c)
	}
	for _, child := range n.children {
		child.closeAllWatches()
	}
}

func (n *node) nextSequence() string {
	n.sequence++
	return fmt.Sprintf("%010d", n.sequence)
}

func (n *node) fprintRecursive(level int, buf *bytes.Buffer) {
	start := strings.Repeat("  ", level)
	fmt.Fprintf(buf, "%v-%v:\n", start, n.name)
	if len(n.content) > 0 {
		fmt.Fprintf(buf, "%v content: %q\n\n", start, n.content)
	}
	if len(n.children) > 0 {
		for _, child := range n.children {
			child.fprintRecursive(level+1, buf)
		}
	}
}

func (conn *fakeConn) String() string {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	b := new(bytes.Buffer)
	conn.root.fprintRecursive(0, b)
	return b.String()
}

func (conn *fakeConn) getZxid() int64 {
	conn.zxid++
	return conn.zxid
}
