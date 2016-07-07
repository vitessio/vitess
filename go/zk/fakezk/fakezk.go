// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakezk is a pretty complete mock implementation of a
// Zookeper connection (see go/zk/zk.Conn). All operations
// work as expected with the exceptions of zk.Conn.ACL and
// zk.Conn.SetACL. zk.Conn.SetACL will succeed, but it is a noop (and
// the ACLs won't be respected). zk.Conn.ACL will panic. It is OK to
// access the connection from multiple goroutines, but the locking is
// very naive (every operation locks the whole connection).
package fakezk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	zookeeper "github.com/samuel/go-zookeeper/zk"

	"github.com/youtube/vitess/go/zk"
)

type zconn struct {
	mu           sync.Mutex
	root         *node
	zxid         int64
	existWatches map[string][]chan zookeeper.Event
}

func (conn *zconn) getZxid() int64 {
	conn.zxid++
	return conn.zxid
}

// NewConn returns a fake zk.Conn implementation. Data is stored in
// memory, and there's a global connection lock for concurrent access.
func NewConn() zk.Conn {
	return &zconn{
		root: &node{
			name:     "/",
			children: make(map[string]*node),
		},
		existWatches: make(map[string][]chan zookeeper.Event)}
}

// NewConnFromFile returns a fake zk.Conn implementation, that is seeded
// with the json data extracted from the input file.
func NewConnFromFile(filename string) zk.Conn {
	result := &zconn{
		root: &node{
			name:     "/",
			children: make(map[string]*node),
		},
		existWatches: make(map[string][]chan zookeeper.Event)}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(fmt.Errorf("NewConnFromFile failed to read file %v: %v", filename, err))
	}
	values := make(map[string]interface{})
	if err := json.Unmarshal(data, &values); err != nil {
		panic(fmt.Errorf("NewConnFromFile failed to json.Unmarshal file %v: %v", filename, err))
	}
	for k, v := range values {
		jv, err := json.Marshal(v)
		if err != nil {
			panic(fmt.Errorf("NewConnFromFile failed to json.Marshal value %v: %v", k, err))
		}

		// CreateRecursive will work for a leaf node where the parent
		// doesn't exist, but not for a node in the middle of a tree
		// that already exists. So have to use 'Set' as a backup.
		if _, err := zk.CreateRecursive(result, k, string(jv), 0, nil); err != nil {
			if err == zookeeper.ErrNodeExists {
				_, err = result.Set(k, string(jv), -1)
			}
			if err != nil {
				panic(fmt.Errorf("NewConnFromFile failed to zk.CreateRecursive value %v: %v", k, err))
			}
		}
	}
	return result
}

func (conn *zconn) Get(zkPath string) (data string, stat *zookeeper.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	node, _, rest, err := conn.getNode(zkPath, "get")
	if err != nil {
		return "", nil, err
	}
	if len(rest) != 0 {
		return "", nil, zookeeper.ErrNoNode
	}
	return node.content, node.statCopy(), nil
}

func (conn *zconn) GetW(zkPath string) (data string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	node, _, rest, err := conn.getNode(zkPath, "getw")
	if err != nil {
		return "", nil, nil, err
	}

	if len(rest) != 0 {
		return "", nil, nil, zookeeper.ErrNoNode
	}
	c := make(chan zookeeper.Event, 1)
	node.changeWatches = append(node.changeWatches, c)
	return node.content, node.statCopy(), c, nil
}

func (conn *zconn) Children(zkPath string) (children []string, stat *zookeeper.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "children")
	if err != nil {
		return nil, nil, err
	}

	if len(rest) != 0 {
		return nil, nil, zookeeper.ErrNoNode
	}
	for name := range node.children {
		children = append(children, name)
	}
	return children, node.statCopy(), nil
}

func (conn *zconn) ChildrenW(zkPath string) (children []string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "childrenw")
	if err != nil {
		return nil, nil, nil, err
	}

	if len(rest) != 0 {
		return nil, nil, nil, zookeeper.ErrNoNode
	}
	c := make(chan zookeeper.Event, 1)
	node.childrenWatches = append(node.childrenWatches, c)
	for name := range node.children {
		children = append(children, name)
	}
	return children, node.statCopy(), c, nil
}

func (conn *zconn) Exists(zkPath string) (stat *zookeeper.Stat, err error) {
	// FIXME(szopa): if the path is bad, Op will be "get."
	_, stat, err = conn.Get(zkPath)
	if err == zookeeper.ErrNoNode {
		return nil, nil
	}
	return stat, err
}

func (conn *zconn) ExistsW(zkPath string) (stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	c := make(chan zookeeper.Event, 1)
	node, _, rest, err := conn.getNode(zkPath, "existsw")
	if err != nil {
		return nil, nil, err
	}

	if len(rest) != 0 {
		watches, ok := conn.existWatches[zkPath]
		if !ok {
			watches = make([]chan zookeeper.Event, 0)
			conn.existWatches[zkPath] = watches
		}
		conn.existWatches[zkPath] = append(watches, c)
		return nil, c, nil
	}
	node.existWatches = append(node.existWatches, c)
	return node.statCopy(), c, nil

}

func (conn *zconn) Create(zkPath, value string, flags int, aclv []zookeeper.ACL) (zkPathCreated string, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	parent, _, rest, err := conn.getNode(zkPath, "create")
	if err != nil {
		return "", err
	}
	if len(rest) == 0 {
		return "", zookeeper.ErrNodeExists
	}

	if len(rest) > 1 {
		return "", zookeeper.ErrNoNode
	}

	zxid := conn.getZxid()
	name := rest[0]
	if (flags & zookeeper.FlagSequence) != 0 {
		sequence := parent.nextSequence()
		name += sequence
		zkPath = zkPath + sequence
	}

	parent.children[name] = &node{
		acl:          aclv,
		children:     make(map[string]*node),
		existWatches: make([]chan zookeeper.Event, 0),
		name:         name,
		content:      value,
		stat: zookeeper.Stat{
			Mtime: zk.ZkTime(time.Now()),
			Ctime: zk.ZkTime(time.Now()),
			Czxid: zxid,
			Mzxid: zxid,
		},
	}
	event := zookeeper.Event{
		Type:  zookeeper.EventNodeCreated,
		Path:  zkPath,
		State: zookeeper.StateConnected,
	}
	if watches, ok := conn.existWatches[zkPath]; ok {
		delete(conn.existWatches, zkPath)
		for _, watch := range watches {
			watch <- event
		}
	}
	childrenEvent := zookeeper.Event{
		Type:  zookeeper.EventNodeChildrenChanged,
		Path:  zkPath,
		State: zookeeper.StateConnected,
	}
	for _, watch := range parent.childrenWatches {
		watch <- childrenEvent
		close(watch)
	}
	parent.childrenWatches = nil

	parent.stat.Cversion++

	return zkPath, nil
}

func (conn *zconn) Set(zkPath, value string, version int32) (stat *zookeeper.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "set")
	if err != nil {
		return nil, err
	}

	if len(rest) != 0 {
		return nil, zookeeper.ErrNoNode
	}

	if version != -1 && node.stat.Version != version {
		return nil, zookeeper.ErrBadVersion
	}
	node.content = value
	node.stat.Version++
	for _, watch := range node.changeWatches {
		watch <- zookeeper.Event{
			Type:  zookeeper.EventNodeDataChanged,
			Path:  zkPath,
			State: zookeeper.StateConnected,
		}
	}
	node.changeWatches = nil
	return node.statCopy(), nil
}

func (conn *zconn) Delete(zkPath string, version int32) (err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, parent, rest, err := conn.getNode(zkPath, "delete")
	if err != nil {
		return err
	}

	if len(rest) > 0 {
		return zookeeper.ErrNoNode
	}
	if len(node.children) > 0 {
		return zookeeper.ErrNotEmpty
	}
	if version != -1 && node.stat.Version != version {
		return zookeeper.ErrBadVersion
	}
	delete(parent.children, node.name)
	event := zookeeper.Event{
		Type:  zookeeper.EventNodeDeleted,
		Path:  zkPath,
		State: zookeeper.StateConnected,
	}
	for _, watch := range node.existWatches {
		watch <- event
	}
	for _, watch := range node.changeWatches {
		watch <- event
	}
	node.existWatches = nil
	node.changeWatches = nil
	childrenEvent := zookeeper.Event{
		Type:  zookeeper.EventNodeChildrenChanged,
		Path:  zkPath,
		State: zookeeper.StateConnected,
	}
	for _, watch := range parent.childrenWatches {
		watch <- childrenEvent
	}
	return nil
}

func (conn *zconn) Close() error {
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

func (conn *zconn) ACL(zkPath string) (acl []zookeeper.ACL, stat *zookeeper.Stat, err error) {
	panic("not implemented")
}

func (conn *zconn) SetACL(zkPath string, aclv []zookeeper.ACL, version int32) (err error) {
	return nil
}

func (conn *zconn) getNode(zkPath string, op string) (node *node, parent *node, rest []string, err error) {
	// FIXME(szopa): Make sure the path starts with /.
	parts := strings.Split(zkPath, "/")
	if parts[0] != "" {
		return nil, nil, nil, zookeeper.ErrInvalidPath
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
	content string
	stat    zookeeper.Stat

	acl      []zookeeper.ACL
	children map[string]*node
	sequence int

	existWatches    []chan zookeeper.Event
	changeWatches   []chan zookeeper.Event
	childrenWatches []chan zookeeper.Event
}

func (n *node) statCopy() *zookeeper.Stat {
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
	if n.content != "" {
		fmt.Fprintf(buf, "%v content: %q\n\n", start, n.content)
	}
	if len(n.children) > 0 {
		for _, child := range n.children {
			child.fprintRecursive(level+1, buf)
		}
	}
}

func (conn *zconn) String() string {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	b := new(bytes.Buffer)
	conn.root.fprintRecursive(0, b)
	return b.String()
}
