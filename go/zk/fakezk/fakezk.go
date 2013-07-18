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
	"fmt"
	"strings"
	"sync"
	"time"

	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

type zconn struct {
	mu           sync.Mutex
	root         *stat
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
		root: &stat{
			name:     "/",
			children: make(map[string]*stat),
		},
		existWatches: make(map[string][]chan zookeeper.Event)}
}

func (conn *zconn) Get(zkPath string) (data string, stat zk.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	node, _, rest, err := conn.getNode(zkPath, "get")
	if err != nil {
		return "", nil, err
	}
	if len(rest) != 0 {
		return "", nil, zkError(zookeeper.ZNONODE, "get", zkPath)
	}
	return node.content, node, nil
}

func (conn *zconn) GetW(zkPath string) (data string, stat zk.Stat, watch <-chan zookeeper.Event, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	node, _, rest, err := conn.getNode(zkPath, "getw")
	if err != nil {
		return "", nil, nil, err
	}

	if len(rest) != 0 {
		return "", nil, nil, zkError(zookeeper.ZNONODE, "getw", zkPath)
	}
	c := make(chan zookeeper.Event, 1)
	node.changeWatches = append(node.changeWatches, c)
	return node.content, node, c, nil
}

func (conn *zconn) Children(zkPath string) (children []string, stat zk.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "children")
	if err != nil {
		return nil, nil, err
	}

	if len(rest) != 0 {
		return nil, nil, zkError(zookeeper.ZNONODE, "children", zkPath)
	}
	for name := range node.children {
		children = append(children, name)
	}
	return children, node, nil
}

func (conn *zconn) ChildrenW(zkPath string) (children []string, stat zk.Stat, watch <-chan zookeeper.Event, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "childrenw")
	if err != nil {
		return nil, nil, nil, err
	}

	if len(rest) != 0 {
		return nil, nil, nil, zkError(zookeeper.ZNONODE, "childrenw", zkPath)
	}
	c := make(chan zookeeper.Event, 1)
	node.childrenWatches = append(node.childrenWatches, c)
	for name := range node.children {
		children = append(children, name)
	}
	return children, node, c, nil
}

func (conn *zconn) Exists(zkPath string) (stat zk.Stat, err error) {
	// FIXME(szopa): if the path is bad, Op will be "get."
	_, stat, err = conn.Get(zkPath)
	if err != nil && zookeeper.IsError(err, zookeeper.ZNONODE) {
		err = nil
	}
	return stat, err
}

func (conn *zconn) ExistsW(zkPath string) (stat zk.Stat, watch <-chan zookeeper.Event, err error) {
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
	return node, c, nil

}

func (conn *zconn) Create(zkPath, value string, flags int, aclv []zookeeper.ACL) (zkPathCreated string, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "create")
	if err != nil {
		return "", err
	}
	if len(rest) == 0 {
		return "", zkError(zookeeper.ZNODEEXISTS, "create", zkPath)
	}

	if len(rest) > 1 {
		return "", zkError(zookeeper.ZNONODE, "create", zkPath)
	}

	zxid := conn.getZxid()
	name := rest[0]
	if flags == zookeeper.SEQUENCE && name == "" {
		sequence := node.nextSequence()
		name = sequence
		zkPath = zkPath + sequence
	}

	stat := &stat{
		name:         name,
		content:      value,
		children:     make(map[string]*stat),
		acl:          aclv,
		mtime:        time.Now(),
		ctime:        time.Now(),
		czxid:        zxid,
		mzxid:        zxid,
		existWatches: make([]chan zookeeper.Event, 0),
	}
	node.children[name] = stat
	event := zookeeper.Event{
		Type:  zookeeper.EVENT_CREATED,
		Path:  zkPath,
		State: zookeeper.STATE_CONNECTED,
	}
	if watches, ok := conn.existWatches[zkPath]; ok {
		delete(conn.existWatches, zkPath)
		for _, watch := range watches {
			watch <- event

		}
	}
	childrenEvent := zookeeper.Event{
		Type:  zookeeper.EVENT_CHILD,
		Path:  zkPath,
		State: zookeeper.STATE_CONNECTED,
	}
	for _, watch := range node.childrenWatches {
		watch <- childrenEvent
	}

	node.cversion++

	return zkPath, nil
}

func (conn *zconn) Set(zkPath, value string, version int) (stat zk.Stat, err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, _, rest, err := conn.getNode(zkPath, "set")
	if err != nil {
		return nil, err
	}

	if len(rest) != 0 {
		return nil, zkError(zookeeper.ZNONODE, "set", zkPath)
	}

	if version != -1 && node.version != version {
		return nil, zkError(zookeeper.ZBADVERSION, "set", zkPath)
	}
	node.content = value
	node.version++
	for _, watch := range node.changeWatches {
		watch <- zookeeper.Event{
			Type:  zookeeper.EVENT_CHANGED,
			Path:  zkPath,
			State: zookeeper.STATE_CONNECTED,
		}
	}
	node.changeWatches = nil
	return node, nil
}

func (conn *zconn) Delete(zkPath string, version int) (err error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	node, parent, rest, err := conn.getNode(zkPath, "delete")
	if err != nil {
		return err
	}

	if len(rest) > 0 {
		return zkError(zookeeper.ZNONODE, "delete", zkPath)
	}
	if len(node.children) > 0 {
		return zkError(zookeeper.ZNOTEMPTY, "delete", zkPath)
	}
	delete(parent.children, node.name)
	event := zookeeper.Event{
		Type:  zookeeper.EVENT_DELETED,
		Path:  zkPath,
		State: zookeeper.STATE_CONNECTED,
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
		Type:  zookeeper.EVENT_CHILD,
		Path:  zkPath,
		State: zookeeper.STATE_CONNECTED}

	for _, watch := range parent.childrenWatches {
		watch <- childrenEvent
	}
	return nil
}

func (conn *zconn) Close() error {
	for _, watches := range conn.existWatches {
		for _, c := range watches {
			close(c)
		}
	}
	conn.root.closeAllWatches()
	return nil
}

func (conn *zconn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc zk.ChangeFunc) error {
	for {
		oldValue, oldStat, err := conn.Get(path)
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return err
		}
		newValue, err := changeFunc(oldValue, oldStat)
		if err != nil {
			return err
		}
		if oldStat == nil {
			_, err := conn.Create(path, newValue, flags, acl)
			if err == nil || !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
				return err
			}
			continue
		}
		if newValue == oldValue {
			return nil // Nothing to do.
		}
		_, err = conn.Set(path, newValue, oldStat.Version())
		if err == nil || !zookeeper.IsError(err, zookeeper.ZBADVERSION) && !zookeeper.IsError(err, zookeeper.ZNONODE) {
			return err
		}
	}
}

func (conn *zconn) ACL(zkPath string) (acl []zookeeper.ACL, stat zk.Stat, err error) {
	panic("not implemented")
}

func (conn *zconn) SetACL(zkPath string, aclv []zookeeper.ACL, version int) (err error) {
	return nil
}

func (conn *zconn) getNode(zkPath string, op string) (node *stat, parent *stat, rest []string, err error) {
	// FIXME(szopa): Make sure the path starts with /.
	parts := strings.Split(zkPath, "/")
	if parts[0] != "" {
		return nil, nil, nil, &zookeeper.Error{Code: zookeeper.ZBADARGUMENTS, Path: zkPath, Op: op}
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

// zkError creates an appropriate error return from
// a ZooKeeper status
func zkError(code zookeeper.ErrorCode, op, path string) error {
	return &zookeeper.Error{
		Op:   op,
		Code: code,
		Path: path,
	}
}

type stat struct {
	name     string
	content  string
	children map[string]*stat
	acl      []zookeeper.ACL
	mtime    time.Time
	ctime    time.Time
	czxid    int64
	mzxid    int64
	pzxid    int64
	version  int
	cversion int
	aversion int

	sequence int

	existWatches    []chan zookeeper.Event
	changeWatches   []chan zookeeper.Event
	childrenWatches []chan zookeeper.Event
}

func (st stat) closeAllWatches() {
	for _, c := range st.existWatches {
		close(c)
	}
	for _, c := range st.changeWatches {
		close(c)
	}
	for _, c := range st.childrenWatches {
		close(c)
	}
	for _, child := range st.children {
		child.closeAllWatches()
	}
}

func (st stat) Czxid() int64 {
	return st.czxid
}
func (st stat) Mzxid() int64 {
	return st.mzxid
}
func (st stat) CTime() time.Time {
	return st.ctime
}
func (st stat) MTime() time.Time {
	return st.mtime
}
func (st stat) Version() int {
	return st.version
}
func (st stat) CVersion() int {
	return st.cversion
}
func (st stat) AVersion() int {
	return st.aversion
}
func (st stat) EphemeralOwner() int64 {
	return 0
}

func (st stat) DataLength() int {
	return len(st.content)
}

func (st stat) NumChildren() int {
	return len(st.children)
}

func (st stat) Pzxid() int64 {
	return st.pzxid
}

func (st *stat) nextSequence() string {
	st.sequence++
	return fmt.Sprintf("%010d", st.sequence)
}

func (st stat) fprintRecursive(level int, buf *bytes.Buffer) {
	start := strings.Repeat("  ", level)
	fmt.Fprintf(buf, "%v-%v:\n", start, st.name)
	if st.content != "" {
		fmt.Fprintf(buf, "%v content: %q\n\n", start, st.content)
	}
	if len(st.children) > 0 {
		for _, child := range st.children {
			child.fprintRecursive(level+1, buf)
		}
	}
}

func (conn zconn) String() string {
	b := new(bytes.Buffer)
	conn.root.fprintRecursive(0, b)
	return b.String()
}
