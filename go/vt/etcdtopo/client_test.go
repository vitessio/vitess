// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type nodeEvent struct {
	index uint64
	resp  *etcd.Response
}

type fakeNode struct {
	node *etcd.Node

	watchIndex int
	watches    map[int]chan *etcd.Response

	// history contains a record of all changes to this node.
	// The contract of Watch() specifies that it should replay all changes
	// since the provided cluster index before subscribing to live updates.
	history []nodeEvent

	// expiration is the time at which the node should be deleted (TTL).
	expiration time.Time
}

func newFakeNode(node *etcd.Node) *fakeNode {
	return &fakeNode{
		node:    node,
		watches: make(map[int]chan *etcd.Response),
	}
}

func (fn *fakeNode) notify(modifiedIndex uint64, action string) {
	var node *etcd.Node
	if fn.node != nil {
		node = &etcd.Node{}
		*node = *fn.node
	}
	resp := &etcd.Response{
		Action: action,
		Node:   node,
	}

	// Log all changes.
	fn.history = append(fn.history, nodeEvent{index: modifiedIndex, resp: resp})

	// Notify anyone waiting for live updates.
	for _, w := range fn.watches {
		w <- resp
	}
}

type fakeClient struct {
	cell  string
	nodes map[string]*fakeNode
	index uint64

	// This mutex protects all the above fields, including subfields of each node.
	sync.Mutex
}

func newTestClient(machines []string) Client {
	// In tests, the first machine address is just the cell name.
	return &fakeClient{
		cell: machines[0],
		nodes: map[string]*fakeNode{
			"/": newFakeNode(&etcd.Node{Key: "/", Dir: true}),
		},
	}
}

func (c *fakeClient) expire() {
	for _, n := range c.nodes {
		if !n.expiration.IsZero() && time.Now().After(n.expiration) {
			c.index++
			n.node = nil
			n.expiration = time.Time{}
			n.notify(c.index, "expire")
		}
	}
}

func (c *fakeClient) createParentDirs(key string) {
	dir := path.Dir(key)
	for dir != "" {
		fn, ok := c.nodes[dir]
		if ok && fn.node != nil {
			return
		}
		if !ok {
			fn = newFakeNode(nil)
			c.nodes[dir] = fn
		}
		fn.node = &etcd.Node{Key: dir, Dir: true, CreatedIndex: c.index, ModifiedIndex: c.index}
		dir = path.Dir(dir)
	}
}

func (c *fakeClient) CompareAndDelete(key string, prevValue string, prevIndex uint64) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()
	c.expire()

	if prevValue != "" {
		panic("not implemented")
	}

	n, ok := c.nodes[key]
	if !ok || n.node == nil {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}
	if n.node.ModifiedIndex != prevIndex {
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}

	c.index++
	n.node = nil
	n.notify(c.index, "compareAndDelete")
	return &etcd.Response{}, nil
}

var ignoreTTLRefresh bool

func (c *fakeClient) CompareAndSwap(key string, value string, ttl uint64,
	prevValue string, prevIndex uint64) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()
	c.expire()

	n, ok := c.nodes[key]
	if !ok || n.node == nil {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}
	if prevValue != "" && n.node.Value != prevValue {
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}
	if prevIndex != 0 && n.node.ModifiedIndex != prevIndex {
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}

	c.index++
	n.node.ModifiedIndex = c.index
	n.node.Value = value
	if ttl > 0 && !ignoreTTLRefresh {
		n.expiration = time.Now().Add(time.Duration(ttl) * time.Second)
	}
	c.nodes[key] = n
	node := *n.node
	n.notify(c.index, "compareAndSwap")
	return &etcd.Response{Node: &node}, nil
}

func (c *fakeClient) Create(key string, value string, ttl uint64) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()
	c.expire()

	n, ok := c.nodes[key]
	if ok && n.node != nil {
		return nil, &etcd.EtcdError{ErrorCode: EcodeNodeExist}
	}

	c.index++
	c.createParentDirs(key)
	if !ok {
		n = newFakeNode(nil)
		c.nodes[key] = n
	}
	n.node = &etcd.Node{
		Key:           key,
		Value:         value,
		CreatedIndex:  c.index,
		ModifiedIndex: c.index,
	}
	if ttl > 0 {
		n.expiration = time.Now().Add(time.Duration(ttl) * time.Second)
	}
	node := *n.node
	n.notify(c.index, "create")
	return &etcd.Response{Node: &node}, nil
}

func (c *fakeClient) Delete(key string, recursive bool) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()
	c.expire()

	n, ok := c.nodes[key]
	if !ok || n.node == nil {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}

	c.index++
	n.node = nil
	notifyList := []*fakeNode{n}

	if recursive {
		for k, n := range c.nodes {
			if strings.HasPrefix(k, key+"/") {
				n.node = nil
				notifyList = append(notifyList, n)
			}
		}
	}
	for _, n = range notifyList {
		n.notify(c.index, "delete")
	}
	return &etcd.Response{}, nil
}

func (c *fakeClient) DeleteDir(key string) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()
	c.expire()

	n, ok := c.nodes[key]
	if !ok || n.node == nil {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}

	if n.node.Dir {
		// If it's a dir, it must be empty.
		for k := range c.nodes {
			if strings.HasPrefix(k, key+"/") {
				return nil, &etcd.EtcdError{ErrorCode: EcodeDirNotEmpty}
			}
		}
	}

	c.index++
	n.node = nil
	n.notify(c.index, "delete")

	return &etcd.Response{}, nil
}

func (c *fakeClient) Get(key string, sortFiles, recursive bool) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()
	c.expire()

	if recursive {
		panic("not implemented")
	}

	n, ok := c.nodes[key]
	if !ok || n.node == nil {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}
	node := *n.node
	resp := &etcd.Response{Node: &node}
	if !n.node.Dir {
		return resp, nil
	}

	// List the directory.
	targetDir := key + "/"
	for k, n := range c.nodes {
		if n.node == nil {
			continue
		}
		dir, file := path.Split(k)
		if dir == targetDir && !strings.HasPrefix(file, "_") {
			node := *n.node
			resp.Node.Nodes = append(resp.Node.Nodes, &node)
		}
	}
	if sortFiles {
		sort.Sort(resp.Node.Nodes)
	}
	return resp, nil
}

func (c *fakeClient) Set(key string, value string, ttl uint64) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()
	c.expire()

	c.index++

	c.createParentDirs(key)
	n, ok := c.nodes[key]
	if !ok {
		n = newFakeNode(nil)
		c.nodes[key] = n
	}
	if n.node != nil {
		n.node.Value = value
		n.node.ModifiedIndex = c.index
	} else {
		n.node = &etcd.Node{Key: key, Value: value, CreatedIndex: c.index, ModifiedIndex: c.index}
	}
	if ttl > 0 {
		n.expiration = time.Now().Add(time.Duration(ttl) * time.Second)
	}
	node := *n.node

	n.notify(c.index, "set")
	return &etcd.Response{Node: &node}, nil
}

func (c *fakeClient) SetCluster(machines []string) bool {
	c.Lock()
	defer c.Unlock()

	c.cell = machines[0]
	return true
}

func (c *fakeClient) Watch(prefix string, waitIndex uint64, recursive bool,
	receiver chan *etcd.Response, stop chan bool) (*etcd.Response, error) {

	if recursive {
		panic("not implemented")
	}

	// We need to close the receiver, as the real client's Watch()
	// method also does it, and we depend on it.
	defer close(receiver)

	// We need a buffered forwarder for 2 reasons:
	// - In the select loop below, we only write to receiver if
	//   stop has not been closed. Otherwise we introduce race
	//   conditions.
	// - We are waiting on forwarder and taking the mutex.
	//   Callers of fakeNode.notify write to forwarder, and also take
	//   the mutex. Both can deadlock each other. By buffering the
	//   channel, we make sure multiple notify() calls can finish and not
	//   deadlock. We do a few of them in the serial locking code
	//   in tests.
	forwarder := make(chan *etcd.Response, 100)

	// Fetch history and subscribe to live updates.
	c.Lock()
	c.expire()
	c.createParentDirs(prefix)
	n, ok := c.nodes[prefix]
	if !ok {
		n = newFakeNode(nil)
		c.nodes[prefix] = n
	}
	index := n.watchIndex
	n.watchIndex++
	n.watches[index] = forwarder
	history := n.history
	c.Unlock()

	defer func() {
		// Unsubscribe from live updates.
		c.Lock()
		delete(n.watches, index)
		c.Unlock()
	}()

	// Before we begin processing live updates, catch up on history as requested.
	for _, event := range history {
		if event.index >= waitIndex {
			select {
			case <-stop:
				return nil, etcd.ErrWatchStoppedByUser
			case receiver <- event.resp:
			}
		}
	}

	// Process live updates.
	for {
		select {
		case <-stop:
			return nil, etcd.ErrWatchStoppedByUser
		case r := <-forwarder:
			receiver <- r
		}
	}
}
