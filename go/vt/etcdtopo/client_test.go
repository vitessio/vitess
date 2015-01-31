// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/coreos/go-etcd/etcd"
)

type fakeNode struct {
	node *etcd.Node

	mu         sync.Mutex
	watchIndex int
	watches    map[int]chan *etcd.Response
}

func newFakeNode(node *etcd.Node) *fakeNode {
	return &fakeNode{
		node:    node,
		watches: make(map[int]chan *etcd.Response),
	}
}

func (fn *fakeNode) notify(action string) {
	fn.mu.Lock()
	defer fn.mu.Unlock()
	for _, w := range fn.watches {
		var node *etcd.Node
		if fn.node != nil {
			node = &etcd.Node{}
			*node = *fn.node
		}
		w <- &etcd.Response{
			Action: action,
			Node:   node,
		}
	}
}

type fakeClient struct {
	cell  string
	nodes map[string]*fakeNode
	index uint64

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

	if prevValue != "" {
		panic("not implemented")
	}

	n, ok := c.nodes[key]
	if !ok || n.node == nil {
		c.Unlock()
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}
	if n.node.ModifiedIndex != prevIndex {
		c.Unlock()
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}

	c.index++
	n.node = nil
	c.Unlock()
	n.notify("compareAndDelete")
	return &etcd.Response{}, nil
}

func (c *fakeClient) CompareAndSwap(key string, value string, ttl uint64,
	prevValue string, prevIndex uint64) (*etcd.Response, error) {
	c.Lock()

	n, ok := c.nodes[key]
	if !ok || n.node == nil {
		c.Unlock()
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}
	if prevValue != "" && n.node.Value != prevValue {
		c.Unlock()
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}
	if prevIndex != 0 && n.node.ModifiedIndex != prevIndex {
		c.Unlock()
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}

	c.index++
	n.node.ModifiedIndex = c.index
	n.node.Value = value
	c.nodes[key] = n
	node := *n.node
	c.Unlock()
	n.notify("compareAndSwap")
	return &etcd.Response{Node: &node}, nil
}

func (c *fakeClient) Create(key string, value string, ttl uint64) (*etcd.Response, error) {
	c.Lock()

	n, ok := c.nodes[key]
	if ok && n.node != nil {
		c.Unlock()
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
	node := *n.node
	c.Unlock()
	n.notify("create")
	return &etcd.Response{Node: &node}, nil
}

func (c *fakeClient) Delete(key string, recursive bool) (*etcd.Response, error) {
	c.Lock()

	n, ok := c.nodes[key]
	if !ok || n.node == nil {
		c.Unlock()
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}

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
	c.Unlock()
	for _, n = range notifyList {
		n.notify("delete")
	}
	return &etcd.Response{}, nil
}

func (c *fakeClient) Get(key string, sortFiles, recursive bool) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()

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
	node := *n.node
	c.Unlock()

	n.notify("set")
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

	// We need a buffered forwarderfor 2 reasons:
	// - in the select loop below, we only write to receiver if
	// stop has not been closed. Otherwise we introduce race
	// conditions.
	// - we are waiting on forwarder and taking the node mutex.
	// fakeNode.notify write to forwarder, and also takes the node
	// mutex. Both can deadlock each-other. By buffering the
	// channel, we make sure 10 notify() call can finish and not
	// deadlock. We do a few of them in the serial locking code
	// in tests.
	forwarder := make(chan *etcd.Response, 10)

	// add the watch under the lock
	c.Lock()
	c.createParentDirs(prefix)
	n, ok := c.nodes[prefix]
	if !ok {
		n = newFakeNode(nil)
		c.nodes[prefix] = n
	}
	c.Unlock()

	n.mu.Lock()
	index := n.watchIndex
	n.watchIndex++
	n.watches[index] = forwarder
	n.mu.Unlock()

	// and wait until we stop, each action will write to forwarder, send
	// these along.
	for {
		select {
		case <-stop:
			n.mu.Lock()
			delete(n.watches, index)
			n.mu.Unlock()
			return &etcd.Response{}, nil
		case r := <-forwarder:
			receiver <- r
		}
	}
}
