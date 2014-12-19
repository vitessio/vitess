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

type fakeClient struct {
	cell  string
	nodes map[string]etcd.Node
	index uint64

	sync.Mutex
}

func newTestClient(machines []string) Client {
	// In tests, the first machine address is just the cell name.
	return &fakeClient{
		cell:  machines[0],
		nodes: map[string]etcd.Node{"/": etcd.Node{Key: "/", Dir: true}},
	}
}

func (c *fakeClient) createParentDirs(key string) {
	dir := path.Dir(key)
	for dir != "" {
		if _, ok := c.nodes[dir]; ok {
			return
		}
		c.nodes[dir] = etcd.Node{Key: dir, Dir: true, CreatedIndex: c.index, ModifiedIndex: c.index}
		dir = path.Dir(dir)
	}
}

func (c *fakeClient) CompareAndDelete(key string, prevValue string, prevIndex uint64) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()

	if prevValue != "" {
		panic("not implemented")
	}

	n, ok := c.nodes[key]
	if !ok {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}
	if n.ModifiedIndex != prevIndex {
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}

	c.index++
	delete(c.nodes, key)
	return &etcd.Response{}, nil
}

func (c *fakeClient) CompareAndSwap(key string, value string, ttl uint64,
	prevValue string, prevIndex uint64) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()

	n, ok := c.nodes[key]
	if !ok {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}
	if prevValue != "" && n.Value != prevValue {
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}
	if prevIndex != 0 && n.ModifiedIndex != prevIndex {
		return nil, &etcd.EtcdError{ErrorCode: EcodeTestFailed}
	}

	c.index++
	n.ModifiedIndex = c.index
	n.Value = value
	c.nodes[key] = n
	return &etcd.Response{Node: &n}, nil
}

func (c *fakeClient) Create(key string, value string, ttl uint64) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.nodes[key]; ok {
		return nil, &etcd.EtcdError{ErrorCode: EcodeNodeExist}
	}

	c.index++
	c.createParentDirs(key)
	n := etcd.Node{
		Key:           key,
		Value:         value,
		CreatedIndex:  c.index,
		ModifiedIndex: c.index,
	}
	c.nodes[key] = n
	return &etcd.Response{Node: &n}, nil
}

func (c *fakeClient) Delete(key string, recursive bool) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.nodes[key]; !ok {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}

	delete(c.nodes, key)

	if recursive {
		for k, _ := range c.nodes {
			if strings.HasPrefix(k, key+"/") {
				delete(c.nodes, k)
			}
		}
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
	if !ok {
		return nil, &etcd.EtcdError{ErrorCode: EcodeKeyNotFound}
	}
	resp := &etcd.Response{Node: &n}
	if !n.Dir {
		return resp, nil
	}

	// List the directory.
	targetDir := key + "/"
	for k, n := range c.nodes {
		dir, file := path.Split(k)
		if dir == targetDir && !strings.HasPrefix(file, "_") {
			node := n
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

	c.index++

	c.createParentDirs(key)
	n, ok := c.nodes[key]
	if ok {
		n.Value = value
		n.ModifiedIndex = c.index
		c.nodes[key] = n
	} else {
		n = etcd.Node{Key: key, Value: value, CreatedIndex: c.index, ModifiedIndex: c.index}
		c.nodes[key] = n
	}
	return &etcd.Response{Node: &n}, nil
}

func (c *fakeClient) SetCluster(machines []string) bool {
	c.Lock()
	defer c.Unlock()

	c.cell = machines[0]
	return true
}

func (c *fakeClient) Watch(prefix string, waitIndex uint64, recursive bool,
	receiver chan *etcd.Response, stop chan bool) (*etcd.Response, error) {
	c.Lock()
	defer c.Unlock()

	return &etcd.Response{}, nil
}
