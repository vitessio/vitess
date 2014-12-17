// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"github.com/coreos/go-etcd/etcd"
)

func newEtcdClient(machines []string) Client {
	return etcd.NewClient(machines)
}

// Client contains the parts of etcd.Client that are needed.
type Client interface {
	CompareAndDelete(key string, prevValue string, prevIndex uint64) (*etcd.Response, error)
	CompareAndSwap(key string, value string, ttl uint64,
		prevValue string, prevIndex uint64) (*etcd.Response, error)
	Create(key string, value string, ttl uint64) (*etcd.Response, error)
	Delete(key string, recursive bool) (*etcd.Response, error)
	Get(key string, sort, recursive bool) (*etcd.Response, error)
	Set(key string, value string, ttl uint64) (*etcd.Response, error)
	SetCluster(machines []string) bool
	Watch(prefix string, waitIndex uint64, recursive bool,
		receiver chan *etcd.Response, stop chan bool) (*etcd.Response, error)
}
