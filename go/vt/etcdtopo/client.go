/*
Copyright 2017 Google Inc.

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

package etcdtopo

import (
	"github.com/coreos/go-etcd/etcd"
)

func newEtcdClient(machines []string) Client {
	c := etcd.NewClient(machines)
	// Vitess requires strong consistency mode for etcd.
	if err := c.SetConsistency(etcd.STRONG_CONSISTENCY); err != nil {
		panic("failed to set consistency on etcd client: " + err.Error())
	}
	return c
}

// Client contains the parts of etcd.Client that are needed.
type Client interface {
	CompareAndDelete(key string, prevValue string, prevIndex uint64) (*etcd.Response, error)
	CompareAndSwap(key string, value string, ttl uint64,
		prevValue string, prevIndex uint64) (*etcd.Response, error)
	Create(key string, value string, ttl uint64) (*etcd.Response, error)
	Delete(key string, recursive bool) (*etcd.Response, error)
	DeleteDir(key string) (*etcd.Response, error)
	Get(key string, sort, recursive bool) (*etcd.Response, error)
	Set(key string, value string, ttl uint64) (*etcd.Response, error)
	SetCluster(machines []string) bool
	Watch(prefix string, waitIndex uint64, recursive bool,
		receiver chan *etcd.Response, stop chan bool) (*etcd.Response, error)
}
