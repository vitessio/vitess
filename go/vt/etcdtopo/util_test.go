// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"reflect"
	"testing"

	"github.com/coreos/go-etcd/etcd"
)

func TestGetNodeNamesNil(t *testing.T) {
	input := &etcd.Response{}
	if _, got := getNodeNames(input); got != ErrBadResponse {
		t.Errorf("wrong error: got %#v, want %#v", got, ErrBadResponse)
	}
}

func TestGetNodeNames(t *testing.T) {
	input := &etcd.Response{
		Node: &etcd.Node{
			Nodes: etcd.Nodes{
				&etcd.Node{Key: "/dir/dir/node1"},
				&etcd.Node{Key: "/dir/dir/node2"},
				&etcd.Node{Key: "/dir/dir/node3"},
			},
		},
	}
	want := []string{"node1", "node2", "node3"}
	got, err := getNodeNames(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("getNodeNames() = %#v, want %#v", got, want)
	}
}
