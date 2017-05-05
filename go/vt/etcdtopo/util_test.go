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
