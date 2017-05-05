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

package zk

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	zookeeper "github.com/samuel/go-zookeeper/zk"
)

// test implementation of zk Conn
type TestZkConn struct {
	exists   []string
	children map[string][]string
}

func (conn *TestZkConn) Get(path string) (data []byte, stat *zookeeper.Stat, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) GetW(path string) (data []byte, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Children(path string) (children []string, stat *zookeeper.Stat, err error) {
	result, ok := conn.children[path]
	if !ok {
		return nil, nil, zookeeper.ErrNoNode
	}
	s := &zookeeper.Stat{}
	return result, s, nil
}

func (conn *TestZkConn) ChildrenW(path string) (children []string, stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Exists(path string) (stat *zookeeper.Stat, err error) {
	for _, e := range conn.exists {
		if path == e {
			s := &zookeeper.Stat{}
			return s, nil
		}
	}
	return nil, nil
}

func (conn *TestZkConn) ExistsW(path string) (stat *zookeeper.Stat, watch <-chan zookeeper.Event, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Create(path string, value []byte, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Set(path string, value []byte, version int32) (stat *zookeeper.Stat, err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Delete(path string, version int32) (err error) {
	panic("Should not be used")
}

func (conn *TestZkConn) Close() error {
	panic("Should not be used")
}

func (conn *TestZkConn) ACL(path string) ([]zookeeper.ACL, *zookeeper.Stat, error) {
	panic("Should not be used")
}

func (conn *TestZkConn) SetACL(path string, aclv []zookeeper.ACL, version int32) error {
	panic("Should not be used")
}

func checkResult(t *testing.T, expectedResult []string, expectedError string, actualResult []string, actualError error) {
	// check the error
	if expectedError == "" {
		if actualError != nil {
			t.Errorf("Got unexpected error: %v", actualError)
			return
		}
	} else {
		if actualError == nil {
			t.Errorf("Expected error %v but got nothing", expectedError)
			return
		}
		if actualError.Error() != expectedError {
			t.Errorf("Got error '%v' but was expecting error '%v'", actualError.Error(), expectedError)
			return
		}
	}

	if len(expectedResult) != len(actualResult) {
		t.Errorf("Got wrong number of results: was expecting %v but got %v", expectedResult, actualResult)
		return
	}
	if len(expectedResult) == 0 {
		return
	}
	for i, expected := range expectedResult {
		if expected != actualResult[i] {
			t.Errorf("Got wrong result[%v]: was expecting %v but got %v", i, expectedResult, actualResult)
			return
		}
	}
}

func TestResolveWildcards(t *testing.T) {
	zconn := &TestZkConn{}

	// path that doesn't exist, no wildcard
	result, err := ResolveWildcards(zconn, []string{"/zk/nyc/path"})
	checkResult(t, []string{"/zk/nyc/path"}, "", result, err)

	// path that doesn't exist, with wildcard
	result, err = ResolveWildcards(zconn, []string{"/zk/nyc/path*"})
	checkResult(t, nil, "", result, err)

	// single path that exists
	zconn.exists = []string{
		"/zk/nyc/path",
	}
	result, err = ResolveWildcards(zconn, []string{"/zk/nyc/path"})
	checkResult(t, []string{"/zk/nyc/path"}, "", result, err)

	// terminal wildcard
	zconn.exists = []string{
		"/zk/nyc/path1",
		"/zk/nyc/path2",
	}
	zconn.children = map[string][]string{
		"/zk/nyc": {"path1", "path2"},
	}
	result, err = ResolveWildcards(zconn, []string{"/zk/nyc/*"})
	checkResult(t, []string{
		"/zk/nyc/path1",
		"/zk/nyc/path2",
	}, "", result, err)

	// in-the-middle wildcard
	zconn.exists = []string{
		"/zk/nyc/path1/actionlog",
		"/zk/nyc/path2/actionlog",
	}
	zconn.children = map[string][]string{
		"/zk/nyc": {"path1", "path2"},
	}
	result, err = ResolveWildcards(zconn, []string{"/zk/nyc/*/actionlog"})
	checkResult(t, []string{
		"/zk/nyc/path1/actionlog",
		"/zk/nyc/path2/actionlog",
	}, "", result, err)

	// double wildcard, with one leaf node missing the actionlog file
	zconn.exists = []string{
		"/zk/nyc/path1/shards/subpath1.1/actionlog",
		"/zk/nyc/path1/shards/subpath1.2/actionlog",
		"/zk/nyc/path2/shards/subpath2.1/actionlog",
	}
	zconn.children = map[string][]string{
		"/zk/nyc":              {"path1", "path2"},
		"/zk/nyc/path1/shards": {"subpath1.1", "subpath1.2"},
		"/zk/nyc/path2/shards": {"subpath2.1", "subpath2.2"},
	}
	result, err = ResolveWildcards(zconn, []string{
		"/zk/nyc/*/shards/*/actionlog"})
	checkResult(t, []string{
		"/zk/nyc/path1/shards/subpath1.1/actionlog",
		"/zk/nyc/path1/shards/subpath1.2/actionlog",
		"/zk/nyc/path2/shards/subpath2.1/actionlog",
	}, "", result, err)

	// a parent path doesn't exist
	zconn.exists = nil
	zconn.children = nil
	result, err = ResolveWildcards(zconn, []string{
		"/zk/nyc/shards/*/actionlog"})
	checkResult(t, nil, "", result, err)

	// multiple toplevel paths given, some don't exist
	zconn.exists = []string{
		"/zk/nyc/path1",
		"/zk/nyc/path2",
	}
	zconn.children = nil
	result, err = ResolveWildcards(zconn, []string{
		"/zk/nyc/path1",
		"/zk/nyc/path2",
		"/zk/nyc/path3",
	})
	checkResult(t, []string{
		"/zk/nyc/path1",
		"/zk/nyc/path2",
		"/zk/nyc/path3",
	}, "", result, err)
}

func TestResolveWildcardsCell(t *testing.T) {
	// create the ZK config
	configPath := fmt.Sprintf("%v/.zk-test-conf-%v", os.TempDir(), time.Now().UnixNano())
	defer func() {
		os.Remove(configPath)
	}()
	if err := os.Setenv("ZK_CLIENT_CONFIG", configPath); err != nil {
		t.Errorf("setenv failed: %v", err)
	}
	configMap := map[string]string{"ny": "127.0.0.1", "nj": "127.0.0.2"}
	file, err := os.Create(configPath)
	if err != nil {
		t.Errorf("create failed: %v", err)
	}
	err = json.NewEncoder(file).Encode(configMap)
	if err != nil {
		t.Errorf("encode failed: %v", err)
	}
	file.Close()

	zconn := &TestZkConn{}

	// test with just cell wildcard
	zconn.exists = []string{
		"/zk/nj/shards",
		"/zk/ny/shards",
	}
	result, err := ResolveWildcards(zconn, []string{
		"/zk/*/shards"})
	checkResult(t, []string{
		"/zk/nj/shards",
		"/zk/ny/shards",
	}, "", result, err)

	// test with cell wildcard and path wildcard
	zconn.exists = []string{
		"/zk/nj/shards/subpath.nj.1/actionlog",
		"/zk/nj/shards/subpath.nj.2/actionlog",
		"/zk/ny/shards/subpath.ny.1/actionlog",
		"/zk/ny/shards/subpath.ny.2/actionlog",
	}
	zconn.children = map[string][]string{
		"/zk/nj/shards": {"subpath.nj.2", "subpath.nj.1"},
		"/zk/ny/shards": {"subpath.ny.1", "subpath.ny.2"},
	}
	result, err = ResolveWildcards(zconn, []string{
		"/zk/*/shards/*/actionlog"})
	checkResult(t, []string{
		"/zk/nj/shards/subpath.nj.1/actionlog",
		"/zk/nj/shards/subpath.nj.2/actionlog",
		"/zk/ny/shards/subpath.ny.1/actionlog",
		"/zk/ny/shards/subpath.ny.2/actionlog",
	}, "", result, err)

}
