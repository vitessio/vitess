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

package zktopo

import (
	"net/http"
	"sort"

	"github.com/youtube/vitess/go/vt/vtctld/explorer"
	"github.com/youtube/vitess/go/zk"
)

// ZkExplorer implements explorer.Explorer
type ZkExplorer struct {
	zconn zk.Conn
}

// NewZkExplorer returns an Explorer implementation for Zookeeper
func NewZkExplorer(zconn zk.Conn) *ZkExplorer {
	return &ZkExplorer{zconn}
}

// HandlePath is part of the Explorer interface
func (ex ZkExplorer) HandlePath(zkPath string, r *http.Request) *explorer.Result {
	result := &explorer.Result{}

	if zkPath == "/" {
		cells, err := zk.ResolveWildcards(ex.zconn, []string{"/zk/*"})
		if err != nil {
			result.Error = err.Error()
			return result
		}
		for i, cell := range cells {
			cells[i] = cell[4:] // cut off "/zk/"
		}
		result.Children = cells
		sort.Strings(result.Children)
		return result
	}

	zkPath = "/zk" + zkPath
	data, _, err := ex.zconn.Get(zkPath)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Data = string(data)
	children, _, err := ex.zconn.Children(zkPath)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Children = children
	sort.Strings(result.Children)
	return result
}
