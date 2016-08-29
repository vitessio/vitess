// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
