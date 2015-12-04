// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the Zookeeper TopologyServer

import (
	"net/http"
	"sort"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
)

func init() {
	// Wait until flags are parsed, so we can check which topo server is in use.
	servenv.OnRun(func() {
		if zkServer, ok := topo.GetServer().Impl.(*zktopo.Server); ok {
			HandleExplorer("zk", "/zk/", "zk.html", NewZkExplorer(zkServer.GetZConn()))
		}
	})
}

// ZkExplorer implements Explorer
type ZkExplorer struct {
	zconn zk.Conn
}

// NewZkExplorer returns an Explorer implementation for Zookeeper
func NewZkExplorer(zconn zk.Conn) *ZkExplorer {
	return &ZkExplorer{zconn}
}

// HandlePath is part of the Explorer interface
func (ex ZkExplorer) HandlePath(zkPath string, r *http.Request) interface{} {
	result := NewZkResult(zkPath)

	if zkPath == "/zk" {
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

	data, _, err := ex.zconn.Get(zkPath)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Data = data
	children, _, err := ex.zconn.Children(zkPath)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.Children = children
	sort.Strings(result.Children)
	return result
}

// ZkResult is the node for a zk path
type ZkResult struct {
	Path     string
	Data     string
	Children []string
	Error    string
}

// NewZkResult creates a new ZkResult for the path.
func NewZkResult(zkPath string) *ZkResult {
	return &ZkResult{
		Path: zkPath,
	}
}
