// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the Zookeeper TopologyServer

import (
	"html/template"
	"path"
	"sort"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
	"github.com/youtube/vitess/go/zk"
)

func init() {
	// handles /zk paths
	ts := topo.GetServerByName("zookeeper")
	if ts == nil {
		log.Error("zookeeper explorer disabled: no zktopo.Server")
		return
	}

	HandleExplorer("/zk/", "zk.html", NewZkExplorer(ts.(*zktopo.Server).GetZConn()))

	// adds links for keyspaces and shards
	funcMap["keyspace"] = func(keyspace string) template.HTML {
		return template.HTML("<a href=\"/zk/global/vt/keyspaces/" + keyspace + "\">" + keyspace + "</a>")
	}
	funcMap["shard"] = func(keyspace, shard string) template.HTML {
		return template.HTML("<a href=\"/zk/global/vt/keyspaces/" + keyspace + "/shards/" + shard + "\">" + shard + "</a>")
	}

	// add toplevel link for zookeeper
	indexContent.ToplevelLinks["Zookeeper Explorer"] = "/zk"
}

type ZkExplorer struct {
	zconn zk.Conn
}

func NewZkExplorer(zconn zk.Conn) *ZkExplorer {
	return &ZkExplorer{zconn}
}

func (ex ZkExplorer) HandlePath(actionRepo *ActionRepository, zkPath string) interface{} {
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
	if m, _ := path.Match("/zk/global/vt/keyspaces/*", zkPath); m {
		keyspace := path.Base(zkPath)
		actionRepo.PopulateKeyspaceActions(result.Actions, keyspace)
	} else if m, _ := path.Match("/zk/global/vt/keyspaces/*/shards/*", zkPath); m {
		zkPathParts := strings.Split(zkPath, "/")
		keyspace := zkPathParts[5]
		shard := zkPathParts[7]
		actionRepo.PopulateShardActions(result.Actions, keyspace, shard)
	} else if m, _ := path.Match("/zk/*/vt/tablets/*", result.Path); m {
		zkPathParts := strings.Split(result.Path, "/")
		alias := zkPathParts[2] + "-" + zkPathParts[5]
		actionRepo.PopulateTabletActions(result.Actions, alias)
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

type ZkResult struct {
	Path     string
	Data     string
	Children []string
	Actions  map[string]template.URL
	Error    string
}

func NewZkResult(zkPath string) *ZkResult {
	return &ZkResult{Actions: make(map[string]template.URL), Path: zkPath}
}
