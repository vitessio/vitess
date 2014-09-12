// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the Zookeeper TopologyServer

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
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

	HandleExplorer("zk", "/zk/", "zk.html", NewZkExplorer(ts.(*zktopo.Server).GetZConn()))
}

type ZkExplorer struct {
	zconn zk.Conn
}

func NewZkExplorer(zconn zk.Conn) *ZkExplorer {
	return &ZkExplorer{zconn}
}

func (ex ZkExplorer) GetKeyspacePath(keyspace string) string {
	return path.Join("/zk/global/vt/keyspaces", keyspace)
}

func (ex ZkExplorer) GetShardPath(keyspace, shard string) string {
	return path.Join("/zk/global/vt/keyspaces", keyspace, "shards", shard)
}

func (ex ZkExplorer) GetSrvKeyspacePath(cell, keyspace string) string {
	return path.Join("/zk", cell, "vt/ns", keyspace)
}

func (ex ZkExplorer) GetSrvShardPath(cell, keyspace, shard string) string {
	return path.Join("/zk", cell, "/vt/ns", keyspace, shard)
}

func (ex ZkExplorer) GetSrvTypePath(cell, keyspace, shard string, tabletType topo.TabletType) string {
	return path.Join("/zk", cell, "/vt/ns", keyspace, shard, string(tabletType))
}

func (ex ZkExplorer) GetTabletPath(alias topo.TabletAlias) string {
	return path.Join("/zk", alias.Cell, "vt/tablets", alias.TabletUidStr())
}

func (ex ZkExplorer) GetReplicationSlaves(cell, keyspace, shard string) string {
	return path.Join("/zk", cell, "vt/replication", keyspace, shard)
}

func (ex ZkExplorer) HandlePath(actionRepo *ActionRepository, zkPath string, r *http.Request) interface{} {
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
		actionRepo.PopulateTabletActions(result.Actions, alias, r)
		ex.addTabletLinks(data, result)
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

func (ex ZkExplorer) addTabletLinks(data string, result *ZkResult) {
	t := &topo.Tablet{}
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return
	}

	if port, ok := t.Portmap["vt"]; ok {
		result.Links["status"] = template.URL(fmt.Sprintf("http://%v:%v/debug/status", t.Hostname, port))
	}

	if !t.Parent.IsZero() {
		result.Links["parent"] = template.URL(fmt.Sprintf("/zk/%v/vt/tablets/%v", t.Parent.Cell, t.Parent.TabletUidStr()))
	}
}

type ZkResult struct {
	Path     string
	Data     string
	Links    map[string]template.URL
	Children []string
	Actions  map[string]template.URL
	Error    string
}

func NewZkResult(zkPath string) *ZkResult {
	return &ZkResult{
		Links:   make(map[string]template.URL),
		Actions: make(map[string]template.URL),
		Path:    zkPath,
	}
}
