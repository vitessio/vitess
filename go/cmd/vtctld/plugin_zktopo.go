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
	"github.com/youtube/vitess/go/cmd/vtctld/proto"
	"github.com/youtube/vitess/go/netutil"
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

// ZkExplorer implements Explorer
type ZkExplorer struct {
	zconn zk.Conn
}

// NewZkExplorer returns an Explorer implementation for Zookeeper
func NewZkExplorer(zconn zk.Conn) *ZkExplorer {
	return &ZkExplorer{zconn}
}

// GetKeyspacePath is part of the Explorer interface
func (ex ZkExplorer) GetKeyspacePath(keyspace string) string {
	return path.Join("/zk/global/vt/keyspaces", keyspace)
}

// GetShardPath is part of the Explorer interface
func (ex ZkExplorer) GetShardPath(keyspace, shard string) string {
	return path.Join("/zk/global/vt/keyspaces", keyspace, "shards", shard)
}

// GetSrvKeyspacePath is part of the Explorer interface
func (ex ZkExplorer) GetSrvKeyspacePath(cell, keyspace string) string {
	return path.Join("/zk", cell, "vt/ns", keyspace)
}

// GetSrvShardPath is part of the Explorer interface
func (ex ZkExplorer) GetSrvShardPath(cell, keyspace, shard string) string {
	return path.Join("/zk", cell, "/vt/ns", keyspace, shard)
}

// GetSrvTypePath is part of the Explorer interface
func (ex ZkExplorer) GetSrvTypePath(cell, keyspace, shard string, tabletType topo.TabletType) string {
	return path.Join("/zk", cell, "/vt/ns", keyspace, shard, string(tabletType))
}

// GetTabletPath is part of the Explorer interface
func (ex ZkExplorer) GetTabletPath(alias topo.TabletAlias) string {
	return path.Join("/zk", alias.Cell, "vt/tablets", alias.TabletUIDStr())
}

// GetReplicationSlaves is part of the Explorer interface
func (ex ZkExplorer) GetReplicationSlaves(cell, keyspace, shard string) string {
	return path.Join("/zk", cell, "vt/replication", keyspace, shard)
}

// HandlePath is part of the Explorer interface
func (ex ZkExplorer) HandlePath(actionRepo proto.ActionRepository, zkPath string, r *http.Request) interface{} {
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
		result.Links["status"] = template.URL(fmt.Sprintf("http://%v/debug/status", netutil.JoinHostPort(t.Hostname, port)))
	}
}

// ZkResult is the node for a zk path
type ZkResult struct {
	Path     string
	Data     string
	Links    map[string]template.URL
	Children []string
	Actions  map[string]template.URL
	Error    string
}

// NewZkResult creates a new ZkResult for the path with no links nor actions.
func NewZkResult(zkPath string) *ZkResult {
	return &ZkResult{
		Links:   make(map[string]template.URL),
		Actions: make(map[string]template.URL),
		Path:    zkPath,
	}
}
