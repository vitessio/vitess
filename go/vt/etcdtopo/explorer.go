// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"path"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	ctlproto "github.com/youtube/vitess/go/cmd/vtctld/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

const (
	explorerRoot = "/etcd"
	globalCell   = "global"
)

// Explorer is an implementation of vtctld's Explorer interface for etcd.
type Explorer struct {
	ts *Server
}

// NewExplorer implements vtctld Explorer.
func NewExplorer(ts *Server) *Explorer {
	return &Explorer{ts: ts}
}

// GetKeyspacePath implements vtctld Explorer.
func (ex Explorer) GetKeyspacePath(keyspace string) string {
	return path.Join(explorerRoot, globalCell, keyspaceDirPath(keyspace))
}

// GetShardPath implements vtctld Explorer.
func (ex Explorer) GetShardPath(keyspace, shard string) string {
	return path.Join(explorerRoot, globalCell, shardDirPath(keyspace, shard))
}

// GetSrvKeyspacePath implements vtctld Explorer.
func (ex Explorer) GetSrvKeyspacePath(cell, keyspace string) string {
	return path.Join(explorerRoot, cell, srvKeyspaceDirPath(keyspace))
}

// GetSrvShardPath implements vtctld Explorer.
func (ex Explorer) GetSrvShardPath(cell, keyspace, shard string) string {
	return path.Join(explorerRoot, cell, srvShardDirPath(keyspace, shard))
}

// GetSrvTypePath implements vtctld Explorer.
func (ex Explorer) GetSrvTypePath(cell, keyspace, shard string, tabletType topo.TabletType) string {
	return path.Join(explorerRoot, cell, endPointsDirPath(keyspace, shard, string(tabletType)))
}

// GetTabletPath implements vtctld Explorer.
func (ex Explorer) GetTabletPath(alias topo.TabletAlias) string {
	return path.Join(explorerRoot, alias.Cell, tabletDirPath(alias.String()))
}

// GetReplicationSlaves implements vtctld Explorer.
func (ex Explorer) GetReplicationSlaves(cell, keyspace, shard string) string {
	return path.Join(explorerRoot, cell, shardReplicationDirPath(keyspace, shard))
}

// HandlePath implements vtctld Explorer.
func (ex Explorer) HandlePath(actionRepo ctlproto.ActionRepository, rPath string, r *http.Request) interface{} {
	result := newExplorerResult(rPath)

	// Cut off explorerRoot prefix.
	if !strings.HasPrefix(rPath, explorerRoot) {
		result.Error = "invalid etcd explorer path: " + rPath
		return result
	}
	rPath = rPath[len(explorerRoot):]

	// Root is a list of cells.
	if rPath == "" {
		cells, err := ex.ts.getCellList()
		if err != nil {
			result.Error = err.Error()
			return result
		}
		result.Children = append([]string{globalCell}, cells...)
		return result
	}

	// Get a client for the requested cell.
	var client Client
	cell, rPath, err := splitCellPath(rPath)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if cell == globalCell {
		client = ex.ts.getGlobal()
	} else {
		client, err = ex.ts.getCell(cell)
		if err != nil {
			result.Error = "Can't get cell: " + err.Error()
			return result
		}
	}

	// Get the requested node data.
	resp, err := client.Get(rPath, true /* sort */, false /* recursive */)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if resp.Node == nil {
		result.Error = ErrBadResponse.Error()
		return result
	}
	result.Data = getNodeData(client, resp.Node)

	// Populate children.
	for _, node := range resp.Node.Nodes {
		result.Children = append(result.Children, path.Base(node.Key))
	}

	// Populate actions.
	if m, _ := path.Match(keyspaceDirPath("*"), rPath); m {
		actionRepo.PopulateKeyspaceActions(result.Actions, path.Base(rPath))
	} else if m, _ := path.Match(shardDirPath("*", "*"), rPath); m {
		if keyspace, shard, err := splitShardDirPath(rPath); err == nil {
			actionRepo.PopulateShardActions(result.Actions, keyspace, shard)
		}
	} else if m, _ := path.Match(tabletDirPath("*"), rPath); m {
		actionRepo.PopulateTabletActions(result.Actions, path.Base(rPath), r)
		addTabletLinks(result, result.Data)
	}
	return result
}

type explorerResult struct {
	Path     string
	Data     string
	Links    map[string]template.URL
	Children []string
	Actions  map[string]template.URL
	Error    string
}

func newExplorerResult(p string) *explorerResult {
	return &explorerResult{
		Links:   make(map[string]template.URL),
		Actions: make(map[string]template.URL),
		Path:    p,
	}
}

func getNodeData(client Client, node *etcd.Node) string {
	if !node.Dir {
		return node.Value
	}
	// Directories don't have data, but some directories have a special data file.
	resp, err := client.Get(path.Join(node.Key, dataFilename), false /* sort */, false /* recursive */)
	if err != nil || resp.Node == nil {
		return ""
	}
	return resp.Node.Value
}

// splitCellPath returns the cell name, and the rest of the path.
// For example: "/cell/rest/of/path" -> "cell", "/rest/of/path"
func splitCellPath(p string) (cell, rest string, err error) {
	parts := strings.SplitN(p, "/", 3)
	if len(parts) < 2 || parts[0] != "" {
		return "", "", fmt.Errorf("invalid etcd explorer path: %v", p)
	}
	if len(parts) < 3 {
		return parts[1], "/", nil
	}
	return parts[1], "/" + parts[2], nil
}

// splitShardDirPath takes a path that matches the path.Match() pattern
// shardDirPath("*", "*") and returns the keyspace and shard.
//
// We assume the path is of the form "/vt/keyspaces/*/*".
// If that ever changes, the unit test for this function will detect it.
func splitShardDirPath(p string) (keyspace, shard string, err error) {
	parts := strings.Split(p, "/")
	if len(parts) != 5 {
		return "", "", fmt.Errorf("invalid shard dir path: %v", p)
	}
	return parts[3], parts[4], nil
}

func addTabletLinks(result *explorerResult, data string) {
	t := &topo.Tablet{}
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return
	}

	if port, ok := t.Portmap["vt"]; ok {
		result.Links["status"] = template.URL(fmt.Sprintf("http://%v:%v/debug/status", t.Hostname, port))
	}

	if !t.Parent.IsZero() {
		result.Links["parent"] = template.URL(
			path.Join(explorerRoot, t.Parent.Cell, tabletDirPath(t.Parent.String())))
	}
}
