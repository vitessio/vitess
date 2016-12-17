// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/coreos/go-etcd/etcd"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctld/explorer"
)

// Explorer is an implementation of vtctld's Explorer interface for etcd.
type Explorer struct {
	ts *Server
}

// NewExplorer implements vtctld Explorer.
func NewExplorer(ts *Server) *Explorer {
	return &Explorer{ts: ts}
}

// HandlePath implements vtctld Explorer.
func (ex Explorer) HandlePath(rPath string, r *http.Request) *explorer.Result {
	result := &explorer.Result{}

	// Root is a list of cells.
	if rPath == "/" {
		cells, err := ex.ts.getCellList()
		if err != nil {
			result.Error = err.Error()
			return result
		}
		result.Children = append([]string{topo.GlobalCell}, cells...)
		return result
	}

	// Get a client for the requested cell.
	var client Client
	cell, rPath, err := splitCellPath(rPath)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if cell == topo.GlobalCell {
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

	return result
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
