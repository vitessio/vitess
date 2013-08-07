// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// Imports and register the Zookeeper TopologyServer

import (
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
	http.HandleFunc("/zk/", func(w http.ResponseWriter, r *http.Request) {
		zkTopoServ := topo.GetServerByName("zookeeper")
		if zkTopoServ == nil {
			http.Error(w, "can only look at zk with zktopo.Server", http.StatusInternalServerError)
			return
		}
		zconn := zkTopoServ.(*zktopo.Server).GetZConn()

		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		zkPath := r.URL.Path[strings.Index(r.URL.Path, "/zk"):]

		if cleanPath := path.Clean(zkPath); zkPath != cleanPath && zkPath != cleanPath+"/" {
			log.Infof("redirecting to %v", cleanPath)
			http.Redirect(w, r, cleanPath, http.StatusTemporaryRedirect)
			return
		}

		if strings.HasSuffix(zkPath, "/") {
			zkPath = zkPath[:len(zkPath)-1]
		}

		result := NewZkResult(zkPath)

		if zkPath == "/zk" {
			cells, err := zk.ResolveWildcards(zconn, []string{"/zk/*"})
			if err != nil {
				httpError(w, "zk error: %v", err)
				return
			}
			for i, cell := range cells {
				cells[i] = cell[4:] // cut off "/zk/"
			}
			result.Children = cells
			sort.Strings(result.Children)
		} else {

			if data, _, err := zconn.Get(zkPath); err != nil {
				result.Error = err.Error()
			} else {

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
				if children, _, err := zconn.Children(zkPath); err != nil {
					result.Error = err.Error()
				} else {
					result.Children = children
					sort.Strings(result.Children)
				}
			}
		}
		templateLoader.ServeTemplate("zk.html", result, w, r)
	})

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
