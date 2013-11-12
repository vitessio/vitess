package main

import (
	"net/http"
	"path"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

var explorers = make(map[string]Explorer)

// Explorer allows exploring a topology server.
type Explorer interface {
	// HandlePath returns a result (suitable to be passed to a
	// template) appropriate for url, using actionRepo to populate
	// the actions in result.
	HandlePath(actionRepo *ActionRepository, url string) interface{}

	// GetKeyspacePath returns an explorer path that will contain
	// information about the named keyspace.
	GetKeyspacePath(keyspace string) string

	// GetShardPath returns an explorer path that will contain
	// information about the named shard in the named keyspace.
	GetShardPath(keyspace, shard string) string

	// GetSrvKeyspacePath returns an explorer path that will
	// contain information about the named keyspace in the serving
	// graph for cell.
	GetSrvKeyspacePath(cell, keyspace string) string

	// GetShardPath returns an explorer path that will contain
	// information about the named shard in the named keyspace in
	// the serving graph for cell.
	GetSrvShardPath(cell, keyspace, shard string) string

	// GetShardTypePath returns an explorer path that will contain
	// information about the named tablet type in the named shard
	// in the named keyspace in the serving graph for cell.
	GetSrvTypePath(cell, keyspace, shard string, tabletType topo.TabletType) string

	// GetTabletPath returns an explorer path that will contain
	// information about the tablet named by alias.
	GetTabletPath(alias topo.TabletAlias) string

	// GetReplicationSlaves returns an explorer path that contains
	// replication slaves for the named cell, keyspace, and shard.
	GetReplicationSlaves(cell, keyspace, shard string) string
}

// HandleExplorer serves explorer under url, using a template named
// templateName.
func HandleExplorer(name, url, templateName string, explorer Explorer) {
	explorers[name] = explorer
	indexContent.ToplevelLinks[name+" explorer"] = url
	http.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			httpError(w, "cannot parse form: %s", err)
			return
		}
		topoPath := r.URL.Path[strings.Index(r.URL.Path, url):]
		if cleanPath := path.Clean(topoPath); topoPath != cleanPath && topoPath != cleanPath+"/" {
			log.Infof("redirecting to %v", cleanPath)
			http.Redirect(w, r, cleanPath, http.StatusTemporaryRedirect)
			return
		}

		if strings.HasSuffix(topoPath, "/") {
			topoPath = topoPath[:len(topoPath)-1]
		}
		result := explorer.HandlePath(actionRepo, topoPath)
		templateLoader.ServeTemplate(templateName, result, w, r)
	})
}
