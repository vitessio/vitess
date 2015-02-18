package main

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/cmd/vtctld/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// Explorer allows exploring a topology server.
type Explorer interface {
	// HandlePath returns a result (suitable to be passed to a
	// template) appropriate for url, using actionRepo to populate
	// the actions in result.
	HandlePath(actionRepo proto.ActionRepository, url string, r *http.Request) interface{}

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

var (
	// explorerMutex protects against concurrent registration attempts (e.g. OnRun).
	// Other than registration, the explorer should never change.
	explorerMutex sync.Mutex
	explorerName  string
	explorer      Explorer
)

// HandleExplorer registers the Explorer under url, using the given template.
// It should be called by a plugin either from init() or from servenv.OnRun().
// Only one Explorer can be registered in a given instance of vtctld.
func HandleExplorer(name, url, templateName string, exp Explorer) {
	explorerMutex.Lock()
	defer explorerMutex.Unlock()

	if explorer != nil {
		panic(fmt.Sprintf("Only one Explorer can be registered in vtctld. Trying to register %q, but %q was already registered.", name, explorerName))
	}

	explorer = exp
	explorerName = name
	indexContent.ToplevelLinks[name+" Explorer"] = url
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
		result := explorer.HandlePath(actionRepo, topoPath, r)
		templateLoader.ServeTemplate(templateName, result, w, r)
	})
}

// handleExplorerRedirect returns the redirect target URL.
func handleExplorerRedirect(r *http.Request) (string, error) {
	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")
	cell := r.FormValue("cell")

	switch r.FormValue("type") {
	case "keyspace":
		if keyspace == "" {
			return "", errors.New("keyspace is required for this redirect")
		}
		return explorer.GetKeyspacePath(keyspace), nil
	case "shard":
		if keyspace == "" || shard == "" {
			return "", errors.New("keyspace and shard are required for this redirect")
		}
		return explorer.GetShardPath(keyspace, shard), nil
	case "srv_keyspace":
		if keyspace == "" || cell == "" {
			return "", errors.New("keyspace and cell are required for this redirect")
		}
		return explorer.GetSrvKeyspacePath(cell, keyspace), nil
	case "srv_shard":
		if keyspace == "" || shard == "" || cell == "" {
			return "", errors.New("keyspace, shard, and cell are required for this redirect")
		}
		return explorer.GetSrvShardPath(cell, keyspace, shard), nil
	case "srv_type":
		tabletType := r.FormValue("tablet_type")
		if keyspace == "" || shard == "" || cell == "" || tabletType == "" {
			return "", errors.New("keyspace, shard, cell, and tablet_type are required for this redirect")
		}
		return explorer.GetSrvTypePath(cell, keyspace, shard, topo.TabletType(tabletType)), nil
	case "tablet":
		alias := r.FormValue("alias")
		if alias == "" {
			return "", errors.New("alias is required for this redirect")
		}
		tabletAlias, err := topo.ParseTabletAliasString(alias)
		if err != nil {
			return "", fmt.Errorf("bad tablet alias %q: %v", alias, err)
		}
		return explorer.GetTabletPath(tabletAlias), nil
	case "replication":
		if keyspace == "" || shard == "" || cell == "" {
			return "", errors.New("keyspace, shard, and cell are required for this redirect")
		}
		return explorer.GetReplicationSlaves(cell, keyspace, shard), nil
	default:
		return "", errors.New("bad redirect type")
	}
}
