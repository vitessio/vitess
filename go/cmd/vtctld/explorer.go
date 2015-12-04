package main

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
)

// Explorer allows exploring a topology server.
type Explorer interface {
	// HandlePath returns a result (suitable to be passed to a
	// template) appropriate for url, using actionRepo to populate
	// the actions in result.
	HandlePath(url string, r *http.Request) interface{}
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

	// Topo explorer API for client-side vtctld app.
	handleCollection("topodata", func(r *http.Request) (interface{}, error) {
		return exp.HandlePath(path.Clean(url+getItemPath(r.URL.Path)), r), nil
	})

	// Old server-side explorer.
	explorer = exp
	explorerName = name
	http.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		// Get the part after the prefix url.
		topoPath := r.URL.Path[strings.Index(r.URL.Path, url)+len(url):]
		// Redirect to the new client-side topo browser.
		http.Redirect(w, r, appPrefix+"#"+path.Join("/topo", topoPath), http.StatusFound)
	})
}

// handleExplorerRedirect returns the redirect target URL.
func handleExplorerRedirect(ctx context.Context, ts topo.Server, r *http.Request) (string, error) {
	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")
	cell := r.FormValue("cell")

	switch r.FormValue("type") {
	case "keyspace":
		if keyspace == "" {
			return "", errors.New("keyspace is required for this redirect")
		}
		return appPrefix + "#/keyspaces/", nil
	case "shard":
		if keyspace == "" || shard == "" {
			return "", errors.New("keyspace and shard are required for this redirect")
		}
		return appPrefix + fmt.Sprintf("#/shard/%s/%s", keyspace, shard), nil
	case "srv_keyspace":
		if keyspace == "" || cell == "" {
			return "", errors.New("keyspace and cell are required for this redirect")
		}
		return appPrefix + "#/keyspaces/", nil
	case "srv_shard":
		if keyspace == "" || shard == "" || cell == "" {
			return "", errors.New("keyspace, shard, and cell are required for this redirect")
		}
		return appPrefix + fmt.Sprintf("#/shard/%s/%s", keyspace, shard), nil
	case "srv_type":
		tabletType := r.FormValue("tablet_type")
		if keyspace == "" || shard == "" || cell == "" || tabletType == "" {
			return "", errors.New("keyspace, shard, cell, and tablet_type are required for this redirect")
		}
		return appPrefix + fmt.Sprintf("#/shard/%s/%s", keyspace, shard), nil
	case "tablet":
		alias := r.FormValue("alias")
		if alias == "" {
			return "", errors.New("alias is required for this redirect")
		}
		tabletAlias, err := topoproto.ParseTabletAlias(alias)
		if err != nil {
			return "", fmt.Errorf("bad tablet alias %q: %v", alias, err)
		}
		ti, err := ts.GetTablet(ctx, tabletAlias)
		if err != nil {
			return "", fmt.Errorf("can't get tablet %q: %v", alias, err)
		}
		return appPrefix + fmt.Sprintf("#/shard/%s/%s", ti.Keyspace, ti.Shard), nil
	case "replication":
		if keyspace == "" || shard == "" || cell == "" {
			return "", errors.New("keyspace, shard, and cell are required for this redirect")
		}
		return appPrefix + fmt.Sprintf("#/shard/%s/%s", keyspace, shard), nil
	default:
		return "", errors.New("bad redirect type")
	}
}
