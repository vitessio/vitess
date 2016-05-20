package vtctld

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"sync"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtctld/explorer"
)

var (
	// explorerMutex protects against concurrent registration attempts (e.g. OnRun).
	// Other than registration, the explorer should never change.
	explorerMutex    sync.Mutex
	explorerName     string
	explorerInstance explorer.Explorer
)

// HandleExplorer registers the Explorer under url, using the given template.
// It should be called by a plugin either from init() or from servenv.OnRun().
// Only one Explorer can be registered in a given instance of vtctld.
func HandleExplorer(name string, exp explorer.Explorer) {
	explorerMutex.Lock()
	defer explorerMutex.Unlock()

	if explorerInstance != nil {
		panic(fmt.Sprintf("Only one Explorer can be registered in vtctld. Trying to register %q, but %q was already registered.", name, explorerName))
	}

	// Topo explorer API for client-side vtctld app.
	handleCollection("topodata", func(r *http.Request) (interface{}, error) {
		return exp.HandlePath(path.Clean("/"+getItemPath(r.URL.Path)), r), nil
	})

	// save our instance
	explorerInstance = exp
	explorerName = name
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

// initExplorer initializes the redirects for explorer
func initExplorer(ts topo.Server) {
	// redirects for explorers
	http.HandleFunc("/explorers/redirect", func(w http.ResponseWriter, r *http.Request) {
		if explorerInstance == nil {
			http.Error(w, "no explorer configured", http.StatusInternalServerError)
			return
		}
		if err := r.ParseForm(); err != nil {
			httpErrorf(w, r, "cannot parse form: %s", err)
			return
		}

		target, err := handleExplorerRedirect(context.Background(), ts, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		http.Redirect(w, r, target, http.StatusFound)
	})
}
