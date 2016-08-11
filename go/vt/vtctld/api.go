package vtctld

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	"github.com/youtube/vitess/go/vt/schemamanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	localCell = flag.String("cell", "", "cell to use")
)

// This file implements a REST-style API for the vtctld web interface.

const (
	apiPrefix = "/api/"

	jsonContentType = "application/json; charset=utf-8"
)

func httpErrorf(w http.ResponseWriter, r *http.Request, format string, args ...interface{}) {
	errMsg := fmt.Sprintf(format, args...)
	log.Errorf("HTTP error on %v: %v, request: %#v", r.URL.Path, errMsg, r)
	http.Error(w, errMsg, http.StatusInternalServerError)
}

func handleCollection(collection string, getFunc func(*http.Request) (interface{}, error)) {
	http.HandleFunc(apiPrefix+collection+"/", func(w http.ResponseWriter, r *http.Request) {
		// Get the requested object.
		obj, err := getFunc(r)
		if err != nil {
			if err == topo.ErrNoNode {
				http.NotFound(w, r)
				return
			}
			httpErrorf(w, r, "can't get %v: %v", collection, err)
			return
		}

		// JSON marshals a nil slice as "null", but we prefer "[]".
		if val := reflect.ValueOf(obj); val.Kind() == reflect.Slice && val.IsNil() {
			w.Header().Set("Content-Type", jsonContentType)
			w.Write([]byte("[]"))
			return
		}

		// JSON encode response.
		data, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			httpErrorf(w, r, "json error: %v", err)
			return
		}
		w.Header().Set("Content-Type", jsonContentType)
		w.Write(data)
	})
}

func getItemPath(url string) string {
	// Strip API prefix.
	if !strings.HasPrefix(url, apiPrefix) {
		return ""
	}
	url = url[len(apiPrefix):]

	// Strip collection name.
	parts := strings.SplitN(url, "/", 2)
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

func unmarshalRequest(r *http.Request, v interface{}) error {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func addSrvkeyspace(ctx context.Context, ts topo.Server, cell, keyspace string, srvKeyspaces map[string]interface{}) error {
	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
	if err != nil {
		return fmt.Errorf("invalid keyspace name: %q ", keyspace)
	}
	srvKeyspaces[keyspace] = srvKeyspace
	return nil
}

func initAPI(ctx context.Context, ts topo.Server, actions *ActionRepository, realtimeStats *realtimeStats) {
	tabletHealthCache := newTabletHealthCache(ts)
	tmClient := tmclient.NewTabletManagerClient()

	// Cells
	handleCollection("cells", func(r *http.Request) (interface{}, error) {
		if getItemPath(r.URL.Path) != "" {
			return nil, errors.New("cells can only be listed, not retrieved")
		}
		return ts.GetKnownCells(ctx)
	})

	// Keyspaces
	handleCollection("keyspaces", func(r *http.Request) (interface{}, error) {
		keyspace := getItemPath(r.URL.Path)
		switch r.Method {
		case "GET":
			// List all keyspaces.
			if keyspace == "" {
				return ts.GetKeyspaces(ctx)
			}
			// Get the keyspace record.
			return ts.GetKeyspace(ctx, keyspace)
			// Perform an action on a keyspace.
		case "POST":
			if keyspace == "" {
				return nil, errors.New("A POST request needs a keyspace in the URL")
			}
			if err := r.ParseForm(); err != nil {
				return nil, err
			}

			action := r.FormValue("action")
			if action == "" {
				return nil, errors.New("A POST request must specify action")
			}
			return actions.ApplyKeyspaceAction(ctx, action, keyspace, r), nil
		default:
			return nil, fmt.Errorf("unsupported HTTP method: %v", r.Method)
		}
	})

	// Shards
	handleCollection("shards", func(r *http.Request) (interface{}, error) {
		shardPath := getItemPath(r.URL.Path)
		if !strings.Contains(shardPath, "/") {
			return nil, fmt.Errorf("invalid shard path: %q", shardPath)
		}
		parts := strings.SplitN(shardPath, "/", 2)
		keyspace := parts[0]
		shard := parts[1]

		// List the shards in a keyspace.
		if shard == "" {
			return ts.GetShardNames(ctx, keyspace)
		}

		// Perform an action on a shard.
		if r.Method == "POST" {
			if err := r.ParseForm(); err != nil {
				return nil, err
			}
			action := r.FormValue("action")
			if action == "" {
				return nil, errors.New("must specify action")
			}
			return actions.ApplyShardAction(ctx, action, keyspace, shard, r), nil
		}

		// Get the shard record.
		return ts.GetShard(ctx, keyspace, shard)
	})

	// SrvKeyspace
	handleCollection("srv_keyspace", func(r *http.Request) (interface{}, error) {
		keyspacePath := getItemPath(r.URL.Path)
		parts := strings.SplitN(keyspacePath, "/", 2)

		// Request was incorrectly formatted.
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid srvkeyspace path: %q  expected path: /srv_keyspace/<cell>/<keyspace>", keyspacePath)
		}

		cell := parts[0]
		keyspace := parts[1]

		if cell == "local" {
			if *localCell == "" {
				return nil, fmt.Errorf("local cell requested, but not specified. Please set with -cell flag")
			}
			cell = *localCell
		}

		// If a keyspace is provided then return the specified srvkeyspace.
		if keyspace != "" {
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			if err != nil {
				return nil, fmt.Errorf("Can't get server keyspace: %v", err)
			}
			return srvKeyspace, nil
		}

		// Else return the srvKeyspace from all keyspaces.
		srvKeyspaces := make(map[string]interface{})
		keyspaceNamesList, err := ts.GetSrvKeyspaceNames(ctx, cell)
		if err != nil {
			return nil, fmt.Errorf("can't get list of SrvKeyspaceNames for cell %q: GetSrvKeyspaceNames returned: %v", cell, err)
		}
		for _, keyspaceName := range keyspaceNamesList {
			err := addSrvkeyspace(ctx, ts, cell, keyspaceName, srvKeyspaces)
			if err != nil {
				return nil, err
			}
		}
		return srvKeyspaces, nil

	})

	// Tablets
	handleCollection("tablets", func(r *http.Request) (interface{}, error) {
		tabletPath := getItemPath(r.URL.Path)

		// List tablets based on query params.
		if tabletPath == "" {
			if err := r.ParseForm(); err != nil {
				return nil, err
			}
			shardRef := r.FormValue("shard")
			cell := r.FormValue("cell")

			if shardRef != "" {
				// Look up by keyspace/shard, and optionally cell.
				keyspace, shard, err := topoproto.ParseKeyspaceShard(shardRef)
				if err != nil {
					return nil, err
				}
				if cell != "" {
					return ts.FindAllTabletAliasesInShardByCell(ctx, keyspace, shard, []string{cell})
				}
				return ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
			}

			// Get all tablets in a cell.
			if cell == "" {
				return nil, errors.New("cell param required")
			}
			return ts.GetTabletsByCell(ctx, cell)
		}

		// Get tablet health.
		if parts := strings.Split(tabletPath, "/"); len(parts) == 2 && parts[1] == "health" {
			tabletAlias, err := topoproto.ParseTabletAlias(parts[0])
			if err != nil {
				return nil, err
			}
			return tabletHealthCache.Get(ctx, tabletAlias)
		}

		tabletAlias, err := topoproto.ParseTabletAlias(tabletPath)
		if err != nil {
			return nil, err
		}

		// Perform an action on a tablet.
		if r.Method == "POST" {
			if err := r.ParseForm(); err != nil {
				return nil, err
			}
			action := r.FormValue("action")
			if action == "" {
				return nil, errors.New("must specify action")
			}
			return actions.ApplyTabletAction(ctx, action, tabletAlias, r), nil
		}

		// Get the tablet record.
		return ts.GetTablet(ctx, tabletAlias)
	})

	// Healthcheck real time status per (cell, keyspace, shard, tablet type).
	handleCollection("tablet_statuses", func(r *http.Request) (interface{}, error) {
		targetPath := getItemPath(r.URL.Path)

		// Get the heatmap data based on query parameters.
		if targetPath == "" {
			if err := r.ParseForm(); err != nil {
				return nil, err
			}
			keyspace := r.FormValue("keyspace")
			cell := r.FormValue("cell")
			tabletType := r.FormValue("type")
			_, err := topoproto.ParseTabletType(tabletType)
			if err != nil {
				return nil, fmt.Errorf("invalid tablet type: %v ", tabletType)
			}
			metric := r.FormValue("metric")

			if realtimeStats == nil {
				return nil, fmt.Errorf("realtimeStats not initialized")
			}

			heatmap, err := realtimeStats.heatmapData(keyspace, cell, tabletType, metric)
			if err != nil {
				return nil, fmt.Errorf("couldn't get heatmap data: %v", err)
			}

			return heatmap, nil
		}

		return nil, fmt.Errorf("invalid target path: %q  expected path: ?keyspace=<keyspace>&cell=<cell>&type=<type>&metric=<metric>", targetPath)
	})

	// Schema Change
	http.HandleFunc(apiPrefix+"schema/apply", func(w http.ResponseWriter, r *http.Request) {
		req := struct {
			Keyspace, SQL       string
			SlaveTimeoutSeconds int
		}{}
		if err := unmarshalRequest(r, &req); err != nil {
			httpErrorf(w, r, "can't unmarshal request: %v", err)
			return
		}
		if req.SlaveTimeoutSeconds <= 0 {
			req.SlaveTimeoutSeconds = 10
		}

		logger := logutil.NewCallbackLogger(func(ev *logutilpb.Event) {
			w.Write([]byte(logutil.EventString(ev)))
		})
		wr := wrangler.New(logger, ts, tmClient)

		executor := schemamanager.NewTabletExecutor(
			wr, time.Duration(req.SlaveTimeoutSeconds)*time.Second)

		schemamanager.Run(ctx,
			schemamanager.NewUIController(req.SQL, req.Keyspace, w), executor)
	})
}
