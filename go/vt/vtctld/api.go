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

	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/schemamanager"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/workflow"
	"github.com/youtube/vitess/go/vt/wrangler"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	localCell        = flag.String("cell", "", "cell to use")
	showTopologyCRUD = flag.Bool("vtctld_show_topology_crud", true, "Controls the display of the CRUD topology actions in the vtctld UI.")
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

func handleAPI(apiPath string, handlerFunc func(w http.ResponseWriter, r *http.Request) error) {
	http.HandleFunc(apiPrefix+apiPath, func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if x := recover(); x != nil {
				httpErrorf(w, r, "uncaught panic: %v", x)
			}
		}()
		if err := handlerFunc(w, r); err != nil {
			httpErrorf(w, r, "%v", err)
		}
	})
}

func handleCollection(collection string, getFunc func(*http.Request) (interface{}, error)) {
	handleAPI(collection+"/", func(w http.ResponseWriter, r *http.Request) error {
		// Get the requested object.
		obj, err := getFunc(r)
		if err != nil {
			if err == topo.ErrNoNode {
				http.NotFound(w, r)
				return nil
			}
			return fmt.Errorf("can't get %v: %v", collection, err)
		}

		// JSON marshals a nil slice as "null", but we prefer "[]".
		if val := reflect.ValueOf(obj); val.Kind() == reflect.Slice && val.IsNil() {
			w.Header().Set("Content-Type", jsonContentType)
			w.Write([]byte("[]"))
			return nil
		}

		// JSON encode response.
		data, err := vtctl.MarshalJSON(obj)
		if err != nil {
			return fmt.Errorf("cannot marshal data: %v", err)
		}
		w.Header().Set("Content-Type", jsonContentType)
		w.Write(data)
		return nil
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
			k, err := ts.GetKeyspace(ctx, keyspace)
			if err != nil {
				return nil, err
			}
			// Pass the embedded proto directly or jsonpb will panic.
			return k.Keyspace, err
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
		si, err := ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return nil, err
		}
		// Pass the embedded proto directly or jsonpb will panic.
		return si.Shard, err
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
					result, err := ts.FindAllTabletAliasesInShardByCell(ctx, keyspace, shard, []string{cell})
					if err != nil && err != topo.ErrPartialResult {
						return result, err
					}
					return result, nil
				}
				result, err := ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
				if err != nil && err != topo.ErrPartialResult {
					return result, err
				}
				return result, nil
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
		t, err := ts.GetTablet(ctx, tabletAlias)
		if err != nil {
			return nil, err
		}
		// Pass the embedded proto directly or jsonpb will panic.
		return t.Tablet, err
	})

	// Healthcheck real time status per (cell, keyspace, tablet type, metric).
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
			// Excluding the case where parse fails because all tabletTypes was chosen.
			if err != nil && tabletType != "all" {
				return nil, fmt.Errorf("invalid tablet type: %v ", err)
			}
			metric := r.FormValue("metric")

			// Setting default values if none was specified in the query params.
			if keyspace == "" {
				keyspace = "all"
			}
			if cell == "" {
				cell = "all"
			}
			if tabletType == "" {
				tabletType = "all"
			}
			if metric == "" {
				metric = "health"
			}

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

	handleCollection("tablet_health", func(r *http.Request) (interface{}, error) {
		tabletPath := getItemPath(r.URL.Path)
		parts := strings.SplitN(tabletPath, "/", 2)

		// Request was incorrectly formatted.
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid tablet_health path: %q  expected path: /tablet_health/<cell>/<uid>", tabletPath)
		}

		if realtimeStats == nil {
			return nil, fmt.Errorf("realtimeStats not initialized")
		}

		cell := parts[0]
		uidStr := parts[1]
		uid, err := topoproto.ParseUID(uidStr)
		if err != nil {
			return nil, fmt.Errorf("incorrect uid: %v", err)
		}

		tabletAlias := topodatapb.TabletAlias{
			Cell: cell,
			Uid:  uid,
		}
		tabletStat, err := realtimeStats.tabletStats(&tabletAlias)
		if err != nil {
			return nil, fmt.Errorf("could not get tabletStats: %v", err)
		}
		return tabletStat, nil
	})

	handleCollection("topology_info", func(r *http.Request) (interface{}, error) {
		targetPath := getItemPath(r.URL.Path)

		// Retrieving topology information (keyspaces, cells, and types) based on query params.
		if targetPath == "" {
			if err := r.ParseForm(); err != nil {
				return nil, err
			}
			keyspace := r.FormValue("keyspace")
			cell := r.FormValue("cell")

			// Setting default values if none was specified in the query params.
			if keyspace == "" {
				keyspace = "all"
			}
			if cell == "" {
				cell = "all"
			}

			if realtimeStats == nil {
				return nil, fmt.Errorf("realtimeStats not initialized")
			}

			return realtimeStats.topologyInfo(keyspace, cell), nil
		}
		return nil, fmt.Errorf("invalid target path: %q  expected path: ?keyspace=<keyspace>&cell=<cell>", targetPath)
	})

	// Vtctl Command
	handleAPI("vtctl/", func(w http.ResponseWriter, r *http.Request) error {
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			http.Error(w, "403 Forbidden", http.StatusForbidden)
			return nil
		}
		var args []string
		resp := struct {
			Error  string
			Output string
		}{}
		if err := unmarshalRequest(r, &args); err != nil {
			return fmt.Errorf("can't unmarshal request: %v", err)
		}

		logstream := logutil.NewMemoryLogger()

		wr := wrangler.New(logstream, ts, tmClient)
		// TODO(enisoc): Context for run command should be request-scoped.
		err := vtctl.RunCommand(ctx, wr, args)
		if err != nil {
			resp.Error = err.Error()
		}
		resp.Output = logstream.String()
		data, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return fmt.Errorf("json error: %v", err)
		}
		w.Header().Set("Content-Type", jsonContentType)
		w.Write(data)
		return nil
	})

	// Schema Change
	handleAPI("schema/apply", func(w http.ResponseWriter, r *http.Request) error {
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			http.Error(w, "403 Forbidden", http.StatusForbidden)
			return nil
		}
		req := struct {
			Keyspace, SQL       string
			SlaveTimeoutSeconds int
		}{}
		if err := unmarshalRequest(r, &req); err != nil {
			return fmt.Errorf("can't unmarshal request: %v", err)
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

		return schemamanager.Run(ctx,
			schemamanager.NewUIController(req.SQL, req.Keyspace, w), executor)
	})

	// Features
	handleAPI("features", func(w http.ResponseWriter, r *http.Request) error {
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			http.Error(w, "403 Forbidden", http.StatusForbidden)
			return nil
		}

		resp := make(map[string]interface{})
		resp["activeReparents"] = !*vtctl.DisableActiveReparents
		resp["showStatus"] = *enableRealtimeStats
		resp["showTopologyCRUD"] = *showTopologyCRUD
		resp["showWorkflows"] = *workflowManagerInit
		resp["workflows"] = workflow.AvailableFactories()
		data, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return fmt.Errorf("json error: %v", err)
		}
		w.Header().Set("Content-Type", jsonContentType)
		w.Write(data)
		return nil
	})
}
