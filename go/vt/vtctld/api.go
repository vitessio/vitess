/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtctld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/schemamanager"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	localCell        string
	proxyTablets     bool
	showTopologyCRUD = true
)

// This file implements a REST-style API for the vtctld web interface.

const (
	apiPrefix       = "/api/"
	jsonContentType = "application/json; charset=utf-8"
)

// TabletWithURL wraps topo.Tablet, adding a URL property.
type TabletWithURL struct {
	Alias                *topodatapb.TabletAlias `json:"alias,omitempty"`
	Hostname             string                  `json:"hostname,omitempty"`
	PortMap              map[string]int32        `json:"port_map,omitempty"`
	Keyspace             string                  `json:"keyspace,omitempty"`
	Shard                string                  `json:"shard,omitempty"`
	KeyRange             *topodatapb.KeyRange    `json:"key_range,omitempty"`
	Type                 topodatapb.TabletType   `json:"type,omitempty"`
	DbNameOverride       string                  `json:"db_name_override,omitempty"`
	Tags                 map[string]string       `json:"tags,omitempty"`
	MysqlHostname        string                  `json:"mysql_hostname,omitempty"`
	MysqlPort            int32                   `json:"mysql_port,omitempty"`
	PrimaryTermStartTime *vttime.Time            `json:"primary_term_start_time,omitempty"`
	URL                  string                  `json:"url,omitempty"`
}

func init() {
	for _, cmd := range []string{"vtcombo", "vtctld"} {
		servenv.OnParseFor(cmd, registerVtctldAPIFlags)
	}
}

func registerVtctldAPIFlags(fs *pflag.FlagSet) {
	fs.StringVar(&localCell, "cell", localCell, "cell to use")
	fs.BoolVar(&proxyTablets, "proxy_tablets", proxyTablets, "Setting this true will make vtctld proxy the tablet status instead of redirecting to them")
	fs.BoolVar(&showTopologyCRUD, "vtctld_show_topology_crud", showTopologyCRUD, "Controls the display of the CRUD topology actions in the vtctld UI.")
	fs.MarkDeprecated("vtctld_show_topology_crud", "It is no longer applicable because vtctld no longer provides a UI.")
}

func newTabletWithURL(t *topodatapb.Tablet) *TabletWithURL {
	tablet := &TabletWithURL{
		Alias:                t.Alias,
		Hostname:             t.Hostname,
		PortMap:              t.PortMap,
		Keyspace:             t.Keyspace,
		Shard:                t.Shard,
		KeyRange:             t.KeyRange,
		Type:                 t.Type,
		DbNameOverride:       t.DbNameOverride,
		Tags:                 t.Tags,
		MysqlHostname:        t.MysqlHostname,
		MysqlPort:            t.MysqlPort,
		PrimaryTermStartTime: t.PrimaryTermStartTime,
	}

	if proxyTablets {
		tablet.URL = fmt.Sprintf("/vttablet/%s-%d/debug/status", t.Alias.Cell, t.Alias.Uid)
	} else {
		tablet.URL = "http://" + netutil.JoinHostPort(t.Hostname, t.PortMap["vt"])
	}

	return tablet
}

func httpErrorf(w http.ResponseWriter, r *http.Request, format string, args ...any) {
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

func handleCollection(collection string, getFunc func(*http.Request) (any, error)) {
	handleAPI(collection+"/", func(w http.ResponseWriter, r *http.Request) error {
		// Get the requested object.
		obj, err := getFunc(r)
		if err != nil {
			if topo.IsErrType(err, topo.NoNode) {
				http.NotFound(w, r)
				return nil
			}
			return fmt.Errorf("can't get %v: %v", collection, err)
		}

		// JSON encode response.
		data, err := vtctl.MarshalJSON(obj)
		log.Flush()
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

func unmarshalRequest(r *http.Request, v any) error {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func initAPI(ctx context.Context, ts *topo.Server, actions *ActionRepository) {
	tabletHealthCache := newTabletHealthCache(ts)
	tmClient := tmclient.NewTabletManagerClient()

	// Cells
	handleCollection("cells", func(r *http.Request) (any, error) {
		if getItemPath(r.URL.Path) != "" {
			return nil, errors.New("cells can only be listed, not retrieved")
		}
		return ts.GetKnownCells(ctx)
	})

	// Keyspaces
	handleCollection("keyspaces", func(r *http.Request) (any, error) {
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
				return nil, errors.New("a POST request needs a keyspace in the URL")
			}
			if err := r.ParseForm(); err != nil {
				return nil, err
			}

			action := r.FormValue("action")
			if action == "" {
				return nil, errors.New("a POST request must specify action")
			}
			return actions.ApplyKeyspaceAction(ctx, action, keyspace), nil
		default:
			return nil, fmt.Errorf("unsupported HTTP method: %v", r.Method)
		}
	})

	handleCollection("keyspace", func(r *http.Request) (any, error) {
		// Valid requests: api/keyspace/my_ks/tablets (all shards)
		// Valid requests: api/keyspace/my_ks/tablets/-80 (specific shard)
		itemPath := getItemPath(r.URL.Path)
		parts := strings.SplitN(itemPath, "/", 3)

		malformedRequestError := fmt.Errorf("invalid keyspace path: %q  expected path: /keyspace/<keyspace>/tablets or /keyspace/<keyspace>/tablets/<shard>", itemPath)
		if len(parts) < 2 {
			return nil, malformedRequestError
		}
		if parts[1] != "tablets" {
			return nil, malformedRequestError
		}

		keyspace := parts[0]
		if keyspace == "" {
			return nil, errors.New("keyspace is required")
		}
		var shardNames []string
		if len(parts) > 2 && parts[2] != "" {
			shardNames = []string{parts[2]}
		} else {
			var err error
			shardNames, err = ts.GetShardNames(ctx, keyspace)
			if err != nil {
				return nil, err
			}
		}

		if err := r.ParseForm(); err != nil {
			return nil, err
		}
		cell := r.FormValue("cell")
		cells := r.FormValue("cells")
		filterCells := []string{} // empty == all cells
		if cell != "" {
			filterCells = []string{cell} // single cell
		} else if cells != "" {
			filterCells = strings.Split(cells, ",") // list of cells
		}

		tablets := [](*TabletWithURL){}
		for _, shard := range shardNames {
			// Get tablets for this shard.
			tabletAliases, err := ts.FindAllTabletAliasesInShardByCell(ctx, keyspace, shard, filterCells)
			if err != nil && !topo.IsErrType(err, topo.PartialResult) {
				return nil, err
			}
			for _, tabletAlias := range tabletAliases {
				t, err := ts.GetTablet(ctx, tabletAlias)
				if err != nil {
					return nil, err
				}
				tablet := newTabletWithURL(t.Tablet)
				tablets = append(tablets, tablet)
			}
		}
		return tablets, nil
	})

	// Shards
	handleCollection("shards", func(r *http.Request) (any, error) {
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
			return actions.ApplyShardAction(ctx, action, keyspace, shard), nil
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
	handleCollection("srv_keyspace", func(r *http.Request) (any, error) {
		keyspacePath := getItemPath(r.URL.Path)
		parts := strings.SplitN(keyspacePath, "/", 2)

		// Request was incorrectly formatted.
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid srvkeyspace path: %q  expected path: /srv_keyspace/<cell>/<keyspace>", keyspacePath)
		}

		cell := parts[0]
		keyspace := parts[1]

		if cell == "local" {
			if localCell == "" {
				cells, err := ts.GetCellInfoNames(ctx)
				if err != nil {
					return nil, fmt.Errorf("could not fetch cell info: %v", err)
				}
				if len(cells) == 0 {
					return nil, fmt.Errorf("no local cells have been created yet")
				}
				cell = cells[0]
			} else {
				cell = localCell
			}
		}

		// If a keyspace is provided then return the specified srvkeyspace.
		if keyspace != "" {
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspace)
			if err != nil {
				return nil, fmt.Errorf("can't get server keyspace: %v", err)
			}
			return srvKeyspace, nil
		}

		// Else return the srvKeyspace from all keyspaces.
		srvKeyspaces := make(map[string]any)
		keyspaceNamesList, err := ts.GetSrvKeyspaceNames(ctx, cell)
		if err != nil {
			return nil, fmt.Errorf("can't get list of SrvKeyspaceNames for cell %q: GetSrvKeyspaceNames returned: %v", cell, err)
		}
		for _, keyspaceName := range keyspaceNamesList {
			srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, keyspaceName)
			if err != nil {
				// If a keyspace is in the process of being set up, it exists
				// in the list of keyspaces but GetSrvKeyspace fails.
				//
				// Instead of returning this error, simply skip it in the
				// loop so we still return the other valid keyspaces.
				continue
			}
			srvKeyspaces[keyspaceName] = srvKeyspace
		}
		return srvKeyspaces, nil

	})

	// Tablets
	handleCollection("tablets", func(r *http.Request) (any, error) {
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
					if err != nil && !topo.IsErrType(err, topo.PartialResult) {
						return result, err
					}
					return result, nil
				}
				result, err := ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
				if err != nil && !topo.IsErrType(err, topo.PartialResult) {
					return result, err
				}
				return result, nil
			}

			// Get all tablets in a cell.
			if cell == "" {
				return nil, errors.New("cell param required")
			}
			return ts.GetTabletAliasesByCell(ctx, cell)
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

		return newTabletWithURL(t.Tablet), nil
	})

	// Healthcheck real time status per (cell, keyspace, tablet type, metric).
	handleAPI("tablet_statuses/", func(w http.ResponseWriter, r *http.Request) error {
		http.NotFound(w, r)
		return nil
	})

	handleAPI("tablet_health/", func(w http.ResponseWriter, r *http.Request) error {
		http.NotFound(w, r)
		return nil
	})

	handleAPI("topology_info/", func(w http.ResponseWriter, r *http.Request) error {
		http.NotFound(w, r)
		return nil
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
		err := vtctl.RunCommand(r.Context(), wr, args)
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
			Keyspace, SQL         string
			ReplicaTimeoutSeconds int
			DDLStrategy           string `json:"ddl_strategy,omitempty"`
		}{}
		if err := unmarshalRequest(r, &req); err != nil {
			return fmt.Errorf("can't unmarshal request: %v", err)
		}
		if req.ReplicaTimeoutSeconds <= 0 {
			req.ReplicaTimeoutSeconds = 10
		}

		logger := logutil.NewCallbackLogger(func(ev *logutilpb.Event) {
			w.Write([]byte(logutil.EventString(ev)))
		})
		wr := wrangler.New(logger, ts, tmClient)

		apiCallUUID, err := schema.CreateUUID()
		if err != nil {
			return err
		}

		requestContext := fmt.Sprintf("vtctld/api:%s", apiCallUUID)
		executor := schemamanager.NewTabletExecutor(requestContext, wr.TopoServer(), wr.TabletManagerClient(), wr.Logger(), time.Duration(req.ReplicaTimeoutSeconds)*time.Second)
		if err := executor.SetDDLStrategy(req.DDLStrategy); err != nil {
			return fmt.Errorf("error setting DDL strategy: %v", err)
		}

		_, err = schemamanager.Run(ctx,
			schemamanager.NewUIController(req.SQL, req.Keyspace, w), executor)
		return err
	})

	// Features
	handleAPI("features", func(w http.ResponseWriter, r *http.Request) error {
		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			http.Error(w, "403 Forbidden", http.StatusForbidden)
			return nil
		}

		resp := make(map[string]any)
		resp["activeReparents"] = !mysqlctl.DisableActiveReparents
		resp["showStatus"] = false /* enableRealtimeStats = false, always */
		data, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return fmt.Errorf("json error: %v", err)
		}
		w.Header().Set("Content-Type", jsonContentType)
		w.Write(data)
		return nil
	})
}
