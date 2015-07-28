package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/youtube/vitess/go/vt/schemamanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// This file implements a REST-style API for the vtctld web interface.

const (
	apiPrefix = "/api/"

	jsonContentType = "application/json; charset=utf-8"
)

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

func initAPI(ctx context.Context, ts topo.Server, actions *ActionRepository) {
	tabletHealthCache := newTabletHealthCache(ts)

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

		// List all keyspaces.
		if keyspace == "" {
			return ts.GetKeyspaces(ctx)
		}

		// Perform an action on a keyspace.
		if r.Method == "POST" {
			if err := r.ParseForm(); err != nil {
				return nil, err
			}
			action := r.FormValue("action")
			if action == "" {
				return nil, errors.New("must specify action")
			}
			return actions.ApplyKeyspaceAction(ctx, action, keyspace, r), nil
		}

		// Get the keyspace record.
		return ts.GetKeyspace(ctx, keyspace)
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
				keyspace, shard, err := topo.ParseKeyspaceShardString(shardRef)
				if err != nil {
					return nil, err
				}
				if cell != "" {
					return topo.FindAllTabletAliasesInShardByCell(ctx, ts, keyspace, shard, []string{cell})
				}
				return topo.FindAllTabletAliasesInShard(ctx, ts, keyspace, shard)
			}

			// Get all tablets in a cell.
			if cell == "" {
				return nil, errors.New("cell param required")
			}
			return ts.GetTabletsByCell(ctx, cell)
		}

		// Get tablet health.
		if parts := strings.Split(tabletPath, "/"); len(parts) == 2 && parts[1] == "health" {
			tabletAlias, err := topo.ParseTabletAliasString(parts[0])
			if err != nil {
				return nil, err
			}
			return tabletHealthCache.Get(ctx, tabletAlias)
		}

		tabletAlias, err := topo.ParseTabletAliasString(tabletPath)
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

	// EndPoints
	handleCollection("endpoints", func(r *http.Request) (interface{}, error) {
		// We expect cell/keyspace/shard/tabletType.
		epPath := getItemPath(r.URL.Path)
		parts := strings.Split(epPath, "/")
		if len(parts) != 4 {
			return nil, fmt.Errorf("invalid cell/keyspace/shard/tabletType: %q", epPath)
		}

		if parts[3] == "" {
			// tabletType is empty, so list the tablet types.
			return ts.GetSrvTabletTypesPerShard(ctx, parts[0], parts[1], parts[2])
		}

		// Get the endpoints object for a specific type.
		ep, _, err := ts.GetEndPoints(ctx, parts[0], parts[1], parts[2], topo.TabletType(parts[3]))
		return ep, err
	})

	// Schema Change
	http.HandleFunc(apiPrefix+"schema/apply", func(w http.ResponseWriter, r *http.Request) {
		req := struct{ Keyspace, SQL string }{}
		if err := unmarshalRequest(r, &req); err != nil {
			httpErrorf(w, r, "can't unmarshal request: %v", err)
			return
		}

		executor := schemamanager.NewTabletExecutor(
			tmclient.NewTabletManagerClient(),
			ts)

		schemamanager.Run(ctx,
			schemamanager.NewUIController(req.SQL, req.Keyspace, w), executor)
	})

	// VSchema
	http.HandleFunc(apiPrefix+"vschema/", func(w http.ResponseWriter, r *http.Request) {
		schemafier, ok := ts.(topo.Schemafier)
		if !ok {
			httpErrorf(w, r, "%T doesn't support schemafier API", ts)
			return
		}

		// Save VSchema
		if r.Method == "POST" {
			vschema, err := ioutil.ReadAll(r.Body)
			if err != nil {
				httpErrorf(w, r, "can't read request body: %v", err)
				return
			}
			if err := schemafier.SaveVSchema(ctx, string(vschema)); err != nil {
				httpErrorf(w, r, "can't save vschema: %v", err)
			}
			return
		}

		// Get VSchema
		vschema, err := schemafier.GetVSchema(ctx)
		if err != nil {
			httpErrorf(w, r, "can't get vschema: %v", err)
			return
		}
		w.Header().Set("Content-Type", jsonContentType)
		w.Write([]byte(vschema))
	})
}
