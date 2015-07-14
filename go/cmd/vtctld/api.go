package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// This file implements a REST-style API for the vtctld web interface.

const apiPrefix = "/api/"

func handleGet(collection string, getFunc func(*http.Request) (interface{}, error)) {
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
			w.Write([]byte("[]"))
			return
		}

		// JSON encode response.
		data, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			httpErrorf(w, r, "json error: %v", err)
			return
		}
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

func initAPI(ctx context.Context, ts topo.Server) {
	tabletHealthCache := newTabletHealthCache(ts)

	// Get Cells
	handleGet("cells", func(r *http.Request) (interface{}, error) {
		if getItemPath(r.URL.Path) != "" {
			return nil, errors.New("cells can only be listed, not retrieved")
		}
		return ts.GetKnownCells(ctx)
	})

	// Get Keyspaces
	handleGet("keyspaces", func(r *http.Request) (interface{}, error) {
		keyspace := getItemPath(r.URL.Path)
		if keyspace == "" {
			return ts.GetKeyspaces(ctx)
		}
		return ts.GetKeyspace(ctx, keyspace)
	})

	// Get Shards
	handleGet("shards", func(r *http.Request) (interface{}, error) {
		shardPath := getItemPath(r.URL.Path)
		if !strings.Contains(shardPath, "/") {
			return nil, fmt.Errorf("invalid shard path: %q", shardPath)
		}
		parts := strings.SplitN(shardPath, "/", 2)
		if parts[1] == "" {
			// It's just a keyspace. List the shards.
			return ts.GetShardNames(ctx, parts[0])
		}
		// It's a keyspace/shard reference.
		return ts.GetShard(ctx, parts[0], parts[1])
	})

	// Get Tablets
	handleGet("tablets", func(r *http.Request) (interface{}, error) {
		tabletPath := getItemPath(r.URL.Path)
		if tabletPath == "" {
			// List tablets based on query params.
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

		// Tablet Health
		if parts := strings.Split(tabletPath, "/"); len(parts) == 2 && parts[1] == "health" {
			tabletAlias, err := topo.ParseTabletAliasString(parts[0])
			if err != nil {
				return nil, err
			}
			return tabletHealthCache.Get(ctx, tabletAlias)
		}

		// Get a specific tablet.
		tabletAlias, err := topo.ParseTabletAliasString(tabletPath)
		if err != nil {
			return nil, err
		}
		return ts.GetTablet(ctx, tabletAlias)
	})

	// Get EndPoints
	handleGet("endpoints", func(r *http.Request) (interface{}, error) {
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
}
