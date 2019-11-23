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
	"errors"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl"
)

// backendExplorer is a class that uses the Backend interface of a
// topo.Server to display content. It is separated in its own class
// now for easier testing.
type backendExplorer struct {
	ts *topo.Server
}

// Result is what the backendExplorer returns. It represents one directory node.
// It is exported so the JSON encoder can print it.
type Result struct {
	Data     string
	Children []string
	Error    string
}

func newBackendExplorer(ts *topo.Server) *backendExplorer {
	return &backendExplorer{
		ts: ts,
	}
}

// HandlePath is the main function for this class.
func (ex *backendExplorer) HandlePath(nodePath string, r *http.Request) *Result {
	ctx := context.Background()
	result := &Result{}

	// Handle toplevel display: global, then one line per cell.
	if nodePath == "/" {
		cells, err := ex.ts.GetKnownCells(ctx)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		sort.Strings(cells)
		result.Children = append([]string{topo.GlobalCell}, cells...)
		return result
	}

	// Now find the cell.
	parts := strings.Split(nodePath, "/")
	if parts[0] != "" || len(parts) < 2 {
		result.Error = "Invalid path: " + nodePath
		return result
	}
	cell := parts[1]
	relativePath := nodePath[len(cell)+1:]
	conn, err := ex.ts.ConnForCell(ctx, cell)
	if err != nil {
		result.Error = fmt.Sprintf("Invalid cell: %v", err)
		return result
	}

	// Get the file contents, if any.
	data, _, err := conn.Get(ctx, relativePath)
	switch err {
	case nil:
		if len(data) > 0 {
			// It has contents, we just use it if possible.
			decoded, err := vtctl.DecodeContent(relativePath, data, false)
			if err != nil {
				result.Error = err.Error()
			} else {
				result.Data = decoded
			}

			// With contents, it can't have children, so we're done.
			return result
		}
	default:
		// Something is wrong. Might not be a file.
		result.Error = err.Error()
	}

	// Get the children, if any.
	children, err := conn.ListDir(ctx, relativePath, false /*full*/)
	if err != nil {
		// It failed as a directory, let's just return what it did
		// as a file.
		return result
	}

	// It worked as a directory, clear any file error.
	result.Error = ""
	result.Children = topo.DirEntriesToStringArray(children)
	return result
}

// handleExplorerRedirect returns the redirect target URL.
func handleExplorerRedirect(ctx context.Context, ts *topo.Server, r *http.Request) (string, error) {
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
func initExplorer(ts *topo.Server) {
	// Main backend explorer functions.
	be := newBackendExplorer(ts)
	handleCollection("topodata", func(r *http.Request) (interface{}, error) {
		return be.HandlePath(path.Clean("/"+getItemPath(r.URL.Path)), r), nil
	})

	// Redirects for explorers.
	http.HandleFunc("/explorers/redirect", func(w http.ResponseWriter, r *http.Request) {
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
