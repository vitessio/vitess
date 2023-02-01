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
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"

	"context"

	"vitess.io/vitess/go/vt/topo"
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
			decoded, err := topo.DecodeContent(relativePath, data, false)
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

// initExplorer initializes the redirects for explorer
func initExplorer(ts *topo.Server) {
	// Main backend explorer functions.
	be := newBackendExplorer(ts)
	handleCollection("topodata", func(r *http.Request) (any, error) {
		return be.HandlePath(path.Clean("/"+getItemPath(r.URL.Path)), r), nil
	})
}
