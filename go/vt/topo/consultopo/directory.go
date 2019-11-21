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

package consultopo

import (
	"path"
	"strings"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
)

// ListDir is part of the topo.Conn interface.
func (s *Server) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	nodePath := path.Join(s.root, dirPath) + "/"
	if nodePath == "//" {
		// Special case where c.root is "/", dirPath is empty,
		// we would end up with "//". in that case, we want "/".
		nodePath = "/"
	}

	isRoot := false
	if dirPath == "" || dirPath == "/" {
		isRoot = true
	}

	keys, _, err := s.kv.Keys(nodePath, "", nil)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// No key starts with this prefix, means the directory
		// doesn't exist.
		return nil, topo.NewError(topo.NoNode, nodePath)
	}

	prefixLen := len(nodePath)
	var result []topo.DirEntry
	for _, p := range keys {
		// Remove the prefix, base path.
		if !strings.HasPrefix(p, nodePath) {
			return nil, ErrBadResponse
		}
		p = p[prefixLen:]

		// Keep only the part until the first '/'.
		t := topo.TypeFile
		if i := strings.Index(p, "/"); i >= 0 {
			p = p[:i]
			t = topo.TypeDirectory
		}

		// Remove duplicates, add to list.
		if len(result) == 0 || result[len(result)-1].Name != p {
			e := topo.DirEntry{
				Name: p,
			}
			if full {
				e.Type = t
				if isRoot && p == electionsPath {
					e.Ephemeral = true
				}
				if p == locksFilename && t == topo.TypeFile {
					// A file called 'Lock' is always ephemeral.
					// (A directory called 'Lock' could be a keyspace or
					// a shard named Lock).
					e.Ephemeral = true
				}
			}
			result = append(result, e)
		}
	}

	return result, nil
}
