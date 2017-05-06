/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
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

	"github.com/youtube/vitess/go/vt/topo"
)

// ListDir is part of the topo.Backend interface.
func (s *Server) ListDir(ctx context.Context, cell, dirPath string) ([]string, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	nodePath := path.Join(c.root, dirPath) + "/"

	keys, _, err := c.kv.Keys(nodePath, "", nil)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		// No key starts with this prefix, means the directory
		// doesn't exist.
		return nil, topo.ErrNoNode
	}

	prefixLen := len(nodePath)
	var result []string
	for _, p := range keys {
		// Remove the prefix, base path.
		if !strings.HasPrefix(p, nodePath) {
			return nil, ErrBadResponse
		}
		p = p[prefixLen:]

		// Keep only the part until the first '/'.
		if i := strings.Index(p, "/"); i >= 0 {
			p = p[:i]
		}

		// Remove duplicates, add to list.
		if len(result) == 0 || result[len(result)-1] != p {
			result = append(result, p)
		}
	}

	return result, nil
}
