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

package zk2topo

import (
	"path"
	"sort"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// ListDir is part of the topo.Conn interface.
func (zs *Server) ListDir(ctx context.Context, dirPath string) ([]topo.DirEntry, error) {
	zkPath := path.Join(zs.root, dirPath)

	children, _, err := zs.conn.Children(ctx, zkPath)
	if err != nil {
		return nil, convertError(err)
	}
	sort.Strings(children)

	result := make([]topo.DirEntry, len(children))
	for i, child := range children {
		result[i] = topo.DirEntry{
			Name: child,
		}
	}
	return result, nil
}
