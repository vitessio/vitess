/*
Copyright 2020 The Vitess Authors.

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

package k8stopo

import (
	"path/filepath"
	"sort"
	"strings"

	"context"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	vtv1beta1 "vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1"
)

// ListDir is part of the topo.Conn interface.
// It uses an internal cache to find all the objects matching a specific key and returns
// a slice of results sorted alphabetically to emulate the behavior of etcd, zk, consul, etc
func (s *Server) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	dirPath = filepath.Join(s.root, dirPath)

	log.V(7).Infof("Listing dir at: '%s', full: %v", dirPath, full)

	dirMap := map[string]topo.DirEntry{}

	if children, err := s.memberIndexer.ByIndex("by_parent", dirPath); err == nil {
		for _, obj := range children {
			vtn := obj.(*vtv1beta1.VitessTopoNode)

			key := vtn.Data.Key

			// skip duplicates
			if _, ok := dirMap[key]; ok {
				continue
			}

			// new empty entry
			e := topo.DirEntry{
				Ephemeral: vtn.Data.Ephemeral,
			}

			// Clean dirPath from key to get name
			key = strings.TrimPrefix(key, dirPath+"/")

			// If the key represents a directory
			if strings.Contains(key, "/") {
				if full {
					e.Type = topo.TypeDirectory
				}

				// get first part of path as name
				key = strings.Split(filepath.Dir(key), "/")[0]
			} else if full {
				e.Type = topo.TypeFile
			}

			// set name
			e.Name = key

			// add to results
			dirMap[e.Name] = e
		}
	} else {
		return nil, err
	}

	// An empty map means not found
	if len(dirMap) == 0 {
		return nil, topo.NewError(topo.NoNode, dirPath)
	}

	// Get slice of keys
	var keys []string
	for key := range dirMap {
		keys = append(keys, key)
	}

	// sort keys
	sort.Strings(keys)

	// Get ordered result
	var result []topo.DirEntry
	for _, k := range keys {
		result = append(result, dirMap[k])
	}

	return result, nil
}
